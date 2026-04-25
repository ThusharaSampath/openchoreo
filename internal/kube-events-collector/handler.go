// Copyright 2026 The OpenChoreo Authors
// SPDX-License-Identifier: Apache-2.0

package collector

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

// Handler processes Kubernetes Events and Pod status changes.
// It enriches events with labels from their involved objects and writes
// them as JSON to stdout via the output writer.
type Handler struct {
	enricher   *Enricher
	checkpoint *Checkpoint
	output     *JSONOutput
	logger     *slog.Logger

	// knownNamespaces tracks namespaces that already have informers running.
	knownNamespaces map[string]bool
}

// NewHandler creates a new Handler.
func NewHandler(enricher *Enricher, checkpoint *Checkpoint, output *JSONOutput, logger *slog.Logger) *Handler {
	return &Handler{
		enricher:        enricher,
		checkpoint:      checkpoint,
		output:          output,
		logger:          logger,
		knownNamespaces: make(map[string]bool),
	}
}

// Start begins watching Kubernetes Events and Pod status changes.
// It discovers OpenChoreo-managed namespaces using the provided label selector,
// then sets up informers for Events and Pods in those namespaces.
func (h *Handler) Start(ctx context.Context, client kubernetes.Interface, namespaceSelector string) error {
	// Discover managed namespaces
	namespaces, err := h.discoverNamespaces(ctx, client, namespaceSelector)
	if err != nil {
		return fmt.Errorf("failed to discover namespaces: %w", err)
	}

	if len(namespaces) == 0 {
		h.logger.Warn("No namespaces found matching selector, will watch all namespaces",
			"selector", namespaceSelector)
	}

	h.logger.Info("Discovered managed namespaces",
		"count", len(namespaces),
		"namespaces", namespaces)

	// Start informers per namespace for targeted watching.
	// If no namespaces found, watch all namespaces as fallback.
	if len(namespaces) == 0 {
		h.startInformersForNamespace(ctx, client, metav1.NamespaceAll)
		h.knownNamespaces[metav1.NamespaceAll] = true
	} else {
		for _, ns := range namespaces {
			h.startInformersForNamespace(ctx, client, ns)
			h.knownNamespaces[ns] = true
		}
	}

	// Start a goroutine to periodically re-discover namespaces
	go h.watchNamespaces(ctx, client, namespaceSelector)

	return nil
}

// discoverNamespaces lists namespaces matching the given label selector.
func (h *Handler) discoverNamespaces(ctx context.Context, client kubernetes.Interface, selector string) ([]string, error) {
	parsedSelector, err := labels.Parse(selector)
	if err != nil {
		return nil, fmt.Errorf("failed to parse namespace selector %q: %w", selector, err)
	}

	nsList, err := client.CoreV1().Namespaces().List(ctx, metav1.ListOptions{
		LabelSelector: parsedSelector.String(),
	})
	if err != nil {
		return nil, fmt.Errorf("failed to list namespaces: %w", err)
	}

	var namespaces []string
	for i := range nsList.Items {
		namespaces = append(namespaces, nsList.Items[i].Name)
	}
	return namespaces, nil
}

// watchNamespaces periodically re-discovers namespaces and starts informers for new ones.
func (h *Handler) watchNamespaces(ctx context.Context, client kubernetes.Interface, selector string) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			namespaces, err := h.discoverNamespaces(ctx, client, selector)
			if err != nil {
				h.logger.Error("Failed to re-discover namespaces", "error", err)
				continue
			}
			for _, ns := range namespaces {
				if !h.knownNamespaces[ns] {
					h.logger.Info("Discovered new namespace, starting informers", "namespace", ns)
					h.startInformersForNamespace(ctx, client, ns)
					h.knownNamespaces[ns] = true
				}
			}
		}
	}
}

// startInformersForNamespace starts Event and Pod informers for a given namespace.
func (h *Handler) startInformersForNamespace(ctx context.Context, client kubernetes.Interface, namespace string) {
	factory := informers.NewSharedInformerFactoryWithOptions(
		client,
		0, // no resync
		informers.WithNamespace(namespace),
	)

	// Event Informer
	eventInformer := factory.Core().V1().Events().Informer()
	eventInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{ //nolint:errcheck // informer handler registration
		AddFunc: func(obj interface{}) {
			event, ok := obj.(*corev1.Event)
			if !ok {
				return
			}
			h.handleEvent(ctx, event)
		},
		UpdateFunc: func(_, newObj interface{}) {
			event, ok := newObj.(*corev1.Event)
			if !ok {
				return
			}
			h.handleEvent(ctx, event)
		},
	})

	// Pod Informer - watches for container status changes not captured as K8s Events
	// (e.g., OOMKilled, CrashLoopBackOff container states)
	podInformer := factory.Core().V1().Pods().Informer()
	podInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{ //nolint:errcheck // informer handler registration
		UpdateFunc: func(oldObj, newObj interface{}) {
			oldPod, ok1 := oldObj.(*corev1.Pod)
			newPod, ok2 := newObj.(*corev1.Pod)
			if !ok1 || !ok2 {
				return
			}
			h.handlePodStatusChange(ctx, oldPod, newPod)
		},
	})

	factory.Start(ctx.Done())
	h.logger.Info("Started informers", "namespace", namespace)
}

// handleEvent processes a Kubernetes Event, enriches it, and writes it to output.
func (h *Handler) handleEvent(ctx context.Context, event *corev1.Event) {
	// Build checkpoint key from event UID and resource version
	checkpointKey := fmt.Sprintf("event:%s:%s", event.UID, event.ResourceVersion)

	// Check if already processed
	processed, err := h.checkpoint.IsProcessed(checkpointKey)
	if err != nil {
		h.logger.Error("Failed to check checkpoint", "error", err, "key", checkpointKey)
		// Continue processing to avoid data loss
	}
	if processed {
		return
	}

	// Enrich with involvedObject labels
	enrichedLabels, err := h.enricher.GetLabels(ctx, event.InvolvedObject)
	if err != nil {
		h.logger.Debug("Failed to enrich event, proceeding without labels",
			"error", err,
			"involvedObject", fmt.Sprintf("%s/%s", event.InvolvedObject.Kind, event.InvolvedObject.Name))
	}

	// Build enriched event
	enriched := h.buildEnrichedEvent(event, enrichedLabels)

	// Write to output
	if err := h.output.Write(enriched); err != nil {
		h.logger.Error("Failed to write event", "error", err)
		return
	}

	// Mark as processed
	if err := h.checkpoint.MarkProcessed(checkpointKey); err != nil {
		h.logger.Error("Failed to mark event as processed", "error", err, "key", checkpointKey)
	}

	h.logger.Debug("Processed event",
		"reason", event.Reason,
		"kind", event.InvolvedObject.Kind,
		"name", event.InvolvedObject.Name,
		"namespace", event.InvolvedObject.Namespace)
}

// handlePodStatusChange generates synthetic events for container status changes
// that Kubernetes does not emit as Events (e.g., OOMKilled, CrashLoopBackOff).
func (h *Handler) handlePodStatusChange(ctx context.Context, oldPod, newPod *corev1.Pod) {
	for i, newCS := range newPod.Status.ContainerStatuses {
		var oldCS *corev1.ContainerStatus
		if i < len(oldPod.Status.ContainerStatuses) {
			oldCS = &oldPod.Status.ContainerStatuses[i]
		}

		// Detect OOMKilled
		if newCS.State.Terminated != nil && newCS.State.Terminated.Reason == "OOMKilled" {
			if oldCS == nil || oldCS.State.Terminated == nil || oldCS.State.Terminated.Reason != "OOMKilled" {
				h.emitPodContainerEvent(ctx, newPod, newCS.Name, "OOMKilled",
					fmt.Sprintf("Container %s was OOMKilled (exit code %d)", newCS.Name, newCS.State.Terminated.ExitCode))
			}
		}

		// Detect CrashLoopBackOff
		if newCS.State.Waiting != nil && newCS.State.Waiting.Reason == "CrashLoopBackOff" {
			if oldCS == nil || oldCS.State.Waiting == nil || oldCS.State.Waiting.Reason != "CrashLoopBackOff" {
				h.emitPodContainerEvent(ctx, newPod, newCS.Name, "CrashLoopBackOff",
					fmt.Sprintf("Container %s is in CrashLoopBackOff (restart count: %d)", newCS.Name, newCS.RestartCount))
			}
		}
	}
}

// emitPodContainerEvent creates a synthetic event for a container status change.
func (h *Handler) emitPodContainerEvent(ctx context.Context, pod *corev1.Pod, containerName, reason, message string) {
	checkpointKey := fmt.Sprintf("podstatus:%s:%s:%s:%s", pod.UID, pod.ResourceVersion, containerName, reason)

	processed, err := h.checkpoint.IsProcessed(checkpointKey)
	if err != nil {
		h.logger.Error("Failed to check checkpoint", "error", err)
	}
	if processed {
		return
	}

	now := time.Now().UTC().Format(time.RFC3339)

	enriched := &EnrichedEvent{
		Timestamp:      now,
		FirstTimestamp: now,
		LastTimestamp:  now,
		Reason:        reason,
		Message:       message,
		Type:          "Warning",
		Count:         1,
		Source:        EventSource{Component: "kube-events-collector"},
		InvolvedObject: EnrichedInvolvedObject{
			APIVersion: "v1",
			Kind:       "Pod",
			Name:       pod.Name,
			Namespace:  pod.Namespace,
			UID:        string(pod.UID),
			Labels:     pod.Labels,
		},
	}

	if err := h.output.Write(enriched); err != nil {
		h.logger.Error("Failed to write pod status event", "error", err)
		return
	}

	if err := h.checkpoint.MarkProcessed(checkpointKey); err != nil {
		h.logger.Error("Failed to mark pod status as processed", "error", err)
	}

	h.logger.Debug("Emitted pod container status event",
		"pod", pod.Name,
		"container", containerName,
		"reason", reason)
}

// buildEnrichedEvent converts a Kubernetes Event to an EnrichedEvent.
func (h *Handler) buildEnrichedEvent(event *corev1.Event, enrichedLabels map[string]string) *EnrichedEvent {
	firstTS := event.FirstTimestamp.UTC().Format(time.RFC3339)
	lastTS := event.LastTimestamp.UTC().Format(time.RFC3339)

	// Use event time if first/last timestamps are zero (newer events use EventTime)
	if event.FirstTimestamp.IsZero() && !event.EventTime.IsZero() {
		firstTS = event.EventTime.UTC().Format(time.RFC3339)
	}
	if event.LastTimestamp.IsZero() {
		if !event.EventTime.IsZero() {
			lastTS = event.EventTime.UTC().Format(time.RFC3339)
		} else {
			lastTS = firstTS
		}
	}

	// Use lastTimestamp as the primary @timestamp for OpenSearch
	timestamp := lastTS
	if timestamp == "" {
		timestamp = time.Now().UTC().Format(time.RFC3339)
	}

	return &EnrichedEvent{
		Timestamp:      timestamp,
		FirstTimestamp: firstTS,
		LastTimestamp:  lastTS,
		Reason:        event.Reason,
		Message:       event.Message,
		Type:          event.Type,
		Count:         event.Count,
		Source:        EventSource{Component: event.Source.Component},
		InvolvedObject: EnrichedInvolvedObject{
			APIVersion: event.InvolvedObject.APIVersion,
			Kind:       event.InvolvedObject.Kind,
			Name:       event.InvolvedObject.Name,
			Namespace:  event.InvolvedObject.Namespace,
			UID:        string(event.InvolvedObject.UID),
			Labels:     enrichedLabels,
		},
	}
}
