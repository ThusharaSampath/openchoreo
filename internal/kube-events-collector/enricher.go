// Copyright 2026 The OpenChoreo Authors
// SPDX-License-Identifier: Apache-2.0

package collector

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const openchoreoLabelPrefix = "openchoreo.dev/"

// Enricher fetches labels from the involvedObject of a Kubernetes Event.
// It uses an in-memory cache to avoid excessive Kubernetes API calls.
type Enricher struct {
	client kubernetes.Interface
	cache  *LabelCache
	logger *slog.Logger
}

// NewEnricher creates a new Enricher with the given cache TTL.
func NewEnricher(client kubernetes.Interface, cacheTTL time.Duration, logger *slog.Logger) *Enricher {
	return &Enricher{
		client: client,
		cache:  NewLabelCache(cacheTTL, logger),
		logger: logger,
	}
}

// GetLabels returns the openchoreo.dev/* labels for the involvedObject of an event.
// It first checks the cache, then falls back to fetching from the Kubernetes API.
func (e *Enricher) GetLabels(ctx context.Context, obj corev1.ObjectReference) (map[string]string, error) {
	cacheKey := buildCacheKey(obj)

	// Check cache
	if labels, ok := e.cache.Get(cacheKey); ok {
		return labels, nil
	}

	// Fetch from Kubernetes API
	labels, err := e.fetchLabels(ctx, obj)
	if err != nil {
		if errors.IsNotFound(err) {
			e.cache.SetNotFound(cacheKey)
			e.logger.Debug("Involved object not found, caching as not-found",
				"kind", obj.Kind, "name", obj.Name, "namespace", obj.Namespace)
			return nil, nil
		}
		return nil, err
	}

	// Filter to only openchoreo.dev/* labels
	filtered := filterOpenChoreoLabels(labels)

	// Cache the result
	e.cache.Set(cacheKey, filtered)

	return filtered, nil
}

// filterOpenChoreoLabels returns only labels with the "openchoreo.dev/" prefix.
func filterOpenChoreoLabels(labels map[string]string) map[string]string {
	if len(labels) == 0 {
		return nil
	}

	filtered := make(map[string]string)
	for k, v := range labels {
		if strings.HasPrefix(k, openchoreoLabelPrefix) {
			filtered[k] = v
		}
	}

	if len(filtered) == 0 {
		return nil
	}
	return filtered
}

// fetchLabels retrieves labels from the Kubernetes API for the given object reference.
func (e *Enricher) fetchLabels(ctx context.Context, obj corev1.ObjectReference) (map[string]string, error) {
	namespace := obj.Namespace

	switch obj.Kind {
	case "Pod":
		resource, err := e.client.CoreV1().Pods(namespace).Get(ctx, obj.Name, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}
		return resource.Labels, nil

	case "ReplicaSet":
		resource, err := e.client.AppsV1().ReplicaSets(namespace).Get(ctx, obj.Name, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}
		return resource.Labels, nil

	case "Deployment":
		resource, err := e.client.AppsV1().Deployments(namespace).Get(ctx, obj.Name, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}
		return resource.Labels, nil

	case "StatefulSet":
		resource, err := e.client.AppsV1().StatefulSets(namespace).Get(ctx, obj.Name, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}
		return resource.Labels, nil

	case "Job":
		resource, err := e.client.BatchV1().Jobs(namespace).Get(ctx, obj.Name, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}
		return resource.Labels, nil

	case "CronJob":
		resource, err := e.client.BatchV1().CronJobs(namespace).Get(ctx, obj.Name, metav1.GetOptions{})
		if err != nil {
			return nil, err
		}
		return resource.Labels, nil

	default:
		e.logger.Debug("Unsupported kind for label enrichment, skipping",
			"kind", obj.Kind,
			"name", obj.Name,
			"namespace", namespace)
		return nil, nil
	}
}

// buildCacheKey creates a unique cache key for an object reference.
func buildCacheKey(obj corev1.ObjectReference) string {
	if obj.UID != "" {
		return string(obj.UID)
	}
	return fmt.Sprintf("%s/%s/%s", obj.Namespace, obj.Kind, obj.Name)
}
