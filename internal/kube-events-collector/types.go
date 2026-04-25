// Copyright 2026 The OpenChoreo Authors
// SPDX-License-Identifier: Apache-2.0

package collector

import "time"

// EnrichedEvent represents a Kubernetes Event enriched with labels from its involvedObject.
// This is the document format written to stdout as JSON for Fluent Bit to pick up
// and route to the kube-events OpenSearch index.
type EnrichedEvent struct {
	// Timestamp is the time this event was processed/emitted.
	Timestamp string `json:"@timestamp"`

	// FirstTimestamp is when the event was first observed.
	FirstTimestamp string `json:"firstTimestamp"`

	// LastTimestamp is when the event was last observed.
	LastTimestamp string `json:"lastTimestamp"`

	// Reason is a short, machine-readable string (e.g., "Completed", "BackoffLimitExceeded").
	Reason string `json:"reason"`

	// Message is a human-readable description of the event.
	Message string `json:"message"`

	// Type is the event severity: "Normal" or "Warning".
	Type string `json:"type"`

	// Count is how many times this event has occurred.
	Count int32 `json:"count"`

	// Source identifies the component that generated this event.
	Source EventSource `json:"source"`

	// InvolvedObject describes the object this event is about, enriched with labels.
	InvolvedObject EnrichedInvolvedObject `json:"involvedObject"`
}

// EventSource identifies the component that generated the event.
type EventSource struct {
	Component string `json:"component"`
}

// EnrichedInvolvedObject is the Kubernetes object involved in the event,
// enriched with labels fetched from the actual resource.
type EnrichedInvolvedObject struct {
	APIVersion string            `json:"apiVersion"`
	Kind       string            `json:"kind"`
	Name       string            `json:"name"`
	Namespace  string            `json:"namespace"`
	UID        string            `json:"uid"`
	Labels     map[string]string `json:"labels,omitempty"`
}

// LabelCacheEntry holds cached labels for a Kubernetes resource.
type LabelCacheEntry struct {
	Labels   map[string]string
	NotFound bool
	CachedAt time.Time
}
