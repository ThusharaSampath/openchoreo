// Copyright 2026 The OpenChoreo Authors
// SPDX-License-Identifier: Apache-2.0

package types

// TriggersQueryRequest represents the request for listing triggers (Jobs) of a scheduled task component.
type TriggersQueryRequest struct {
	SearchScope *ComponentSearchScope `json:"searchScope" validate:"required"`
	StartTime   string                `json:"startTime" validate:"required"`
	EndTime     string                `json:"endTime" validate:"required"`
	Limit       int                   `json:"limit,omitempty"`
	Offset      int                   `json:"offset,omitempty"`
	SortOrder   string                `json:"sortOrder,omitempty"` // asc or desc, default: desc
	// IncludeEvents controls whether each TriggerEntry's Events array is populated.
	// Defaults to false: callers that only render the trigger row (status, times, count) get
	// a smaller payload and the backend skips the per-trigger top_hits sub-aggregation.
	IncludeEvents bool `json:"includeEvents,omitempty"`
}

// TriggerEvent represents a single event within a trigger.
type TriggerEvent struct {
	Reason    string `json:"reason"`
	Message   string `json:"message"`
	Timestamp string `json:"timestamp"`
	Type      string `json:"type"` // Normal or Warning
}

// TriggerEntry represents a single CronJob trigger (Job) with derived metadata.
type TriggerEntry struct {
	// JobName is the Kubernetes Job name (unique identifier for this trigger).
	JobName string `json:"jobName"`

	// Status is derived from events: "succeeded", "failed", "running", "unknown".
	Status string `json:"status"`

	// StartTime is the earliest event timestamp for this trigger.
	StartTime string `json:"startTime"`

	// CompletionTime is the latest event timestamp (approximate completion time).
	CompletionTime string `json:"completionTime,omitempty"`

	// EventCount is the total number of events for this trigger.
	EventCount int `json:"eventCount"`

	// FailureReason is the K8s event reason that caused the failure
	// (e.g. "BackoffLimitExceeded", "DeadlineExceeded"). Only set when Status == "failed";
	// derived from the same reasons aggregation that drives Status, so it costs nothing extra.
	FailureReason string `json:"failureReason,omitempty"`

	// Events is the list of events associated with this trigger.
	Events []TriggerEvent `json:"events,omitempty"`
}

// TriggersQueryResponse is the response for listing triggers.
type TriggersQueryResponse struct {
	Triggers []TriggerEntry `json:"triggers"`
	Total    int            `json:"total"`
	TookMs   int            `json:"tookMs"`
}

// RetriesQueryRequest represents the request for listing retries (Pods) of a specific trigger.
type RetriesQueryRequest struct {
	SearchScope *ComponentSearchScope `json:"searchScope" validate:"required"`
}

// RetryEvent represents a single event within a retry.
type RetryEvent struct {
	Reason    string `json:"reason"`
	Message   string `json:"message"`
	Timestamp string `json:"timestamp"`
	Type      string `json:"type"`
}

// RetryEntry represents a single retry (Pod) within a trigger.
type RetryEntry struct {
	// PodName is the Kubernetes Pod name.
	PodName string `json:"podName"`

	// Status is derived from events: "Succeeded", "Failed", "Running", "Unknown".
	Status string `json:"status"`

	// StartTime is the earliest event timestamp for this pod.
	StartTime string `json:"startTime"`

	// EventCount is the total number of events for this pod.
	EventCount int `json:"eventCount"`

	// Events is the list of events associated with this retry.
	Events []RetryEvent `json:"events,omitempty"`
}

// RetriesQueryResponse is the response for listing retries.
type RetriesQueryResponse struct {
	Retries []RetryEntry `json:"retries"`
	Total   int          `json:"total"`
	TookMs  int          `json:"tookMs"`
}
