// Copyright 2026 The OpenChoreo Authors
// SPDX-License-Identifier: Apache-2.0

package collector

import "time"

// Config holds configuration for the kube-events-collector.
type Config struct {
	// Kubeconfig is the path to kubeconfig file. Empty means in-cluster config.
	Kubeconfig string

	// CheckpointPath is the path to the SQLite checkpoint database.
	CheckpointPath string

	// CheckpointMaxAge is the maximum age of checkpoint entries before cleanup.
	CheckpointMaxAge time.Duration

	// CheckpointCleanupInterval is the interval between checkpoint cleanup runs.
	CheckpointCleanupInterval time.Duration

	// LabelCacheTTL is the TTL for in-memory label cache entries.
	LabelCacheTTL time.Duration

	// NamespaceSelector is the label selector to filter namespaces to watch.
	// Only events from namespaces matching this selector are collected.
	NamespaceSelector string
}
