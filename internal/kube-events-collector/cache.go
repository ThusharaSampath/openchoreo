// Copyright 2026 The OpenChoreo Authors
// SPDX-License-Identifier: Apache-2.0

package collector

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// LabelCache is a thread-safe in-memory cache for Kubernetes resource labels.
// Entries expire after the configured TTL to ensure freshness.
type LabelCache struct {
	mu      sync.RWMutex
	entries map[string]LabelCacheEntry
	ttl     time.Duration
	logger  *slog.Logger
}

// NewLabelCache creates a new LabelCache with the given TTL.
func NewLabelCache(ttl time.Duration, logger *slog.Logger) *LabelCache {
	return &LabelCache{
		entries: make(map[string]LabelCacheEntry),
		ttl:     ttl,
		logger:  logger,
	}
}

// Get retrieves labels from the cache. Returns the labels and true if found
// and not expired, or nil and false otherwise.
func (c *LabelCache) Get(key string) (map[string]string, bool) {
	c.mu.RLock()
	entry, ok := c.entries[key]
	c.mu.RUnlock()

	if !ok {
		return nil, false
	}

	// Check expiration
	if time.Since(entry.CachedAt) > c.ttl {
		c.mu.Lock()
		delete(c.entries, key)
		c.mu.Unlock()
		return nil, false
	}

	if entry.NotFound {
		return nil, true
	}

	return entry.Labels, true
}

// SetNotFound caches a "not found" marker so we avoid repeated API calls
// for objects that no longer exist (e.g., deleted ReplicaSets, old Pods).
func (c *LabelCache) SetNotFound(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.entries[key] = LabelCacheEntry{
		NotFound: true,
		CachedAt: time.Now(),
	}
}

// StartEviction runs a periodic eviction loop to remove expired entries.
// It blocks until the context is cancelled.
func (c *LabelCache) StartEviction(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.evict()
		}
	}
}

func (c *LabelCache) evict() {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	evicted := 0
	for key, entry := range c.entries {
		if now.Sub(entry.CachedAt) > c.ttl {
			delete(c.entries, key)
			evicted++
		}
	}
	if evicted > 0 {
		c.logger.Debug("Evicted expired label cache entries", "count", evicted)
	}
}

// Set stores labels in the cache with the current timestamp.
func (c *LabelCache) Set(key string, labels map[string]string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Make a copy of the labels map to avoid mutations
	copied := make(map[string]string, len(labels))
	for k, v := range labels {
		copied[k] = v
	}

	c.entries[key] = LabelCacheEntry{
		Labels:   copied,
		CachedAt: time.Now(),
	}
}

// Len returns the number of entries in the cache.
func (c *LabelCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.entries)
}
