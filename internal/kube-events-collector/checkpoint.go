// Copyright 2026 The OpenChoreo Authors
// SPDX-License-Identifier: Apache-2.0

package collector

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	_ "modernc.org/sqlite" // Pure Go SQLite driver
)

// Checkpoint tracks which events have been processed to avoid duplicates
// across collector restarts. Uses a file-based SQLite database.
type Checkpoint struct {
	db     *sql.DB
	logger *slog.Logger
}

// NewCheckpoint creates a new Checkpoint backed by SQLite at the given path.
func NewCheckpoint(dbPath string, logger *slog.Logger) (*Checkpoint, error) {
	// Ensure directory exists
	dir := filepath.Dir(dbPath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create checkpoint directory %s: %w", dir, err)
	}

	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open checkpoint database: %w", err)
	}

	// Enable WAL mode for better concurrent access
	if _, err := db.Exec("PRAGMA journal_mode=WAL"); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to set WAL mode: %w", err)
	}

	// Create table if not exists
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS checkpoints (
			key TEXT PRIMARY KEY,
			processed_at DATETIME NOT NULL DEFAULT (datetime('now'))
		)
	`)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to create checkpoints table: %w", err)
	}

	logger.Info("Checkpoint database initialized", "path", dbPath)
	return &Checkpoint{db: db, logger: logger}, nil
}

// IsProcessed checks if a given key has already been processed.
func (c *Checkpoint) IsProcessed(key string) (bool, error) {
	var count int
	err := c.db.QueryRow("SELECT COUNT(*) FROM checkpoints WHERE key = ?", key).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("failed to check checkpoint: %w", err)
	}
	return count > 0, nil
}

// MarkProcessed records that a key has been processed.
func (c *Checkpoint) MarkProcessed(key string) error {
	_, err := c.db.Exec(
		"INSERT OR IGNORE INTO checkpoints (key) VALUES (?)",
		key,
	)
	if err != nil {
		return fmt.Errorf("failed to mark checkpoint: %w", err)
	}
	return nil
}

// Cleanup removes checkpoint entries older than maxAge.
func (c *Checkpoint) Cleanup(maxAge time.Duration) (int64, error) {
	cutoff := time.Now().Add(-maxAge).UTC().Format(time.RFC3339)
	result, err := c.db.Exec("DELETE FROM checkpoints WHERE processed_at < ?", cutoff)
	if err != nil {
		return 0, fmt.Errorf("failed to cleanup checkpoints: %w", err)
	}
	return result.RowsAffected()
}

// StartCleanup runs periodic cleanup of old checkpoint entries.
func (c *Checkpoint) StartCleanup(ctx context.Context, maxAge, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			deleted, err := c.Cleanup(maxAge)
			if err != nil {
				c.logger.Error("Checkpoint cleanup failed", "error", err)
			} else if deleted > 0 {
				c.logger.Info("Checkpoint cleanup completed", "deletedEntries", deleted)
			}
		}
	}
}

// Close closes the checkpoint database.
func (c *Checkpoint) Close() error {
	if c.db != nil {
		return c.db.Close()
	}
	return nil
}
