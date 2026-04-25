// Copyright 2026 The OpenChoreo Authors
// SPDX-License-Identifier: Apache-2.0

package collector

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
)

// JSONOutput writes enriched events as JSON lines to stdout.
// Fluent Bit picks up these JSON logs and routes them to the
// kube-events OpenSearch index.
type JSONOutput struct {
	logger *slog.Logger
}

// NewJSONOutput creates a new JSONOutput.
func NewJSONOutput(logger *slog.Logger) *JSONOutput {
	return &JSONOutput{logger: logger}
}

// Write serializes an EnrichedEvent to JSON and writes it to stdout.
func (o *JSONOutput) Write(event *EnrichedEvent) error {
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %w", err)
	}

	// Write as a single line to stdout for Fluent Bit tail input
	_, err = fmt.Fprintln(os.Stdout, string(data))
	if err != nil {
		return fmt.Errorf("failed to write event to stdout: %w", err)
	}

	return nil
}
