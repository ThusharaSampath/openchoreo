// Copyright 2026 The OpenChoreo Authors
// SPDX-License-Identifier: Apache-2.0

package collector

import (
	"context"
	"fmt"
	"log/slog"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// Collector is the main orchestrator for the kube-events-collector.
// It watches Kubernetes Events and Pod status changes, enriches them
// with labels from their involved objects, and outputs them as JSON
// for Fluent Bit to pick up and route to OpenSearch.
type Collector struct {
	config     Config
	logger     *slog.Logger
	k8sClient  kubernetes.Interface
	enricher   *Enricher
	checkpoint *Checkpoint
	output     *JSONOutput
	handler    *Handler
}

// New creates a new Collector instance.
func New(cfg Config, logger *slog.Logger) (*Collector, error) {
	k8sClient, err := buildK8sClient(cfg.Kubeconfig)
	if err != nil {
		return nil, fmt.Errorf("failed to build kubernetes client: %w", err)
	}

	checkpoint, err := NewCheckpoint(cfg.CheckpointPath, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize checkpoint: %w", err)
	}

	enricher := NewEnricher(k8sClient, cfg.LabelCacheTTL, logger)
	output := NewJSONOutput(logger)
	handler := NewHandler(enricher, checkpoint, output, logger)

	return &Collector{
		config:     cfg,
		logger:     logger,
		k8sClient:  k8sClient,
		enricher:   enricher,
		checkpoint: checkpoint,
		output:     output,
		handler:    handler,
	}, nil
}

// Run starts the collector and blocks until the context is cancelled.
func (c *Collector) Run(ctx context.Context) error {
	c.logger.Info("Starting kube-events-collector")

	// Start checkpoint cleanup goroutine
	go c.checkpoint.StartCleanup(ctx, c.config.CheckpointMaxAge, c.config.CheckpointCleanupInterval)

	// Start label cache eviction in background
	go c.enricher.cache.StartEviction(ctx, c.config.LabelCacheTTL)

	// Start watching events and pod status changes
	if err := c.handler.Start(ctx, c.k8sClient, c.config.NamespaceSelector); err != nil {
		return fmt.Errorf("failed to start event handler: %w", err)
	}

	<-ctx.Done()
	c.logger.Info("Context cancelled, shutting down")

	if err := c.checkpoint.Close(); err != nil {
		c.logger.Error("Failed to close checkpoint", "error", err)
	}

	return nil
}

// buildK8sClient creates a Kubernetes clientset from kubeconfig or in-cluster config.
func buildK8sClient(kubeconfig string) (kubernetes.Interface, error) {
	var config *rest.Config
	var err error

	if kubeconfig != "" {
		config, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
	} else {
		config, err = rest.InClusterConfig()
	}
	if err != nil {
		return nil, fmt.Errorf("failed to build kubernetes config: %w", err)
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create kubernetes clientset: %w", err)
	}

	return clientset, nil
}
