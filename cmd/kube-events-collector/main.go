// Copyright 2026 The OpenChoreo Authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	collector "github.com/openchoreo/openchoreo/internal/kube-events-collector"
	"github.com/openchoreo/openchoreo/internal/cmdutil"
)

func main() {
	var (
		kubeconfig           string
		logLevel             string
		checkpointPath       string
		checkpointMaxAge     time.Duration
		checkpointCleanupInt time.Duration
		labelCacheTTL        time.Duration
		namespaceSelector    string
	)

	flag.StringVar(&kubeconfig, "kubeconfig", cmdutil.GetEnv("KUBECONFIG", ""),
		"Path to kubeconfig file (defaults to in-cluster config)")
	flag.StringVar(&logLevel, "log-level", cmdutil.GetEnv("LOG_LEVEL", "info"),
		"Log level (debug, info, warn, error)")
	flag.StringVar(&checkpointPath, "checkpoint-path",
		cmdutil.GetEnv("CHECKPOINT_PATH", "/data/checkpoint.db"),
		"Path to SQLite checkpoint database file")
	flag.DurationVar(&checkpointMaxAge, "checkpoint-max-age", 24*time.Hour,
		"Maximum age of checkpoint entries before cleanup")
	flag.DurationVar(&checkpointCleanupInt, "checkpoint-cleanup-interval", 1*time.Hour,
		"Interval between checkpoint cleanup runs")
	flag.DurationVar(&labelCacheTTL, "label-cache-ttl", 10*time.Minute,
		"TTL for in-memory label cache entries")
	flag.StringVar(&namespaceSelector, "namespace-selector",
		cmdutil.GetEnv("NAMESPACE_SELECTOR", "openchoreo.dev/created-by=renderedrelease-controller"),
		"Label selector to filter namespaces to watch for events")
	flag.Parse()

	logger := cmdutil.SetupLogger(logLevel)
	logger.Info("Starting kube-events-collector",
		"checkpointPath", checkpointPath,
		"labelCacheTTL", labelCacheTTL,
		"namespaceSelector", namespaceSelector,
	)

	cfg := collector.Config{
		Kubeconfig:               kubeconfig,
		CheckpointPath:           checkpointPath,
		CheckpointMaxAge:         checkpointMaxAge,
		CheckpointCleanupInterval: checkpointCleanupInt,
		LabelCacheTTL:            labelCacheTTL,
		NamespaceSelector:        namespaceSelector,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle shutdown signals
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		logger.Info("Received shutdown signal", "signal", sig)
		cancel()
	}()

	c, err := collector.New(cfg, logger)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create collector: %v\n", err)
		os.Exit(1)
	}

	if err := c.Run(ctx); err != nil {
		logger.Error("Collector exited with error", "error", err)
		os.Exit(1)
	}

	logger.Info("Kube-events-collector shut down gracefully")
}
