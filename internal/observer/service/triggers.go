// Copyright 2026 The OpenChoreo Authors
// SPDX-License-Identifier: Apache-2.0

package service

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/openchoreo/openchoreo/internal/observer/opensearch"
	"github.com/openchoreo/openchoreo/internal/observer/types"
)

const kubeEventsIndexPattern = "kube-events-*"

// QueryTriggers queries triggers (Jobs) for a scheduled task component from the kube-events index.
func (s *LogsService) QueryTriggers(ctx context.Context, req *types.TriggersQueryRequest) (*types.TriggersQueryResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("request is required")
	}

	s.logger.Info("QueryTriggers called",
		"namespace", req.SearchScope.Namespace,
		"component", req.SearchScope.Component,
		"environment", req.SearchScope.Environment,
		"startTime", req.StartTime,
		"endTime", req.EndTime)

	// Use direct UIDs if provided, otherwise resolve from names
	var componentUID, environmentUID, projectUID, namespaceName string
	if req.SearchScope.ComponentUID != "" && req.SearchScope.EnvironmentUID != "" {
		componentUID = req.SearchScope.ComponentUID
		environmentUID = req.SearchScope.EnvironmentUID
		projectUID = req.SearchScope.ProjectUID
		namespaceName = req.SearchScope.Namespace
	} else {
		scope, err := s.resolveSearchScope(ctx, &types.SearchScope{Component: req.SearchScope})
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrLogsResolveSearchScope, err)
		}
		componentUID = scope.ComponentUID
		environmentUID = scope.EnvironmentUID
		projectUID = scope.ProjectUID
		namespaceName = scope.NamespaceName
	}

	params := opensearch.TriggersQueryParams{
		StartTime:     req.StartTime,
		EndTime:       req.EndTime,
		NamespaceName: namespaceName,
		ComponentID:   componentUID,
		EnvironmentID: environmentUID,
		ProjectID:     projectUID,
		Limit:         req.Limit,
		Offset:        req.Offset,
		SortOrder:     req.SortOrder,
	}

	if s.defaultAdaptor == nil {
		return nil, fmt.Errorf("default adaptor is not initialized")
	}

	// Build and execute query
	qb := &opensearch.QueryBuilder{}
	query, err := qb.BuildTriggersQuery(params)
	if err != nil {
		return nil, fmt.Errorf("failed to build triggers query: %w", err)
	}

	indices := []string{kubeEventsIndexPattern}
	result, err := s.defaultAdaptor.SearchRaw(ctx, indices, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query triggers: %w", err)
	}

	return parseTriggerAggregation(result, params.Offset)
}

// QueryRetries queries retries (Pods) for a specific trigger (Job) from the kube-events index.
func (s *LogsService) QueryRetries(ctx context.Context, jobName string, req *types.RetriesQueryRequest) (*types.RetriesQueryResponse, error) {
	if req == nil || jobName == "" {
		return nil, fmt.Errorf("request and job name are required")
	}

	s.logger.Info("QueryRetries called",
		"jobName", jobName,
		"namespace", req.SearchScope.Namespace)

	// Use direct UIDs if provided, otherwise resolve from names
	var componentUID, environmentUID, projectUID, namespaceName string
	if req.SearchScope.ComponentUID != "" && req.SearchScope.EnvironmentUID != "" {
		componentUID = req.SearchScope.ComponentUID
		environmentUID = req.SearchScope.EnvironmentUID
		projectUID = req.SearchScope.ProjectUID
		namespaceName = req.SearchScope.Namespace
	} else {
		scope, err := s.resolveSearchScope(ctx, &types.SearchScope{Component: req.SearchScope})
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrLogsResolveSearchScope, err)
		}
		componentUID = scope.ComponentUID
		environmentUID = scope.EnvironmentUID
		projectUID = scope.ProjectUID
		namespaceName = scope.NamespaceName
	}

	params := opensearch.RetriesQueryParams{
		JobName:       jobName,
		NamespaceName: namespaceName,
		ComponentID:   componentUID,
		EnvironmentID: environmentUID,
		ProjectID:     projectUID,
	}

	if s.defaultAdaptor == nil {
		return nil, fmt.Errorf("default adaptor is not initialized")
	}

	qb := &opensearch.QueryBuilder{}
	query, err := qb.BuildRetriesQuery(params)
	if err != nil {
		return nil, fmt.Errorf("failed to build retries query: %w", err)
	}

	indices := []string{kubeEventsIndexPattern}
	result, err := s.defaultAdaptor.SearchRaw(ctx, indices, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query retries: %w", err)
	}

	return parseRetriesAggregation(result)
}

// parseTriggerAggregation parses the aggregation response into TriggerEntry list.
func parseTriggerAggregation(resp *opensearch.SearchResponse, offset int) (*types.TriggersQueryResponse, error) {
	if resp == nil || resp.Aggregations == nil {
		return &types.TriggersQueryResponse{Triggers: []types.TriggerEntry{}}, nil
	}

	triggersAgg, ok := resp.Aggregations["triggers"]
	if !ok {
		return &types.TriggersQueryResponse{Triggers: []types.TriggerEntry{}}, nil
	}

	var aggResult struct {
		Buckets []triggerBucket `json:"buckets"`
	}
	if err := json.Unmarshal(triggersAgg, &aggResult); err != nil {
		return nil, fmt.Errorf("failed to parse triggers aggregation: %w", err)
	}

	// Parse total count
	total := len(aggResult.Buckets)
	if totalAgg, ok := resp.Aggregations["total_triggers"]; ok {
		var cardResult struct {
			Value int `json:"value"`
		}
		if err := json.Unmarshal(totalAgg, &cardResult); err == nil {
			total = cardResult.Value
		}
	}

	// Apply offset
	buckets := aggResult.Buckets
	if offset > 0 && offset < len(buckets) {
		buckets = buckets[offset:]
	} else if offset >= len(buckets) {
		buckets = nil
	}

	triggers := make([]types.TriggerEntry, 0, len(buckets))
	for _, bucket := range buckets {
		trigger := types.TriggerEntry{
			JobName:    bucket.Key,
			EventCount: bucket.DocCount,
			Status:     deriveTriggerStatus(bucket.Reasons),
		}

		if bucket.FirstSeen.Value != nil {
			trigger.StartTime = formatMillisTimestamp(*bucket.FirstSeen.Value)
		}
		if bucket.LastSeen.Value != nil {
			trigger.CompletionTime = formatMillisTimestamp(*bucket.LastSeen.Value)
		}

		trigger.Events = parseTopHitEvents(bucket.Events)
		triggers = append(triggers, trigger)
	}

	return &types.TriggersQueryResponse{
		Triggers: triggers,
		Total:    total,
		TookMs:   resp.Took,
	}, nil
}

// parseRetriesAggregation parses the aggregation response into RetryEntry list.
func parseRetriesAggregation(resp *opensearch.SearchResponse) (*types.RetriesQueryResponse, error) {
	if resp == nil || resp.Aggregations == nil {
		return &types.RetriesQueryResponse{Retries: []types.RetryEntry{}}, nil
	}

	retriesAgg, ok := resp.Aggregations["retries"]
	if !ok {
		return &types.RetriesQueryResponse{Retries: []types.RetryEntry{}}, nil
	}

	var aggResult struct {
		Buckets []retryBucket `json:"buckets"`
	}
	if err := json.Unmarshal(retriesAgg, &aggResult); err != nil {
		return nil, fmt.Errorf("failed to parse retries aggregation: %w", err)
	}

	retries := make([]types.RetryEntry, 0, len(aggResult.Buckets))
	for _, bucket := range aggResult.Buckets {
		retry := types.RetryEntry{
			PodName:    bucket.Key,
			EventCount: bucket.DocCount,
			Status:     deriveRetryStatus(bucket.Reasons),
		}

		if bucket.FirstSeen.Value != nil {
			retry.StartTime = formatMillisTimestamp(*bucket.FirstSeen.Value)
		}

		retry.Events = parseTopHitRetryEvents(bucket.Events)
		retries = append(retries, retry)
	}

	return &types.RetriesQueryResponse{
		Retries: retries,
		Total:   len(retries),
		TookMs:  resp.Took,
	}, nil
}

// triggerBucket represents a single bucket in the triggers aggregation.
type triggerBucket struct {
	Key      string         `json:"key"`
	DocCount int            `json:"doc_count"`
	FirstSeen metricValue   `json:"first_seen"`
	LastSeen  metricValue   `json:"last_seen"`
	Reasons   reasonsBucket `json:"reasons"`
	Events    topHitsResult `json:"events"`
}

// retryBucket represents a single bucket in the retries aggregation.
type retryBucket struct {
	Key       string         `json:"key"`
	DocCount  int            `json:"doc_count"`
	FirstSeen metricValue    `json:"first_seen"`
	Reasons   reasonsBucket  `json:"reasons"`
	Events    topHitsResult  `json:"events"`
}

type metricValue struct {
	Value *float64 `json:"value"`
}

type reasonsBucket struct {
	Buckets []struct {
		Key      string `json:"key"`
		DocCount int    `json:"doc_count"`
	} `json:"buckets"`
}

type topHitsResult struct {
	Hits struct {
		Hits []struct {
			Source map[string]interface{} `json:"_source"`
		} `json:"hits"`
	} `json:"hits"`
}

// deriveTriggerStatus determines trigger status from event reasons.
func deriveTriggerStatus(reasons reasonsBucket) string {
	reasonSet := make(map[string]bool)
	for _, r := range reasons.Buckets {
		reasonSet[r.Key] = true
	}

	if reasonSet["Completed"] {
		return "succeeded"
	}
	if reasonSet["BackoffLimitExceeded"] || reasonSet["DeadlineExceeded"] {
		return "failed"
	}
	if reasonSet["SuccessfulCreate"] {
		return "running"
	}
	return "unknown"
}

// deriveRetryStatus determines retry (Pod) status from event reasons.
func deriveRetryStatus(reasons reasonsBucket) string {
	reasonSet := make(map[string]bool)
	for _, r := range reasons.Buckets {
		reasonSet[r.Key] = true
	}

	if reasonSet["Completed"] {
		return "Succeeded"
	}
	if reasonSet["OOMKilled"] || reasonSet["CrashLoopBackOff"] || reasonSet["BackOff"] {
		return "Failed"
	}
	if reasonSet["Started"] || reasonSet["Pulled"] {
		return "Running"
	}
	return "Unknown"
}

// formatMillisTimestamp converts epoch milliseconds to RFC3339 string.
func formatMillisTimestamp(millis float64) string {
	sec := int64(millis / 1000)
	nsec := int64((millis - float64(sec)*1000) * 1e6)
	return time.Unix(sec, nsec).UTC().Format(time.RFC3339)
}

// parseTopHitEvents extracts TriggerEvent list from a top_hits aggregation.
func parseTopHitEvents(hits topHitsResult) []types.TriggerEvent {
	events := make([]types.TriggerEvent, 0, len(hits.Hits.Hits))
	for _, hit := range hits.Hits.Hits {
		event := types.TriggerEvent{}
		if v, ok := hit.Source["reason"].(string); ok {
			event.Reason = v
		}
		if v, ok := hit.Source["message"].(string); ok {
			event.Message = v
		}
		if v, ok := hit.Source["@timestamp"].(string); ok {
			event.Timestamp = v
		}
		if v, ok := hit.Source["type"].(string); ok {
			event.Type = v
		}
		events = append(events, event)
	}
	return events
}

// parseTopHitRetryEvents extracts RetryEvent list from a top_hits aggregation.
func parseTopHitRetryEvents(hits topHitsResult) []types.RetryEvent {
	events := make([]types.RetryEvent, 0, len(hits.Hits.Hits))
	for _, hit := range hits.Hits.Hits {
		event := types.RetryEvent{}
		if v, ok := hit.Source["reason"].(string); ok {
			event.Reason = v
		}
		if v, ok := hit.Source["message"].(string); ok {
			event.Message = v
		}
		if v, ok := hit.Source["@timestamp"].(string); ok {
			event.Timestamp = v
		}
		if v, ok := hit.Source["type"].(string); ok {
			event.Type = v
		}
		events = append(events, event)
	}
	return events
}
