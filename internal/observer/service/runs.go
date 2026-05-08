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

// QueryRuns queries runs (Jobs) for a scheduled task component from the kube-events index.
func (s *LogsService) QueryRuns(ctx context.Context, req *types.RunsQueryRequest) (*types.RunsQueryResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("request is required")
	}

	s.logger.Info("QueryRuns called",
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

	params := opensearch.RunsQueryParams{
		StartTime:     req.StartTime,
		EndTime:       req.EndTime,
		NamespaceName: namespaceName,
		ComponentID:   componentUID,
		EnvironmentID: environmentUID,
		ProjectID:     projectUID,
		Limit:         req.Limit,
		Offset:        req.Offset,
		SortOrder:     req.SortOrder,
		IncludeEvents: req.IncludeEvents,
	}

	if s.defaultAdaptor == nil {
		return nil, fmt.Errorf("default adaptor is not initialized")
	}

	// Build and execute query
	qb := &opensearch.QueryBuilder{}
	query, err := qb.BuildRunsQuery(params)
	if err != nil {
		return nil, fmt.Errorf("failed to build runs query: %w", err)
	}

	indices := []string{kubeEventsIndexPattern}
	result, err := s.defaultAdaptor.SearchRaw(ctx, indices, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query runs: %w", err)
	}

	return parseRunAggregation(result, params.Offset)
}

// QueryRetries queries retries (Pods) for a specific run (Job) from the kube-events index.
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

// parseRunAggregation parses the aggregation response into RunEntry list.
func parseRunAggregation(resp *opensearch.SearchResponse, offset int) (*types.RunsQueryResponse, error) {
	if resp == nil || resp.Aggregations == nil {
		return &types.RunsQueryResponse{Runs: []types.RunEntry{}}, nil
	}

	runsAgg, ok := resp.Aggregations["runs"]
	if !ok {
		return &types.RunsQueryResponse{Runs: []types.RunEntry{}}, nil
	}

	var aggResult struct {
		Buckets []runBucket `json:"buckets"`
	}
	if err := json.Unmarshal(runsAgg, &aggResult); err != nil {
		return nil, fmt.Errorf("failed to parse runs aggregation: %w", err)
	}

	// Parse total count
	total := len(aggResult.Buckets)
	if totalAgg, ok := resp.Aggregations["total_runs"]; ok {
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

	runs := make([]types.RunEntry, 0, len(buckets))
	for _, bucket := range buckets {
		run := types.RunEntry{
			JobName:    bucket.Key,
			EventCount: bucket.DocCount,
			Status:     deriveRunStatus(bucket.Reasons),
		}

		if run.Status == "failed" {
			run.FailureReason = deriveFailureReason(bucket.Reasons)
		}

		if bucket.FirstSeen.Value != nil {
			run.StartTime = formatMillisTimestamp(*bucket.FirstSeen.Value)
		}
		if bucket.LastSeen.Value != nil {
			run.CompletionTime = formatMillisTimestamp(*bucket.LastSeen.Value)
		}

		run.Events = parseTopHitEvents(bucket.Events)
		runs = append(runs, run)
	}

	return &types.RunsQueryResponse{
		Runs:   runs,
		Total:  total,
		TookMs: resp.Took,
	}, nil
}

// parseRetriesAggregation parses the aggregation response into RetryEntry list.
//
// The retries aggregation is a filter+terms (filter on Pod kind, terms on involvedObject.name).
// A sibling "job_reasons" filter aggregation captures the parent Job's event reasons in the same
// query, so we can derive the run status and override per-retry status here.
//
// Status override (Option 2 from the design doc — works around the fact that K8s does not emit
// a Pod-level "Completed" / "Failed" event on container exit, so deriveRetryStatus from pod
// events alone reports "Running" forever for finished pods):
//   - If the parent Job failed (BackoffLimitExceeded / DeadlineExceeded): all retries → Failed.
//   - If the parent Job succeeded: the last retry (by start time, ascending) → Succeeded; any
//     earlier retries are by definition the reason for the retry, so → Failed.
//   - If the parent Job is still running and has 2+ retries: every retry except the last must
//     have failed (the Job controller only spawns a new pod when the previous one failed under
//     restartPolicy: Never). Earlier retries → Failed; the last keeps deriveRetryStatus output.
//   - If the Job is unknown (or running with a single retry): keep whatever deriveRetryStatus produced.
//
// Future (Milestone 4): emit synthetic Pod-level events from kube-events-collector on
// pod.Status.Phase transitions to Succeeded/Failed (per-container exit code) so this
// override is no longer necessary. See docs/contributors/run-based-logs.md.
func parseRetriesAggregation(resp *opensearch.SearchResponse) (*types.RetriesQueryResponse, error) {
	if resp == nil || resp.Aggregations == nil {
		return &types.RetriesQueryResponse{Retries: []types.RetryEntry{}}, nil
	}

	retriesAgg, ok := resp.Aggregations["retries"]
	if !ok {
		return &types.RetriesQueryResponse{Retries: []types.RetryEntry{}}, nil
	}

	var podsWrapper struct {
		Pods struct {
			Buckets []retryBucket `json:"buckets"`
		} `json:"pods"`
	}
	if err := json.Unmarshal(retriesAgg, &podsWrapper); err != nil {
		return nil, fmt.Errorf("failed to parse retries aggregation: %w", err)
	}

	jobStatus := "unknown"
	if jobReasonsAgg, ok := resp.Aggregations["job_reasons"]; ok {
		var jobReasons struct {
			Reasons reasonsBucket `json:"reasons"`
		}
		if err := json.Unmarshal(jobReasonsAgg, &jobReasons); err == nil {
			jobStatus = deriveRunStatus(jobReasons.Reasons)
		}
	}

	retries := make([]types.RetryEntry, 0, len(podsWrapper.Pods.Buckets))
	for _, bucket := range podsWrapper.Pods.Buckets {
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

	applyRunStatusOverride(retries, jobStatus)

	return &types.RetriesQueryResponse{
		Retries: retries,
		Total:   len(retries),
		TookMs:  resp.Took,
	}, nil
}

// applyRunStatusOverride mutates retries in-place so their statuses reflect the parent
// Job's outcome. retries is expected to already be ordered by first_seen ascending (the
// retries-pods aggregation orders by first_seen asc).
func applyRunStatusOverride(retries []types.RetryEntry, jobStatus string) {
	if len(retries) == 0 {
		return
	}
	switch jobStatus {
	case "failed":
		for i := range retries {
			retries[i].Status = "Failed"
		}
	case "succeeded":
		for i := range retries {
			if i == len(retries)-1 {
				retries[i].Status = "Succeeded"
			} else {
				retries[i].Status = "Failed"
			}
		}
	case "running":
		// Mark every retry except the last as Failed: under restartPolicy: Never the Job
		// controller only spawns a fresh pod when the previous one failed, so the existence
		// of an N+1th pod is itself proof that pods 1..N failed. Leave the last pod's status
		// to deriveRetryStatus — it is the only one that could legitimately still be running.
		for i := 0; i < len(retries)-1; i++ {
			retries[i].Status = "Failed"
		}
	}
}

// runBucket represents a single bucket in the runs aggregation.
type runBucket struct {
	Key       string        `json:"key"`
	DocCount  int           `json:"doc_count"`
	FirstSeen metricValue   `json:"first_seen"`
	LastSeen  metricValue   `json:"last_seen"`
	Reasons   reasonsBucket `json:"reasons"`
	Events    topHitsResult `json:"events"`
}

// retryBucket represents a single bucket in the retries aggregation.
type retryBucket struct {
	Key       string        `json:"key"`
	DocCount  int           `json:"doc_count"`
	FirstSeen metricValue   `json:"first_seen"`
	Reasons   reasonsBucket `json:"reasons"`
	Events    topHitsResult `json:"events"`
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

// deriveRunStatus determines run status from event reasons.
func deriveRunStatus(reasons reasonsBucket) string {
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

// deriveFailureReason returns the K8s event reason that caused the Job to fail,
// or "" if no failure-indicating reason is present in the bucket. Reuses the same
// reasons aggregation as deriveRunStatus, so it costs nothing extra.
func deriveFailureReason(reasons reasonsBucket) string {
	for _, r := range reasons.Buckets {
		switch r.Key {
		case "BackoffLimitExceeded", "DeadlineExceeded", "FailedCreate":
			return r.Key
		}
	}
	return ""
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

// parseTopHitEvents extracts RunEvent list from a top_hits aggregation.
func parseTopHitEvents(hits topHitsResult) []types.RunEvent {
	events := make([]types.RunEvent, 0, len(hits.Hits.Hits))
	for _, hit := range hits.Hits.Hits {
		event := types.RunEvent{}
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
