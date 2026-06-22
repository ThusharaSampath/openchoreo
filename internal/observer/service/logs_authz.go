// Copyright 2026 The OpenChoreo Authors
// SPDX-License-Identifier: Apache-2.0

package service

import (
	"context"
	"log/slog"

	authzcore "github.com/openchoreo/openchoreo/internal/authz/core"
	observerAuthz "github.com/openchoreo/openchoreo/internal/observer/authz"
	"github.com/openchoreo/openchoreo/internal/observer/types"
)

// logsServiceWithAuthz wraps a LogsQuerier and adds authorization checks.
// Both the HTTP handlers and the MCP handler should use this via NewLogsServiceWithAuthz.
// Other services that call LogsQuerier internally should use the unwrapped service directly.
type logsServiceWithAuthz struct {
	internal LogsQuerier
	pdp      authzcore.PDP
	logger   *slog.Logger
}

var _ LogsQuerier = (*logsServiceWithAuthz)(nil)

// NewLogsServiceWithAuthz wraps the provided LogsQuerier with authorization checks.
func NewLogsServiceWithAuthz(s LogsQuerier, pdp authzcore.PDP, logger *slog.Logger) LogsQuerier {
	return &logsServiceWithAuthz{internal: s, pdp: pdp, logger: logger}
}

func (s *logsServiceWithAuthz) QueryLogs(ctx context.Context, req *types.LogsQueryRequest) (*types.LogsQueryResponse, error) {
	resourceType, resourceName, hierarchy, err := observerAuthz.LogsScopeAuthz(req)
	if err != nil {
		return nil, err
	}
	// TODO: currently the obs API is not equipped to provide cluster level environments,
	// once that is done update false to proper isClusterScoped value.
	authzCtx := authzcore.Context{}
	if req.SearchScope != nil && req.SearchScope.Component != nil {
		scope := req.SearchScope.Component
		authzCtx.Resource = authzcore.ResourceAttribute{
			Environment: observerAuthz.FormatDualScopedResourceName(scope.Namespace, scope.Environment, false),
		}
	}
	if err := observerAuthz.CheckAuthorization(
		ctx, s.logger, s.pdp,
		observerAuthz.ActionViewLogs,
		resourceType, resourceName, hierarchy,
		authzCtx,
	); err != nil {
		return nil, err
	}
	return s.internal.QueryLogs(ctx, req)
}

func (s *logsServiceWithAuthz) QueryRuns(ctx context.Context, req *types.RunsQueryRequest) (*types.RunsQueryResponse, error) {
	// Reuse the same authorization check as logs: user must have ViewLogs permission
	logsReq := &types.LogsQueryRequest{
		SearchScope: &types.SearchScope{Component: req.SearchScope},
		StartTime:   req.StartTime,
		EndTime:     req.EndTime,
	}
	resourceType, resourceName, hierarchy, err := observerAuthz.LogsScopeAuthz(logsReq)
	if err != nil {
		return nil, err
	}
	if err := observerAuthz.CheckAuthorization(
		ctx, s.logger, s.pdp,
		observerAuthz.ActionViewLogs,
		resourceType, resourceName, hierarchy,
	); err != nil {
		return nil, err
	}
	return s.internal.QueryRuns(ctx, req)
}

func (s *logsServiceWithAuthz) QueryRetries(ctx context.Context, jobName string, req *types.RetriesQueryRequest) (*types.RetriesQueryResponse, error) {
	logsReq := &types.LogsQueryRequest{
		SearchScope: &types.SearchScope{Component: req.SearchScope},
	}
	resourceType, resourceName, hierarchy, err := observerAuthz.LogsScopeAuthz(logsReq)
	if err != nil {
		return nil, err
	}
	if err := observerAuthz.CheckAuthorization(
		ctx, s.logger, s.pdp,
		observerAuthz.ActionViewLogs,
		resourceType, resourceName, hierarchy,
	); err != nil {
		return nil, err
	}
	return s.internal.QueryRetries(ctx, jobName, req)
}
