// Copyright 2026 The OpenChoreo Authors
// SPDX-License-Identifier: Apache-2.0

package handlers

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/openchoreo/openchoreo/internal/observer/api/gen"
	observerAuthz "github.com/openchoreo/openchoreo/internal/observer/authz"
	"github.com/openchoreo/openchoreo/internal/observer/httputil"
	"github.com/openchoreo/openchoreo/internal/observer/service"
	"github.com/openchoreo/openchoreo/internal/observer/types"
)

// QueryTriggers handles POST /api/v1/scheduled-tasks/triggers/query
func (h *Handler) QueryTriggers(w http.ResponseWriter, r *http.Request) {
	var req types.TriggersQueryRequest
	if err := httputil.BindJSON(r, &req); err != nil {
		h.logger.Error("Failed to bind request", "error", err)
		h.writeErrorResponse(w, http.StatusBadRequest, gen.BadRequest, "", "Invalid request format")
		return
	}

	if err := validateTriggersQueryRequest(&req); err != nil {
		h.logger.Debug("Validation failed", "error", err)
		h.writeErrorResponse(w, http.StatusBadRequest, gen.BadRequest, "", err.Error())
		return
	}

	ctx := r.Context()
	if h.logsService == nil {
		h.writeErrorResponse(w, http.StatusInternalServerError, gen.InternalServerError, "", "Logs service is not initialized")
		return
	}

	result, err := h.logsService.QueryTriggers(ctx, &req)
	if err != nil {
		if errors.Is(err, observerAuthz.ErrAuthzForbidden) {
			h.writeErrorResponse(w, http.StatusForbidden, gen.Forbidden, "", "Access denied")
			return
		}
		if errors.Is(err, observerAuthz.ErrAuthzUnauthorized) {
			h.writeErrorResponse(w, http.StatusUnauthorized, gen.Unauthorized, "", "Unauthorized")
			return
		}
		h.logger.Error("Failed to query triggers", "error", err)
		if errors.Is(err, service.ErrLogsResolveSearchScope) {
			h.writeErrorResponse(w, http.StatusInternalServerError, gen.InternalServerError, types.ErrorCodeV1LogsResolverFailed, "Failed to resolve search scope")
			return
		}
		h.writeErrorResponse(w, http.StatusInternalServerError, gen.InternalServerError, "", "Failed to retrieve triggers")
		return
	}

	h.writeJSON(w, http.StatusOK, result)
}

// QueryRetries handles POST /api/v1/scheduled-tasks/triggers/{jobName}/retries/query
func (h *Handler) QueryRetries(w http.ResponseWriter, r *http.Request) {
	jobName := r.PathValue("jobName")
	if jobName == "" {
		h.writeErrorResponse(w, http.StatusBadRequest, gen.BadRequest, "", "jobName path parameter is required")
		return
	}

	var req types.RetriesQueryRequest
	if err := httputil.BindJSON(r, &req); err != nil {
		h.logger.Error("Failed to bind request", "error", err)
		h.writeErrorResponse(w, http.StatusBadRequest, gen.BadRequest, "", "Invalid request format")
		return
	}

	if req.SearchScope == nil {
		h.writeErrorResponse(w, http.StatusBadRequest, gen.BadRequest, "", "searchScope is required")
		return
	}

	ctx := r.Context()
	if h.logsService == nil {
		h.writeErrorResponse(w, http.StatusInternalServerError, gen.InternalServerError, "", "Logs service is not initialized")
		return
	}

	result, err := h.logsService.QueryRetries(ctx, jobName, &req)
	if err != nil {
		if errors.Is(err, observerAuthz.ErrAuthzForbidden) {
			h.writeErrorResponse(w, http.StatusForbidden, gen.Forbidden, "", "Access denied")
			return
		}
		if errors.Is(err, observerAuthz.ErrAuthzUnauthorized) {
			h.writeErrorResponse(w, http.StatusUnauthorized, gen.Unauthorized, "", "Unauthorized")
			return
		}
		h.logger.Error("Failed to query retries", "error", err)
		h.writeErrorResponse(w, http.StatusInternalServerError, gen.InternalServerError, "", "Failed to retrieve retries")
		return
	}

	h.writeJSON(w, http.StatusOK, result)
}

// validateTriggersQueryRequest validates the triggers query request.
func validateTriggersQueryRequest(req *types.TriggersQueryRequest) error {
	if req.SearchScope == nil {
		return fmt.Errorf("searchScope is required")
	}
	if req.SearchScope.Namespace == "" {
		return fmt.Errorf("searchScope.namespace is required")
	}
	if req.StartTime == "" {
		return fmt.Errorf("startTime is required")
	}
	if req.EndTime == "" {
		return fmt.Errorf("endTime is required")
	}
	if req.SearchScope.Component == "" {
		return fmt.Errorf("searchScope.component is required for trigger queries")
	}
	if req.SearchScope.Environment == "" {
		return fmt.Errorf("searchScope.environment is required for trigger queries")
	}
	return nil
}
