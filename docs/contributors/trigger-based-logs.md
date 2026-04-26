# Trigger-Based Logs for Scheduled Tasks

## Problem

OpenChoreo scheduled tasks (CronJob components) run discrete executions on a schedule. The current observability logs UI shows a flat time-based log stream, which doesn't match the execution model. Users need logs organized as:

**Component > Triggers (Jobs) > Retries (Pods) > Logs**

Each CronJob schedule tick creates a Job (trigger). Each Job can spawn multiple Pods (retries, due to failures/backoffLimit). Users need to browse triggers, see their status, drill into retries, and view logs per retry.

Related: [Discussion #1894 - Kube Events Persistence](https://github.com/openchoreo/openchoreo/discussions/1894)

---

## Architecture

### Data Pipeline

```
Data Plane Cluster:
+----------------------------------+
| kube-events-collector (Deployment)|
|  +-- Event Informer (K8s SDK)    |-- Watch K8s Events
|  +-- Pod Informer                |-- Watch Pod status changes (OOMKilled, etc.)
|  +-- Label Cache (in-memory TTL) |-- Cache involvedObject labels
|  +-- Event Handler               |-- Enrich events with involvedObject labels
|  +-- Checkpoint DB (SQLite)      |-- Dedup across restarts
|  +-- JSON stdout logger          |-- Print enriched events as JSON logs
+----------------+-----------------+
                 | stdout (JSON lines)
                 v
+----------------------------------+
| Fluent Bit (existing DaemonSet)  |
|  +-- Tail input (picks up logs)  |
|  +-- Route to kube-events-* idx  |-- Separate from container logs
+----------------+-----------------+
                 |
                 v
+----------------------------------+
| OpenSearch                       |
|  +-- logs-YYYY-MM-DD (existing)  |
|  +-- kube-events-* (NEW)         |-- Enriched K8s events
+----------------------------------+
                 ^
                 | queries
+----------------------------------+
| Observer Service (Obs. Plane)    |
|  +-- POST /api/v1/scheduled-tasks/triggers/query
|  +-- POST /api/v1/scheduled-tasks/triggers/{jobName}/retries/query
|  +-- POST /api/v1/logs/query (extended with podName filter)
+----------------------------------+
```

### Key Insight: Label Enrichment

Native K8s Events reference an `involvedObject` but don't carry that object's labels. The kube-events-collector enriches events at collection time by fetching labels from the K8s API (with caching), attaching `openchoreo.dev/component-uid`, `project-uid`, `environment-uid` etc. This enables filtering events by component/environment in OpenSearch.

### Primary/Foreign Key Relationships

```
Component (component-uid + environment-uid)
  +-- Trigger/Job (involvedObject.name, unique job name, carries component labels)
       +-- Retry/Pod (involvedObject.name, pod name, linked via job-name prefix)
            +-- Logs (linked via kubernetes.pod_name in logs-* index)
```

### Trigger Status Derivation

Status is derived from K8s Event reasons in the kube-events index:
- `Completed` event present -> **succeeded**
- `BackoffLimitExceeded` or `DeadlineExceeded` present -> **failed**
- Only `SuccessfulCreate` present -> **running**
- Otherwise -> **unknown**

---

## Enriched Event Document Schema (kube-events-* index)

```json
{
  "@timestamp": "2026-03-23T10:00:00Z",
  "firstTimestamp": "2026-03-23T10:00:00Z",
  "lastTimestamp": "2026-03-23T10:00:03Z",
  "reason": "Completed",
  "message": "Job completed",
  "type": "Normal",
  "count": 1,
  "source": { "component": "job-controller" },
  "involvedObject": {
    "apiVersion": "batch/v1",
    "kind": "Job",
    "name": "mycomp-dev-a1b2c3d4-28612345",
    "namespace": "dp-default-myproject-dev-hash",
    "uid": "job-uid-123",
    "labels": {
      "openchoreo.dev/component-uid": "comp-uid",
      "openchoreo.dev/environment-uid": "env-uid",
      "openchoreo.dev/project-uid": "proj-uid",
      "openchoreo.dev/component": "mycomponent",
      "openchoreo.dev/environment": "dev",
      "openchoreo.dev/project": "myproject",
      "openchoreo.dev/namespace": "default"
    }
  }
}
```

---

## API Endpoints

### POST /api/v1/scheduled-tasks/triggers/query

List triggers (Jobs) for a scheduled task component.

**Request:**
```json
{
  "searchScope": {
    "namespace": "default",
    "project": "myproject",
    "component": "mytask",
    "environment": "dev"
  },
  "startTime": "2026-03-01T00:00:00Z",
  "endTime": "2026-03-24T00:00:00Z",
  "limit": 20,
  "offset": 0,
  "sortOrder": "desc"
}
```

**Response:**
```json
{
  "triggers": [
    {
      "jobName": "mytask-dev-a1b2c3d4-28612345",
      "status": "succeeded",
      "startTime": "2026-03-23T10:00:00Z",
      "completionTime": "2026-03-23T10:00:45Z",
      "eventCount": 3,
      "events": [
        {"reason": "SuccessfulCreate", "message": "Created pod: ...", "timestamp": "...", "type": "Normal"},
        {"reason": "Completed", "message": "Job completed", "timestamp": "...", "type": "Normal"}
      ]
    }
  ],
  "total": 42,
  "tookMs": 15
}
```

### POST /api/v1/scheduled-tasks/triggers/{jobName}/retries/query

List retries (Pods) for a specific trigger.

**Request:**
```json
{
  "searchScope": {
    "namespace": "default",
    "project": "myproject",
    "component": "mytask",
    "environment": "dev"
  }
}
```

**Response:**
```json
{
  "retries": [
    {
      "podName": "mytask-dev-a1b2c3d4-28612345-xxxxx",
      "status": "Succeeded",
      "startTime": "2026-03-23T10:00:02Z",
      "eventCount": 4,
      "events": [
        {"reason": "Scheduled", "message": "Successfully assigned ...", "timestamp": "..."},
        {"reason": "Started", "message": "Started container main", "timestamp": "..."}
      ]
    }
  ],
  "total": 1,
  "tookMs": 8
}
```

### POST /api/v1/logs/query (extended)

Existing endpoint, now supports optional `podName` in `searchScope` to filter logs to a specific pod (retry).

---

## Implementation Checklist

### Part A: Kube Events Collector

- [x] `cmd/kube-events-collector/main.go` - Entry point with CLI flags
- [x] `internal/kube-events-collector/config.go` - Configuration struct
- [x] `internal/kube-events-collector/types.go` - EnrichedEvent document types
- [x] `internal/kube-events-collector/collector.go` - Main orchestrator, K8s client setup
- [x] `internal/kube-events-collector/handler.go` - Event + Pod informers, namespace discovery, event processing
- [x] `internal/kube-events-collector/enricher.go` - Label enrichment with `openchoreo.dev/*` filtering and not-found caching
- [x] `internal/kube-events-collector/cache.go` - Thread-safe in-memory TTL label cache with not-found markers and background eviction
- [x] `internal/kube-events-collector/checkpoint.go` - SQLite dedup checkpoint with periodic cleanup
- [x] `internal/kube-events-collector/output.go` - JSON stdout writer for Fluent Bit
- [x] `make/golang.mk` - Added `kube-events-collector` binary target
- [x] `cmd/kube-events-collector/Dockerfile` - Distroless container image
- [x] `make/docker.mk` - Added `kube-events-collector` to DOCKER_BUILD_IMAGES
- [x] `make/k3d.mk` - Added build, load, update targets for kube-events-collector
- [x] Bug fix: duplicate informers on namespace re-scan (knownNamespaces tracking)
- [x] Bug fix: namespace selector `created-by` not `managed-by`
- [ ] Unit tests for enricher, cache, checkpoint, handler
- [ ] Integration test with fake K8s client

### Part B: Helm & OpenSearch Setup

- [x] `install/helm/openchoreo-data-plane/templates/kube-events-collector/deployment.yaml`
- [x] `install/helm/openchoreo-data-plane/templates/kube-events-collector/serviceaccount.yaml`
- [x] `install/helm/openchoreo-data-plane/templates/kube-events-collector/clusterrole.yaml`
- [x] `install/helm/openchoreo-data-plane/templates/kube-events-collector/clusterrolebinding.yaml`
- [x] `install/helm/openchoreo-data-plane/templates/kube-events-collector/pvc.yaml`
- [x] `install/helm/openchoreo-data-plane/templates/_helpers.tpl` - Added name/SA helpers
- [x] `install/helm/openchoreo-data-plane/values.yaml` - Added `kubeEventsCollector` section (disabled by default)
- [x] `install/init/observability/opensearch/setup-opensearch-cluster.sh` - Added kube-events index template + ISM policy (30d retention)
- [x] `cmd/kube-events-collector/Dockerfile` - Distroless container image
- [x] `hack/setup-trigger-logs-dev.sh` - Dev setup script (build, deploy, index, test)
- [x] Fluent Bit configuration to route kube-events-collector stdout to `kube-events-*` index (via `hack/setup-trigger-logs-dev.sh`)

### Part C: Observer API (Trigger-Based Logs)

- [x] `internal/observer/types/triggers.go` - Request/response types for triggers and retries
- [x] `internal/observer/opensearch/types.go` - Added `Aggregations` to `SearchResponse`, `TriggersQueryParams`, `RetriesQueryParams`, `PodName` to `ComponentLogsQueryParamsV1`
- [x] `internal/observer/opensearch/queries.go` - Added `BuildTriggersQuery`, `BuildRetriesQuery`, added `PodName` filter to `BuildComponentLogsQueryV1`
- [x] `internal/observer/adaptor/logs_default.go` - Added `SearchRaw` method, propagated `PodName`
- [x] `internal/observer/service/interfaces.go` - Extended `LogsQuerier` with `QueryTriggers`, `QueryRetries`
- [x] `internal/observer/service/triggers.go` - Service methods with aggregation parsing, status derivation
- [x] `internal/observer/service/logs.go` - Added `PodName` to scope resolution and query params
- [x] `internal/observer/service/logs_authz.go` - Authz wrappers for new methods
- [x] `internal/observer/api/handlers/triggers.go` - HTTP handlers with validation
- [x] `internal/observer/types/logs.go` - Added `PodName` and direct UID fields (`ComponentUID`, `EnvironmentUID`, `ProjectUID`) to `ComponentSearchScope`
- [x] `pkg/observability/logs.go` - Added `PodName` to `ComponentApplicationLogsParams`
- [x] `cmd/observer/main.go` - Registered new routes (public + internal no-auth for testing)
- [x] `internal/observer/mcp/server_test.go` - Updated mock
- [x] `internal/observer/api/handlers/scope_auth_test.go` - Updated mock
- [x] Direct UID bypass in `triggers.go` - skip scope resolution when UIDs provided in request
- [x] `openapi/observer-triggers-api.yaml` - OpenAPI spec for triggers/retries endpoints (importable into Postman)
- [ ] Unit tests for `BuildTriggersQuery`, `BuildRetriesQuery`
- [ ] Unit tests for `parseTriggerAggregation`, `parseRetriesAggregation`, `deriveTriggerStatus`
- [ ] Integration tests for trigger/retry service methods

### Part D: Verification (Manual)

- [x] Deploy to k3d cluster with kube-events-collector enabled
- [x] Create a scheduled task component (test CronJob with openchoreo labels)
- [x] Wait for CronJob to trigger
- [x] Verify events in `kube-events-*` OpenSearch index (518+ events indexed)
- [x] Verify enrichment (`involvedObject.labels.openchoreo.dev/component-uid` present)
- [x] Run raw aggregation queries against OpenSearch (triggers + retries aggs work)
- [x] Test Observer API endpoints (triggers query: 72 triggers, retries query: correct pod grouping)
- [x] Test successful task → status `"succeeded"`, single retry pod
- [x] Test failing task (exit 1, backoffLimit=2) → status `"failed"` (BackoffLimitExceeded), 3 retry pods
- [x] Test running task → status `"running"` (only SuccessfulCreate events so far)
- [x] Verify pagination (limit/offset working with 72+ triggers)

### Part E: Backstage UI (Future)

- [ ] TriggerListView component - replaces flat log stream for scheduled-task components
- [ ] RetryListView component - shows pods for a selected trigger
- [ ] RetryLogView component - reuses existing LogsTable with podName filter
- [ ] Component type detection - show trigger view vs flat log view based on component type
- [ ] API client methods in ObservabilityApi.ts for triggers/retries
- [ ] URL filter synchronization for trigger/retry navigation

---

## Key Files Reference

### Kube Events Collector

| File | Purpose |
|------|---------|
| `cmd/kube-events-collector/main.go` | Entry point |
| `internal/kube-events-collector/collector.go` | Orchestrator |
| `internal/kube-events-collector/handler.go` | Event/Pod informers, processing |
| `internal/kube-events-collector/enricher.go` | Label enrichment from K8s API |
| `internal/kube-events-collector/cache.go` | In-memory TTL label cache |
| `internal/kube-events-collector/checkpoint.go` | SQLite dedup checkpoint |
| `internal/kube-events-collector/output.go` | JSON stdout writer |

### Observer API

| File | Purpose |
|------|---------|
| `internal/observer/types/triggers.go` | Trigger/retry request/response types |
| `internal/observer/service/triggers.go` | Service methods, aggregation parsing, status derivation |
| `internal/observer/opensearch/queries.go` | OpenSearch query builders (BuildTriggersQuery, BuildRetriesQuery) |
| `internal/observer/api/handlers/triggers.go` | HTTP handlers |

### Helm/Infrastructure

| File | Purpose |
|------|---------|
| `install/helm/openchoreo-data-plane/templates/kube-events-collector/` | All K8s resources |
| `install/helm/openchoreo-data-plane/values.yaml` | Config under `kubeEventsCollector` |
| `install/init/observability/opensearch/setup-opensearch-cluster.sh` | Index template + ISM policy |

### Build & Dev

| File | Purpose |
|------|---------|
| `cmd/kube-events-collector/Dockerfile` | Container image for collector |
| `make/docker.mk` | Docker build targets (includes kube-events-collector) |
| `make/k3d.mk` | k3d build/load/update targets (includes kube-events-collector) |
| `make/golang.mk` | Go build targets (includes kube-events-collector) |
| `hack/setup-trigger-logs-dev.sh` | Dev setup script (build, deploy, configure fluent-bit, test) |
| `openapi/observer-triggers-api.yaml` | OpenAPI 3.0 spec for triggers/retries API (import into Postman) |

---

## Bugs Found & Fixed During Testing

1. **Duplicate informers on namespace re-scan** — `handler.go` started informers in `Start()` but didn't record them in the `known` map. `watchNamespaces()` used a separate local map, so every namespace got duplicate informers after 30s. **Fix**: single `knownNamespaces` map on the `Handler` struct, populated during `Start()`.

2. **Wrong namespace label selector** — Default was `openchoreo.dev/managed-by=renderedrelease-controller` but actual namespace label is `openchoreo.dev/created-by=renderedrelease-controller`. **Fix**: updated default in `main.go` and `values.yaml`.

3. **SQLite BUSY errors** — caused by duplicate informers writing concurrently. Resolved by fix #1.

4. **Fluent Bit `Replace_Dots` breaking label queries** — The `Replace_Dots On` setting in the OpenSearch output converts `openchoreo.dev/component-uid` to `openchoreo_dev/component-uid`, which breaks the observer's OpenSearch queries that filter on `involvedObject.labels.openchoreo.dev/component-uid`. **Fix**: `Replace_Dots` is disabled for the `kube-events-*` output (only enabled for `container-logs-*` where it's needed for Kubernetes metadata fields).

5. **All labels included in enriched events** — The enricher was returning all labels from the involved object (including `batch.kubernetes.io/*`, `controller-uid`, etc.), adding noise. **Fix**: added `filterOpenChoreoLabels()` to only keep labels with the `openchoreo.dev/` prefix.

6. **Repeated API calls for deleted objects** — Events referencing deleted objects (old ReplicaSets, completed Pods) caused repeated K8s API calls that returned 404. **Fix**: added not-found caching (`SetNotFound()`) in the label cache — deleted objects are cached as "not found" for the TTL duration, avoiding wasted API calls.

7. **Unbounded label cache growth** — The in-memory label cache had no eviction, only lazy deletion on read. **Fix**: added background eviction goroutine (`StartEviction()`) that periodically purges expired entries.

## Known Gaps

1. **Retry pod status shows "Running" for finished containers** — K8s does not emit native Pod-level Events for container `Succeeded` / non-zero `Terminated` exits. The collector's `handlePodStatusChange` only detects `OOMKilled` and `CrashLoopBackOff`. The **trigger-level** status is correct (derived from Job events: `Completed` / `BackoffLimitExceeded` / `DeadlineExceeded`).

   **Current workaround (in observer):** `parseRetriesAggregation` overrides per-retry status based on the parent Job's status:
   - Job failed → all retries marked `Failed`.
   - Job succeeded → last retry (by start time) `Succeeded`, all prior retries `Failed` (they are why the Job retried).
   - Job running/unknown → fall back to pod-event–derived status.

   This is not 100% accurate — it doesn't distinguish e.g. a retry that's still actively running inside a still-running Job from a pod that already exited with success. But for finished triggers it gives the user the right outcome with no extra collector work.

   **Proper fix (Milestone 4):** emit synthetic events from `kube-events-collector` on pod phase transitions. See "Milestone 4" below.

2. **Observer scope resolution requires in-cluster DNS** — The quick-start setup uses external hostnames (`thunder.openchoreo.localhost`, `api.openchoreo.localhost`) that aren't resolvable from inside the cluster. Workaround: patch ConfigMap (done by `setup-trigger-logs-dev.sh`) or pass UIDs directly via `componentUid`/`environmentUid`/`projectUid` fields in request.

3. **Fluent Bit config is not in Helm chart** — The `setup-trigger-logs-dev.sh` script patches the fluent-bit configmap to route kube-events-collector logs to `kube-events-*` index. This patch is overwritten if the `observability-logs-opensearch` Helm chart is re-installed. Production fix: add the routing config to the Helm chart. After a Colima restart, re-run `setup-trigger-logs-dev.sh` to restore the config.

## Local Dev Workflow

### Prerequisites

- k3d cluster `openchoreo-quick-start` running with all planes deployed
- Docker available locally
- Go toolchain installed

### Speed Up Image Pulls with Registry Cache

To avoid re-downloading images on every cluster reinstall, set up the local pull-through cache **before** creating the cluster:

```bash
# Start the registry caches (persists across cluster reinstalls)
cd install/k3d/registry-cache
docker compose up -d
docker compose ps   # verify all 4 caches are running
cd -
```

Then create the k3d cluster with `--registry-config` pointing to the mirrors. See `install/k3d/registry-cache/README.md` for the full registries YAML config. The caches run on ports 5601-5604 and survive cluster deletes — only `docker compose down -v` clears cached data.

### Initial Setup

Run the setup script from the project root. This builds binaries, Docker images, loads them into k3d, creates the OpenSearch index template, deploys kube-events-collector, configures Fluent Bit routing, and patches the observer:

```bash
# Full setup (~3 min)
./hack/setup-trigger-logs-dev.sh

# Full setup + create a test CronJob that runs every minute
./hack/setup-trigger-logs-dev.sh --with-test-cronjob
```

The script performs these steps:
1. Cross-compiles `kube-events-collector` and `observer` for linux/arm64
2. Builds Docker images
3. Loads images into the k3d cluster
4. Creates the `kube-events-*` OpenSearch index template
5. Deploys kube-events-collector (ServiceAccount, ClusterRole, ClusterRoleBinding, Deployment)
6. Patches the observer ConfigMap for in-cluster service URLs
7. Restarts the observer with the new image
8. Patches Fluent Bit to route kube-events-collector logs to `kube-events-*` index
9. Optionally creates a test CronJob with openchoreo labels
10. Waits ~70s for events to flow through Fluent Bit into OpenSearch

### After a Colima Restart

1. Run `fix-openchoreo.sh` to fix IP/DNS issues (this does **not** touch Fluent Bit or kube-events-collector)
2. If the Helm chart was re-installed (which resets the Fluent Bit configmap), re-run `setup-trigger-logs-dev.sh` to restore the Fluent Bit routing config

### Quick Rebuild Cycle (after code changes)

```bash
# 1. Build
ARCH=$(go env GOARCH)
CGO_ENABLED=0 GOOS=linux GOARCH=${ARCH} go build -o bin/dist/linux/${ARCH}/observer -ldflags "-s -w" ./cmd/observer/
CGO_ENABLED=0 GOOS=linux GOARCH=${ARCH} go build -o bin/dist/linux/${ARCH}/kube-events-collector -ldflags "-s -w" ./cmd/kube-events-collector/

# 2. Docker build
docker build --build-arg TARGETOS=linux --build-arg TARGETARCH=${ARCH} -t ghcr.io/openchoreo/observer:latest-dev -f cmd/observer/Dockerfile .
docker build --build-arg TARGETOS=linux --build-arg TARGETARCH=${ARCH} -t ghcr.io/openchoreo/kube-events-collector:latest-dev -f cmd/kube-events-collector/Dockerfile .

# 3. Load + restart
k3d image import ghcr.io/openchoreo/observer:latest-dev ghcr.io/openchoreo/kube-events-collector:latest-dev --cluster openchoreo-quick-start
docker exec k3d-openchoreo-quick-start-server-0 kubectl rollout restart deployment/observer -n openchoreo-observability-plane
docker exec k3d-openchoreo-quick-start-server-0 kubectl rollout restart deployment/kube-events-collector -n openchoreo-data-plane
```

### Test API (no auth, internal port)

The observer internal port (8081) does not require authentication. There are two ways to test:

#### Option A: Port-forward (recommended for Postman/curl from your machine)

```bash
# Port-forward the internal observer service to localhost
docker exec k3d-openchoreo-quick-start-server-0 kubectl port-forward \
  svc/observer-internal 8081:8081 -n openchoreo-observability-plane
```

Then hit `http://localhost:8081/...` from curl, Postman, or any HTTP client.

An OpenAPI spec is available at `openapi/observer-triggers-api.yaml` — import it into Postman for ready-made requests with examples.

#### Option B: wget from inside the k3d node

```bash
# Get the observer internal service ClusterIP
OBSERVER_IP=$(docker exec k3d-openchoreo-quick-start-server-0 kubectl get svc observer-internal \
  -n openchoreo-observability-plane -o jsonpath='{.spec.clusterIP}')

# Query last 10 triggers (pass UIDs to bypass scope resolution auth)
docker exec k3d-openchoreo-quick-start-server-0 wget -qO- --post-data='{
  "searchScope":{
    "namespace":"default","project":"default",
    "component":"scheduled-task-2","environment":"development",
    "componentUID":"<COMPONENT_UID>",
    "environmentUID":"<ENV_UID>",
    "projectUID":"<PROJECT_UID>"
  },
  "startTime":"2026-04-01T00:00:00Z",
  "endTime":"2026-04-30T00:00:00Z",
  "limit":10,"sortOrder":"desc"
}' --header='Content-Type: application/json' \
  "http://${OBSERVER_IP}:8081/api/v1/scheduled-tasks/triggers/query"

# Query retries for a specific trigger (use jobName from triggers response)
docker exec k3d-openchoreo-quick-start-server-0 wget -qO- --post-data='{
  "searchScope":{
    "namespace":"default","project":"default",
    "component":"scheduled-task-2","environment":"development",
    "componentUID":"<COMPONENT_UID>",
    "environmentUID":"<ENV_UID>",
    "projectUID":"<PROJECT_UID>"
  }
}' --header='Content-Type: application/json' \
  "http://${OBSERVER_IP}:8081/api/v1/scheduled-tasks/triggers/<JOB_NAME>/retries/query"
```

#### Finding UIDs

To find the UIDs for your test CronJob, check the kube-events-collector logs:
```bash
docker exec k3d-openchoreo-quick-start-server-0 kubectl logs -n openchoreo-data-plane \
  -l app=kube-events-collector --tail=5 | grep -o '"labels":{[^}]*}' | head -1
```

> **Note on auth bypass:** The `componentUID`/`environmentUID`/`projectUID` fields in the request body skip the observer's scope resolution, which otherwise requires OAuth2 tokens from Thunder. This is the recommended approach for local testing.

### Verify Data Pipeline

```bash
# Check kube-events-collector is running and emitting events
docker exec k3d-openchoreo-quick-start-server-0 kubectl logs -f -n openchoreo-data-plane \
  -l app=kube-events-collector --tail=10

# Check Fluent Bit is routing to kube-events-* index
docker exec k3d-openchoreo-quick-start-server-0 kubectl logs -n openchoreo-observability-plane \
  -l app.kubernetes.io/name=fluent-bit --tail=20

# Check OpenSearch has kube-events data
OS_PASS=$(docker exec k3d-openchoreo-quick-start-server-0 kubectl get secret opensearch-admin-credentials \
  -n openchoreo-observability-plane -o jsonpath='{.data.password}' | base64 -d)
docker exec k3d-openchoreo-quick-start-server-0 kubectl exec opensearch-master-0 \
  -n openchoreo-observability-plane -- curl -sk -u "admin:${OS_PASS}" \
  'https://localhost:9200/_cat/indices?v' | grep kube-events
```

## Test Results Summary

| Scenario | Trigger Status | Retry Count | Events | Correct? |
|----------|---------------|-------------|--------|----------|
| Successful task (exit 0) | `succeeded` | 1 pod | SuccessfulCreate, Completed | Yes |
| Failed task (exit 1, backoffLimit=2) | `failed` | 3 pods | 3x SuccessfulCreate, BackoffLimitExceeded | Yes |
| Running task (in progress) | `running` | 1+ pods | SuccessfulCreate only | Yes |
| Per-retry events | N/A | N/A | Scheduled, Pulling, Pulled, Created, Started | Yes |
| Pagination (limit=5, total=72) | N/A | N/A | 5 returned, total=72 | Yes |

## Milestone 4 (Future): Accurate Per-Retry Pod Status via Synthetic Events

The current observer-side override (see Known Gap #1) is a heuristic. To get truly accurate per-retry status — including correctly labelling pods that are *currently* running inside a *currently* running Job, and distinguishing OOMKilled vs generic exit-code failures vs successful completion — the kube-events-collector should emit synthetic events from the Pod informer.

### Scope

In `internal/kube-events-collector/handler.go`, extend `handlePodStatusChange` to:

1. Track pod phase per pod (cache previous `pod.Status.Phase`).
2. On transition to `Succeeded`: emit synthetic event `reason: PodSucceeded`, type `Normal`, message includes container exit codes.
3. On transition to `Failed`: emit synthetic event `reason: PodFailed`, type `Warning`, message includes the failing container name and exit code; if `containerStatuses[*].state.terminated.reason` is `OOMKilled` use that as the reason instead.
4. Emit through the same enrichment + dedup + JSON output path as native events so the kube-events index stores them identically.

### Observer changes

- Extend `deriveRetryStatus` to honour the new reasons:
  - `PodSucceeded` → `Succeeded`
  - `PodFailed` → `Failed`
- Once synthetic events are flowing reliably, remove `applyTriggerStatusOverride` from `parseRetriesAggregation`.

### Migration

- The observer override and the synthetic-event path are compatible — keep the override in place during rollout. Once collectors with synthetic events are deployed everywhere and historical data is past retention (30d), drop the override.

### Open questions

- Dedup key: synthetic events have no native UID. Use `<pod-uid>:<phase>` to ensure idempotency across collector restarts.
- Retroactive backfill on collector startup: the collector should not emit `PodSucceeded` for pods that finished while the collector was down (would cause noise). Either skip pods whose `lastTransitionTime` is older than collector start, or drive it strictly off informer transitions.

---

## Design Decisions

1. **Kube-events-collector as stdout -> Fluent Bit pipeline** (per #1894) rather than direct OpenSearch writes. Keeps the collector stateless regarding OpenSearch connectivity.
2. **Enrichment at collection time** (not query time) - avoids complex joins and ensures labels are available even after resource deletion.
3. **Aggregation-based trigger listing** - uses OpenSearch `terms` aggregation on `involvedObject.name` to group events by Job, avoiding a separate trigger summary index.
4. **Trigger status derived from event reasons** - `Completed` = succeeded, `BackoffLimitExceeded`/`DeadlineExceeded` = failed, otherwise running/unknown.
5. **Reuse existing logs query for retry logs** - just add `podName` filter to `ComponentSearchScope`.
6. **Checkpoint DB for dedup** - SQLite file on PVC, prevents re-processing events after collector restart.
7. **Namespace discovery with periodic re-scan** - watches namespaces with `openchoreo.dev/created-by=renderedrelease-controller` label, re-discovers every 30s.
8. **Label filtering at collection time** - only `openchoreo.dev/*` labels are kept in enriched events, reducing noise and index size.
9. **Not-found caching** - deleted objects (old ReplicaSets, completed Pods) are cached as "not found" to avoid repeated 404 API calls.
10. **Fluent Bit routing with `Replace_Dots Off`** - the `kube-events-*` output must not replace dots in field names, otherwise `openchoreo.dev/component-uid` becomes `openchoreo_dev/component-uid` and breaks OpenSearch queries. The `container-logs-*` output keeps `Replace_Dots On` for Kubernetes metadata compatibility.

## OpenSearch Queries for Verification

### List triggers for a component
```json
POST /kube-events-*/_search
{
  "size": 0,
  "query": {
    "bool": {
      "must": [
        { "term": { "involvedObject.kind": "Job" } },
        { "term": { "involvedObject.labels.openchoreo.dev/component-uid": "<uid>" } },
        { "term": { "involvedObject.labels.openchoreo.dev/environment-uid": "<uid>" } },
        { "range": { "@timestamp": { "gte": "<start>", "lte": "<end>" } } }
      ]
    }
  },
  "aggs": {
    "triggers": {
      "terms": { "field": "involvedObject.name", "size": 20, "order": { "first_seen": "desc" } },
      "aggs": {
        "first_seen": { "min": { "field": "@timestamp" } },
        "last_seen": { "max": { "field": "@timestamp" } },
        "reasons": { "terms": { "field": "reason" } },
        "events": { "top_hits": { "size": 10, "sort": [{ "@timestamp": "asc" }] } }
      }
    }
  }
}
```

### List retries for a trigger
```json
POST /kube-events-*/_search
{
  "size": 0,
  "query": {
    "bool": {
      "must": [
        { "term": { "involvedObject.kind": "Pod" } },
        { "wildcard": { "involvedObject.name": "<job-name>-*" } }
      ]
    }
  },
  "aggs": {
    "retries": {
      "terms": { "field": "involvedObject.name", "size": 20, "order": { "first_seen": "asc" } },
      "aggs": {
        "first_seen": { "min": { "field": "@timestamp" } },
        "reasons": { "terms": { "field": "reason" } },
        "events": { "top_hits": { "size": 10, "sort": [{ "@timestamp": "asc" }] } }
      }
    }
  }
}
```

### Get logs for a specific retry (pod)
```json
POST /logs-*/_search
{
  "query": {
    "bool": {
      "must": [
        { "term": { "kubernetes.pod_name": "<pod-name>" } },
        { "range": { "@timestamp": { "gte": "<start>", "lte": "<end>" } } }
      ]
    }
  },
  "sort": [{ "@timestamp": "asc" }],
  "size": 100
}
```
