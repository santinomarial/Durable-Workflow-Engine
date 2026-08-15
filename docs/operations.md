# Production operations

## Process health

The API exposes unauthenticated probe endpoints that contain no workflow data:

- `GET /api/health/live` reports that the HTTP process can serve requests.
- `GET /api/health/ready` verifies a PostgreSQL round trip and confirms that the
  database has the exact latest migration known to the running package.
- `GET /api/health` is a compatibility alias for readiness.

Readiness uses `DWE_HEALTH_TIMEOUT_SECONDS` (default: 2). Configure an
orchestrator to remove an instance from service when readiness fails and to
restart it only when liveness fails. Do not restart a process solely because a
dependency is temporarily unready.

Workers write heartbeats to PostgreSQL when they start and every ten seconds by
default. `GET /api/workers` reports their host, process, queue, roles, last seen
time, graceful stop time, and health classification. `engine worker` handles
`SIGINT` and `SIGTERM` by stopping polling, allowing the current bounded
transition to return, marking its heartbeat stopped, and closing the pool. The
workflow lease and token-fencing model remains the fallback if a process exceeds
its orchestrator termination grace period and is killed.

## Metrics

`GET /metrics` returns Prometheus text exposition and requires an `admin` bearer
key. Configure the scraper with that key and TLS. Labels are deliberately
bounded to method, route template, status, worker role, and outcome; workflow
IDs and task IDs never become labels.

The endpoint includes:

- `dwe_http_requests_total` and `dwe_http_request_duration_seconds`;
- `dwe_tasks_pending`, `dwe_tasks_leased`, and `dwe_tasks_dead`;
- `dwe_workflows_running` and `dwe_workers_healthy`; and
- in worker processes, `dwe_worker_steps_total`,
  `dwe_worker_step_errors_total`, and `dwe_worker_step_duration_seconds`.

Worker-process metrics currently live in each process registry. Database gauges
and worker heartbeat state are visible from the API. If worker-local metrics
are required, run workers under a sidecar or process metrics exporter rather
than opening an unauthenticated port.

Recommended initial alerts, to be tuned with workload-specific SLOs:

- readiness failing for more than two minutes;
- no healthy worker for an expected queue or role;
- dead tasks increasing;
- pending tasks increasing continuously while workers are healthy;
- HTTP 5xx rate above 1% for five minutes; and
- p99 API latency above the deployment's measured objective.

## Structured logs

`DWE_LOG_FORMAT=json` is the default. Each record includes an RFC 3339 UTC
timestamp, level, logger, and message. HTTP completion records add the
server-generated request ID, authenticated key ID, method, bounded route
template, status, and duration. Worker lifecycle records add worker ID, queue,
and roles. Exceptions are rendered in one JSON field so collectors do not need
multiline heuristics.

Set `DWE_LOG_FORMAT=text` only for local interactive use and set `LOG_LEVEL` to
a standard Python level such as `INFO` or `WARNING`. Logs deliberately exclude
authorization headers and workflow/signal payloads. Join an operator mutation
to the immutable database audit by `request_id`.
