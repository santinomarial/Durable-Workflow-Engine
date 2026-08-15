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
- `dwe_workflows_running`, `dwe_workflows_paused`, and `dwe_workers_healthy`; and
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

## Pause and recovery

Pausing an execution prevents workers and maintenance pollers from acquiring
new work for it. An activity or workflow transition already holding a valid
lease may finish; token fencing and atomic transition rules still apply. On
resume, pending timer visibility and activity schedule-to-start deadlines move
forward by the paused duration, so an intentional pause does not manufacture
timeouts.

The recovery center lists dead tasks with their latest persisted outcome. A
failed or terminated workflow can be retried as a new linked execution. Retry
copies the immutable definition version, input, queue, and search attributes,
adds `dwe.retry_of`, and never edits the original history. Operators should
repair the underlying activity or configuration before retrying; external
effects still require idempotency at their boundary.

## Durable schedules

Schedules use standard five-field cron expressions and IANA timezone names.
The maintenance role materializes each occurrence in the same PostgreSQL
transaction as its execution, first history event, and workflow task. A unique
`(schedule_id, scheduled_at)` key prevents duplicate runs across concurrent
maintenance workers.

Overlap policy is explicit: `allow` starts concurrent executions, `skip` records
a skipped occurrence while another run is open, and `buffer_one` retains one
coalesced occurrence to start after the active run closes. Pausing a schedule
does not pause executions it already started. Resuming computes the next future
cron time; intentional historical runs use the admin-only bounded backfill API.

## Child workflow operations

`ctx.child_workflow` records a deterministic child command in parent history and
creates the child execution, its initial history, and task in the same database
transaction. The child pins its own definition version and may use the parent's
queue or an explicit queue. Its terminal transition appends the corresponding
child result/failure event to the parent and wakes parent replay atomically.

The default `terminate` parent-close policy recursively terminates open
descendants and fences their outstanding tasks. `abandon` intentionally leaves
a child running after the parent closes and should be used only when the child
owns an independent business lifecycle. Child external effects retain the same
at-least-once and idempotency requirements as every other activity.
