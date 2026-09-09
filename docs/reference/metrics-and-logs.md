# Metrics and log fields reference

## Prometheus metrics (`GET /metrics`, admin key required)

Labels are deliberately bounded to method, route template, status, worker
role, and outcome; workflow IDs and task IDs never become labels.

| Metric | Emitted by |
| --- | --- |
| `dwe_http_requests_total` | API |
| `dwe_http_request_duration_seconds` | API |
| `dwe_tasks_pending` | API (database gauge) |
| `dwe_tasks_leased` | API (database gauge) |
| `dwe_tasks_dead` | API (database gauge) |
| `dwe_workflows_running` | API (database gauge) |
| `dwe_workflows_paused` | API (database gauge) |
| `dwe_workers_healthy` | API (database gauge) |
| `dwe_worker_steps_total` | worker process |
| `dwe_worker_step_errors_total` | worker process |
| `dwe_worker_step_duration_seconds` | worker process |

Worker-process metrics (the last three) live in each process's own registry,
not the API's. If you need them scraped, run workers under a sidecar or
process metrics exporter rather than opening an unauthenticated port on the
worker itself.

## Structured log fields

Default format is `DWE_LOG_FORMAT=json`; every record includes an RFC 3339
UTC timestamp, level, logger, and message.

| Context | Additional fields |
| --- | --- |
| HTTP completion | server-generated request ID, authenticated key ID, method, bounded route template, status, duration |
| Worker lifecycle | worker ID, queue, roles |
| Exceptions | rendered in one JSON field (no multiline heuristics needed) |

Logs deliberately exclude authorization headers and workflow/signal payloads.
