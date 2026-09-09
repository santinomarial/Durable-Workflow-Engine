# Monitor and operate

## Check process health

The API exposes unauthenticated probe endpoints that contain no workflow
data:

- `GET /api/health/live` reports that the HTTP process can serve requests.
- `GET /api/health/ready` verifies a PostgreSQL round trip and confirms that
  the database has the exact latest migration known to the running package.
- `GET /api/health` is a compatibility alias for readiness.

Configure an orchestrator to remove an instance from service when readiness
fails and to restart it only when liveness fails. Do not restart a process
solely because a dependency is temporarily unready. Readiness uses
`DWE_HEALTH_TIMEOUT_SECONDS` — see the
[configuration reference](../reference/configuration.md).

Check `GET /api/workers` for each worker's host, process, queue, roles, last
seen time, graceful stop time, and health classification. `engine worker`
handles `SIGINT`/`SIGTERM` by stopping polling, letting the current bounded
transition finish, marking its heartbeat stopped, and closing the pool.

## Scrape metrics

`GET /metrics` returns Prometheus text exposition and requires an `admin`
bearer key (see
[provision and rotate API keys](provision-and-rotate-api-keys.md)). Configure
the scraper with that key and TLS. See
[metrics and log reference](../reference/metrics-and-logs.md) for the full
metric list.

Recommended initial alerts, to be tuned with workload-specific SLOs:

- readiness failing for more than two minutes;
- no healthy worker for an expected queue or role;
- dead tasks increasing;
- pending tasks increasing continuously while workers are healthy;
- HTTP 5xx rate above 1% for five minutes; and
- p99 API latency above the deployment's measured objective.

## Read structured logs

`DWE_LOG_FORMAT=json` is the default; set `DWE_LOG_FORMAT=text` only for
local interactive use. Exceptions render as one JSON field, so collectors
don't need multiline heuristics. Logs deliberately exclude authorization
headers and workflow/signal payloads. Join an operator mutation to the
immutable database audit by `request_id`.

## Pause and recover executions

Pausing an execution prevents workers and maintenance pollers from acquiring
new work for it; an activity or workflow transition already holding a valid
lease may still finish, and token fencing still applies. On resume, pending
timer visibility and activity schedule-to-start deadlines move forward by the
paused duration, so an intentional pause doesn't manufacture timeouts.

The recovery center lists dead tasks with their latest persisted outcome. A
failed or terminated workflow can be retried as a new linked execution: retry
copies the immutable definition version, input, queue, and search
attributes, adds `dwe.retry_of`, and never edits the original history.
**Repair the underlying activity or configuration before retrying** —
external effects still require idempotency at their boundary.

## Operate durable schedules

Schedules use standard five-field cron expressions and IANA timezone names.
Choose an overlap policy explicitly: `allow` starts concurrent executions,
`skip` records a skipped occurrence while another run is open, and
`buffer_one` retains one coalesced occurrence to start after the active run
closes.

Pausing a schedule does not pause executions it already started. Resuming a
schedule computes the next future cron time; for intentional historical runs,
use the admin-only bounded backfill API instead.
