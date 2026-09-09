# Configuration reference

Every secret-shaped variable below also accepts a `NAME_FILE` form pointing
at a file (for container secret mounts, e.g. `DATABASE_URL_FILE`). Setting
both `NAME` and `NAME_FILE` for the same variable fails at startup.

## Database and pool

| Variable | Default | Purpose |
| --- | --- | --- |
| `DATABASE_URL` | — (required) | PostgreSQL connection string |
| `DWE_DB_POOL_MIN_SIZE` | `2` | warm connections per process |
| `DWE_DB_POOL_MAX_SIZE` | `10` | maximum connections per process |
| `DWE_DB_COMMAND_TIMEOUT_SECONDS` | `30` | client command timeout |
| `DWE_DB_STATEMENT_TIMEOUT_MS` | `30000` | PostgreSQL statement timeout |

Startup fails if `DWE_DB_POOL_MAX_SIZE` is below `DWE_DB_POOL_MIN_SIZE`. Size
the aggregate pool maximum across all API and worker replicas below the
database's reserved application connection budget.

## Authentication

| Variable | Default | Purpose |
| --- | --- | --- |
| `DWE_AUTH_MODE` | `required` | `required` or `disabled` (local dev only) |
| `DWE_API_KEYS` | — | comma-separated `key-id:role:sha256-digest` entries |

Startup fails if `DWE_API_KEYS` is set while `DWE_AUTH_MODE=disabled`, or if
authentication is required but no keys are configured. See
[provision and rotate API keys](../how-to/provision-and-rotate-api-keys.md).

## HTTP server and request protection

| Variable | Default | Purpose |
| --- | --- | --- |
| `DWE_BIND_ADDRESS` | `127.0.0.1` (Compose reference) | host interface the API binds to |
| `DWE_PORT` | `8000` (Compose reference) | host port the API binds to |
| `DWE_MAX_REQUEST_BYTES` | `1048576` | HTTP request-body limit (fixed and streamed) |
| `DWE_RATE_LIMIT_PER_MINUTE` | `300` | requests per authenticated key, per API process |
| `DWE_HEALTH_TIMEOUT_SECONDS` | `2` | readiness dependency timeout |

`DWE_BIND_ADDRESS`/`DWE_PORT` are read by `compose.production.yaml`, not by
the application itself; set them when the ingress cannot use the loopback
binding.

## Logging

| Variable | Default | Purpose |
| --- | --- | --- |
| `DWE_LOG_FORMAT` | `json` | `json` or `text` (text for local interactive use only) |
| `LOG_LEVEL` | `INFO` | standard Python log level |

## Testing and maintenance scripts

| Variable | Default | Purpose |
| --- | --- | --- |
| `DWE_TEST_DATABASE_URL` | — | dedicated database used by integration/chaos test suites |
| `RESTORE_DATABASE_URL` | — | target database for `scripts/restore.sh` (must differ from any live URL) |
| `DWE_RESTORE_CONFIRM` | — | must equal `replace-target-database` for `scripts/restore.sh` to proceed |
