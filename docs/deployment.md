# Production deployment

The included image runs as UID/GID 10001 with no Linux capabilities, a
read-only root filesystem, bounded `/tmp`, JSON logs, an explicit stop signal,
and an HTTP liveness check. Dependencies are installed from `uv.lock`, and the
wheel contains migrations and console assets.

## Build and inspect

```shell
docker build --pull --tag durable-workflow-engine:0.1.0 .
docker inspect durable-workflow-engine:0.1.0 \
  --format '{{.Config.User}} {{json .Config.Healthcheck}}'
```

The runtime image contains the example workflow only as a deployment smoke
target. A real service should build a derived image containing its own versioned
workflow module and override the worker's `--definitions` and `--queue` values.
Keep every pinned workflow version available until no running execution refers
to it.

## Reference Compose topology

`compose.production.yaml` is a hardened single-host reference, not a
high-availability database architecture. It keeps PostgreSQL off the host
network, exposes the API on `127.0.0.1` by default, uses read-only non-root
engine containers, and reads credentials through Compose secrets.

Create the secret files with mode `0600` outside source control:

```text
secrets/postgres_password  # one high-entropy database password
secrets/database_url       # postgresql://durable:PASSWORD@postgres:5432/durable
secrets/api_keys           # key-id:role:sha256-digest[,more entries]
```

Then validate and start:

```shell
docker compose -f compose.production.yaml config --quiet
docker compose -f compose.production.yaml up --build -d
curl --fail http://127.0.0.1:8000/api/health/ready
```

Terminate TLS and shared rate limits at a trusted ingress. Set
`DWE_BIND_ADDRESS` only when the ingress cannot use the loopback binding. For a
business-critical deployment, replace the Compose PostgreSQL service with a
managed or supervised PostgreSQL 17 cluster with synchronous durability,
automated encrypted backups, point-in-time recovery, connection limits, and
tested failover. The engine requires one PostgreSQL consistency boundary per
workflow; it does not provide multi-region active/active operation.

## Configuration contract

Secrets support `NAME_FILE` and reject simultaneous `NAME` plus `NAME_FILE`.
Startup fails on missing authentication, missing database credentials, invalid
numeric values, or a pool maximum below its minimum. Database connections set
an application name, a command timeout, a server-side statement timeout, and an
idle-in-transaction timeout.

Important settings:

| Variable | Default | Purpose |
| --- | ---: | --- |
| `DWE_DB_POOL_MIN_SIZE` | 2 | Warm connections per process |
| `DWE_DB_POOL_MAX_SIZE` | 10 | Maximum connections per process |
| `DWE_DB_COMMAND_TIMEOUT_SECONDS` | 30 | Client command timeout |
| `DWE_DB_STATEMENT_TIMEOUT_MS` | 30000 | PostgreSQL statement timeout |
| `DWE_HEALTH_TIMEOUT_SECONDS` | 2 | Readiness dependency timeout |
| `DWE_MAX_REQUEST_BYTES` | 1048576 | HTTP request-body limit |
| `DWE_RATE_LIMIT_PER_MINUTE` | 300 | Per-key, per-API-process limit |

Size the aggregate pool maximum across all API and worker replicas below the
database's reserved application connection budget. Run migrations as a
controlled release step when the platform supports jobs; startup migration is
advisory-lock protected, but it should not replace deployment change control.

## Rollout and rollback

1. Back up and verify the database before a migration-bearing release.
2. Run `engine replay-check` on representative open executions against the new
   workflow definitions.
3. Deploy workers that support both old and new pinned versions.
4. Apply forward-only migrations, then roll API and workers gradually.
5. Watch readiness, worker heartbeats, dead tasks, queue growth, and 5xx rates.

Database migrations are forward-only. Application rollback is safe only while
the old binary understands every applied migration and pinned definition.
Otherwise restore into an isolated database and perform a planned recovery
rather than attempting an ad hoc schema downgrade.
