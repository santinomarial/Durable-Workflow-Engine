# Deploy to production

## Build and inspect the image

The included image runs as UID/GID 10001 with no Linux capabilities, a
read-only root filesystem, bounded `/tmp`, JSON logs, an explicit stop signal,
and an HTTP liveness check. Dependencies are installed from `uv.lock`, and the
wheel contains migrations and console assets.

```shell
docker build --pull --tag durable-workflow-engine:0.1.0 .
docker inspect durable-workflow-engine:0.1.0 \
  --format '{{.Config.User}} {{json .Config.Healthcheck}}'
```

The runtime image contains the example workflow only as a deployment smoke
target. Build a derived image containing your own versioned workflow module
and override the worker's `--definitions` and `--queue` values. Keep every
pinned workflow version available until no running execution refers to it —
see [versioning](../explanation/replay-and-determinism.md#versioning).

## Stand up the reference Compose topology

`compose.production.yaml` is a hardened single-host reference, not a
high-availability database architecture. It keeps PostgreSQL off the host
network, exposes the API on `127.0.0.1` by default, uses read-only non-root
engine containers, and reads credentials through Compose secrets.

1. Create the secret files with mode `0600`, outside source control:

   ```text
   secrets/postgres_password  # one high-entropy database password
   secrets/database_url       # postgresql://durable:PASSWORD@postgres:5432/durable
   secrets/api_keys           # key-id:role:sha256-digest[,more entries]
   ```

   See [provision and rotate API keys](provision-and-rotate-api-keys.md) for
   how to generate the `api_keys` entries.

2. Validate and start:

   ```shell
   docker compose -f compose.production.yaml config --quiet
   docker compose -f compose.production.yaml up --build -d
   curl --fail http://127.0.0.1:8000/api/health/ready
   ```

3. Terminate TLS and shared rate limits at a trusted ingress. Set
   `DWE_BIND_ADDRESS` only when the ingress cannot use the loopback binding.

For a business-critical deployment, replace the Compose PostgreSQL service
with a managed or supervised PostgreSQL 17 cluster with synchronous
durability, automated encrypted backups, point-in-time recovery, connection
limits, and tested failover. The engine requires one PostgreSQL consistency
boundary per workflow; it does not provide multi-region active/active
operation.

## Size the connection pool

Startup fails on missing authentication, missing database credentials,
invalid numeric values, or a pool maximum below its minimum — see the
[configuration reference](../reference/configuration.md) for every variable
and default. Size the aggregate pool maximum across **all** API and worker
replicas below the database's reserved application connection budget. Run
migrations as a controlled release step when your platform supports jobs;
startup migration is advisory-lock protected, but that should not replace
deployment change control.

## Roll out and roll back

1. Back up and verify the database before a migration-bearing release — see
   [back up and restore](back-up-and-restore.md).
2. Run `engine replay-check` on representative open executions against the
   new workflow definitions.
3. Deploy workers that support both old and new pinned versions.
4. Apply forward-only migrations, then roll API and workers gradually.
5. Watch readiness, worker heartbeats, dead tasks, queue growth, and 5xx
   rates — see [monitor and operate](monitor-and-operate.md).

Database migrations are forward-only. Application rollback is safe only while
the old binary understands every applied migration and pinned definition.
Otherwise restore into an isolated database and perform a planned recovery
(see [back up and restore](back-up-and-restore.md)) rather than attempting an
ad hoc schema downgrade.
