# Try the populated local demo

This is the fastest way to inspect the engine without opening several
terminals or setting up Python locally. You need Docker Desktop or a working
Docker daemon.

From the repository root, run:

```shell
docker compose -f compose.demo.yaml up --build -d
```

Open [http://127.0.0.1:8001](http://127.0.0.1:8001). The isolated demo starts
PostgreSQL, an API, a worker, and a one-shot seed process. It adds four
repeatable scenarios: an order waiting for a signal, a completed order, a
paused order, and a deliberately declined payment. The worker processes the
completed and declined scenarios in the background, so give it a few seconds
and press **Refresh** if the statuses are still changing.

Try selecting the paused order and resuming it, or select the waiting order
and send a `delivery_confirmed` signal with `{"received": true}`. Explore
**History**, **Graph**, and **Debugger** for the persisted causal record.

The seed is idempotent: running `up` again does not duplicate those four
executions. Docker stores this demo's data in a separate `dwe-demo` volume,
and the database has no host port. The UI is bound to `127.0.0.1:8001` so it
will not collide with the default local API on port 8000. To change the demo
port, set `DWE_DEMO_PORT` before the command.

Check status or stop the demo without deleting its data:

```shell
docker compose -f compose.demo.yaml ps
docker compose -f compose.demo.yaml down
```

**Local use only:** the demo intentionally disables API authentication and
uses a disposable database password. Never expose it to the network or use
this Compose file as a production deployment. Use the
[production deployment guide](../how-to/deploy-to-production.md) instead.
