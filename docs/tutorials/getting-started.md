# Getting started

This tutorial takes you from nothing to a durable workflow that has survived
a signal, a retried activity, a timer, and a parallel join — all inspectable
in the console. You will run three processes and interact with them from a
fourth terminal. Follow it top to bottom the first time; every command is
copy-pasteable.

You need Python 3.12, [uv](https://docs.astral.sh/uv/), Docker, and Docker
Compose.

## 1. Install dependencies and start PostgreSQL

```shell
uv sync --python 3.12 --all-groups
docker compose up -d postgres
```

## 2. Configure your shell

Every terminal you open for this tutorial needs the same database URL. Export
it now, and again in each new terminal:

```shell
export DATABASE_URL=postgresql://durable:durable@localhost:5432/durable
export DWE_AUTH_MODE=disabled  # isolated local development only — never in production
```

`DWE_AUTH_MODE=disabled` skips bearer-token authentication so you can reach
the API immediately. Leave it unset once you move past this tutorial; the
[API key how-to](../how-to/provision-and-rotate-api-keys.md) covers real
authentication.

## 3. Start a worker

Open a **second terminal**, export the same `DATABASE_URL`, and run:

```shell
uv run engine worker \
  --definitions examples.order_workflow:registry \
  --queue orders
```

This single command applies pending database migrations, registers the
example's immutable workflow and activity definitions, and starts polling the
`orders` queue for work. Leave it running — this is the process that will
actually execute your workflow's activities and timers.

## 4. Start the API and console

Open a **third terminal**, export `DATABASE_URL` and `DWE_AUTH_MODE` again,
and run:

```shell
uv run uvicorn engine.api.app:app --reload
```

This serves both the JSON API and the workflow console (a static single-page
app) from the same process. Leave it running too.

## 5. Start the example workflow

Open a **fourth terminal**, export `DATABASE_URL`, and start an execution of
the bundled `order-fulfillment` workflow:

```shell
uv run engine start order-fulfillment \
  --version 1 \
  --queue orders \
  --input '{"order_id":"demo-1"}'
```

The command prints the new execution's ID as JSON. This workflow (source at
[`examples/order_workflow.py`](../../examples/order_workflow.py)) runs a
retried charge activity, starts a durable timer, dispatches two activities in
parallel, and then waits for a signal before finishing.

## 6. Watch it run in the console

Open [http://127.0.0.1:8000](http://127.0.0.1:8000) in a browser. You should
see `demo-1`'s execution in the list. Select it and look at the **history**
tab — you'll see the charge activity's attempts, the timer, and the two
parallel activities, each as a discrete recorded event. The workflow is now
parked, waiting for a signal named `delivery_confirmed`.

## 7. Send the signal that unblocks it

From the console, use the signal control to send:

```json
{"received": true}
```

to the `delivery_confirmed` signal. Within a couple of seconds the worker
picks the signal up, the workflow resumes from exactly where it left off, and
its status moves to completed. Watch the history tab update live.

## 8. Confirm the workflow would replay identically

Everything you just watched happen is reconstructed, not remembered — the
worker never kept the workflow's Python stack in memory between steps. You
can prove this independently, without mutating anything, using the same
execution ID the `start` command printed:

```shell
uv run engine replay-check WORKFLOW_ID \
  --against-version 1 \
  --definition examples.order_workflow:order_fulfillment
```

A `"compatible": true` result means replaying the committed history against
this exact code produces the same commands that were actually recorded. This
is the same check you'd run before deploying new code against workflows that
are still open — see the
[replay and determinism explanation](../explanation/replay-and-determinism.md)
for why that guarantee holds.

## What you've done

You ran all three worker roles and the API from one queue, started a workflow
with retries/timers/parallel activities/a signal wait, watched it durably
resume after a real wait, and independently verified its replay. From here:

- Write your own workflow: see the [SDK reference](../reference/sdk-api.md).
- Understand *why* replay works this way: see
  [Replay and determinism](../explanation/replay-and-determinism.md).
- Take this to production: start with
  [Deploy to production](../how-to/deploy-to-production.md).
