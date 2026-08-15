# Durable Workflow Engine

[![CI](https://github.com/santinomarial/Durable-Workflow-Engine/actions/workflows/ci.yml/badge.svg)](https://github.com/santinomarial/Durable-Workflow-Engine/actions/workflows/ci.yml)

A compact Python/PostgreSQL engine that reconstructs durable workflows from an
append-only event history and safely resumes them after process death.

This repository implements the mechanisms behind durable execution rather than
claiming to replace Temporal. The centerpiece is a
[real-`SIGKILL` chaos profile](docs/chaos.md); its short terminal recording can
be replayed with `asciinema play docs/chaos-demo.cast`.

## Guarantees and non-guarantees

The engine guarantees, within one PostgreSQL instance:

- ordered, append-only history per workflow;
- atomic history, task, and execution-state transitions;
- deterministic replay with command fingerprints and recorded marker values;
- exclusive temporary leases and token-fenced completion;
- at-least-once activities with a stable idempotency key across attempts;
- durable timers, signals, retries, timeouts, cancellation requests, and joins;
- pinned immutable workflow versions and non-mutating compatibility checks; and
- no accepted transition after completion, failure, or termination.

It does not guarantee exactly-once arbitrary side effects. An activity can
perform an external effect and die before recording completion. Exactly-once
behavior exists only when that external boundary atomically deduplicates the
engine-provided idempotency key. It also does not provide multi-region storage,
sharding, authentication, multi-tenancy, child workflows, cron, search
attributes, or automatic migration of running workflow code.

The central guarantee is:

> For every accepted workflow transition, its history events and runnable tasks
> commit atomically, and replaying the committed history produces the same next
> commands.

## Quickstart

Requirements are Python 3.12, [uv](https://docs.astral.sh/uv/), Docker, and
Docker Compose.

```shell
uv sync --python 3.12 --all-groups
docker compose up -d postgres
export DATABASE_URL=postgresql://durable:durable@localhost:5432/durable
export DWE_AUTH_MODE=disabled  # isolated local development only
```

Run all three worker roles. The worker applies migrations and registers the
example's immutable definitions before polling:

```shell
uv run engine worker \
  --definitions examples.order_workflow:registry \
  --queue orders
```

In a second terminal, start the API and embedded observability UI:

```shell
export DATABASE_URL=postgresql://durable:durable@localhost:5432/durable
export DWE_AUTH_MODE=disabled  # isolated local development only
uv run uvicorn engine.api.app:app --reload
```

In a third terminal, start the representative workflow:

```shell
export DATABASE_URL=postgresql://durable:durable@localhost:5432/durable
uv run engine start order-fulfillment \
  --version 1 \
  --queue orders \
  --input '{"order_id":"demo-1"}'
```

Open [http://127.0.0.1:8000](http://127.0.0.1:8000), select the execution, and
send a `delivery_confirmed` signal such as `{"received":true}`. The example
executes a retried charge, a durable timer, two parallel activities, a signal
wait, and recorded deterministic values. Its source is
[`examples/order_workflow.py`](examples/order_workflow.py).

The UI provides status filtering, an execution summary, chronological history,
activity attempts and retry timing, a command/entity graph, signal and
cancellation controls, and termination. OpenAPI documentation is at `/docs`.
Production authentication is fail closed and uses hashed bearer-key
configuration with viewer, operator, and administrator roles. See the
[security guide](docs/security.md) before exposing the control plane.

## Replay model

Workflow workers never restore a serialized Python stack. They run the pinned
function from the beginning and resolve each SDK call from committed history.

```mermaid
flowchart LR
    A["Lease workflow task"] --> B["Load pinned definition + history"]
    B --> C["Replay from function entry"]
    C --> D{"Next command in history?"}
    D -->|"completed"| E["Return recorded result and continue"]
    D -->|"scheduled"| F["Suspend until completion"]
    D -->|"missing"| G["Emit deterministic command"]
    D -->|"type or fingerprint differs"| H["NonDeterminismError"]
    G --> I["Atomic history + task commit"]
    C -->|"function returns"| J["Atomic terminal commit"]
```

Activity, timer, and marker calls receive ordinal command IDs. Scheduled events
store the type, canonical argument fingerprint, and logical entity identity.
Signals and attempt events are indexed separately because they can interleave
with commands. `ctx.gather` assigns children in source order and returns that
order regardless of completion order. `ctx.now()`, `ctx.random()`, and
`ctx.uuid()` commit `MarkerRecorded` values and reuse them on every replay.

`engine replay-check` runs this model without writes and reports the first
divergent command or terminal value:

```shell
uv run engine replay-check WORKFLOW_ID \
  --against-version 1 \
  --definition examples.order_workflow:order_fulfillment
```

## Transaction and lease invariants

A workflow transition validates the workflow-task token, locks the execution
row, allocates consecutive sequence numbers from `next_seq`, appends every new
event, inserts each resulting activity/timer/workflow task, and completes the
leased task in one transaction. No event/task pair can be observed halfway.

Activity execution occurs outside a transaction. Leasing an activity and
appending `ActivityStarted` are atomic. Completion then validates the same live
token, locks the execution, appends the terminal attempt event, completes the
attempt, and either inserts a retry or wakes replay. A stale token matches no
row, so late and duplicate workers cannot commit.

An expired workflow lease is returned to `pending` without a history event
because no workflow fact occurred. An expired activity lease is different: it
commits `ActivityTimedOut`, closes that attempt, records the chosen jittered
retry time, and creates a new task ID and token. Heartbeats may extend the lease
and heartbeat deadline but never the start-to-close deadline.

Signals use a caller-provided unique ID. Signal ingestion, timer firing, and
workflow replay all lock the same execution row before allocating history
sequence numbers. Therefore a signal-timeout race is permanently decided by
commit order; replay compares those recorded sequence numbers. A signal that
wins before the timeout is needed also records `TimerCanceled`.

## Activity delivery and idempotency

Activities are at least once. Every logical activity has a deterministic
`entity_id`; all retry tasks receive that value as `current_activity_context().idempotency_key`.
Attempts have independent task IDs and lease tokens. The bundled effect ledger
demonstrates the cooperating-boundary pattern with a unique key, but it is test
evidence rather than a claim about arbitrary APIs.

Cancellation is cooperative. The API commits one
`WorkflowCancellationRequested`, prevents new activity/timer work, wakes replay,
and exposes the request through `ctx.cancellation_requested`. Activity
heartbeats raise `ActivityCancellationRequested`. Termination and cancellation
fence future completion, but neither can reverse an effect already in progress.

## Versioning

Every execution pins `(workflow_type, definition_version)`. Definitions are
immutable in both the runtime registry and PostgreSQL; registering different
code at the same identity fails. Workers advertise their supported versions and
release unsupported tasks without changing history. Old code must remain
deployed until no open execution references it. `replay-check` detects
incompatibility but does not migrate histories.

## Chaos methodology and result

The chaos profile starts six workflows with sequential effects, parallel work,
a timer, and an offline signal. A seeded schedule launches subprocess workers,
then:

- sends real `SIGKILL` to workflow workers after leasing;
- commits each idempotent effect and `SIGKILL`s the activity worker before its
  completion transition;
- forces original leases to expire and submits a stale completion;
- delivers duplicate offline signals and restarts the connection pool; and
- drives recovery, independently replays every terminal history, checks
  contiguous sequence numbers, and verifies one ledger row per logical key.

The current profile passes all six workflows. Its exact supported claim is that
under those injected worker and connection failure windows, no committed
workflow transition is lost, stale completion is rejected, and the cooperating
ledger observes one effect per key. See [the methodology](docs/chaos.md) and run:

```shell
export DWE_TEST_DATABASE_URL=postgresql://durable:durable@localhost:5432/durable
uv run pytest -m chaos -v
```

## Benchmarks and bottlenecks

The published full profile ran on an Apple M4/16 GB MacBook Pro with PostgreSQL
15.17 (`shared_buffers=128MB`, synchronous commit enabled). Selected results:

| Measurement | Full-profile result |
| --- | ---: |
| Replay, 100,000 events | 422 ms / 237k events/s |
| Workflow starts | 7,759/s |
| Activity dispatch | 0.646 ms p50 / 1.589 ms p99 |
| Timer delay after deadline | 0.965 ms p50 / 1.382 ms p99 |
| Workflow/activity death recovery | 0.762 ms / 1.783 ms |
| Dispatch at depth 1,000 | 1.27 ms p99 |
| Dispatch at depth 10,000 | 11.07 ms p99 |

The first practical knee is PostgreSQL task-index scanning at 10,000 pending
tasks. No hard failure occurred in the tested range, so the raw failure point is
`null`; 10,000 is the observed latency breakpoint, not a universal capacity
limit. Replay cost is linear and reaches 422 ms at 100,000 events. A larger
design would first add snapshots/continuation and partition task indexes plus
execution/history ownership by queue or workflow ID.

Full metadata, workloads, sample sizes, warmup policy, limitations, raw JSON,
and reproduction commands are in [the benchmark report](docs/benchmarks.md).

## Verification

GitHub Actions runs formatting, lint, strict typing, unit tests, PostgreSQL
integration tests, and the `SIGKILL` chaos profile against PostgreSQL 17.
The runtime provides JSON request/worker logs, database-visible worker
heartbeats, separate liveness/readiness probes, and authenticated Prometheus
metrics; deployment guidance is in [production operations](docs/operations.md).
Locally:

```shell
uv run ruff format --check .
uv run ruff check .
uv run mypy
export DWE_TEST_DATABASE_URL=postgresql://durable:durable@localhost:5432/durable
uv run pytest
```

## Known limitations

- Histories are loaded in full; snapshots, pagination, continue-as-new, and
  archival are not implemented.
- PostgreSQL is a single coordination boundary; there is no sharding,
  multi-region failover, or independent message broker.
- The API/UI provide hashed bearer-key authentication, role authorization,
  request limits, and immutable mutation audits, but not tenant isolation. TLS
  termination and shared multi-replica rate limiting belong at the ingress.
- Workflow sandboxing is documented but not enforced by bytecode or process
  isolation; authors must keep workflow code deterministic and put I/O in
  activities.
- Cancellation cannot forcibly stop or undo external work.
- There are no child workflows, cron schedules, search attributes, DLQ tooling,
  cross-language SDKs, or automatic workflow-code migrations.
- No software license has been selected.

The detailed design rationale and milestone acceptance criteria remain in the
[implementation blueprint](docs/implementation-plan.md).
