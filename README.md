<!-- markdownlint-disable MD013 MD033 MD041 -->

<div align="center">
  <h1>Durable Workflow Engine</h1>
  <p><strong>Deterministic, crash-resilient workflow orchestration built from first principles.</strong></p>
  <p>
    A compact Python and PostgreSQL engine that rebuilds workflow state from an
    append-only history and safely resumes execution after process death.
  </p>
  <p>
    <a href="https://github.com/santinomarial/Durable-Workflow-Engine/actions/workflows/ci.yml"><img alt="CI" src="https://img.shields.io/github/actions/workflow/status/santinomarial/Durable-Workflow-Engine/ci.yml?branch=main&label=CI&style=flat-square&color=111111"></a>
    <a href="https://github.com/santinomarial/Durable-Workflow-Engine/actions/workflows/security.yml"><img alt="Security" src="https://img.shields.io/github/actions/workflow/status/santinomarial/Durable-Workflow-Engine/security.yml?branch=main&label=security&style=flat-square&color=111111"></a>
    <img alt="Python 3.12" src="https://img.shields.io/badge/Python-3.12-111111?style=flat-square&logo=python&logoColor=white">
    <img alt="PostgreSQL" src="https://img.shields.io/badge/PostgreSQL-15%20%7C%2017-111111?style=flat-square&logo=postgresql&logoColor=white">
    <img alt="FastAPI" src="https://img.shields.io/badge/FastAPI-control%20plane-111111?style=flat-square&logo=fastapi&logoColor=white">
  </p>
  <p>
    <a href="#one-command-demo">Demo</a> ·
    <a href="#architecture">Architecture</a> ·
    <a href="#programming-model">SDK</a> ·
    <a href="#measured-performance">Benchmarks</a> ·
    <a href="#correctness-under-failure">Chaos testing</a> ·
    <a href="docs/index.md">Documentation</a>
  </p>
</div>

<p align="center">
  <img src="docs/assets/operations-console.png" alt="Durable Workflow Engine operations console showing seeded running, completed, paused, and failed workflows" width="100%">
</p>
<p align="center"><sub>The actual operations console, populated by the one-command local demo.</sub></p>

---

## Why this exists

Ordinary background jobs lose their in-memory call stack when a process dies.
This engine treats the database history as the source of truth instead. A
workflow function is re-executed from the beginning, and every durable SDK call
is resolved against previously committed events until execution reaches new
work.

That model provides:

- **Deterministic replay:** command fingerprints detect incompatible workflow
  code instead of silently corrupting an execution.
- **Atomic transitions:** history events, runnable tasks, and execution state
  commit in one PostgreSQL transaction.
- **Crash recovery:** expiring leases and token-fenced completion let another
  worker safely continue after a hard process failure.
- **Durable orchestration:** activities, timers, signals, retries, timeouts,
  cancellation, child workflows, updates, schedules, and continue-as-new.
- **Operational evidence:** real `SIGKILL` tests, reproducible benchmarks,
  structured telemetry, a control plane, and a production-oriented container.

This is an implementation of the core mechanisms behind durable execution. It
does not claim to replace Temporal or provide a globally distributed control
plane.

## One-command demo

With Docker Desktop running:

```shell
docker compose -f compose.demo.yaml up --build -d
```

Open **[http://127.0.0.1:8001](http://127.0.0.1:8001)**.

The isolated demo starts PostgreSQL, the API, and all worker roles, then seeds
four workflows that are ready to inspect:

| Scenario | What it demonstrates |
| --- | --- |
| Running order | A workflow blocked on an external signal |
| Completed order | Activities, a durable timer, parallel work, and final output |
| Paused order | Frozen deadlines and safe operator recovery |
| Failed payment review | Failure history, diagnostics, and retry-as-new |

The demo binds only to `127.0.0.1` and intentionally disables authentication.
It is not a deployment topology. Follow the
[guided demo](docs/tutorials/one-command-demo.md), or stop it with:

```shell
docker compose -f compose.demo.yaml down
# Add --volumes to reset the seeded data.
```

## Architecture

PostgreSQL is both the durable state store and coordination boundary. The API
never owns workflow state, and workers can disappear without taking execution
state with them.

```mermaid
flowchart LR
    subgraph Interfaces["Interfaces"]
        direction TB
        Console["Operations console"]
        APIClient["HTTP / OpenAPI clients"]
        SDK["Embedded Python SDK"]
        CLI["CLI"]
    end

    subgraph Control["Control plane"]
        API["FastAPI API<br/>RBAC · rate limits · mutation audit"]
    end

    subgraph Runtime["Worker runtime"]
        direction TB
        Workflow["Workflow worker<br/>replay + command emission"]
        Activity["Activity worker<br/>side effects + heartbeats"]
        Maintenance["Maintenance worker<br/>timers + leases + schedules"]
    end

    Store[("PostgreSQL<br/>histories · tasks · leases<br/>definitions · schedules")]
    Replay["Deterministic replay engine"]
    External["External systems<br/>payments · email · APIs"]

    Console --> API
    APIClient --> API
    API --> Store
    SDK --> Store
    CLI --> Store
    Store <--> Workflow
    Store <--> Activity
    Store <--> Maintenance
    Workflow --> Replay
    Activity --> External
```

### Durable execution, step by step

```mermaid
sequenceDiagram
    autonumber
    participant Control as API / SDK
    participant DB as PostgreSQL
    participant Workflow as Workflow worker
    participant Activity as Activity worker
    participant External as External system

    Control->>DB: Start workflow in one transaction
    Note over Control,DB: execution + WorkflowStarted + workflow task

    Workflow->>DB: Lease workflow task with token
    DB-->>Workflow: Token + committed history
    Note right of Workflow: Replay pinned definition from function entry
    Workflow->>DB: Commit ActivityScheduled + activity task atomically

    Activity->>DB: Lease activity attempt with token
    Activity->>External: Perform effect with stable idempotency key
    External-->>Activity: Result
    Activity->>DB: Commit ActivityCompleted and wake workflow

    Workflow->>DB: Lease next workflow task
    DB-->>Workflow: Token + expanded history
    Note right of Workflow: Replay resolves recorded result and continues
```

The engine does not serialize Python stacks. It persists decisions and their
results, then reconstructs control flow through replay. Calls such as
`ctx.now()`, `ctx.random()`, and `ctx.uuid()` record marker values once and
reuse them on every later replay.

## Programming model

Workflows look like ordinary asynchronous Python. Durable SDK calls are the
boundary between deterministic orchestration and side-effecting activities.

```python
@workflow(version=1, name="order-fulfillment")
async def order_fulfillment(ctx: WorkflowContext, order: JSONValue) -> JSONValue:
    charge = await ctx.activity(
        charge_card,
        order,
        retry=RetryPolicy(max_attempts=5, initial_interval=timedelta(seconds=1)),
        start_to_close=timedelta(seconds=30),
    )

    await ctx.sleep(timedelta(seconds=1))

    inventory, courier = await ctx.gather(
        ctx.activity(reserve_inventory, order),
        ctx.activity(book_courier, order),
    )

    confirmation = await ctx.wait_signal("delivery_confirmed")
    return {
        "charge": charge,
        "inventory": inventory,
        "courier": courier,
        "confirmation": confirmation,
        "completed_at": ctx.now().isoformat(),
        "confirmation_id": str(ctx.uuid()),
    }
```

The complete example is in
[`examples/order_workflow.py`](examples/order_workflow.py). Typed workflow
handles support start, describe, snapshot queries, signals, cancellation,
termination, updates, and result waiting. See the
[Python SDK reference](docs/reference/sdk-api.md).

## Capability map

| Area | Implemented |
| --- | --- |
| Workflow semantics | Deterministic replay, immutable version pinning, command fingerprints, recorded time/random/UUID markers, parallel joins |
| Durable primitives | Activities, timers, signals, retries with persisted jitter, start-to-close and schedule-to-start timeouts, cancellation, termination |
| Advanced orchestration | Child workflows, close policies, result-bearing updates, timezone-aware cron schedules, backfills, pause/resume, retry-as-new, continue-as-new |
| Concurrency safety | Row locking, monotonic history sequence allocation, temporary leases, opaque lease tokens, stale-completion rejection |
| Visibility | Cursor-paginated histories, indexed JSON search attributes, activity attempts, command graph, replay debugger, dead-letter inspection |
| Operations | Worker heartbeats, liveness/readiness probes, Prometheus metrics, structured JSON logs, backup/restore scripts, incident runbook |
| Security | Hashed bearer keys, viewer/operator/administrator RBAC, request limits, immutable mutation audit, CodeQL, dependency and container scanning |
| Delivery | Non-root multi-stage image, local and hardened Compose topologies, SBOM generation, release checksums, provenance attestations |

## Correctness model

The central invariant is:

> For every accepted workflow transition, its history events and runnable tasks
> commit atomically, and replaying the committed history produces the same next
> commands.

Each transition validates a lease token, locks the execution row, allocates
consecutive history sequence numbers, appends events, creates follow-up tasks,
and closes the leased task in one transaction. A stale or duplicate worker
cannot satisfy the token predicate, so it cannot commit late work.

### Recovery after process death

```mermaid
flowchart LR
    Lease["Worker leases task<br/>token A"] --> Execute["Execute outside<br/>the transaction"]
    Execute --> Crash["Process receives<br/>SIGKILL"]
    Crash --> Expire["Lease deadline passes"]
    Expire --> Recover["Maintenance recovers task<br/>or records attempt timeout"]
    Recover --> Reassign["New worker leases task<br/>token B"]
    Reassign --> Replay["Replay committed history"]
    Replay --> Commit["Commit with token B"]
    Late["Late completion<br/>using token A"] --> Reject["Rejected<br/>token mismatch"]
```

- Expired workflow tasks return to `pending` because no new workflow fact was
  committed.
- Expired activity attempts commit `ActivityTimedOut` and the chosen retry
  time before a replacement attempt is created.
- Heartbeats can extend the lease and heartbeat deadline, but never the
  activity's start-to-close deadline.
- Signal and timeout races are decided by database commit order and remain
  deterministic on replay.

### Delivery and idempotency

Activities are **at least once**. Every logical activity receives a stable
idempotency key across attempts, while each attempt gets a fresh task ID and
lease token. Exactly-once external effects are possible only when the external
boundary atomically deduplicates that key. The included effect ledger
demonstrates this cooperating-boundary pattern.

## Correctness under failure

The chaos profile runs six workflows and deliberately:

1. sends real `SIGKILL` to workflow workers after they lease work;
2. kills activity workers after an idempotent effect but before completion is
   recorded;
3. expires original leases and submits a stale completion;
4. delivers duplicate offline signals;
5. restarts the PostgreSQL connection pool; and
6. independently replays every terminal history after recovery.

The profile verifies contiguous sequence numbers, terminal replay equivalence,
stale-token rejection, and one effect-ledger row per logical activity key. The
current scenario passes all six workflows.

```shell
docker compose exec -T postgres createdb -U durable durable_test
export DWE_TEST_DATABASE_URL=postgresql://durable:durable@localhost:5432/durable_test
uv run pytest -m chaos -v
```

Read the exact failure schedule and supported claims in the
[chaos methodology](docs/explanation/chaos-methodology.md).

## Measured performance

Published results come from the full benchmark profile on an Apple M4 MacBook
Pro with 16 GB of memory, Python 3.12.13, and PostgreSQL 15.17 with synchronous
commit enabled.

| Measurement | Result |
| --- | ---: |
| Replay, 100,000 events | **422 ms** / **237k events/s** |
| Workflow starts | **7,759/s** |
| 16-poller lease throughput | **9,119 leases/s** |
| Workflow dispatch | **0.699 ms p50** / **1.281 ms p99** |
| Activity dispatch | **0.646 ms p50** / **1.589 ms p99** |
| Timer delay after deadline | **0.965 ms p50** / **1.382 ms p99** |
| Workflow/activity death recovery | **0.762 ms** / **1.783 ms** |
| Dispatch at 10,000 pending tasks | **11.07 ms p99** |

These are reproducible local-system measurements, not universal capacity
claims. The first observed performance knee is task-index scanning at 10,000
pending tasks. No correctness or capacity failure occurred within the tested
range. Replay cost is linear because a run currently loads its full history.

- [Complete benchmark results](docs/reference/benchmark-results.md)
- [Bottleneck analysis](docs/explanation/benchmark-analysis.md)
- [Reproduction guide](docs/how-to/run-benchmarks.md)

## Production posture

The repository includes a hardened single-host reference topology rather than
pretending that Docker Compose is a multi-region platform.

| Concern | Repository support |
| --- | --- |
| Authentication | Fail-closed bearer-key configuration with SHA-256 digests |
| Authorization | Viewer, operator, and administrator roles |
| Auditability | Immutable records for control-plane mutations |
| Observability | Prometheus metrics, JSON logs, worker heartbeats, live/readiness probes |
| Container security | Non-root UID 10001, minimal runtime image, weekly vulnerability scans |
| Supply chain | Locked dependencies, dependency review, CodeQL, SBOMs, checksums, build provenance |
| Data safety | Backup and restore scripts, migration checksums, documented restore drills |
| Operations | Graceful shutdown, dead-letter recovery, incident response and release runbooks |

Start with:

- [Deploy to production](docs/how-to/deploy-to-production.md)
- [Provision and rotate API keys](docs/how-to/provision-and-rotate-api-keys.md)
- [Monitor and operate](docs/how-to/monitor-and-operate.md)
- [Back up and restore](docs/how-to/back-up-and-restore.md)
- [Respond to an incident](docs/how-to/respond-to-an-incident.md)
- [Security policy](SECURITY.md)

## Run from source

Requirements: Python 3.12, [uv](https://docs.astral.sh/uv/), Docker, and Docker
Compose.

```shell
uv sync --python 3.12 --all-groups
docker compose up -d postgres
export DATABASE_URL=postgresql://durable:durable@localhost:5432/durable
export DWE_AUTH_MODE=disabled  # isolated local development only
```

Start the worker:

```shell
uv run engine worker \
  --definitions examples.order_workflow:registry \
  --queue orders
```

Start the API and console in another terminal:

```shell
uv run uvicorn engine.api.app:app --reload
```

Start an order in a third terminal:

```shell
uv run engine start order-fulfillment \
  --version 1 \
  --queue orders \
  --input '{"order_id":"demo-1"}'
```

Open [http://127.0.0.1:8000](http://127.0.0.1:8000), select the execution, and
send the `delivery_confirmed` signal. For a narrated path, use the
[getting-started tutorial](docs/tutorials/getting-started.md).

## Verification

The automated suite contains 89 unit, PostgreSQL integration, concurrency, and
failure-injection tests. CI also enforces formatting, linting, strict typing,
package builds, JavaScript and shell syntax, Compose validation, dependency
auditing, CodeQL, secret and configuration scans, and production-image
vulnerability scans.

```shell
docker compose exec -T postgres createdb -U durable durable_test
export DWE_TEST_DATABASE_URL=postgresql://durable:durable@localhost:5432/durable_test

uv run ruff format --check .
uv run ruff check .
uv run mypy
uv run bandit --quiet --recursive engine
uv run scripts/security-audit.sh
node --check ui/app.js
sh -n scripts/*.sh
docker compose -f compose.production.yaml config --quiet
uv run pytest
```

Tagged releases build the wheel and source distribution, generate a CycloneDX
SBOM and SHA-256 checksums, and publish GitHub build-provenance attestations.

## Documentation

The documentation follows the Diátaxis model:

| If you want to... | Start here |
| --- | --- |
| Run a guided first workflow | [Getting started](docs/tutorials/getting-started.md) |
| Explore the seeded console | [One-command demo](docs/tutorials/one-command-demo.md) |
| Understand deterministic replay | [Replay and determinism](docs/explanation/replay-and-determinism.md) |
| Use the SDK or CLI | [Python SDK](docs/reference/sdk-api.md) · [CLI](docs/reference/cli.md) |
| Configure the engine | [Configuration reference](docs/reference/configuration.md) |
| Understand API roles | [API and roles](docs/reference/api-and-roles.md) |
| Operate the service | [Metrics and logs](docs/reference/metrics-and-logs.md) |
| Review design decisions | [Implementation blueprint](docs/design/implementation-plan.md) |

Browse the complete [documentation map](docs/index.md).

## Repository map

```text
engine/
├── api/           FastAPI control plane, RBAC, rate limits, audit
├── persistence/   PostgreSQL transitions, leasing, timers, schedules
├── runtime/       Event history, deterministic replay, debugger
├── sdk/           Workflow context, decorators, retry policies
└── workers/       Workflow, activity, and maintenance workers

migrations/        15 ordered, checksummed schema migrations
tests/             Unit, integration, concurrency, and SIGKILL suites
benchmarks/        Reproducible replay and PostgreSQL performance harness
ui/                Embedded dependency-free operations console
docs/              Tutorials, how-to guides, reference, and explanation
```

## Scope and limitations

The engine makes deliberately narrow claims:

- One PostgreSQL instance is the coordination boundary. There is no horizontal
  sharding or multi-region failover.
- Replay loads one run's history in full. Continue-as-new bounds individual
  runs, but snapshots and automated history archival are not implemented.
- Continue-as-new is limited to top-level workflows.
- Control-plane authentication is not tenant isolation. TLS termination and
  shared multi-replica rate limiting belong at the ingress.
- Workflow determinism is documented and checked during replay, but workflow
  code is not isolated in a bytecode or process sandbox.
- Cancellation is cooperative and cannot undo an external effect already in
  progress.
- There are no cross-language SDKs or automatic migrations for running
  workflow code.
- No software license has been selected.

For the detailed contract, read
[guarantees and limitations](docs/explanation/guarantees-and-limitations.md).
