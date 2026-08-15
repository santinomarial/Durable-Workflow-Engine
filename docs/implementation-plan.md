# Durable workflow engine — implementation blueprint

A correct, minimal durable-execution engine: workflow code is reconstructed from an append-only event history, so execution can continue after worker crashes and restarts.

This is not a Temporal competitor. It is an end-to-end implementation of the mechanisms behind durable execution, with explicit failure semantics and evidence that the invariants hold.

---

## What v1 must prove

- A worker can die at any instruction boundary and another worker can safely continue the workflow.
- Workflow state is reconstructed through deterministic replay rather than serialized process memory.
- Activities execute at least once; idempotency keys make exactly-once effects possible when the effect boundary supports them.
- Timers and signals survive a complete engine shutdown.
- Multiple activities can run concurrently and join deterministically.
- Expired leases are reclaimed, while completions from stale workers are rejected.
- History events, task creation, and task completion cannot be partially committed.
- Incompatible workflow-code changes are detected before they corrupt an execution.

## Explicitly out of scope

- Multi-tenancy, authentication, and RBAC
- Cross-language SDKs
- Multi-region or sharded storage
- Child workflows and continue-as-new
- Cron scheduling
- Search attributes and advanced visibility indexing
- Arbitrary code migration for running workflows

---

## Core invariants

These are the project. Features are secondary.

1. **History is the source of truth.** Workflow state can always be reconstructed from the committed history.
2. **History is append-only and ordered per workflow.** Every event has a unique, monotonically increasing sequence number.
3. **Transitions are atomic.** Completing the current task, appending its resulting events, and creating subsequent tasks happen in one database transaction.
4. **Replay is deterministic.** Given the same workflow definition and history, the runtime emits the same sequence of commands.
5. **Only a current lease may commit.** Every task completion is conditional on its current lease token. Late or duplicate completions are rejected.
6. **Activities are at least once.** The engine never claims arbitrary external side effects are exactly once.
7. **A terminal execution never becomes runnable again.** No task may transition a completed, failed, or terminated workflow.

The central invariant:

> For every accepted workflow transition, the resulting history events and runnable tasks are committed atomically, and replaying the committed history produces the same next commands.

---

## Locked technical decisions

| Decision | Choice | Reason |
| --- | --- | --- |
| Host language | Python 3.12 + asyncio | Fastest route to an ergonomic replayable `async` workflow API |
| Storage | PostgreSQL | Transactional history append, leasing, and task creation without a DB/queue dual write |
| Dispatch | `FOR UPDATE SKIP LOCKED` | Safe concurrent polling across workers |
| Execution model | Event history + deterministic replay | Reconstructs workflow state without serializing arbitrary application memory |
| Activity delivery | At least once | A worker can perform an external effect and die before acknowledging it |
| Effect deduplication | Stable idempotency key per logical activity | Exactly-once effects are enforced at a cooperating effect boundary |
| Workflow versioning | Pin executions to immutable definition versions | Running histories always replay against compatible code |

Do not introduce Kafka, Redis, or another coordinator in v1. PostgreSQL is intentionally the single coordination and durability boundary.

---

## Architecture

### Components

- **API:** starts workflows, queries state/history, accepts signals, and requests termination.
- **Workflow worker:** leases a workflow task, replays history, and emits deterministic commands.
- **Activity worker:** leases and executes activity tasks, heartbeats, and records completion or failure.
- **Timer/timeout poller:** fires due timers, reclaims expired leases, and records timeouts.
- **PostgreSQL:** stores executions, history, tasks, definitions, and optional idempotency-test ledger.
- **Web UI:** reads through the API and sends signals; it never mutates engine tables directly.

### The two execution loops

**Workflow transition**

1. Lease a workflow task.
2. Load its pinned workflow definition and complete history.
3. Replay the workflow from the beginning.
4. Resolve commands already represented in history.
5. Stop at the first unresolved command set or terminal result.
6. In one transaction: validate the lease, lock the execution, append events, insert resulting tasks, and complete the workflow task.

**Activity transition**

1. Lease an activity task.
2. Execute it outside the database transaction.
3. In one short transaction: validate the lease token, lock the execution, append the result event, complete the activity task, and enqueue a workflow task.
4. If the token is stale, discard the completion. The activity may already have produced an external effect, which is why it receives a stable idempotency key.

---

## Data model

The exact DDL can evolve, but these identities and constraints should not.

```sql
create type workflow_status as enum (
  'running', 'completed', 'failed', 'terminated'
);

create type task_status as enum (
  'pending', 'leased', 'completed', 'dead'
);

create type task_type as enum (
  'workflow', 'activity', 'timer'
);

create table workflow_executions (
  id                  uuid primary key,
  workflow_type       text not null,
  definition_version  int not null,
  input               jsonb not null,
  status              workflow_status not null,
  result              jsonb,
  failure             jsonb,
  next_seq            bigint not null default 1,
  created_at          timestamptz not null default now(),
  closed_at           timestamptz,
  unique (id, workflow_type, definition_version)
);

create table history_events (
  workflow_id   uuid not null references workflow_executions(id),
  seq           bigint not null,
  event_type    text not null,
  command_id    bigint,
  entity_id     uuid,
  external_id   text,
  attributes    jsonb not null,
  created_at    timestamptz not null default now(),
  primary key (workflow_id, seq)
);

create unique index one_scheduled_command
  on history_events (workflow_id, command_id)
  where command_id is not null
    and event_type in ('ActivityScheduled', 'TimerStarted');

create unique index one_external_event
  on history_events (workflow_id, external_id)
  where external_id is not null;

create table tasks (
  id                         uuid primary key,
  workflow_id                uuid not null references workflow_executions(id),
  task_type                  task_type not null,
  queue_name                 text not null default 'default',
  entity_id                  uuid,
  command_id                 bigint,
  attempt                    int not null default 1,
  input                      jsonb,
  status                     task_status not null default 'pending',
  visible_at                 timestamptz not null default now(),
  schedule_to_start_deadline timestamptz,
  start_to_close_deadline    timestamptz,
  leased_at                  timestamptz,
  lease_token                uuid,
  lease_expires_at           timestamptz,
  heartbeat_at               timestamptz,
  heartbeat_details          jsonb,
  created_at                 timestamptz not null default now(),
  completed_at               timestamptz
);

create index runnable_tasks
  on tasks (queue_name, task_type, visible_at)
  where status = 'pending';

create index expired_leases
  on tasks (lease_expires_at)
  where status = 'leased';
```

`entity_id` identifies a logical activity or timer across attempts. `task.id` identifies one concrete attempt. `command_id` identifies the deterministic SDK call that created the entity.

### Sequence allocation

Before appending events, lock the execution row:

```sql
select status, next_seq
from workflow_executions
where id = $1
for update;
```

Allocate consecutive sequence numbers, append every event, then advance `next_seq` in the same transaction. This serializes workflow transitions from activity completions, signals, timers, and workflow workers.

### Task leasing

```sql
with candidate as (
  select id
  from tasks
  where status = 'pending'
    and task_type = $1
    and queue_name = $2
    and visible_at <= now()
  order by visible_at, created_at
  for update skip locked
  limit 1
)
update tasks t
set status = 'leased',
    leased_at = now(),
    lease_token = gen_random_uuid(),
    lease_expires_at = now() + $3::interval,
    start_to_close_deadline = case
      when t.task_type = 'activity' then now() + $4::interval
      else null
    end
from candidate
where t.id = candidate.id
returning t.*;
```

Leasing is not exactly-once execution. It establishes temporary ownership. Every heartbeat and completion must use:

```sql
where id = $task_id
  and status = 'leased'
  and lease_token = $lease_token
```

For an activity, accepting the lease and appending `ActivityStarted` form one short transition: select the task, lock its execution row, install the lease, append the started event with the attempt number, and commit before running user code. Workflow-task leasing does not need a history event.

### Expired-lease recovery

A maintenance transaction selects expired leased tasks with `FOR UPDATE SKIP LOCKED` and either:

- returns workflow tasks to `pending`, or
- records an activity timeout/failure and creates a fresh retry attempt with a new task ID.

It must not silently change `leased` to `pending` for an activity, because timeout and retry attempts must remain visible in history.

---

## Atomic transition boundaries

### Starting a workflow

One transaction creates:

1. `workflow_executions`
2. `WorkflowExecutionStarted`
3. Initial workflow task

### Scheduling an activity or timer

One transaction:

1. Validates the current workflow-task lease
2. Locks the execution row
3. Appends `ActivityScheduled` or `TimerStarted`
4. Inserts the corresponding task
5. Completes the current workflow task

The event and task must never exist independently.

### Completing an activity

One transaction:

1. Validates the activity lease token
2. Locks the execution and verifies it is still running
3. Appends `ActivityCompleted` or `ActivityFailed`
4. Completes the activity attempt
5. Creates the retry attempt or wakes the workflow

### Firing a timer

One transaction validates and completes the timer task, appends `TimerFired`, and enqueues a workflow task.

### Receiving a signal

The API accepts a caller-provided `signal_id`, stored as `external_id`. One transaction locks the execution, deduplicates the signal ID through the unique index, appends `SignalReceived`, and enqueues a workflow task. A signal remains durable even when no workers are running.

Duplicate workflow wake-up tasks are acceptable initially because replay is idempotent. They can be coalesced later as an optimization, never as a correctness requirement.

---

## Event taxonomy

Start with a small set, but include enough identity to reconstruct attempts and commands.

```text
WorkflowExecutionStarted
WorkflowExecutionCompleted
WorkflowExecutionFailed
WorkflowExecutionTerminated

ActivityScheduled
ActivityStarted
ActivityCompleted
ActivityFailed
ActivityTimedOut

TimerStarted
TimerFired
TimerCanceled

SignalReceived
```

Each activity-related event includes `entity_id` and `attempt`. Scheduled commands also include `command_id` and a canonical argument fingerprint.

Events describe durable facts. An event and the database state that makes it true are committed together.

---

## SDK surface

```python
@workflow(version=1)
async def order_fulfillment(ctx, order):
    charge = await ctx.activity(
        charge_card,
        order,
        retry=RetryPolicy(
            max_attempts=5,
            initial_interval=timedelta(seconds=1),
        ),
        start_to_close=timedelta(seconds=30),
    )

    await ctx.sleep(timedelta(hours=24))

    inventory, courier = await ctx.gather(
        ctx.activity(reserve_inventory, order),
        ctx.activity(book_courier, order),
    )

    confirmation = await ctx.wait_signal(
        "delivery_confirmed",
        timeout=timedelta(days=14),
    )

    return {
        "charge": charge,
        "inventory": inventory,
        "courier": courier,
        "confirmation": confirmation,
    }
```

Workflow code may not directly use wall-clock time, randomness, UUID generation, network I/O, filesystem I/O, mutable globals, or nondeterministic iteration. Provide deterministic equivalents through `ctx`.

For v1, `ctx.now()`, `ctx.random()`, and `ctx.uuid()` should derive their values deterministically from recorded workflow state or emit marker events. Do not merely call the underlying nondeterministic functions during replay.

---

## Replay mechanics

Every replayable SDK call receives a deterministic `command_id`, initially its ordinal position in the execution path. The corresponding scheduled event stores:

- command type
- canonicalized arguments or their stable fingerprint
- command ID
- logical entity ID

During replay:

1. Run the pinned workflow definition from the beginning.
2. For each SDK command, find the history entry with the same `command_id`.
3. If type or canonical arguments differ, raise `NonDeterminismError`.
4. If a completion exists, resolve the awaitable with its recorded result and continue.
5. If it is scheduled but unresolved, suspend without scheduling it again.
6. If it does not exist, emit it as new work.
7. If the workflow returns, atomically append `WorkflowExecutionCompleted` and store the result.

Do not match against the next raw history event. Signals, retries, heartbeats, and activity-attempt events can interleave with workflow commands.

### Parallel commands

`ctx.gather` freezes child commands in source order, assigns their command IDs deterministically, and schedules all missing children in one transition. Completion order may vary, but replay returns results in the original gather order.

### Signal consumption

Signal events are external history entries, not workflow commands. `wait_signal(name)` deterministically consumes the earliest unconsumed matching signal. A timeout races through a durable timer; whichever event obtains the execution lock and commits first determines the recorded outcome.

---

## Workflow-code versioning

Replay checking detects incompatibility; it does not solve it.

For v1:

- Every execution stores `definition_version` at creation.
- Registered workflow definitions are immutable by `(workflow_type, definition_version)`.
- Workers advertise which definition versions they support.
- A worker never replay-runs a history using an unpinned version.
- Old versions remain deployed until no open execution references them.

Provide:

```text
engine replay-check <workflow-id> --against-version <version>
```

This reports the first divergent command without mutating history. Explicit patch/version-marker APIs can be added after the pinned-version model works.

---

## Retries, timeouts, and cancellation

### Retries

- Store the retry policy in `ActivityScheduled` so replay does not depend on current defaults.
- Give all attempts the same logical `entity_id` and idempotency key.
- Give each attempt a new task ID and lease token.
- Compute backoff from recorded attempt state.
- Use full jitter, but record the chosen next `visible_at` so recovery never redraws randomness.
- After the final failure, wake the workflow with the recorded failure. A DLQ may retain the final task for inspection, but it must not become a second source of truth.

### Timeouts

- `schedule_to_start`: checked before an activity is leased.
- `start_to_close`: measured from the accepted lease/start event.
- Heartbeats extend only a separately configured heartbeat timeout, not `start_to_close`.
- Timing out an activity cannot stop an external side effect already in progress.
- A timeout invalidates the old lease; late completion is rejected.

### Cancellation

Treat cancellation as cooperative in v1. Record the request, stop scheduling new work, and expose cancellation to heartbeat-enabled activities. Do not claim the engine can forcibly undo an external operation.

---

## Implementation sequence

The sequence is dependency-based, not calendar-based. Keep every milestone demoable and tested.

### Milestone 1 — persistent sequential replay

Implement Postgres schema, workflow registration/version pinning, workflow start, sequential activities, history replay, and non-determinism detection.

**Complete when:** killing the workflow worker after any committed transition and restarting it produces the correct result without creating a duplicate scheduled command.

### Milestone 2 — atomic transitions and multi-worker leasing

Implement transaction helpers, sequence locking, task leasing, heartbeats, lease renewal, expired workflow-task reclamation, and stale-token rejection.

**Complete when:** multiple workers process a shared queue while randomized worker deaths produce no missing history/task pairs, and no stale worker can commit.

### Milestone 3 — activity failure semantics

Implement attempts, retries, recorded jitter, schedule-to-start timeout, start-to-close timeout, heartbeat timeout, final failure, and inspection/DLQ state.

**Complete when:** an activity that fails several times and then succeeds produces one downstream workflow result, while every attempt and delay is reconstructible from history.

### Milestone 4 — durable timers and signals

Implement timer tasks, signal ingestion/deduplication, signal waits, signal-versus-timeout races, and clock injection for tests.

**Complete when:** the entire engine can remain offline beyond a timer deadline, then restart and resume correctly; signals sent while offline are consumed exactly once by workflow logic.

### Milestone 5 — deterministic concurrency

Implement `ctx.gather`, deterministic child ordering, fan-out, and join.

**Complete when:** activities finish in randomized orders across repeated runs but replay always returns results in source order and never reschedules completed work.

### Milestone 6 — version safety

Implement version-aware routing, immutable definition registration, unknown-version failure behavior, and `replay-check`.

**Complete when:** an incompatible new definition is rejected with the first divergent command while the pinned old definition continues the workflow successfully.

### Milestone 7 — observability UI

Build a deliberately small UI containing:

- workflow list and status filter
- execution summary
- chronological event history
- activity attempts and retry timing
- signal-send control
- simple execution graph derived from command/entity relationships

**Complete when:** someone can explain why a workflow is waiting, retrying, failed, or completed without reading database rows or logs.

### Milestone 8 — chaos and measurement

Build the failure harness, correctness ledger, benchmark runner, CI profiles, and documentation.

**Complete when:** the repository publishes reproducible correctness and performance evidence, including its failure point.

---

## Test strategy

### Deterministic replay tests

- Replay the same history repeatedly and compare emitted commands byte-for-byte.
- Change activity arguments, ordering, branching, and command types; verify the first divergence is reported.
- Fuzz activity completion and signal arrival order.

### Transaction failure-window tests

Inject a process exit before and after every database statement in each transition. After restart, assert that the state is always one of the valid sides of the transaction—never a partial transition.

### Lease tests

- Worker dies before activity execution.
- Worker dies after the external effect but before completion commit.
- Lease expires while the original worker is alive.
- Old and new workers both submit completion.
- Heartbeat races with timeout.

Exactly one current token may commit, while the idempotency ledger deduplicates repeated effects.

### History property tests

For every execution:

- sequence numbers are unique and contiguous
- every scheduled entity has one logical identity
- every terminal entity has a prior schedule event
- a terminal workflow has no accepted later events
- replaying a terminal history returns the stored result or failure

---

## Chaos harness

The centerpiece should test correctness, not merely survival.

1. Launch a configurable number of workflows containing sequential activities, parallel activities, a timer, and a signal wait.
2. Randomly `SIGKILL` workflow and activity workers.
3. Interrupt database connections and restart the worker pool.
4. Force leases to expire while some original workers remain alive.
5. Deliver duplicate signals and duplicate completion requests.
6. Record external effects in a ledger table with a unique idempotency key.
7. Wait for every workflow to reach its expected terminal state.
8. Replay every history independently.
9. Assert no committed history transition is malformed and every ledger effect appears exactly once.

The claim is precise:

> Under injected worker and connection failures, the engine loses no committed workflow transitions; stale completions are rejected, and the cooperating test ledger observes one effect per idempotency key.

Do not generalize this into “all activities execute exactly once.”

---

## Benchmarks to publish

- Workflow starts per second
- Activity dispatch latency: p50, p95, p99
- Timer-fire delay after `visible_at`: p50, p95, p99
- Recovery time after workflow-worker and activity-worker death
- History replay throughput at 10, 1,000, and 100,000 events
- Cost of appending an event as history size grows
- Maximum sustained pending-task depth before dispatch p99 degrades
- Contention as workers increase against one PostgreSQL instance

For every result, publish hardware, PostgreSQL configuration, workload, sample size, warm-up procedure, and the first observed bottleneck. Report where the design breaks.

---

## Repository structure

```text
durable-engine/
  engine/
    api/
    runtime/
      commands.py
      replay.py
      definitions.py
    persistence/
      models.py
      transitions.py
      leasing.py
    workers/
      workflow_worker.py
      activity_worker.py
      maintenance_worker.py
    sdk/
      context.py
      decorators.py
      policies.py
  migrations/
  examples/
  tests/
    replay/
    transitions/
    leases/
    integration/
    chaos/
  benchmarks/
  ui/
  docker-compose.yml
  README.md
```

Keep SQL transition logic concentrated in `persistence/transitions.py`; correctness becomes difficult to audit if task completion and history appending are scattered across workers.

---

## README structure

1. One-sentence description and a short chaos-demo recording
2. Exact guarantees and non-guarantees
3. Quickstart
4. Replay model with one diagram
5. Transaction and lease invariants
6. Activity delivery and idempotency semantics
7. Versioning model
8. Chaos methodology and results
9. Benchmarks and bottlenecks
10. Known limitations

---

## Questions the implementation must answer cold

1. What exact database transaction turns one workflow task into an activity task?
2. What happens if a worker performs an external effect and dies before recording completion?
3. How does an expired lease differ from a failed activity attempt?
4. Why can a stale lease token not commit?
5. How are signal arrival and workflow replay serialized?
6. How does a signal-versus-timeout race become deterministic after replay?
7. Why is matching commands to the next raw history event incorrect?
8. What happens when incompatible workflow code is deployed while executions are running?
9. Which part of exactly-once effects belongs to the engine, and which belongs to the external system?
10. Where does PostgreSQL become the bottleneck, and what would be partitioned first in a larger design?

If the repository, tests, and benchmarks answer these concretely, the project has succeeded.
