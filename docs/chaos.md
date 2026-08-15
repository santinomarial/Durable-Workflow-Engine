# Chaos correctness profile

The chaos profile targets correctness at explicit failure windows rather than
claiming that failures simply did not crash the process.

A seeded random schedule launches disposable subprocess workers. Workflow
workers receive `SIGKILL` after leasing but before replay, and for every workflow
an activity worker commits an idempotent external effect and receives `SIGKILL`
before completion is recorded. The harness then expires those leases while
retaining the original tokens and lets maintenance recover them. It also sends
duplicate signals while workers are offline, restarts the connection pool,
executes parallel activities and a durable timer, and submits a completion from
the stale worker.

The profile succeeds only when:

- every workflow reaches its expected terminal result;
- stale completion is rejected;
- the cooperating effect ledger contains one row per logical activity key;
- every history has contiguous sequence numbers; and
- every terminal history independently replays against its pinned definition.

Run it with a dedicated PostgreSQL database:

```shell
DWE_TEST_DATABASE_URL=postgresql://durable:durable@localhost:5432/durable_test \
  uv run pytest -m chaos -v
```

The precise claim supported by this profile is: under injected worker and
connection failure windows, the engine loses no committed workflow transition,
rejects stale completions, and a cooperating idempotency boundary observes one
effect per logical key. It does not claim arbitrary activity side effects are
exactly once.
