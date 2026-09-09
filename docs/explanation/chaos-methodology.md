# Chaos methodology

The chaos profile targets correctness at explicit failure windows, rather
than claiming that failures simply did not crash the process — "it didn't
crash" is a much weaker claim than "no committed transition was lost."

## What it injects

A seeded random schedule launches disposable subprocess workers. Workflow
workers receive `SIGKILL` after leasing but before replay, and for every
workflow an activity worker commits an idempotent external effect and
receives `SIGKILL` before completion is recorded — these are the two points
where a naive implementation would either lose a transition or double-apply
an effect. The harness then expires those leases while retaining the
original tokens and lets maintenance recover them, which is what exercises
stale-token rejection rather than assuming it. It also sends duplicate
signals while workers are offline, restarts the connection pool, executes
parallel activities and a durable timer, and submits a completion from the
stale worker.

## What "pass" means

The profile succeeds only when:

- every workflow reaches its expected terminal result;
- stale completion is rejected;
- the cooperating effect ledger contains one row per logical activity key;
- every history has contiguous sequence numbers; and
- every terminal history independently replays against its pinned
  definition.

See [run the chaos suite](../how-to/run-the-chaos-suite.md) to execute it.

## The precise claim

Under those injected worker and connection failure windows, the engine loses
no committed workflow transition, rejects stale completions, and a
cooperating idempotency boundary observes one effect per logical key. It
does **not** claim arbitrary activity side effects are exactly once — that
claim depends on the external system's own deduplication, not on this
engine (see
[activity delivery and idempotency](replay-and-determinism.md#activity-delivery-and-idempotency)).
