# Benchmark analysis

See [benchmark results](../reference/benchmark-results.md) for the numbers
this analysis is based on, and
[run benchmarks](../how-to/run-benchmarks.md) to reproduce them. The
benchmark runner exercises replay and PostgreSQL transitions separately so
CPU replay cost is not confused with queue and durability cost.

## Replay is linear in history length

Throughput stayed near 237k–291k events/s, while absolute replay time grew
to 422 ms at 100,000 events. Because replay time is proportional to history
length rather than to work done, a workflow that runs long enough will
eventually make every replay slow — regardless of how little state it
actually carries. This is the reason continue-as-new exists as a
first-class operation (see
[data and history lifecycle](data-and-history-lifecycle.md)) rather than
being purely an optimization: past some point it's the only way to keep
replay bounded. A larger design would add snapshots on top of it.

## Queue depth is the first PostgreSQL bottleneck

Lease-selection p50 rose from 1.06 ms at 1,000 pending tasks to 8.55 ms at
10,000; p99 rose from 1.27 ms to 11.07 ms. This is the practical performance
knee observed in the current schema, not a hard failure point — no
correctness or capacity failure occurred in the tested range, so the actual
failure point is reported as `null` rather than inferred.

Appending a signal remained below 1 ms even with 100,000 history rows,
because the execution row stores `next_seq` directly, avoiding a history
scan to find the next sequence number. Concurrent pollers increased
aggregate throughput through 16 pollers, so this run did not reach the
lock-contention failure point either — the queue-index scan, not lock
contention, is the bottleneck at this scale.

## Where a larger design would go next

Partition task indexes and execution/history ownership by queue or workflow
ID first. That reduces hot-index and row-lock contention while preserving
the invariant that one execution's transition and its follow-up task commit
atomically on the same owner (see
[transaction and lease invariants](replay-and-determinism.md#transaction-and-lease-invariants)) —
sharding the index doesn't require giving up that atomicity, because the
invariant is scoped to one execution at a time.
