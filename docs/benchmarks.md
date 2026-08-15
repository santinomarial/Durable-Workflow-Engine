# Benchmark evidence

The benchmark runner exercises replay and PostgreSQL transitions separately so
CPU replay cost is not confused with queue and durability cost. Every JSON
result includes its runtime and database metadata. These numbers describe one
local run; they are evidence about this implementation, not production capacity
claims.

## Published quick-profile run

The checked-in result was captured on 2026-08-15 on a MacBook Pro (Mac16,1),
Apple M4 with 10 cores and 16 GB memory, running macOS 26.6.1, Python 3.12.13,
and PostgreSQL 15.17. PostgreSQL used `shared_buffers=128MB`,
`max_connections=100`, `synchronous_commit=on`, and `wal_level=replica`.

The quick profile uses one excluded warmup plus two measured samples for each
replay size, 100 workflow starts and dispatches, 50 activity dispatches, 20
timers, 100 dispatch samples at each pending depth, and 20 lease calls per
concurrent poller. It reported:

| Measurement | Result |
| --- | ---: |
| Replay, 10 events | 0.038 ms median |
| Replay, 1,000 events | 4.02 ms median |
| Replay, 100,000 events | 423 ms median |
| Workflow starts | 3,678/s |
| Workflow dispatch call | 0.332 ms p50 / 0.542 ms p99 |
| Activity dispatch call | 0.445 ms p50 / 0.498 ms p99 |
| Timer fire delay after its deadline | 0.952 ms p50 / 1.025 ms p99 |
| Expired workflow lease recovery | 0.724 ms |
| 16-poller lease throughput | 9,595 leases/s |

No correctness or capacity failure occurred within the quick profile. The
largest pending depth tested was 1,000, so the first failure point is explicitly
reported as `null`, not inferred. Run the full profile on deployment-class
hardware before making sizing decisions.

## Bottleneck interpretation

Replay is linear in history length: throughput stayed near 236k–262k events/s,
while absolute replay time grew to 423 ms at 100,000 events. The design therefore
needs history pagination, snapshots, or continuation-as-new before very long
executions become routine.

Queue depth was the first visible PostgreSQL degradation. Lease-selection p50
rose from 0.424 ms at 100 pending tasks to 1.013 ms at 1,000; p99 rose from
0.632 ms to 1.503 ms. Appending a signal remained below 1 ms even with 100,000
history rows because the execution row stores `next_seq`, avoiding a history
scan. Concurrent pollers increased aggregate throughput through 16 pollers, so
this run did not reach the lock-contention failure point.

In a larger design, partition task indexes and execution/history ownership by
queue or workflow ID first. That reduces hot-index and row-lock contention while
preserving the invariant that one execution's transition and its follow-up task
commit atomically on the same owner.

## Reproduce

Use a dedicated empty database because migrations and benchmark rows are
created in the target:

```shell
uv run python -m benchmarks.run --profile quick
uv run python -m benchmarks.run \
  --profile full \
  --database-url postgresql://durable:durable@localhost:5432/durable_bench \
  --output benchmarks/results/full.json
```

The replay-only form needs no database. The full profile increases starts and
activity dispatches by 10x, timers by 10x, replay samples to five, and pending
depth to 10,000. Preserve the generated JSON when comparing revisions.
