# Benchmark results

These numbers describe one local run; they are evidence about this
implementation, not production capacity claims. See
[benchmark analysis](../explanation/benchmark-analysis.md) for what they
imply and [run benchmarks](../how-to/run-benchmarks.md) to reproduce them.

## Published full-profile run

Captured 2026-08-15 on a MacBook Pro (Mac16,1), Apple M4, 10 cores, 16 GB
memory, macOS 26.6.1, Python 3.12.13, PostgreSQL 15.17
(`shared_buffers=128MB`, `max_connections=100`, `synchronous_commit=on`,
`wal_level=replica`).

Sampling: one excluded warmup plus five measured samples for each replay
size; 1,000 workflow starts and dispatches; 500 activity dispatches; 200
timers; 100 dispatch samples at each pending depth; 20 lease calls per
concurrent poller. PostgreSQL measurements have no excluded warmup — append
and each recovery path are single measured transitions.

| Measurement | Result |
| --- | ---: |
| Replay, 10 events | 0.034 ms median |
| Replay, 1,000 events | 4.07 ms median |
| Replay, 100,000 events | 422 ms median |
| Workflow starts | 7,759/s |
| Workflow dispatch call | 0.699 ms p50 / 1.281 ms p99 |
| Activity dispatch call | 0.646 ms p50 / 1.589 ms p99 |
| Timer fire delay after its deadline | 0.965 ms p50 / 1.382 ms p99 |
| Expired workflow lease recovery | 0.762 ms |
| Expired activity lease recovery and retry lease | 1.783 ms |
| 16-poller lease throughput | 9,119 leases/s |
| Dispatch p99 at depth 1,000 pending | 1.27 ms |
| Dispatch p99 at depth 10,000 pending | 11.07 ms |

No correctness or capacity failure occurred within the full profile. The
largest pending depth tested was 10,000, so the first hard failure point is
explicitly reported as `null`, not inferred.

Full metadata, workloads, sample sizes, warmup policy, limitations, and raw
JSON live in the repository's `benchmarks/results/` output.
