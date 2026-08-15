# Benchmarks

The runner reports hardware, Python, and PostgreSQL metadata alongside every
measurement. The quick profile is intended for development; the full profile
increases workload and sample counts.

```shell
uv run python -m benchmarks.run --profile quick
DATABASE_URL=postgresql://durable:durable@localhost:5432/durable \
  uv run python -m benchmarks.run --profile full --output benchmarks/results/full.json
```

Measurements cover exact replay histories of 10, 1,000, and 100,000 events,
workflow starts, dispatch latency, timer-fire delay, expired-lease recovery,
append cost as history grows, pending-depth degradation, and concurrent poller
contention. Results are local-system evidence, not universal capacity claims.

Use an empty, dedicated database: the runner applies migrations and deliberately
leaves benchmark rows behind. See `docs/benchmarks.md` for the published run,
sample sizes, bottleneck analysis, and limitations.
