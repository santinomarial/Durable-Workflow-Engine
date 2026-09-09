# Run benchmarks

Use a dedicated, empty database — migrations and benchmark rows are created
in the target database:

```shell
uv run python -m benchmarks.run --profile quick
uv run python -m benchmarks.run \
  --profile full \
  --database-url postgresql://durable:durable@localhost:5432/durable_bench \
  --output benchmarks/results/full.json
```

The `quick` (replay-only) profile needs no database. The `full` profile
increases workflow starts and activity dispatches by 10x, timers by 10x,
replay samples to five, and pending task depth to 10,000 — run it on
deployment-class hardware, not a laptop, before using its numbers to size a
real deployment.

Preserve the generated JSON when comparing revisions. For the published
numbers and what they mean, see
[benchmark results](../reference/benchmark-results.md) and
[benchmark analysis](../explanation/benchmark-analysis.md).
