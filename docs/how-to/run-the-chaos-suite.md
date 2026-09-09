# Run the chaos suite

The chaos profile starts six workflows with sequential effects, parallel
work, a timer, and an offline signal, then kills real worker processes with
`SIGKILL` at specific points and checks that nothing is lost. See
[chaos methodology](../explanation/chaos-methodology.md) for what the profile
actually proves and why those particular failure windows were chosen.

Run it against a dedicated PostgreSQL database — it creates and mutates real
rows, so never point it at a database anything else depends on:

```shell
export DWE_TEST_DATABASE_URL=postgresql://durable:durable@localhost:5432/durable_test
uv run pytest -m chaos -v
```

A short terminal recording of a passing run is checked in; replay it with:

```shell
asciinema play docs/chaos-demo.cast
```

The suite passes only when every workflow reaches its expected terminal
result, stale completions are rejected, the cooperating effect ledger has one
row per logical key, every history has contiguous sequence numbers, and every
terminal history independently replays against its pinned definition. Run
this before every release (see [release a version](release-a-version.md)).
