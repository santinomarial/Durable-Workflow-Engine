# Durable Workflow Engine

A minimal durable-execution engine built with Python, asyncio, and PostgreSQL.

The project reconstructs workflow state by deterministically replaying an
append-only event history. Its goal is to demonstrate durable execution's core
correctness properties—including atomic transitions, lease fencing, at-least-once
activities, durable timers and signals, deterministic concurrency, and
workflow-version safety—without claiming to be a production replacement for
Temporal.

Development is organized around the milestones and invariants in the
[implementation blueprint](docs/implementation-plan.md).

## Status

Initial design and repository setup. Implementation has not started yet.

## Planned stack

- Python 3.12 and asyncio
- PostgreSQL as the durability and coordination boundary
- Transactional task dispatch with `FOR UPDATE SKIP LOCKED`
- Deterministic workflow replay from append-only history

## Core guarantee

For every accepted workflow transition, the resulting history events and
runnable tasks are committed atomically, and replaying the committed history
produces the same next commands.

## License

No license has been selected yet.
