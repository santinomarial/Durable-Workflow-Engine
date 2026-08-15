# Durable Workflow Engine

[![CI](https://github.com/santinomarial/Durable-Workflow-Engine/actions/workflows/ci.yml/badge.svg)](https://github.com/santinomarial/Durable-Workflow-Engine/actions/workflows/ci.yml)

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

## Development

Install the Python 3.12 environment and development tools with
[uv](https://docs.astral.sh/uv/):

```shell
uv sync --python 3.12 --all-groups
```

Run the repository checks:

```shell
uv run ruff format --check .
uv run ruff check .
uv run mypy
uv run pytest
```

Start PostgreSQL and apply the schema:

```shell
docker compose up -d postgres
export DATABASE_URL=postgresql://durable:durable@localhost:5432/durable
uv run python -m engine.persistence.migrations
```

Start the API and observability UI:

```shell
uv run uvicorn engine.api.app:app --reload
```

Open `http://127.0.0.1:8000` to list executions, inspect event histories and
activity attempts, view the command/entity graph, send signals, and terminate
running workflows. Interactive API documentation is available at `/docs`.

## Core guarantee

For every accepted workflow transition, the resulting history events and
runnable tasks are committed atomically, and replaying the committed history
produces the same next commands.

## License

No license has been selected yet.
