# Contributing

Use Python 3.12 and `uv sync --python 3.12 --all-groups`. Keep changes focused,
add evidence at the same correctness boundary, and never rewrite an applied
migration. New migrations use the next zero-padded version and are forward-only.

Before opening a pull request:

```shell
uv run ruff format --check .
uv run ruff check .
uv run mypy
uv run bandit --quiet --recursive engine
uv run scripts/security-audit.sh
node --check ui/app.js
docker compose -f compose.production.yaml config --quiet
uv run pytest -m "not integration and not chaos"
```

Run the PostgreSQL integration and chaos profiles for persistence, lease,
worker, serialization, replay, timer, signal, or cancellation changes. Include
the database version and environment in performance claims. Do not commit
credentials, production payloads, database dumps, or unredacted logs.

Durable compatibility is the primary review constraint: existing event history
must replay under its pinned definition, stale workers must remain fenced, and
accepted events plus resulting tasks must commit atomically.
