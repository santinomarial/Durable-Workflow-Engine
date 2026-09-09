# CLI reference

The `engine` command is installed by the package (`pyproject.toml` console
script `engine.cli:main`). Every subcommand except `auth-key` accepts
`--database-url`, which defaults to the `DATABASE_URL` environment variable
(or `DATABASE_URL_FILE`) and is required if neither is set.

## `engine auth-key`

Generate a bearer token and its SHA-256 configuration entry. Does not touch
the database.

| Flag | Required | Values |
| --- | --- | --- |
| `--key-id` | yes | any string of letters, numbers, hyphens, underscores |
| `--role` | yes | `viewer`, `operator`, `admin` |

Prints `{"key_id", "role", "token", "configuration"}` as JSON. `configuration`
is the `key-id:role:sha256-digest` triple to place in `DWE_API_KEYS`.

## `engine replay-check WORKFLOW_ID`

Replays a persisted history against candidate workflow code without writing
anything, and reports the first divergent command or terminal value.

| Flag | Required | Notes |
| --- | --- | --- |
| `--against-version` | yes | must equal the loaded definition's version |
| `--definition` | yes | `module:attribute` reference to a `WorkflowDefinition` |
| `--database-url` | no* | see above |

Exit code `0` when compatible, `1` otherwise. Prints a JSON replay report.

## `engine register`

Persists immutable workflow/activity definitions from a registry into
PostgreSQL, applying pending migrations first.

| Flag | Required | Notes |
| --- | --- | --- |
| `--definitions` | yes | `module:attribute` reference to a `DefinitionRegistry` |
| `--database-url` | no* | see above |

## `engine start WORKFLOW_TYPE`

Starts a registered workflow execution.

| Flag | Required | Default | Notes |
| --- | --- | --- | --- |
| `--version` | yes | — | integer definition version |
| `--input` | no | `null` | JSON workflow input |
| `--queue` | no | `default` | queue name |
| `--database-url` | no* | — | see above |

## `engine worker`

Applies migrations, registers definitions, then runs continuous worker
loops until `SIGINT`/`SIGTERM`.

| Flag | Required | Default | Notes |
| --- | --- | --- | --- |
| `--definitions` | yes | — | `module:attribute` reference to a `DefinitionRegistry` |
| `--queue` | no | `default` | queue name |
| `--role` | no | all three | repeatable: `workflow`, `activity`, `maintenance` |
| `--poll-interval` | no | `0.05` (seconds) | idle polling delay |
| `--heartbeat-interval` | no | `10.0` (seconds) | worker heartbeat write interval |
| `--database-url` | no* | — | see above |

\* Required only if `DATABASE_URL`/`DATABASE_URL_FILE` is not set.
