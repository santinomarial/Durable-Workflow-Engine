# Python SDK reference

## Defining workflows and activities

```python
@workflow(version=1, name="account")
async def account(ctx: WorkflowContext, state: JSONValue) -> JSONValue: ...


@activity(name="charge")
async def charge(ctx: ActivityContext, amount: int) -> JSONValue: ...
```

Definitions are identified by `(name, version)` and fingerprinted by a
SHA-256 hash of their source; see
[versioning](../explanation/replay-and-determinism.md#versioning).

## `WorkflowContext` methods

| Method | Effect |
| --- | --- |
| `ctx.activity(fn, *args)` | schedule an activity call, resolved from history on replay |
| `ctx.gather(*calls)` | run calls concurrently; children keep source order in the result regardless of completion order |
| `ctx.sleep(seconds)` | durable timer |
| `ctx.wait_signal(name)` | suspend until a caller-sent signal with that name arrives |
| `ctx.wait_update(name)` | suspend until a caller-sent, result-bearing update arrives |
| `ctx.resolve_update(update, result, accepted=bool)` | resolve a pending update with a caller-visible result |
| `ctx.child_workflow(fn, input, parent_close_policy=...)` | start a deterministic child execution; see [child workflows](../explanation/child-workflows-and-updates.md) |
| `ctx.continue_as_new(fn, state)` | atomically close this run and start a linked fresh execution (top-level workflows only) |
| `ctx.cancellation_requested` | `True` once `WorkflowCancellationRequested` has been recorded |
| `ctx.now()` / `ctx.random()` / `ctx.uuid()` | deterministic values recorded once and replayed thereafter |

## Result-bearing updates

```python
@workflow(version=1, name="account")
async def account(ctx: WorkflowContext, state: JSONValue) -> JSONValue:
    update = await ctx.wait_update("set-limit")
    accepted = isinstance(update.payload, dict) and isinstance(update.payload.get("limit"), int)
    ctx.resolve_update(
        update,
        {"limit": update.payload.get("limit")} if accepted else {"error": "limit required"},
        accepted=accepted,
    )
    return update.payload
```

The caller chooses an idempotency ID and can poll for `pending`, `completed`,
or `rejected`. Closing a workflow rejects unresolved updates rather than
leaving callers waiting forever.

## Continue-as-new

```python
@workflow(version=2, name="account")
async def account_v2(ctx: WorkflowContext, state: JSONValue) -> JSONValue:
    if isinstance(state, dict) and state.get("events", 0) >= 10_000:
        ctx.continue_as_new(account_v2, {**state, "events": 0})
    return state
```

Records `WorkflowExecutionContinuedAsNew`, closes the old execution, and
creates the new execution plus its first task in one transaction. It
inherits search attributes and schedule identity, and exposes
`continued_from`/`continued_to` plus `GET /api/workflows/{id}/continuation-chain`.
The target must keep the workflow type and already be registered. Child
workflows cannot yet continue as new — a parent's join identity is bound to
the original child execution.

## `DurableClient` / `WorkflowHandle`

```python
client = DurableClient(pool)
handle: WorkflowHandle[dict[str, object]] = await client.start(
    account,
    {"limit": 10},
    queue_name="accounts",
)
update = await handle.update("set-limit", {"limit": 20}, update_id="request-42")
update_result = await update.result(timeout=10)
workflow_result = await handle.result(timeout=30)
```

| Call | Effect |
| --- | --- |
| `client.start(fn, input, queue_name=...)` | start an execution, return a typed `WorkflowHandle` |
| `handle.result(timeout=...)` | wait for and return the terminal result |
| `handle.update(name, payload, update_id=...)` | send a result-bearing update, return an `UpdateHandle` |
| `update.result(timeout=...)` | wait for and return the update's resolution |
| `handle.snapshot()` | typed execution metadata plus ordered history |
| `handle.query(projection)` | side-effect-free application projection over durable data; does not execute workflow code or mutate history |
