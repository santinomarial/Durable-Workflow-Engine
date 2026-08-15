# Python SDK and workflow handles

Workflow code can receive result-bearing updates in addition to one-way signals:

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

Update receipt and resolution are replayed as separate deterministic events. The
caller chooses an idempotency ID, can poll the API for `pending`, `completed`, or
`rejected`, and receives the workflow-produced result or failure. Closing a
workflow rejects unresolved updates rather than leaving callers waiting forever.

Embedded applications can use typed handles without manually composing
persistence calls:

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

`handle.snapshot()` returns typed execution metadata plus ordered history.
`handle.query(projection)` runs a side-effect-free application projection over
that durable data. Queries do not execute workflow code or mutate history, so
they cannot compromise deterministic replay.

Long-lived top-level workflows can rotate into a fresh, pinned run without a
race between closing and restarting:

```python
@workflow(version=2, name="account")
async def account_v2(ctx: WorkflowContext, state: JSONValue) -> JSONValue:
    if isinstance(state, dict) and state.get("events", 0) >= 10_000:
        ctx.continue_as_new(account_v2, {**state, "events": 0})
    return state
```

The engine records `WorkflowExecutionContinuedAsNew`, closes the old execution,
creates the new execution and its first task in one transaction. It inherits
search attributes and schedule identity and exposes `continued_from`,
`continued_to`, and `GET /api/workflows/{id}/continuation-chain`. The target
must keep the workflow type and already be registered. Child workflows cannot
yet continue as new because a parent's join identity is bound to the original
child execution.
