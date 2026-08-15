"""Single-step activity worker."""

from __future__ import annotations

import inspect
from datetime import datetime, timedelta

from engine.persistence import (
    ActivityCancellationRequested,
    Pool,
    complete_activity,
    fail_activity,
    heartbeat_activity,
    lease_task,
)
from engine.runtime import DefinitionRegistry
from engine.runtime.serialization import JSONValue
from engine.sdk.activity_context import (
    ActivityExecutionContext,
    reset_activity_context,
    set_activity_context,
)


async def run_activity_task(
    pool: Pool,
    registry: DefinitionRegistry,
    *,
    queue_name: str = "default",
    lease_duration: timedelta = timedelta(seconds=30),
) -> bool:
    """Process at most one activity task and report whether work was found."""
    task = await lease_task(
        pool,
        task_type="activity",
        queue_name=queue_name,
        lease_duration=lease_duration,
    )
    if task is None:
        return False
    if not isinstance(task.input, dict):
        raise TypeError("activity task input must be an object")
    activity_type = task.input.get("activity_type")
    command_input = task.input.get("input")
    if not isinstance(activity_type, str) or not isinstance(command_input, dict):
        raise TypeError("activity task input is malformed")
    args = command_input.get("args")
    kwargs = command_input.get("kwargs")
    if not isinstance(args, list) or not isinstance(kwargs, dict):
        raise TypeError("activity arguments are malformed")
    idempotency_key = task.input.get("idempotency_key")
    if not isinstance(idempotency_key, str):
        raise TypeError("activity task has no idempotency key")

    definition = registry.activity(activity_type)

    async def heartbeat(details: JSONValue, duration: timedelta) -> datetime:
        return await heartbeat_activity(
            pool,
            task_id=task.id,
            lease_token=task.lease_token,
            details=details,
            lease_duration=duration,
        )

    context_token = set_activity_context(
        ActivityExecutionContext(
            idempotency_key=idempotency_key,
            task_id=task.id,
            attempt=task.attempt,
            _heartbeat=heartbeat,
        )
    )
    failure: JSONValue = None
    result: JSONValue = None
    try:
        try:
            activity_result = definition.function(*args, **kwargs)
            if inspect.isawaitable(activity_result):
                result = await activity_result
            else:
                result = activity_result
        except ActivityCancellationRequested:
            raise
        except Exception as error:
            failure = {"type": type(error).__name__, "message": str(error)}
    finally:
        reset_activity_context(context_token)
    if failure is not None:
        await fail_activity(
            pool,
            task=task,
            failure=failure,
        )
    else:
        await complete_activity(pool, task=task, result=result)
    return True
