"""Single-step activity worker."""

from __future__ import annotations

import inspect
from datetime import timedelta

from engine.persistence import Pool, complete_activity, fail_activity, lease_task
from engine.runtime import DefinitionRegistry


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

    definition = registry.activity(activity_type)
    try:
        result = definition.function(*args, **kwargs)
        if inspect.isawaitable(result):
            result = await result
    except Exception as error:
        await fail_activity(
            pool,
            task=task,
            failure={"type": type(error).__name__, "message": str(error)},
        )
    else:
        await complete_activity(pool, task=task, result=result)
    return True
