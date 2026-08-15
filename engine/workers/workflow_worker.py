"""Single-step workflow worker."""

from __future__ import annotations

from datetime import timedelta

from engine.persistence import (
    Pool,
    commit_workflow_replay,
    lease_task,
    load_workflow_replay_state,
    release_workflow_task,
)
from engine.runtime import DefinitionRegistry, replay_workflow
from engine.runtime.definitions import UnknownDefinitionError


async def run_workflow_task(
    pool: Pool,
    registry: DefinitionRegistry,
    *,
    queue_name: str = "default",
    lease_duration: timedelta = timedelta(seconds=30),
) -> bool:
    """Process at most one workflow task and report whether work was found."""
    task = await lease_task(
        pool,
        task_type="workflow",
        queue_name=queue_name,
        lease_duration=lease_duration,
    )
    if task is None:
        return False
    state = await load_workflow_replay_state(pool, task)
    try:
        definition = registry.workflow(state.workflow_type, state.definition_version)
    except UnknownDefinitionError:
        await release_workflow_task(pool, task=task)
        raise
    replay = await replay_workflow(
        definition,
        workflow_id=state.workflow_id,
        workflow_input=state.workflow_input,
        history=state.history,
    )
    await commit_workflow_replay(pool, task=task, replay=replay)
    return True
