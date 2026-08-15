from uuid import UUID

from engine.runtime.commands import StartChildWorkflow
from engine.runtime.history import HistoryEvent
from engine.runtime.replay import ReplayStatus, replay_workflow
from engine.runtime.serialization import JSONValue
from engine.sdk import WorkflowContext, workflow

WORKFLOW_ID = UUID("10000000-0000-0000-0000-000000000001")


@workflow(version=2, name="unit-child")
async def unit_child(ctx: WorkflowContext, value: JSONValue) -> JSONValue:
    del ctx
    return {"child": value}


@workflow(version=1, name="unit-parent")
async def unit_parent(ctx: WorkflowContext, value: JSONValue) -> JSONValue:
    return await ctx.child_workflow(unit_child, value)


async def test_child_workflow_replay_schedules_blocks_and_resolves() -> None:
    started = HistoryEvent(
        seq=1,
        event_type="WorkflowExecutionStarted",
        attributes={"started_at": "2026-01-01T00:00:00+00:00"},
    )
    first = await replay_workflow(
        unit_parent,
        workflow_id=WORKFLOW_ID,
        workflow_input={"order": 9},
        history=(started,),
    )
    assert first.status is ReplayStatus.COMMANDS
    command = first.commands[0]
    assert isinstance(command, StartChildWorkflow)
    assert command.workflow_type == "unit-child"
    assert command.definition_version == 2

    child_started = HistoryEvent(
        seq=2,
        event_type="ChildWorkflowStarted",
        command_id=command.command_id,
        entity_id=command.child_workflow_id,
        attributes={
            "fingerprint": command.fingerprint,
            "workflow_type": command.workflow_type,
        },
    )
    blocked = await replay_workflow(
        unit_parent,
        workflow_id=WORKFLOW_ID,
        workflow_input={"order": 9},
        history=(started, child_started),
    )
    assert blocked.status is ReplayStatus.BLOCKED

    completed = await replay_workflow(
        unit_parent,
        workflow_id=WORKFLOW_ID,
        workflow_input={"order": 9},
        history=(
            started,
            child_started,
            HistoryEvent(
                seq=3,
                event_type="ChildWorkflowCompleted",
                entity_id=command.child_workflow_id,
                attributes={"result": {"child": {"order": 9}}},
            ),
        ),
    )
    assert completed.status is ReplayStatus.COMPLETED
    assert completed.result == {"child": {"order": 9}}
