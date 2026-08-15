from uuid import UUID

from engine.runtime.commands import ResolveWorkflowUpdate
from engine.runtime.history import HistoryEvent
from engine.runtime.replay import ReplayStatus, replay_workflow
from engine.runtime.serialization import JSONValue
from engine.sdk import WorkflowContext, workflow

WORKFLOW_ID = UUID("20000000-0000-0000-0000-000000000001")


@workflow(version=1, name="update-replay")
async def update_replay_workflow(ctx: WorkflowContext, value: JSONValue) -> JSONValue:
    update = await ctx.wait_update("set-value")
    accepted = isinstance(update.payload, dict) and "value" in update.payload
    ctx.resolve_update(
        update,
        {"stored": update.payload.get("value")} if accepted else {"message": "value required"},
        accepted=accepted,
    )
    return {"initial": value, "updated": update.payload}


async def test_update_replay_emits_resolution_and_returns_after_recording() -> None:
    started = HistoryEvent(
        seq=1,
        event_type="WorkflowExecutionStarted",
        attributes={"started_at": "2026-01-01T00:00:00+00:00"},
    )
    received = HistoryEvent(
        seq=2,
        event_type="WorkflowUpdateReceived",
        external_id="update:update-1",
        attributes={"update_id": "update-1", "name": "set-value", "payload": {"value": 8}},
    )
    emitted = await replay_workflow(
        update_replay_workflow,
        workflow_id=WORKFLOW_ID,
        workflow_input={"value": 1},
        history=(started, received),
    )
    assert emitted.status is ReplayStatus.COMMANDS
    command = emitted.commands[0]
    assert isinstance(command, ResolveWorkflowUpdate)
    assert command.accepted
    assert command.outcome == {"stored": 8}

    resolved = HistoryEvent(
        seq=3,
        event_type="WorkflowUpdateResolved",
        command_id=command.command_id,
        external_id="update-result:update-1",
        attributes={
            "update_id": "update-1",
            "accepted": True,
            "outcome": command.outcome,
            "fingerprint": command.fingerprint,
        },
    )
    completed = await replay_workflow(
        update_replay_workflow,
        workflow_id=WORKFLOW_ID,
        workflow_input={"value": 1},
        history=(started, received, resolved),
    )
    assert completed.status is ReplayStatus.COMPLETED
    assert completed.result == {"initial": {"value": 1}, "updated": {"value": 8}}
