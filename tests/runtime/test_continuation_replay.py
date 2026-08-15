from uuid import UUID

from engine.runtime.commands import ContinueAsNew
from engine.runtime.history import HistoryEvent
from engine.runtime.replay import ReplayStatus, replay_workflow
from engine.runtime.serialization import JSONValue
from engine.sdk import WorkflowContext, workflow

WORKFLOW_ID = UUID("d5c9b0b4-4be8-4eb3-8d71-6b48fb195c57")


@workflow(version=2, name="generation-loop")
async def generation_v2(ctx: WorkflowContext, value: JSONValue) -> JSONValue:
    del ctx
    return {"finished": value}


@workflow(version=1, name="generation-loop")
async def generation_v1(ctx: WorkflowContext, value: JSONValue) -> JSONValue:
    assert isinstance(value, dict)
    ctx.continue_as_new(generation_v2, {"generation": int(value["generation"]) + 1})
    raise AssertionError("continue_as_new must suspend replay")


async def test_continue_as_new_emits_and_replays_a_stable_terminal_command() -> None:
    started = HistoryEvent(1, "WorkflowExecutionStarted", {"input": {"generation": 0}})
    emitted = await replay_workflow(
        generation_v1,
        workflow_id=WORKFLOW_ID,
        workflow_input={"generation": 0},
        history=(started,),
    )
    assert emitted.status is ReplayStatus.COMMANDS
    command = emitted.commands[0]
    assert isinstance(command, ContinueAsNew)
    assert command.workflow_type == "generation-loop"
    assert command.definition_version == 2
    assert command.input == {"generation": 1}

    recorded = HistoryEvent(
        2,
        "WorkflowExecutionContinuedAsNew",
        {
            "workflow_type": command.workflow_type,
            "definition_version": command.definition_version,
            "input": command.input,
            "queue_name": "default",
            "fingerprint": command.fingerprint,
        },
        command_id=command.command_id,
        entity_id=command.new_workflow_id,
    )
    replayed = await replay_workflow(
        generation_v1,
        workflow_id=WORKFLOW_ID,
        workflow_input={"generation": 0},
        history=(started, recorded),
    )
    assert replayed.status is ReplayStatus.COMPLETED
    assert replayed.result == {"continued_to": str(command.new_workflow_id)}
