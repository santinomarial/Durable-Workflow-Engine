import pytest

from engine.runtime.definitions import (
    ActivityDefinition,
    DefinitionConflictError,
    DefinitionRegistry,
    UnknownDefinitionError,
    WorkflowDefinition,
)
from engine.runtime.serialization import JSONValue
from engine.sdk import WorkflowContext


async def workflow_one(ctx: WorkflowContext, value: JSONValue) -> JSONValue:
    del ctx
    return value


async def workflow_two(ctx: WorkflowContext, value: JSONValue) -> JSONValue:
    del ctx
    return {"changed": value}


def activity_one(value: JSONValue) -> JSONValue:
    return value


def activity_two(value: JSONValue) -> JSONValue:
    return {"changed": value}


def test_registry_returns_pinned_definition() -> None:
    registry = DefinitionRegistry()
    definition = WorkflowDefinition.create(workflow_one, version=1, name="sample")

    registry.register_workflow(definition)
    registry.register_workflow(definition)

    assert registry.workflow("sample", 1) is definition


def test_registry_rejects_different_code_for_pinned_version() -> None:
    registry = DefinitionRegistry()
    registry.register_workflow(WorkflowDefinition.create(workflow_one, version=1, name="sample"))

    with pytest.raises(DefinitionConflictError, match="different code"):
        registry.register_workflow(
            WorkflowDefinition.create(workflow_two, version=1, name="sample")
        )


def test_registry_rejects_activity_name_collision() -> None:
    registry = DefinitionRegistry()
    registry.register_activity(ActivityDefinition.create(activity_one, name="sample"))

    with pytest.raises(DefinitionConflictError, match="different code"):
        registry.register_activity(ActivityDefinition.create(activity_two, name="sample"))


def test_registry_reports_unknown_pinned_version() -> None:
    registry = DefinitionRegistry()

    with pytest.raises(UnknownDefinitionError, match="unknown workflow 'sample' version 7"):
        registry.workflow("sample", 7)
