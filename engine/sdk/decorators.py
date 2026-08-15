"""Decorators for workflow and activity definitions."""

from __future__ import annotations

from collections.abc import Callable

from engine.runtime.definitions import (
    ActivityDefinition,
    ActivityFunction,
    WorkflowDefinition,
    WorkflowFunction,
)


def workflow(
    *, version: int, name: str | None = None
) -> Callable[[WorkflowFunction], WorkflowDefinition]:
    def decorate(function: WorkflowFunction) -> WorkflowDefinition:
        return WorkflowDefinition.create(function, version=version, name=name)

    return decorate


def activity(*, name: str | None = None) -> Callable[[ActivityFunction], ActivityDefinition]:
    def decorate(function: ActivityFunction) -> ActivityDefinition:
        return ActivityDefinition.create(function, name=name)

    return decorate
