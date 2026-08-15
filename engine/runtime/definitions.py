"""Immutable workflow and activity definitions."""

from __future__ import annotations

import hashlib
import inspect
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from engine.runtime.serialization import JSONValue

if TYPE_CHECKING:
    from engine.sdk.context import WorkflowContext

type WorkflowFunction = Callable[["WorkflowContext", JSONValue], Awaitable[JSONValue]]
type ActivityFunction = Callable[..., Awaitable[JSONValue] | JSONValue]


class DefinitionError(RuntimeError):
    """Base class for definition registration failures."""


class DefinitionConflictError(DefinitionError):
    """Raised when a pinned workflow version is registered with different code."""


class UnknownDefinitionError(DefinitionError):
    """Raised when a requested definition is not registered in this worker."""


def _code_hash(function: Callable[..., Any]) -> str:
    try:
        source = inspect.getsource(function)
    except (OSError, TypeError) as error:
        raise DefinitionError(f"cannot inspect definition {function!r}") from error
    identity = f"{function.__module__}:{function.__qualname__}\n{source}"
    return hashlib.sha256(identity.encode()).hexdigest()


@dataclass(frozen=True, slots=True)
class WorkflowDefinition:
    name: str
    version: int
    function: WorkflowFunction
    code_hash: str

    @classmethod
    def create(
        cls,
        function: WorkflowFunction,
        *,
        version: int,
        name: str | None = None,
    ) -> WorkflowDefinition:
        if version < 1:
            raise DefinitionError("workflow version must be at least 1")
        return cls(
            name=name or function.__name__,
            version=version,
            function=function,
            code_hash=_code_hash(function),
        )


@dataclass(frozen=True, slots=True)
class ActivityDefinition:
    name: str
    function: ActivityFunction

    @classmethod
    def create(cls, function: ActivityFunction, *, name: str | None = None) -> ActivityDefinition:
        return cls(name=name or function.__name__, function=function)


class DefinitionRegistry:
    def __init__(self) -> None:
        self._workflows: dict[tuple[str, int], WorkflowDefinition] = {}
        self._activities: dict[str, ActivityDefinition] = {}

    def register_workflow(self, definition: WorkflowDefinition) -> None:
        key = (definition.name, definition.version)
        existing = self._workflows.get(key)
        if existing is not None and existing.code_hash != definition.code_hash:
            raise DefinitionConflictError(
                f"workflow {definition.name!r} version {definition.version} is already registered "
                "with different code"
            )
        self._workflows[key] = definition

    def register_activity(self, definition: ActivityDefinition) -> None:
        existing = self._activities.get(definition.name)
        if existing is not None and existing.function is not definition.function:
            raise DefinitionConflictError(
                f"activity {definition.name!r} is already registered with different code"
            )
        self._activities[definition.name] = definition

    def workflow(self, name: str, version: int) -> WorkflowDefinition:
        try:
            return self._workflows[(name, version)]
        except KeyError as error:
            raise UnknownDefinitionError(f"unknown workflow {name!r} version {version}") from error

    def activity(self, name: str) -> ActivityDefinition:
        try:
            return self._activities[name]
        except KeyError as error:
            raise UnknownDefinitionError(f"unknown activity {name!r}") from error

    def supported_workflow_versions(self, name: str) -> tuple[int, ...]:
        return tuple(
            sorted(version for workflow_name, version in self._workflows if workflow_name == name)
        )

    @property
    def workflows(self) -> tuple[WorkflowDefinition, ...]:
        """Registered workflows in stable name/version order."""
        return tuple(self._workflows[key] for key in sorted(self._workflows))

    @property
    def activities(self) -> tuple[ActivityDefinition, ...]:
        """Registered activities in stable name order."""
        return tuple(self._activities[name] for name in sorted(self._activities))
