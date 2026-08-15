"""Typed direct client handles for applications embedding the engine SDK."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from typing import TypeVar, cast
from uuid import UUID

from engine.persistence import (
    ExecutionSummary,
    HistoryRecord,
    Pool,
    WorkflowUpdateRecord,
    get_execution,
    get_history,
    get_update,
    request_workflow_cancellation,
    send_signal,
    send_update,
    start_workflow,
    terminate_workflow,
)
from engine.runtime.definitions import WorkflowDefinition
from engine.runtime.serialization import JSONValue

ResultT = TypeVar("ResultT")
QueryT = TypeVar("QueryT")
UpdateT = TypeVar("UpdateT")


class WorkflowClosedError(RuntimeError):
    def __init__(self, execution: ExecutionSummary) -> None:
        self.execution = execution
        super().__init__(
            f"workflow {execution.id} closed as {execution.status}: {execution.failure!r}"
        )


class WorkflowNotFoundError(LookupError):
    pass


class WorkflowUpdateRejectedError(RuntimeError):
    def __init__(self, update: WorkflowUpdateRecord) -> None:
        self.update = update
        super().__init__(f"workflow update {update.update_id!r} was rejected: {update.failure!r}")


@dataclass(frozen=True, slots=True)
class WorkflowSnapshot:
    execution: ExecutionSummary
    history: tuple[HistoryRecord, ...]


@dataclass(frozen=True, slots=True)
class UpdateHandle[UpdateT]:
    pool: Pool
    workflow_id: UUID
    update_id: str

    async def describe(self) -> WorkflowUpdateRecord:
        record = await get_update(self.pool, workflow_id=self.workflow_id, update_id=self.update_id)
        if record is None:
            raise WorkflowNotFoundError(f"workflow update {self.update_id!r} does not exist")
        return record

    async def result(self, *, timeout: float | None = None, poll_interval: float = 0.05) -> UpdateT:
        if poll_interval <= 0:
            raise ValueError("poll_interval must be positive")

        async def wait() -> UpdateT:
            while True:
                record = await self.describe()
                if record.status == "completed":
                    return cast(UpdateT, record.result)
                if record.status == "rejected":
                    raise WorkflowUpdateRejectedError(record)
                await asyncio.sleep(poll_interval)

        if timeout is None:
            return await wait()
        async with asyncio.timeout(timeout):
            return await wait()


@dataclass(frozen=True, slots=True)
class WorkflowHandle[ResultT]:
    pool: Pool
    id: UUID

    async def describe(self) -> ExecutionSummary:
        execution = await get_execution(self.pool, self.id)
        if execution is None:
            raise WorkflowNotFoundError(f"workflow {self.id} does not exist")
        return execution

    async def snapshot(self) -> WorkflowSnapshot:
        execution = await self.describe()
        return WorkflowSnapshot(execution=execution, history=await get_history(self.pool, self.id))

    async def query(self, projection: Callable[[WorkflowSnapshot], QueryT]) -> QueryT:
        """Run a side-effect-free projection over execution metadata and history."""
        return projection(await self.snapshot())

    async def signal(self, name: str, payload: JSONValue = None, *, signal_id: str) -> bool:
        return await send_signal(
            self.pool,
            workflow_id=self.id,
            signal_id=signal_id,
            name=name,
            payload=payload,
        )

    async def update(
        self,
        name: str,
        payload: JSONValue = None,
        *,
        update_id: str,
    ) -> UpdateHandle[UpdateT]:
        await send_update(
            self.pool,
            workflow_id=self.id,
            update_id=update_id,
            name=name,
            payload=payload,
        )
        return UpdateHandle(self.pool, self.id, update_id)

    async def cancel(self, reason: str | None = None) -> bool:
        return await request_workflow_cancellation(self.pool, workflow_id=self.id, reason=reason)

    async def terminate(self, reason: str | None = None) -> bool:
        return await terminate_workflow(self.pool, workflow_id=self.id, reason=reason)

    async def result(self, *, timeout: float | None = None, poll_interval: float = 0.05) -> ResultT:
        if poll_interval <= 0:
            raise ValueError("poll_interval must be positive")

        async def wait() -> ResultT:
            while True:
                execution = await self.describe()
                if execution.status == "completed":
                    return cast(ResultT, execution.result)
                if execution.status in ("failed", "terminated"):
                    raise WorkflowClosedError(execution)
                await asyncio.sleep(poll_interval)

        if timeout is None:
            return await wait()
        async with asyncio.timeout(timeout):
            return await wait()


class DurableClient:
    def __init__(self, pool: Pool) -> None:
        self.pool = pool

    def handle(self, workflow_id: UUID) -> WorkflowHandle[ResultT]:
        return WorkflowHandle(self.pool, workflow_id)

    async def start(
        self,
        definition: WorkflowDefinition,
        input: JSONValue = None,
        *,
        queue_name: str = "default",
        search_attributes: dict[str, JSONValue] | None = None,
        workflow_id: UUID | None = None,
    ) -> WorkflowHandle[ResultT]:
        started = await start_workflow(
            self.pool,
            workflow_type=definition.name,
            definition_version=definition.version,
            workflow_input=input,
            queue_name=queue_name,
            search_attributes=search_attributes,
            workflow_id=workflow_id,
        )
        return WorkflowHandle(self.pool, started.workflow_id)
