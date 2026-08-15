"""FastAPI control and observability surface."""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import asdict
from pathlib import Path
from typing import Annotated, Any, Literal, cast
from uuid import UUID

from fastapi import FastAPI, HTTPException, Query, Request, status
from fastapi.encoders import jsonable_encoder
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from engine.persistence import (
    Pool,
    create_pool,
    get_execution,
    get_history,
    list_executions,
    request_workflow_cancellation,
    send_signal,
    start_workflow,
    terminate_workflow,
)
from engine.persistence.migrations import migrate
from engine.persistence.transitions import TransitionError
from engine.runtime.serialization import JSONValue

_PACKAGED_UI_DIR = Path(__file__).resolve().parents[1] / "_assets" / "ui"
_SOURCE_UI_DIR = Path(__file__).resolve().parents[2] / "ui"
UI_DIR = _PACKAGED_UI_DIR if _PACKAGED_UI_DIR.exists() else _SOURCE_UI_DIR
WorkflowStatus = Literal["running", "completed", "failed", "terminated"]


class StartWorkflowRequest(BaseModel):
    workflow_type: str = Field(min_length=1)
    definition_version: int = Field(ge=1)
    input: Any = None
    queue_name: str = Field(default="default", min_length=1)


class SignalRequest(BaseModel):
    signal_id: str = Field(min_length=1)
    name: str = Field(min_length=1)
    payload: Any = None


class TerminateRequest(BaseModel):
    reason: str | None = None


class CancelRequest(BaseModel):
    reason: str | None = None


def _pool(request: Request) -> Pool:
    return cast(Pool, request.app.state.pool)


def create_app(pool: Pool | None = None) -> FastAPI:
    owned_pool: Pool | None = None

    @asynccontextmanager
    async def lifespan(application: FastAPI) -> AsyncIterator[None]:
        nonlocal owned_pool
        if pool is None:
            database_url = os.environ.get("DATABASE_URL")
            if not database_url:
                raise RuntimeError("DATABASE_URL is required")
            await migrate(database_url)
            owned_pool = await create_pool(database_url)
            application.state.pool = owned_pool
        else:
            application.state.pool = pool
        try:
            yield
        finally:
            if owned_pool is not None:
                await owned_pool.close()

    application = FastAPI(
        title="Durable Workflow Engine",
        version="0.1.0",
        lifespan=lifespan,
    )
    if pool is not None:
        application.state.pool = pool
    application.mount("/static", StaticFiles(directory=UI_DIR), name="static")

    @application.get("/", include_in_schema=False)
    async def index() -> FileResponse:
        return FileResponse(UI_DIR / "index.html")

    @application.get("/api/health")
    async def health(request: Request) -> dict[str, str]:
        async with _pool(request).acquire() as connection:
            await connection.fetchval("select 1")
        return {"status": "ok"}

    @application.get("/api/workflows")
    async def workflows(
        request: Request,
        status_filter: Annotated[WorkflowStatus | None, Query(alias="status")] = None,
        limit: Annotated[int, Query(ge=1, le=1000)] = 100,
    ) -> Any:
        records = await list_executions(_pool(request), status=status_filter, limit=limit)
        return jsonable_encoder([asdict(record) for record in records])

    @application.post("/api/workflows", status_code=status.HTTP_201_CREATED)
    async def start(body: StartWorkflowRequest, request: Request) -> dict[str, Any]:
        try:
            started = await start_workflow(
                _pool(request),
                workflow_type=body.workflow_type,
                definition_version=body.definition_version,
                workflow_input=cast(JSONValue, body.input),
                queue_name=body.queue_name,
            )
        except (TransitionError, ValueError) as error:
            raise HTTPException(status_code=409, detail=str(error)) from error
        return cast(dict[str, Any], jsonable_encoder(asdict(started)))

    @application.get("/api/workflows/{workflow_id}")
    async def execution(workflow_id: UUID, request: Request) -> Any:
        record = await get_execution(_pool(request), workflow_id)
        if record is None:
            raise HTTPException(status_code=404, detail="workflow not found")
        return jsonable_encoder(asdict(record))

    @application.get("/api/workflows/{workflow_id}/history")
    async def history(workflow_id: UUID, request: Request) -> Any:
        if await get_execution(_pool(request), workflow_id) is None:
            raise HTTPException(status_code=404, detail="workflow not found")
        records = await get_history(_pool(request), workflow_id)
        return jsonable_encoder([asdict(record) for record in records])

    @application.post("/api/workflows/{workflow_id}/signals")
    async def signal(workflow_id: UUID, body: SignalRequest, request: Request) -> dict[str, bool]:
        try:
            accepted = await send_signal(
                _pool(request),
                workflow_id=workflow_id,
                signal_id=body.signal_id,
                name=body.name,
                payload=cast(JSONValue, body.payload),
            )
        except TransitionError as error:
            raise HTTPException(status_code=409, detail=str(error)) from error
        return {"accepted": accepted}

    @application.post("/api/workflows/{workflow_id}/terminate")
    async def terminate(
        workflow_id: UUID, body: TerminateRequest, request: Request
    ) -> dict[str, bool]:
        try:
            accepted = await terminate_workflow(
                _pool(request), workflow_id=workflow_id, reason=body.reason
            )
        except TransitionError as error:
            raise HTTPException(status_code=409, detail=str(error)) from error
        return {"accepted": accepted}

    @application.post("/api/workflows/{workflow_id}/cancel")
    async def cancel(workflow_id: UUID, body: CancelRequest, request: Request) -> dict[str, bool]:
        try:
            accepted = await request_workflow_cancellation(
                _pool(request), workflow_id=workflow_id, reason=body.reason
            )
        except TransitionError as error:
            raise HTTPException(status_code=409, detail=str(error)) from error
        return {"accepted": accepted}

    return application


app = create_app()
