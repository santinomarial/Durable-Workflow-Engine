"""FastAPI control and observability surface."""

from __future__ import annotations

import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import asdict
from pathlib import Path
from typing import Annotated, Any, Literal, cast
from uuid import UUID, uuid4

from fastapi import FastAPI, HTTPException, Query, Request, status
from fastapi.encoders import jsonable_encoder
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from starlette.middleware.base import RequestResponseEndpoint
from starlette.responses import JSONResponse, Response

from engine.api.security import (
    PUBLIC_PATHS,
    AuthConfig,
    FixedWindowRateLimiter,
    Principal,
    RequestBodyLimitMiddleware,
    bearer_token,
    require_role,
)
from engine.persistence import (
    AuditContext,
    Pool,
    create_pool,
    get_execution,
    get_execution_stats,
    get_history,
    list_api_audit,
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
    workflow_type: str = Field(min_length=1, max_length=200)
    definition_version: int = Field(ge=1)
    input: Any = None
    queue_name: str = Field(default="default", min_length=1, max_length=200)


class SignalRequest(BaseModel):
    signal_id: str = Field(min_length=1, max_length=200)
    name: str = Field(min_length=1, max_length=200)
    payload: Any = None


class TerminateRequest(BaseModel):
    reason: str | None = Field(default=None, max_length=2000)


class CancelRequest(BaseModel):
    reason: str | None = Field(default=None, max_length=2000)


def _pool(request: Request) -> Pool:
    return cast(Pool, request.app.state.pool)


def _audit(request: Request, principal: Principal, action: str) -> AuditContext:
    return AuditContext(
        request_id=cast(UUID, request.state.request_id),
        actor_key_id=principal.key_id,
        actor_role=principal.role,
        action=action,
    )


def _public_path(path: str) -> bool:
    return path in PUBLIC_PATHS or path.startswith("/static/")


def _secure_headers(response: Response, request_id: UUID, path: str) -> Response:
    response.headers["X-Request-ID"] = str(request_id)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Permissions-Policy"] = "camera=(), microphone=(), geolocation=()"
    if path == "/" or path.startswith("/static/"):
        response.headers["Content-Security-Policy"] = (
            "default-src 'self'; base-uri 'none'; connect-src 'self'; "
            "font-src 'self'; form-action 'self'; frame-ancestors 'none'; "
            "img-src 'self' data:; object-src 'none'; script-src 'self'; style-src 'self'"
        )
    if path.startswith("/api/"):
        response.headers["Cache-Control"] = "no-store"
    elif path.startswith("/static/"):
        response.headers["Cache-Control"] = "public, max-age=300, must-revalidate"
    return response


def create_app(pool: Pool | None = None, *, auth: AuthConfig | None = None) -> FastAPI:
    owned_pool: Pool | None = None
    auth_config = auth or AuthConfig.from_env()
    rate_limiter = FixedWindowRateLimiter(auth_config.requests_per_minute)

    @asynccontextmanager
    async def lifespan(application: FastAPI) -> AsyncIterator[None]:
        nonlocal owned_pool
        auth_config.validate()
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
    application.add_middleware(
        RequestBodyLimitMiddleware,
        max_bytes=auth_config.max_request_bytes,
    )
    application.state.auth_config = auth_config
    if pool is not None:
        application.state.pool = pool
    application.mount("/static", StaticFiles(directory=UI_DIR), name="static")

    @application.middleware("http")
    async def secure_responses(request: Request, call_next: RequestResponseEndpoint) -> Response:
        request_id = uuid4()
        request.state.request_id = request_id
        path = request.url.path
        if not _public_path(path):
            token = bearer_token(request)
            principal = auth_config.authenticate(token or "")
            if principal is None:
                auth_response = JSONResponse(
                    {"detail": "valid bearer authentication required"},
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    headers={"WWW-Authenticate": "Bearer"},
                )
                return _secure_headers(auth_response, request_id, path)
            allowed, retry_after = rate_limiter.allow(principal.key_id)
            if not allowed:
                rate_response = JSONResponse(
                    {"detail": "request rate limit exceeded"},
                    status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                    headers={"Retry-After": str(retry_after)},
                )
                return _secure_headers(rate_response, request_id, path)
            request.state.principal = principal
        response = await call_next(request)
        return _secure_headers(response, request_id, path)

    @application.get("/", include_in_schema=False)
    async def index() -> FileResponse:
        return FileResponse(UI_DIR / "index.html")

    @application.get("/api/health")
    async def health(request: Request) -> dict[str, str]:
        async with _pool(request).acquire() as connection:
            await connection.fetchval("select 1")
        return {"status": "ok"}

    @application.get("/api/session")
    async def session(request: Request) -> dict[str, str]:
        principal = require_role(request, "viewer")
        return {"key_id": principal.key_id, "role": principal.role}

    @application.get("/api/workflows")
    async def workflows(
        request: Request,
        status_filter: Annotated[WorkflowStatus | None, Query(alias="status")] = None,
        limit: Annotated[int, Query(ge=1, le=1000)] = 100,
    ) -> Any:
        require_role(request, "viewer")
        records = await list_executions(_pool(request), status=status_filter, limit=limit)
        return jsonable_encoder([asdict(record) for record in records])

    @application.get("/api/stats")
    async def stats(request: Request) -> Any:
        require_role(request, "viewer")
        return jsonable_encoder(asdict(await get_execution_stats(_pool(request))))

    @application.get("/api/audit")
    async def audit_records(
        request: Request,
        limit: Annotated[int, Query(ge=1, le=1000)] = 100,
    ) -> Any:
        require_role(request, "admin")
        return jsonable_encoder(
            [asdict(record) for record in await list_api_audit(_pool(request), limit=limit)]
        )

    @application.post("/api/workflows", status_code=status.HTTP_201_CREATED)
    async def start(body: StartWorkflowRequest, request: Request) -> dict[str, Any]:
        principal = require_role(request, "operator")
        try:
            started = await start_workflow(
                _pool(request),
                workflow_type=body.workflow_type,
                definition_version=body.definition_version,
                workflow_input=cast(JSONValue, body.input),
                queue_name=body.queue_name,
                audit=_audit(request, principal, "workflow.start"),
            )
        except (TransitionError, ValueError) as error:
            raise HTTPException(status_code=409, detail=str(error)) from error
        return cast(dict[str, Any], jsonable_encoder(asdict(started)))

    @application.get("/api/workflows/{workflow_id}")
    async def execution(workflow_id: UUID, request: Request) -> Any:
        require_role(request, "viewer")
        record = await get_execution(_pool(request), workflow_id)
        if record is None:
            raise HTTPException(status_code=404, detail="workflow not found")
        return jsonable_encoder(asdict(record))

    @application.get("/api/workflows/{workflow_id}/history")
    async def history(workflow_id: UUID, request: Request) -> Any:
        require_role(request, "viewer")
        if await get_execution(_pool(request), workflow_id) is None:
            raise HTTPException(status_code=404, detail="workflow not found")
        records = await get_history(_pool(request), workflow_id)
        return jsonable_encoder([asdict(record) for record in records])

    @application.post("/api/workflows/{workflow_id}/signals")
    async def signal(workflow_id: UUID, body: SignalRequest, request: Request) -> dict[str, bool]:
        principal = require_role(request, "operator")
        try:
            accepted = await send_signal(
                _pool(request),
                workflow_id=workflow_id,
                signal_id=body.signal_id,
                name=body.name,
                payload=cast(JSONValue, body.payload),
                audit=_audit(request, principal, "workflow.signal"),
            )
        except TransitionError as error:
            raise HTTPException(status_code=409, detail=str(error)) from error
        return {"accepted": accepted}

    @application.post("/api/workflows/{workflow_id}/terminate")
    async def terminate(
        workflow_id: UUID, body: TerminateRequest, request: Request
    ) -> dict[str, bool]:
        principal = require_role(request, "admin")
        try:
            accepted = await terminate_workflow(
                _pool(request),
                workflow_id=workflow_id,
                reason=body.reason,
                audit=_audit(request, principal, "workflow.terminate"),
            )
        except TransitionError as error:
            raise HTTPException(status_code=409, detail=str(error)) from error
        return {"accepted": accepted}

    @application.post("/api/workflows/{workflow_id}/cancel")
    async def cancel(workflow_id: UUID, body: CancelRequest, request: Request) -> dict[str, bool]:
        principal = require_role(request, "operator")
        try:
            accepted = await request_workflow_cancellation(
                _pool(request),
                workflow_id=workflow_id,
                reason=body.reason,
                audit=_audit(request, principal, "workflow.cancel"),
            )
        except TransitionError as error:
            raise HTTPException(status_code=409, detail=str(error)) from error
        return {"accepted": accepted}

    return application


app = create_app()
