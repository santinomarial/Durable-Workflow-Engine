"""FastAPI control and observability surface."""

from __future__ import annotations

import asyncio
import json
import logging
import time
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
from engine.config import DatabaseConfig, positive_float
from engine.observability import METRICS, configure_logging
from engine.persistence import (
    AuditContext,
    Pool,
    create_configured_pool,
    get_execution,
    get_execution_stats,
    get_history_page,
    get_history_tail,
    get_operational_gauges,
    list_api_audit,
    list_dead_tasks,
    list_executions,
    list_worker_heartbeats,
    pause_workflow,
    request_workflow_cancellation,
    resume_workflow,
    retry_workflow,
    send_signal,
    start_workflow,
    terminate_workflow,
    update_search_attributes,
)
from engine.persistence.migrations import discover_migrations, migrate
from engine.persistence.transitions import TransitionError
from engine.runtime.serialization import JSONValue

_PACKAGED_UI_DIR = Path(__file__).resolve().parents[1] / "_assets" / "ui"
_SOURCE_UI_DIR = Path(__file__).resolve().parents[2] / "ui"
UI_DIR = _PACKAGED_UI_DIR if _PACKAGED_UI_DIR.exists() else _SOURCE_UI_DIR
WorkflowStatus = Literal["running", "completed", "failed", "terminated", "attention"]
LOGGER = logging.getLogger(__name__)


class StartWorkflowRequest(BaseModel):
    workflow_type: str = Field(min_length=1, max_length=200)
    definition_version: int = Field(ge=1)
    input: Any = None
    queue_name: str = Field(default="default", min_length=1, max_length=200)
    search_attributes: dict[str, Any] = Field(default_factory=dict)


class SignalRequest(BaseModel):
    signal_id: str = Field(min_length=1, max_length=200)
    name: str = Field(min_length=1, max_length=200)
    payload: Any = None


class TerminateRequest(BaseModel):
    reason: str | None = Field(default=None, max_length=2000)


class CancelRequest(BaseModel):
    reason: str | None = Field(default=None, max_length=2000)


class PauseRequest(BaseModel):
    reason: str | None = Field(default=None, max_length=2000)


class SearchAttributesRequest(BaseModel):
    set: dict[str, Any] = Field(default_factory=dict)
    unset: list[str] = Field(default_factory=list, max_length=100)


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
    health_timeout = positive_float("DWE_HEALTH_TIMEOUT_SECONDS", 2)
    latest_migration = discover_migrations()[-1].version

    @asynccontextmanager
    async def lifespan(application: FastAPI) -> AsyncIterator[None]:
        nonlocal owned_pool
        configure_logging()
        auth_config.validate()
        if pool is None:
            database = DatabaseConfig.from_env(application_name="dwe-api")
            await migrate(database.url)
            owned_pool = await create_configured_pool(database)
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
        started_at = time.perf_counter()
        request_id = uuid4()
        request.state.request_id = request_id
        path = request.url.path

        def finish(response: Response, actor: str = "anonymous") -> Response:
            duration = time.perf_counter() - started_at
            route = getattr(request.scope.get("route"), "path", "unmatched")
            labels = {
                "method": request.method,
                "route": route,
                "status": str(response.status_code),
            }
            METRICS.increment("dwe_http_requests_total", labels=labels)
            METRICS.observe(
                "dwe_http_request_duration_seconds",
                duration,
                labels={"method": request.method, "route": route},
            )
            LOGGER.info(
                "HTTP request completed",
                extra={
                    "event": "http_request",
                    "request_id": str(request_id),
                    "actor": actor,
                    "method": request.method,
                    "route": route,
                    "status_code": response.status_code,
                    "duration_ms": round(duration * 1000, 3),
                },
            )
            return _secure_headers(response, request_id, path)

        actor = "anonymous"
        if not _public_path(path):
            token = bearer_token(request)
            principal = auth_config.authenticate(token or "")
            if principal is None:
                auth_response = JSONResponse(
                    {"detail": "valid bearer authentication required"},
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    headers={"WWW-Authenticate": "Bearer"},
                )
                return finish(auth_response)
            actor = principal.key_id
            allowed, retry_after = rate_limiter.allow(principal.key_id)
            if not allowed:
                rate_response = JSONResponse(
                    {"detail": "request rate limit exceeded"},
                    status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                    headers={"Retry-After": str(retry_after)},
                )
                return finish(rate_response, actor)
            request.state.principal = principal
        try:
            response = await call_next(request)
        except Exception:
            duration = time.perf_counter() - started_at
            METRICS.increment(
                "dwe_http_requests_total",
                labels={"method": request.method, "route": "unhandled", "status": "500"},
            )
            LOGGER.exception(
                "HTTP request failed",
                extra={
                    "event": "http_request_error",
                    "request_id": str(request_id),
                    "actor": actor,
                    "method": request.method,
                    "duration_ms": round(duration * 1000, 3),
                },
            )
            raise
        return finish(response, actor)

    @application.get("/", include_in_schema=False)
    async def index() -> FileResponse:
        return FileResponse(UI_DIR / "index.html")

    @application.get("/api/health/live")
    async def liveness() -> dict[str, str]:
        return {"status": "alive"}

    async def readiness_response(request: Request) -> Response:
        try:
            async with asyncio.timeout(health_timeout):
                async with _pool(request).acquire() as connection:
                    database_time = await connection.fetchval("select now()")
                    migration = await connection.fetchval(
                        "select version from schema_migrations order by version desc limit 1"
                    )
            if migration != latest_migration:
                return JSONResponse(
                    {"status": "not_ready", "reason": "schema_migration_mismatch"},
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                )
            return JSONResponse(
                {
                    "status": "ready",
                    "database": "ok",
                    "database_time": str(database_time),
                    "schema_version": str(migration),
                }
            )
        except Exception:
            LOGGER.warning(
                "readiness check failed", exc_info=True, extra={"event": "readiness_failed"}
            )
            return JSONResponse(
                {"status": "not_ready", "reason": "database_unavailable"},
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            )

    @application.get("/api/health")
    async def health(request: Request) -> Response:
        return await readiness_response(request)

    @application.get("/api/health/ready")
    async def readiness(request: Request) -> Response:
        return await readiness_response(request)

    @application.get("/api/session")
    async def session(request: Request) -> dict[str, str]:
        principal = require_role(request, "viewer")
        return {"key_id": principal.key_id, "role": principal.role}

    @application.get("/api/workflows")
    async def workflows(
        request: Request,
        status_filter: Annotated[WorkflowStatus | None, Query(alias="status")] = None,
        workflow_type: Annotated[str | None, Query(max_length=200)] = None,
        queue_name: Annotated[str | None, Query(max_length=200)] = None,
        query: Annotated[str | None, Query(max_length=500)] = None,
        attributes: Annotated[str | None, Query(max_length=16000)] = None,
        limit: Annotated[int, Query(ge=1, le=1000)] = 100,
    ) -> Any:
        require_role(request, "viewer")
        attribute_filter: dict[str, JSONValue] | None = None
        if attributes is not None:
            try:
                decoded = json.loads(attributes)
            except json.JSONDecodeError as error:
                raise HTTPException(
                    status_code=422, detail="attributes must be valid JSON"
                ) from error
            if not isinstance(decoded, dict):
                raise HTTPException(status_code=422, detail="attributes must be a JSON object")
            attribute_filter = cast(dict[str, JSONValue], decoded)
        records = await list_executions(
            _pool(request),
            status=status_filter,
            workflow_type=workflow_type,
            queue_name=queue_name,
            query=query,
            search_attributes=attribute_filter,
            limit=limit,
        )
        return jsonable_encoder([asdict(record) for record in records])

    @application.get("/api/stats")
    async def stats(request: Request) -> Any:
        require_role(request, "viewer")
        return jsonable_encoder(asdict(await get_execution_stats(_pool(request))))

    @application.get("/api/workers")
    async def workers(request: Request) -> Any:
        require_role(request, "viewer")
        return jsonable_encoder(
            [asdict(worker) for worker in await list_worker_heartbeats(_pool(request))]
        )

    @application.get("/metrics", include_in_schema=False)
    async def metrics(request: Request) -> Response:
        require_role(request, "admin")
        gauges = await get_operational_gauges(_pool(request))
        gauges["dwe_build_info"] = 1
        return Response(
            METRICS.render(gauges=gauges),
            headers={"Content-Type": "text/plain; version=0.0.4; charset=utf-8"},
        )

    @application.get("/api/audit")
    async def audit_records(
        request: Request,
        limit: Annotated[int, Query(ge=1, le=1000)] = 100,
    ) -> Any:
        require_role(request, "admin")
        return jsonable_encoder(
            [asdict(record) for record in await list_api_audit(_pool(request), limit=limit)]
        )

    @application.get("/api/dead-letter")
    async def dead_letter_records(
        request: Request,
        limit: Annotated[int, Query(ge=1, le=1000)] = 100,
    ) -> Any:
        require_role(request, "operator")
        return jsonable_encoder(
            [asdict(record) for record in await list_dead_tasks(_pool(request), limit=limit)]
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
                search_attributes=cast(dict[str, JSONValue], body.search_attributes),
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
    async def history(
        workflow_id: UUID,
        request: Request,
        after_seq: Annotated[int, Query(ge=0)] = 0,
        limit: Annotated[int, Query(ge=1, le=1000)] = 500,
    ) -> Any:
        require_role(request, "viewer")
        if await get_execution(_pool(request), workflow_id) is None:
            raise HTTPException(status_code=404, detail="workflow not found")
        page = await get_history_page(_pool(request), workflow_id, after_seq=after_seq, limit=limit)
        return jsonable_encoder(
            {
                "items": [asdict(record) for record in page.items],
                "next_after_seq": page.next_after_seq,
            }
        )

    @application.patch("/api/workflows/{workflow_id}/search-attributes")
    async def patch_search_attributes(
        workflow_id: UUID, body: SearchAttributesRequest, request: Request
    ) -> Any:
        principal = require_role(request, "operator")
        try:
            updated = await update_search_attributes(
                _pool(request),
                workflow_id=workflow_id,
                attributes=cast(dict[str, JSONValue], body.set),
                unset=tuple(body.unset),
                audit=_audit(request, principal, "workflow.search-attributes.update"),
            )
        except (TransitionError, ValueError) as error:
            raise HTTPException(status_code=409, detail=str(error)) from error
        return jsonable_encoder({"search_attributes": updated})

    @application.get("/api/workflows/{workflow_id}/history-tail")
    async def history_tail(
        workflow_id: UUID,
        request: Request,
        limit: Annotated[int, Query(ge=1, le=1000)] = 500,
    ) -> Any:
        require_role(request, "viewer")
        if await get_execution(_pool(request), workflow_id) is None:
            raise HTTPException(status_code=404, detail="workflow not found")
        records = await get_history_tail(_pool(request), workflow_id, limit=limit)
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

    @application.post("/api/workflows/{workflow_id}/pause")
    async def pause(workflow_id: UUID, body: PauseRequest, request: Request) -> dict[str, bool]:
        principal = require_role(request, "operator")
        try:
            accepted = await pause_workflow(
                _pool(request),
                workflow_id=workflow_id,
                reason=body.reason,
                audit=_audit(request, principal, "workflow.pause"),
            )
        except TransitionError as error:
            raise HTTPException(status_code=409, detail=str(error)) from error
        return {"accepted": accepted}

    @application.post("/api/workflows/{workflow_id}/resume")
    async def resume(workflow_id: UUID, body: PauseRequest, request: Request) -> dict[str, bool]:
        principal = require_role(request, "operator")
        try:
            accepted = await resume_workflow(
                _pool(request),
                workflow_id=workflow_id,
                reason=body.reason,
                audit=_audit(request, principal, "workflow.resume"),
            )
        except TransitionError as error:
            raise HTTPException(status_code=409, detail=str(error)) from error
        return {"accepted": accepted}

    @application.post("/api/workflows/{workflow_id}/retry", status_code=status.HTTP_201_CREATED)
    async def retry(workflow_id: UUID, request: Request) -> Any:
        principal = require_role(request, "admin")
        try:
            started = await retry_workflow(
                _pool(request),
                workflow_id=workflow_id,
                audit=_audit(request, principal, "workflow.retry"),
            )
        except TransitionError as error:
            raise HTTPException(status_code=409, detail=str(error)) from error
        return jsonable_encoder(asdict(started))

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
