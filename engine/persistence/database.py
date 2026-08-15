"""Database connection pool construction."""

from __future__ import annotations

from collections.abc import Mapping
from contextlib import AbstractAsyncContextManager
from typing import Protocol, cast

import asyncpg  # type: ignore[import-untyped]

from engine.config import DatabaseConfig


class Connection(Protocol):
    async def execute(self, query: str, *args: object) -> str: ...

    async def fetch(self, query: str, *args: object) -> list[Mapping[str, object]]: ...

    async def fetchrow(self, query: str, *args: object) -> Mapping[str, object] | None: ...

    async def fetchval(self, query: str, *args: object) -> object: ...

    def transaction(self) -> AbstractAsyncContextManager[object]: ...


class Pool(Protocol):
    def acquire(self) -> AbstractAsyncContextManager[Connection]: ...

    async def close(self) -> None: ...


async def create_pool(
    database_url: str,
    *,
    min_size: int = 1,
    max_size: int = 10,
    command_timeout: float = 30,
    statement_timeout_ms: int = 30_000,
    application_name: str = "durable-workflow-engine",
) -> Pool:
    pool = await asyncpg.create_pool(
        database_url,
        min_size=min_size,
        max_size=max_size,
        command_timeout=command_timeout,
        server_settings={
            "application_name": application_name,
            "statement_timeout": str(statement_timeout_ms),
            "idle_in_transaction_session_timeout": "30000",
        },
    )
    if pool is None:
        raise RuntimeError("asyncpg returned no connection pool")
    return cast(Pool, pool)


async def create_configured_pool(config: DatabaseConfig) -> Pool:
    return await create_pool(
        config.url,
        min_size=config.min_pool_size,
        max_size=config.max_pool_size,
        command_timeout=config.command_timeout_seconds,
        statement_timeout_ms=config.statement_timeout_ms,
        application_name=config.application_name,
    )
