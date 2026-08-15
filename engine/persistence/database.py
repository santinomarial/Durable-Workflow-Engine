"""Database connection pool construction."""

from __future__ import annotations

from collections.abc import Mapping
from contextlib import AbstractAsyncContextManager
from typing import Protocol, cast

import asyncpg  # type: ignore[import-untyped]


class Connection(Protocol):
    async def execute(self, query: str, *args: object) -> str: ...

    async def fetch(self, query: str, *args: object) -> list[Mapping[str, object]]: ...

    async def fetchrow(self, query: str, *args: object) -> Mapping[str, object] | None: ...

    async def fetchval(self, query: str, *args: object) -> object: ...

    def transaction(self) -> AbstractAsyncContextManager[object]: ...


class Pool(Protocol):
    def acquire(self) -> AbstractAsyncContextManager[Connection]: ...

    async def close(self) -> None: ...


async def create_pool(database_url: str, *, min_size: int = 1, max_size: int = 10) -> Pool:
    pool = await asyncpg.create_pool(database_url, min_size=min_size, max_size=max_size)
    if pool is None:
        raise RuntimeError("asyncpg returned no connection pool")
    return cast(Pool, pool)
