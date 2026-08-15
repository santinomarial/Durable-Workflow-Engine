"""Validated environment and secret-file configuration."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def secret_value(name: str, *, required: bool = False) -> str | None:
    """Read a value from NAME or NAME_FILE, rejecting ambiguous configuration."""
    direct = os.environ.get(name)
    file_name = os.environ.get(f"{name}_FILE")
    if direct is not None and file_name is not None:
        raise RuntimeError(f"configure only one of {name} and {name}_FILE")
    value: str | None
    if file_name is not None:
        try:
            value = Path(file_name).read_text(encoding="utf-8").strip()
        except OSError as error:
            raise RuntimeError(f"cannot read {name}_FILE: {error}") from error
    else:
        value = direct.strip() if direct is not None else None
    if required and not value:
        raise RuntimeError(f"{name} or {name}_FILE is required")
    return value


def positive_int(name: str, default: int) -> int:
    raw = os.environ.get(name, str(default))
    try:
        value = int(raw)
    except ValueError as error:
        raise RuntimeError(f"{name} must be an integer") from error
    if value < 1:
        raise RuntimeError(f"{name} must be positive")
    return value


def positive_float(name: str, default: float) -> float:
    raw = os.environ.get(name, str(default))
    try:
        value = float(raw)
    except ValueError as error:
        raise RuntimeError(f"{name} must be numeric") from error
    if value <= 0:
        raise RuntimeError(f"{name} must be positive")
    return value


@dataclass(frozen=True, slots=True)
class DatabaseConfig:
    url: str
    min_pool_size: int = 2
    max_pool_size: int = 10
    command_timeout_seconds: float = 30
    statement_timeout_ms: int = 30_000
    application_name: str = "durable-workflow-engine"

    @classmethod
    def from_env(
        cls,
        *,
        url: str | None = None,
        application_name: str = "durable-workflow-engine",
    ) -> DatabaseConfig:
        selected_url = url or secret_value("DATABASE_URL", required=True)
        assert selected_url is not None
        config = cls(
            url=selected_url,
            min_pool_size=positive_int("DWE_DB_POOL_MIN_SIZE", 2),
            max_pool_size=positive_int("DWE_DB_POOL_MAX_SIZE", 10),
            command_timeout_seconds=positive_float("DWE_DB_COMMAND_TIMEOUT_SECONDS", 30),
            statement_timeout_ms=positive_int("DWE_DB_STATEMENT_TIMEOUT_MS", 30_000),
            application_name=application_name,
        )
        if config.max_pool_size < config.min_pool_size:
            raise RuntimeError("DWE_DB_POOL_MAX_SIZE must be at least DWE_DB_POOL_MIN_SIZE")
        return config
