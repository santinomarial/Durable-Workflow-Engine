"""Refuse to run database-backed tests against an application database."""

from __future__ import annotations

import os
from urllib.parse import unquote, urlsplit

import pytest


def _test_database_name(url: str) -> str:
    parsed = urlsplit(url)
    if parsed.scheme not in {"postgres", "postgresql"} or not parsed.hostname:
        raise pytest.UsageError("DWE_TEST_DATABASE_URL must be a PostgreSQL URL")
    name = unquote(parsed.path.removeprefix("/"))
    if not name or "/" in name:
        raise pytest.UsageError("DWE_TEST_DATABASE_URL must name a database")
    return name


def pytest_sessionstart(session: pytest.Session) -> None:
    del session
    url = os.environ.get("DWE_TEST_DATABASE_URL")
    if not url:
        return
    name = _test_database_name(url)
    if not (name.endswith("_test") or name.startswith("test_")):
        raise pytest.UsageError(
            "DWE_TEST_DATABASE_URL must use a dedicated database named "
            "*_test or test_*; never run these tests against the app database"
        )
    app_url = os.environ.get("DATABASE_URL")
    if app_url and _test_database_name(app_url) == name:
        raise pytest.UsageError(
            "DWE_TEST_DATABASE_URL and DATABASE_URL must name different databases"
        )
