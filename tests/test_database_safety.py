"""Guardrails for destructive integration test setup."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from tests.conftest import _test_database_name, pytest_sessionstart


def test_test_database_name_rejects_non_postgres_urls() -> None:
    with pytest.raises(pytest.UsageError, match="PostgreSQL URL"):
        _test_database_name("sqlite:///durable_test")
    with pytest.raises(pytest.UsageError, match="name a database"):
        _test_database_name("postgresql://localhost/")


def test_test_database_name_decodes_path() -> None:
    assert _test_database_name("postgresql://localhost/durable%5Ftest") == "durable_test"


def test_session_rejects_application_database() -> None:
    with (
        patch.dict("os.environ", {"DWE_TEST_DATABASE_URL": "postgresql://localhost/durable"}),
        pytest.raises(pytest.UsageError, match="dedicated database"),
    ):
        pytest_sessionstart(None)  # type: ignore[arg-type]


def test_session_rejects_shared_database() -> None:
    url = "postgresql://localhost/durable_test"
    with (
        patch.dict("os.environ", {"DWE_TEST_DATABASE_URL": url, "DATABASE_URL": url}),
        pytest.raises(pytest.UsageError, match="different databases"),
    ):
        pytest_sessionstart(None)  # type: ignore[arg-type]


def test_session_accepts_dedicated_database() -> None:
    with patch.dict(
        "os.environ",
        {
            "DWE_TEST_DATABASE_URL": "postgresql://localhost/durable_test",
            "DATABASE_URL": "postgresql://localhost/durable",
        },
    ):
        pytest_sessionstart(None)  # type: ignore[arg-type]
