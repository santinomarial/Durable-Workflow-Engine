from pathlib import Path

import pytest

from engine.config import DatabaseConfig, secret_value


def test_secret_file_is_trimmed_and_direct_value_is_rejected(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    secret = tmp_path / "database-url"
    secret.write_text("postgresql://database/example\n", encoding="utf-8")
    monkeypatch.setenv("DATABASE_URL_FILE", str(secret))

    assert secret_value("DATABASE_URL", required=True) == "postgresql://database/example"

    monkeypatch.setenv("DATABASE_URL", "postgresql://ambiguous/example")
    with pytest.raises(RuntimeError, match="only one"):
        secret_value("DATABASE_URL")


def test_database_pool_configuration_is_validated(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql://database/example")
    monkeypatch.setenv("DWE_DB_POOL_MIN_SIZE", "5")
    monkeypatch.setenv("DWE_DB_POOL_MAX_SIZE", "4")

    with pytest.raises(RuntimeError, match="MAX_SIZE"):
        DatabaseConfig.from_env()
