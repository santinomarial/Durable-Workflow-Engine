from pathlib import Path

import pytest

from engine.persistence.migrations import MigrationError, discover_migrations


def test_discovers_ordered_migrations() -> None:
    migrations = discover_migrations()

    assert [migration.version for migration in migrations] == ["0001", "0002", "0003", "0004"]
    assert migrations[0].name == "initial"
    assert "create table workflow_executions" in migrations[0].sql
    assert len(migrations[0].checksum) == 64


def test_rejects_invalid_migration_filename(tmp_path: Path) -> None:
    (tmp_path / "initial.sql").write_text("select 1;", encoding="utf-8")

    with pytest.raises(MigrationError, match="invalid migration filename"):
        discover_migrations(tmp_path)


def test_rejects_duplicate_migration_versions(tmp_path: Path) -> None:
    (tmp_path / "0001_first.sql").write_text("select 1;", encoding="utf-8")
    (tmp_path / "0001_second.sql").write_text("select 2;", encoding="utf-8")

    with pytest.raises(MigrationError, match="duplicate migration version"):
        discover_migrations(tmp_path)
