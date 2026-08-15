"""Forward-only PostgreSQL migration runner."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

import asyncpg  # type: ignore[import-untyped]

MIGRATION_LOCK_ID = 1_196_643_087
_PACKAGED_MIGRATIONS_DIR = Path(__file__).resolve().parents[1] / "_assets" / "migrations"
_SOURCE_MIGRATIONS_DIR = Path(__file__).resolve().parents[2] / "migrations"
DEFAULT_MIGRATIONS_DIR = (
    _PACKAGED_MIGRATIONS_DIR if _PACKAGED_MIGRATIONS_DIR.exists() else _SOURCE_MIGRATIONS_DIR
)


class MigrationError(RuntimeError):
    """Raised when migration history is inconsistent with the local files."""


class Connection(Protocol):
    async def execute(self, query: str, *args: object) -> str: ...

    async def fetch(self, query: str, *args: object) -> list[asyncpg.Record]: ...

    def transaction(self) -> asyncpg.Transaction: ...


@dataclass(frozen=True, slots=True)
class Migration:
    version: str
    name: str
    checksum: str
    sql: str


def discover_migrations(directory: Path = DEFAULT_MIGRATIONS_DIR) -> tuple[Migration, ...]:
    """Load migrations in filename order and reject duplicate versions."""
    migrations: list[Migration] = []
    versions: set[str] = set()
    for path in sorted(directory.glob("*.sql")):
        version, separator, name = path.stem.partition("_")
        if not separator or not version.isdigit() or not name:
            raise MigrationError(f"invalid migration filename: {path.name}")
        if version in versions:
            raise MigrationError(f"duplicate migration version: {version}")
        sql = path.read_text(encoding="utf-8")
        migrations.append(
            Migration(
                version=version,
                name=name,
                checksum=hashlib.sha256(sql.encode()).hexdigest(),
                sql=sql,
            )
        )
        versions.add(version)
    if not migrations:
        raise MigrationError(f"no migrations found in {directory}")
    return tuple(migrations)


async def apply_migrations(connection: Connection, migrations: tuple[Migration, ...]) -> None:
    """Apply pending migrations atomically while holding a database advisory lock."""
    await connection.execute(
        """
        create table if not exists schema_migrations (
          version text primary key,
          name text not null,
          checksum text not null,
          applied_at timestamptz not null default now()
        )
        """
    )
    await connection.execute("select pg_advisory_lock($1)", MIGRATION_LOCK_ID)
    try:
        applied_rows = await connection.fetch(
            "select version, checksum from schema_migrations order by version"
        )
        applied = {row["version"]: row["checksum"] for row in applied_rows}
        local_versions = {migration.version for migration in migrations}
        unknown = applied.keys() - local_versions
        if unknown:
            raise MigrationError(f"database contains unknown migrations: {sorted(unknown)}")

        for migration in migrations:
            existing_checksum = applied.get(migration.version)
            if existing_checksum is not None:
                if existing_checksum != migration.checksum:
                    raise MigrationError(f"migration {migration.version} checksum changed")
                continue
            async with connection.transaction():
                await connection.execute(migration.sql)
                await connection.execute(
                    """
                    insert into schema_migrations (version, name, checksum)
                    values ($1, $2, $3)
                    """,
                    migration.version,
                    migration.name,
                    migration.checksum,
                )
    finally:
        await connection.execute("select pg_advisory_unlock($1)", MIGRATION_LOCK_ID)


async def migrate(database_url: str, directory: Path = DEFAULT_MIGRATIONS_DIR) -> None:
    connection = await asyncpg.connect(database_url)
    try:
        await apply_migrations(connection, discover_migrations(directory))
    finally:
        await connection.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--database-url",
        default=os.environ.get("DATABASE_URL"),
        help="PostgreSQL URL (defaults to DATABASE_URL)",
    )
    args = parser.parse_args()
    if not args.database_url:
        parser.error("--database-url or DATABASE_URL is required")
    asyncio.run(migrate(args.database_url))


if __name__ == "__main__":
    main()
