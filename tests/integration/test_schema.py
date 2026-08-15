import os
from uuid import uuid4

import asyncpg
import pytest

from engine.persistence.migrations import discover_migrations, migrate

DATABASE_URL = os.environ.get("DWE_TEST_DATABASE_URL")

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        DATABASE_URL is None,
        reason="DWE_TEST_DATABASE_URL is required for PostgreSQL integration tests",
    ),
]


async def test_initial_schema_and_migrations_are_enforced() -> None:
    assert DATABASE_URL is not None
    await migrate(DATABASE_URL)
    await migrate(DATABASE_URL)

    connection = await asyncpg.connect(DATABASE_URL)
    try:
        applied = await connection.fetch(
            "select version, checksum from schema_migrations order by version"
        )
        assert [(row["version"], row["checksum"]) for row in applied] == [
            (migration.version, migration.checksum) for migration in discover_migrations()
        ]

        await connection.execute(
            """
            insert into workflow_definitions (workflow_type, version, code_hash)
            values ('schema-test', 1, 'abc123')
            """
        )
        with pytest.raises(asyncpg.RaiseError, match="workflow definitions are immutable"):
            await connection.execute(
                """
                update workflow_definitions
                set code_hash = 'changed'
                where workflow_type = 'schema-test' and version = 1
                """
            )

        workflow_id = uuid4()
        await connection.execute(
            """
            insert into workflow_executions (
              id, workflow_type, definition_version, input
            ) values ($1, 'schema-test', 1, '{}'::jsonb)
            """,
            workflow_id,
        )
        await connection.execute(
            """
            insert into history_events (
              workflow_id, seq, event_type, command_id, attributes
            ) values ($1, 1, 'ActivityScheduled', 0, '{}'::jsonb)
            """,
            workflow_id,
        )
        with pytest.raises(asyncpg.UniqueViolationError):
            await connection.execute(
                """
                insert into history_events (
                  workflow_id, seq, event_type, command_id, attributes
                ) values ($1, 2, 'ActivityScheduled', 0, '{}'::jsonb)
                """,
                workflow_id,
            )
    finally:
        await connection.close()
