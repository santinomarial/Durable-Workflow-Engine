import os

import pytest

from engine.persistence import create_pool, register_workflow_definition, start_workflow
from engine.persistence.migrations import migrate
from engine.runtime import DefinitionRegistry
from engine.runtime.definitions import UnknownDefinitionError
from engine.runtime.serialization import JSONValue
from engine.sdk import WorkflowContext, workflow
from engine.workers import run_workflow_task

DATABASE_URL = os.environ.get("DWE_TEST_DATABASE_URL")

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        DATABASE_URL is None,
        reason="DWE_TEST_DATABASE_URL is required for PostgreSQL integration tests",
    ),
]


@workflow(version=7, name="version-routed")
async def routed_workflow(ctx: WorkflowContext, value: JSONValue) -> JSONValue:
    del ctx
    return value


async def test_worker_releases_task_when_pinned_version_is_unsupported() -> None:
    assert DATABASE_URL is not None
    await migrate(DATABASE_URL)
    pool = await create_pool(DATABASE_URL)
    empty_registry = DefinitionRegistry()
    try:
        await register_workflow_definition(pool, routed_workflow)
        started = await start_workflow(
            pool,
            workflow_type=routed_workflow.name,
            definition_version=routed_workflow.version,
            workflow_input="input",
            queue_name="version-routing-queue",
        )

        with pytest.raises(UnknownDefinitionError, match="version 7"):
            await run_workflow_task(pool, empty_registry, queue_name="version-routing-queue")

        async with pool.acquire() as connection:
            task = await connection.fetchrow(
                """
                select status, lease_token, visible_at > created_at as delayed
                from tasks where workflow_id = $1 and task_type = 'workflow'
                """,
                started.workflow_id,
            )
            history_count = await connection.fetchval(
                "select count(*) from history_events where workflow_id = $1",
                started.workflow_id,
            )
        assert task is not None
        assert task["status"] == "pending"
        assert task["lease_token"] is None
        assert task["delayed"] is True
        assert history_count == 1
    finally:
        await pool.close()
