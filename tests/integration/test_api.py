import os

import pytest
from httpx import ASGITransport, AsyncClient

from engine.api import create_app
from engine.persistence import create_pool, register_workflow_definition
from engine.persistence.migrations import migrate
from engine.runtime.serialization import JSONValue
from engine.sdk import WorkflowContext, workflow

DATABASE_URL = os.environ.get("DWE_TEST_DATABASE_URL")

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        DATABASE_URL is None,
        reason="DWE_TEST_DATABASE_URL is required for PostgreSQL integration tests",
    ),
]


@workflow(version=1, name="api-e2e")
async def api_workflow(ctx: WorkflowContext, value: JSONValue) -> JSONValue:
    await ctx.wait_signal("continue")
    return value


async def test_api_controls_and_inspects_workflow() -> None:
    assert DATABASE_URL is not None
    await migrate(DATABASE_URL)
    pool = await create_pool(DATABASE_URL)
    await register_workflow_definition(pool, api_workflow)
    app = create_app(pool)
    transport = ASGITransport(app=app)
    try:
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            health = await client.get("/api/health")
            assert health.json() == {"status": "ok"}
            assert health.headers["cache-control"] == "no-store"
            assert health.headers["x-content-type-options"] == "nosniff"
            assert health.headers["x-frame-options"] == "DENY"
            assert health.headers["x-request-id"]
            page = await client.get("/")
            assert page.status_code == 200
            assert "Execution observatory" in page.text
            assert "frame-ancestors 'none'" in page.headers["content-security-policy"]

            stats = await client.get("/api/stats")
            assert set(stats.json()) == {
                "total",
                "running",
                "completed",
                "failed",
                "terminated",
            }

            started = await client.post(
                "/api/workflows",
                json={
                    "workflow_type": api_workflow.name,
                    "definition_version": 1,
                    "input": {"request": 42},
                    "queue_name": "api-queue",
                },
            )
            assert started.status_code == 201
            workflow_id = started.json()["workflow_id"]
            listing = await client.get("/api/workflows", params={"status": "running"})
            assert workflow_id in {item["id"] for item in listing.json()}
            detail = await client.get(f"/api/workflows/{workflow_id}")
            assert detail.json()["input"] == {"request": 42}

            signal = await client.post(
                f"/api/workflows/{workflow_id}/signals",
                json={"signal_id": "api-signal", "name": "continue", "payload": True},
            )
            duplicate = await client.post(
                f"/api/workflows/{workflow_id}/signals",
                json={"signal_id": "api-signal", "name": "continue", "payload": False},
            )
            assert signal.json() == {"accepted": True}
            assert duplicate.json() == {"accepted": False}

            terminated = await client.post(
                f"/api/workflows/{workflow_id}/terminate",
                json={"reason": "API test"},
            )
            assert terminated.json() == {"accepted": True}
            history = await client.get(f"/api/workflows/{workflow_id}/history")
            assert [event["event_type"] for event in history.json()] == [
                "WorkflowExecutionStarted",
                "SignalReceived",
                "WorkflowExecutionTerminated",
            ]

            cancel_started = await client.post(
                "/api/workflows",
                json={
                    "workflow_type": api_workflow.name,
                    "definition_version": 1,
                    "queue_name": "api-cancel-queue",
                },
            )
            cancel_id = cancel_started.json()["workflow_id"]
            cancellation = await client.post(
                f"/api/workflows/{cancel_id}/cancel",
                json={"reason": "API cancellation test"},
            )
            duplicate_cancellation = await client.post(
                f"/api/workflows/{cancel_id}/cancel",
                json={"reason": "duplicate"},
            )
            assert cancellation.json() == {"accepted": True}
            assert duplicate_cancellation.json() == {"accepted": False}
            cancelled_detail = await client.get(f"/api/workflows/{cancel_id}")
            assert cancelled_detail.json()["cancellation_reason"] == "API cancellation test"

            missing_definition = await client.post(
                "/api/workflows",
                json={"workflow_type": "missing", "definition_version": 1},
            )
            assert missing_definition.status_code == 409
    finally:
        await pool.close()
