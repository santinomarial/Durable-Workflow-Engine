import json
import os

import pytest
from httpx import ASGITransport, AsyncClient

from engine.api import APIKey, AuthConfig, create_app
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
    admin_token = "integration-admin-token-value"
    viewer_token = "integration-viewer-token-value"
    auth = AuthConfig(
        keys=(
            APIKey.from_token("api-admin", "admin", admin_token),
            APIKey.from_token("api-viewer", "viewer", viewer_token),
        ),
        max_request_bytes=1024,
    )
    app = create_app(pool, auth=auth)
    transport = ASGITransport(app=app)
    try:
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            health = await client.get("/api/health")
            assert health.json()["status"] == "ready"
            assert health.json()["database"] == "ok"
            assert health.json()["schema_version"] == "0013"
            assert health.headers["cache-control"] == "no-store"
            assert health.headers["x-content-type-options"] == "nosniff"
            assert health.headers["x-frame-options"] == "DENY"
            assert health.headers["x-request-id"]
            liveness = await client.get("/api/health/live")
            assert liveness.json() == {"status": "alive"}
            readiness = await client.get("/api/health/ready")
            assert readiness.json()["status"] == "ready"
            page = await client.get("/")
            assert page.status_code == 200
            assert "Workflow operations" in page.text
            assert "Execution fleet" in page.text
            assert "frame-ancestors 'none'" in page.headers["content-security-policy"]
            script = await client.get("/static/app.js")
            assert script.status_code == 200
            assert script.headers["content-type"].startswith("text/javascript")
            assert script.headers["cache-control"] == "public, max-age=300, must-revalidate"
            assert "innerHTML" not in script.text
            stylesheet = await client.get("/static/styles.css")
            assert stylesheet.status_code == 200
            assert stylesheet.headers["content-type"].startswith("text/css")

            unauthenticated = await client.get("/api/stats")
            assert unauthenticated.status_code == 401
            assert unauthenticated.headers["www-authenticate"] == "Bearer"
            viewer_session = await client.get(
                "/api/session", headers={"Authorization": f"Bearer {viewer_token}"}
            )
            assert viewer_session.json() == {"key_id": "api-viewer", "role": "viewer"}
            viewer_write = await client.post(
                "/api/workflows",
                headers={"Authorization": f"Bearer {viewer_token}"},
                json={"workflow_type": api_workflow.name, "definition_version": 1},
            )
            assert viewer_write.status_code == 403
            viewer_metrics = await client.get(
                "/metrics", headers={"Authorization": f"Bearer {viewer_token}"}
            )
            assert viewer_metrics.status_code == 403
            too_large = await client.post(
                "/api/workflows",
                headers={"Authorization": f"Bearer {admin_token}"},
                content=b"x" * 1025,
            )
            assert too_large.status_code == 413

            client.headers["Authorization"] = f"Bearer {admin_token}"

            stats = await client.get("/api/stats")
            assert set(stats.json()) == {
                "total",
                "running",
                "completed",
                "failed",
                "terminated",
            }

            created_schedule = await client.post(
                "/api/schedules",
                json={
                    "name": "api-every-hour",
                    "workflow_type": api_workflow.name,
                    "definition_version": 1,
                    "input": {"scheduled": True},
                    "queue_name": "api-schedule-queue",
                    "cron_expression": "0 0 1 1 *",
                    "timezone": "UTC",
                    "overlap_policy": "buffer_one",
                },
            )
            assert created_schedule.status_code == 201
            schedule_id = created_schedule.json()["id"]
            schedules = await client.get("/api/schedules")
            assert schedule_id in {item["id"] for item in schedules.json()}
            paused_schedule = await client.post(f"/api/schedules/{schedule_id}/pause")
            resumed_schedule = await client.post(f"/api/schedules/{schedule_id}/resume")
            assert paused_schedule.json() == {"accepted": True}
            assert resumed_schedule.json() == {"accepted": True}
            assert (await client.get(f"/api/schedules/{schedule_id}/occurrences")).json() == []
            cleanup_pause = await client.post(f"/api/schedules/{schedule_id}/pause")
            assert cleanup_pause.json() == {"accepted": True}

            started = await client.post(
                "/api/workflows",
                json={
                    "workflow_type": api_workflow.name,
                    "definition_version": 1,
                    "input": {"request": 42},
                    "queue_name": "api-queue",
                    "search_attributes": {
                        "customer_id": "customer-42",
                        "priority": 9,
                        "region": "us-east",
                    },
                },
            )
            assert started.status_code == 201
            workflow_id = started.json()["workflow_id"]
            listing = await client.get("/api/workflows", params={"status": "running"})
            assert workflow_id in {item["id"] for item in listing.json()}
            detail = await client.get(f"/api/workflows/{workflow_id}")
            assert detail.json()["input"] == {"request": 42}
            assert detail.json()["search_attributes"]["customer_id"] == "customer-42"
            attribute_listing = await client.get(
                "/api/workflows",
                params={"attributes": json.dumps({"priority": 9})},
            )
            assert [item["id"] for item in attribute_listing.json()] == [workflow_id]
            text_listing = await client.get("/api/workflows", params={"query": "customer-42"})
            assert [item["id"] for item in text_listing.json()] == [workflow_id]
            changed_attributes = await client.patch(
                f"/api/workflows/{workflow_id}/search-attributes",
                json={"set": {"priority": 10, "team": "fulfillment"}, "unset": ["region"]},
            )
            assert changed_attributes.json()["search_attributes"] == {
                "customer_id": "customer-42",
                "priority": 10,
                "team": "fulfillment",
            }
            paused = await client.post(
                f"/api/workflows/{workflow_id}/pause", json={"reason": "investigating"}
            )
            duplicate_pause = await client.post(
                f"/api/workflows/{workflow_id}/pause", json={"reason": "duplicate"}
            )
            assert paused.json() == {"accepted": True}
            assert duplicate_pause.json() == {"accepted": False}
            paused_detail = await client.get(f"/api/workflows/{workflow_id}")
            assert paused_detail.json()["pause_reason"] == "investigating"
            resumed = await client.post(
                f"/api/workflows/{workflow_id}/resume", json={"reason": "fixed"}
            )
            duplicate_resume = await client.post(
                f"/api/workflows/{workflow_id}/resume", json={"reason": "duplicate"}
            )
            assert resumed.json() == {"accepted": True}
            assert duplicate_resume.json() == {"accepted": False}

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
            assert history.json()["next_after_seq"] is None
            assert [event["event_type"] for event in history.json()["items"]] == [
                "WorkflowExecutionStarted",
                "WorkflowExecutionPaused",
                "WorkflowExecutionResumed",
                "SignalReceived",
                "WorkflowExecutionTerminated",
            ]
            first_history_page = await client.get(
                f"/api/workflows/{workflow_id}/history", params={"limit": 1}
            )
            assert [item["seq"] for item in first_history_page.json()["items"]] == [1]
            assert first_history_page.json()["next_after_seq"] == 1
            second_history_page = await client.get(
                f"/api/workflows/{workflow_id}/history",
                params={"limit": 1, "after_seq": 1},
            )
            assert [item["seq"] for item in second_history_page.json()["items"]] == [2]
            history_tail = await client.get(
                f"/api/workflows/{workflow_id}/history-tail", params={"limit": 1}
            )
            assert [item["seq"] for item in history_tail.json()] == [5]

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

            dead_letter = await client.get("/api/dead-letter")
            assert any(item["workflow_id"] == workflow_id for item in dead_letter.json())
            retried = await client.post(f"/api/workflows/{workflow_id}/retry")
            assert retried.status_code == 201
            retry_id = retried.json()["workflow_id"]
            retry_detail = await client.get(f"/api/workflows/{retry_id}")
            assert retry_detail.json()["retry_of"] == workflow_id

            missing_definition = await client.post(
                "/api/workflows",
                json={"workflow_type": "missing", "definition_version": 1},
            )
            assert missing_definition.status_code == 409

            audit = await client.get("/api/audit")
            actions = [record["action"] for record in audit.json()]
            assert actions == [
                "workflow.retry",
                "workflow.cancel",
                "workflow.cancel",
                "workflow.start",
                "workflow.terminate",
                "workflow.signal",
                "workflow.signal",
                "workflow.resume",
                "workflow.resume",
                "workflow.pause",
                "workflow.pause",
                "workflow.search-attributes.update",
                "workflow.start",
                "schedule.pause",
                "schedule.resume",
                "schedule.pause",
                "schedule.create",
            ]
            assert all(record["actor_key_id"] == "api-admin" for record in audit.json())
            assert all(record["request_id"] for record in audit.json())

            metrics = await client.get("/metrics")
            assert metrics.status_code == 200
            assert metrics.headers["content-type"] == "text/plain; version=0.0.4; charset=utf-8"
            assert "dwe_http_requests_total" in metrics.text
            assert "dwe_tasks_pending" in metrics.text
            assert "dwe_workflows_running" in metrics.text
            workers = await client.get("/api/workers")
            assert workers.json() == []
    finally:
        await pool.close()
