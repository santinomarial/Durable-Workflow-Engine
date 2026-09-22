"""Idempotently populate the local-only showcase with inspectable executions."""

from __future__ import annotations

import asyncio
import json
from uuid import UUID, uuid5

from engine.config import DatabaseConfig
from engine.persistence import (
    Pool,
    create_configured_pool,
    get_execution,
    pause_workflow,
    register_workflow_definition,
    send_signal,
    start_workflow,
)
from engine.persistence.migrations import migrate
from examples.demo_workflows import registry

DEMO_NAMESPACE = UUID("ac0982f7-3063-4f26-a163-048a0a10bca2")
SCENARIOS = ("awaiting-confirmation", "completed-order", "paused-order", "declined-payment")
QUEUE = "demo"


def scenario_id(name: str) -> UUID:
    return uuid5(DEMO_NAMESPACE, name)


async def seed_demo(pool: Pool) -> dict[str, str]:
    """Create each scenario once; a second invocation leaves histories unchanged."""
    for definition in registry.workflows:
        await register_workflow_definition(pool, definition)

    for name in SCENARIOS:
        workflow_id = scenario_id(name)
        if await get_execution(pool, workflow_id) is not None:
            continue
        is_declined = name == "declined-payment"
        await start_workflow(
            pool,
            workflow_type="payment-review" if is_declined else "order-fulfillment",
            definition_version=1,
            workflow_input=(
                {"order_id": "DEMO-104", "approved": False}
                if is_declined
                else {"order_id": f"DEMO-{name.upper()}"}
            ),
            queue_name=QUEUE,
            workflow_id=workflow_id,
            search_attributes={"demo_scenario": name, "source": "local-showcase"},
        )
        if name == "completed-order":
            await send_signal(
                pool,
                workflow_id=workflow_id,
                signal_id="demo-delivery-confirmed",
                name="delivery_confirmed",
                payload={"received": True, "source": "demo"},
            )
        if name == "paused-order":
            await pause_workflow(
                pool,
                workflow_id=workflow_id,
                reason="Demo: inspect the paused execution, then resume it",
            )
    return {name: str(scenario_id(name)) for name in SCENARIOS}


async def main() -> None:
    config = DatabaseConfig.from_env(application_name="dwe-demo-seed")
    await migrate(config.url)
    pool = await create_configured_pool(config)
    try:
        print(json.dumps(await seed_demo(pool), sort_keys=True))
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
