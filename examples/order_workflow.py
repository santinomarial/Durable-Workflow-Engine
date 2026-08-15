"""Representative order workflow used by the README quickstart."""

from __future__ import annotations

from datetime import timedelta

from engine.runtime import DefinitionRegistry
from engine.runtime.serialization import JSONValue
from engine.sdk import RetryPolicy, WorkflowContext, activity, current_activity_context, workflow


@activity(name="charge-card")
async def charge_card(order: JSONValue) -> JSONValue:
    return {
        "status": "charged",
        "order": order,
        "idempotency_key": current_activity_context().idempotency_key,
    }


@activity(name="reserve-inventory")
async def reserve_inventory(order: JSONValue) -> JSONValue:
    return {"status": "reserved", "order": order}


@activity(name="book-courier")
async def book_courier(order: JSONValue) -> JSONValue:
    return {"status": "booked", "order": order}


@workflow(version=1, name="order-fulfillment")
async def order_fulfillment(ctx: WorkflowContext, order: JSONValue) -> JSONValue:
    charge = await ctx.activity(
        charge_card,
        order,
        retry=RetryPolicy(max_attempts=5, initial_interval=timedelta(seconds=1)),
        start_to_close=timedelta(seconds=30),
    )
    await ctx.sleep(timedelta(seconds=1))
    inventory, courier = await ctx.gather(
        ctx.activity(reserve_inventory, order),
        ctx.activity(book_courier, order),
    )
    confirmation = await ctx.wait_signal("delivery_confirmed")
    return {
        "charge": charge,
        "inventory": inventory,
        "courier": courier,
        "confirmation": confirmation,
        "workflow_time": ctx.now().isoformat(),
        "confirmation_id": str(ctx.uuid()),
    }


registry = DefinitionRegistry()
registry.register_workflow(order_fulfillment)
registry.register_activity(charge_card)
registry.register_activity(reserve_inventory)
registry.register_activity(book_courier)
