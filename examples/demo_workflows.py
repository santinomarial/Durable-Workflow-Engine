"""Additional scenarios for the isolated local showcase."""

from __future__ import annotations

from engine.runtime import DefinitionRegistry
from engine.runtime.serialization import JSONValue
from engine.sdk import WorkflowContext, workflow
from examples.order_workflow import registry as order_registry


@workflow(version=1, name="payment-review")
async def payment_review(ctx: WorkflowContext, request: JSONValue) -> JSONValue:
    checked_at = ctx.now().isoformat()
    if not isinstance(request, dict) or request.get("approved") is not False:
        raise ValueError("demo review requires an explicitly declined payment")
    raise ValueError(f"Payment declined by demo risk review at {checked_at}")


registry = DefinitionRegistry()
for definition in order_registry.workflows:
    registry.register_workflow(definition)
for activity_definition in order_registry.activities:
    registry.register_activity(activity_definition)
registry.register_workflow(payment_review)
