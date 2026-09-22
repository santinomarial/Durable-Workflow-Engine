"""The local showcase is idempotent and produces inspectable histories."""

from __future__ import annotations

import os

import pytest

from engine.persistence import create_pool, get_execution, get_history
from engine.persistence.migrations import migrate
from examples.seed_demo import SCENARIOS, scenario_id, seed_demo

DATABASE_URL = os.environ.get("DWE_TEST_DATABASE_URL")

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(DATABASE_URL is None, reason="DWE_TEST_DATABASE_URL is required"),
]


async def test_showcase_seed_is_repeatable() -> None:
    assert DATABASE_URL is not None
    await migrate(DATABASE_URL)
    pool = await create_pool(DATABASE_URL)
    try:
        first = await seed_demo(pool)
        first_histories = {name: await get_history(pool, scenario_id(name)) for name in SCENARIOS}
        second = await seed_demo(pool)
        assert first == second
        assert len(set(first.values())) == len(SCENARIOS)
        for name in SCENARIOS:
            assert first[name] == str(scenario_id(name))
            execution = await get_execution(pool, scenario_id(name))
            assert execution is not None
            assert execution.search_attributes["demo_scenario"] == name
            assert await get_history(pool, scenario_id(name)) == first_histories[name]
        completed_history = first_histories["completed-order"]
        assert any(event.event_type == "SignalReceived" for event in completed_history)
        paused = await get_execution(pool, scenario_id("paused-order"))
        assert paused is not None and paused.paused_at is not None
    finally:
        await pool.close()
