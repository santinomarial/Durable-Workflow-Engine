"""Lease one task and terminate the process at a controlled failure window."""

from __future__ import annotations

import asyncio
import json
import os
import signal
import sys
from dataclasses import asdict
from typing import Literal, cast

from engine.persistence import create_pool, lease_task, record_idempotent_effect


async def kill_after_lease(mode: Literal["workflow", "activity"], queue_name: str) -> None:
    database_url = os.environ["DWE_TEST_DATABASE_URL"]
    pool = await create_pool(database_url)
    task = await lease_task(pool, task_type=mode, queue_name=queue_name)
    if task is None:
        raise RuntimeError(f"no {mode} task was available")
    if mode == "activity":
        if not isinstance(task.input, dict):
            raise TypeError("activity input is malformed")
        key = task.input.get("idempotency_key")
        if not isinstance(key, str):
            raise TypeError("activity has no idempotency key")
        await record_idempotent_effect(
            pool,
            idempotency_key=key,
            payload=task.input.get("input"),
        )
    print(json.dumps(asdict(task), default=str, sort_keys=True), flush=True)
    os.kill(os.getpid(), signal.SIGKILL)


def main() -> None:
    if len(sys.argv) != 3 or sys.argv[1] not in ("workflow", "activity"):
        raise SystemExit("usage: python -m tests.chaos.kill_worker MODE QUEUE")
    asyncio.run(
        kill_after_lease(
            cast(Literal["workflow", "activity"], sys.argv[1]),
            sys.argv[2],
        )
    )


if __name__ == "__main__":
    main()
