"""Atomic activity timeout detection, history recording, and retry creation."""

from __future__ import annotations

import json
import random
from datetime import UTC, datetime, timedelta
from typing import cast
from uuid import uuid4

from engine.persistence.database import Pool
from engine.persistence.transitions import retry_delay
from engine.runtime.serialization import JSONValue, canonical_json


def _json_object(value: object) -> dict[str, JSONValue]:
    if not isinstance(value, str):
        raise TypeError("activity task input must be PostgreSQL JSON text")
    decoded = json.loads(value)
    if not isinstance(decoded, dict):
        raise TypeError("activity task input must be an object")
    return cast(dict[str, JSONValue], decoded)


async def process_activity_timeout(
    pool: Pool,
    *,
    queue_name: str | None = None,
    random_value: float | None = None,
) -> bool:
    """Process at most one due activity timeout and report whether one was found."""
    selected_random = random.random() if random_value is None else random_value
    async with pool.acquire() as connection, connection.transaction():
        task = await connection.fetchrow(
            """
            select t.*,
              case
                when t.status = 'pending' then 'schedule_to_start'
                when t.start_to_close_deadline is not null
                  and t.start_to_close_deadline <= now() then 'start_to_close'
                when t.heartbeat_timeout is not null
                  and t.heartbeat_at + t.heartbeat_timeout <= now() then 'heartbeat'
                else 'lease_expired'
              end as timeout_type
            from tasks t
            join workflow_executions e on e.id = t.workflow_id
            where t.task_type = 'activity'
              and e.status = 'running'
              and ($1::text is null or t.queue_name = $1)
              and (
                (t.status = 'pending' and t.schedule_to_start_deadline is not null
                  and t.schedule_to_start_deadline <= now())
                or
                (t.status = 'leased' and (
                  t.lease_expires_at <= now()
                  or (t.start_to_close_deadline is not null
                    and t.start_to_close_deadline <= now())
                  or (t.heartbeat_timeout is not null
                    and t.heartbeat_at + t.heartbeat_timeout <= now())
                ))
              )
            order by coalesce(
              t.schedule_to_start_deadline,
              t.start_to_close_deadline,
              t.heartbeat_at + t.heartbeat_timeout,
              t.lease_expires_at
            ), t.id
            for update of t skip locked
            limit 1
            """,
            queue_name,
        )
        if task is None:
            return False

        execution = await connection.fetchrow(
            """
            select status, next_seq
            from workflow_executions where id = $1 for update
            """,
            task["workflow_id"],
        )
        if execution is None or execution["status"] != "running":
            return False

        task_input = _json_object(task["input"])
        policy_value = task_input.get("retry_policy")
        if not isinstance(policy_value, dict):
            raise TypeError("activity task lacks its recorded retry policy")
        policy = policy_value
        max_attempts = policy.get("max_attempts")
        if not isinstance(max_attempts, int):
            raise TypeError("activity retry policy has invalid max_attempts")
        attempt = cast(int, task["attempt"])
        is_final = attempt >= max_attempts
        delay = (
            timedelta(0)
            if is_final
            else retry_delay(
                policy,
                failed_attempt=attempt,
                random_value=selected_random,
            )
        )
        next_visible_at = datetime.now(UTC) + delay
        timeout_type = cast(str, task["timeout_type"])
        failure: dict[str, JSONValue] = {
            "type": "ActivityTimeout",
            "message": f"activity attempt timed out: {timeout_type}",
            "timeout_type": timeout_type,
        }
        next_seq = cast(int, execution["next_seq"])
        await connection.execute(
            """
            insert into history_events (
              workflow_id, seq, event_type, entity_id, attributes
            ) values ($1, $2, 'ActivityTimedOut', $3, $4::jsonb)
            """,
            task["workflow_id"],
            next_seq,
            task["entity_id"],
            canonical_json(
                {
                    "attempt": attempt,
                    "failure": failure,
                    "final": is_final,
                    "timeout_type": timeout_type,
                    "next_visible_at": next_visible_at.isoformat() if not is_final else None,
                }
            ),
        )
        await connection.execute(
            """
            update tasks
            set status = $2::task_status, completed_at = now()
            where id = $1
            """,
            task["id"],
            "dead" if is_final else "completed",
        )
        if is_final:
            await connection.execute(
                """
                insert into tasks (id, workflow_id, task_type, queue_name)
                values ($1, $2, 'workflow', $3)
                """,
                uuid4(),
                task["workflow_id"],
                task["queue_name"],
            )
        else:
            schedule_timeout = task["schedule_to_start_timeout"]
            await connection.execute(
                """
                insert into tasks (
                  id, workflow_id, task_type, queue_name, entity_id, command_id,
                  attempt, input, visible_at, schedule_to_start_deadline,
                  schedule_to_start_timeout, start_to_close_timeout, heartbeat_timeout
                ) values (
                  $1, $2, 'activity', $3, $4, $5, $6, $7::jsonb, $8,
                  case when $9::interval is null then null else $8::timestamptz + $9 end,
                  $9, $10, $11
                )
                """,
                uuid4(),
                task["workflow_id"],
                task["queue_name"],
                task["entity_id"],
                task["command_id"],
                attempt + 1,
                task["input"],
                next_visible_at,
                schedule_timeout,
                task["start_to_close_timeout"],
                task["heartbeat_timeout"],
            )
        await connection.execute(
            "update workflow_executions set next_seq = $2 where id = $1",
            task["workflow_id"],
            next_seq + 1,
        )
    return True
