"""Cooperating idempotency ledger used by correctness tests and examples."""

from __future__ import annotations

from engine.persistence.database import Pool
from engine.runtime.serialization import JSONValue, canonical_json, clone_json


async def record_idempotent_effect(
    pool: Pool,
    *,
    idempotency_key: str,
    payload: JSONValue,
) -> bool:
    """Record one effect per key, returning false for repeated attempts."""
    if not idempotency_key:
        raise ValueError("idempotency_key cannot be empty")
    encoded_payload = canonical_json(clone_json(payload))
    async with pool.acquire() as connection:
        inserted = await connection.fetchval(
            """
            insert into effect_ledger (idempotency_key, payload)
            values ($1, $2::jsonb)
            on conflict (idempotency_key) do nothing
            returning true
            """,
            idempotency_key,
            encoded_payload,
        )
    return inserted is True
