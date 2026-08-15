"""Canonical JSON serialization used by deterministic workflow commands."""

from __future__ import annotations

import hashlib
import json
from typing import cast

type JSONValue = bool | int | float | str | list[JSONValue] | dict[str, JSONValue] | None


class SerializationError(ValueError):
    """Raised when workflow data cannot be represented as canonical JSON."""


def canonical_json(value: JSONValue) -> str:
    try:
        return json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
    except (TypeError, ValueError) as error:
        raise SerializationError(f"value is not canonical JSON: {error}") from error


def fingerprint(value: JSONValue) -> str:
    return hashlib.sha256(canonical_json(value).encode()).hexdigest()


def clone_json(value: JSONValue) -> JSONValue:
    """Detach mutable user values and validate their JSON representation."""
    return cast(JSONValue, json.loads(canonical_json(value)))
