import pytest

from engine.runtime.serialization import SerializationError, canonical_json, clone_json, fingerprint


def test_canonical_json_has_stable_mapping_order() -> None:
    left = {"b": [2, 1], "a": {"nested": True}}
    right = {"a": {"nested": True}, "b": [2, 1]}

    assert canonical_json(left) == canonical_json(right)
    assert fingerprint(left) == fingerprint(right)


def test_clone_json_detaches_mutable_values() -> None:
    value = {"items": [1, 2]}

    cloned = clone_json(value)
    value["items"].append(3)

    assert cloned == {"items": [1, 2]}


def test_rejects_non_finite_numbers() -> None:
    with pytest.raises(SerializationError, match="Out of range float values"):
        canonical_json(float("nan"))
