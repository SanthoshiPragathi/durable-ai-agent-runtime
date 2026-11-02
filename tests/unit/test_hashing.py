import pytest

from app.engine.errors import StepSerializationError
from app.engine.hashing import canonical_json, hash_step_input, normalize_json


def test_hash_ignores_dict_key_order() -> None:
    a = hash_step_input((), {"x": 1, "y": {"b": 2, "a": 1}})
    b = hash_step_input((), {"y": {"a": 1, "b": 2}, "x": 1})
    assert a == b


def test_hash_changes_when_input_changes() -> None:
    assert hash_step_input((2000,), {}) != hash_step_input((3000,), {})
    assert hash_step_input((1,), {}) != hash_step_input((), {"x": 1})


def test_hash_is_64_hex_chars() -> None:
    digest = hash_step_input(("a",), {})
    assert len(digest) == 64 and int(digest, 16) >= 0


@pytest.mark.parametrize("bad", [object(), {1, 2}, float("nan"), float("inf")])
def test_non_json_values_are_rejected(bad: object) -> None:
    with pytest.raises(StepSerializationError):
        canonical_json(bad)
    with pytest.raises(StepSerializationError):
        normalize_json(bad)


def test_normalize_makes_first_run_match_replay() -> None:
    # A tuple would come back from PostgreSQL as a list, so it is a list on the first run too.
    assert normalize_json({"pair": (1, 2)}) == {"pair": [1, 2]}
    assert normalize_json(None) is None
