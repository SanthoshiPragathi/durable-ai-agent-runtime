"""Stable hashing and JSON normalization for step inputs and results.

Phase 2 (replay) compares the hash of a step's current input with the hash recorded in the
event log to detect a changed workflow. For that to work the hash must not depend on dict key
order or Python object identity, only on the JSON content.
"""

import hashlib
import json
from typing import Any

from app.engine.errors import StepSerializationError


def canonical_json(value: Any) -> str:
    """Deterministic JSON: sorted keys, no whitespace, NaN/Infinity rejected."""
    try:
        return json.dumps(
            value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
        )
    except (TypeError, ValueError) as exc:
        raise StepSerializationError(f"value is not JSON-serializable: {exc}") from exc


def hash_step_input(args: tuple[Any, ...], kwargs: dict[str, Any]) -> str:
    """SHA-256 (hex) of the step's arguments."""
    payload = canonical_json({"args": list(args), "kwargs": kwargs})
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def normalize_json(value: Any) -> Any:
    """Round-trip a value through JSON.

    A step's first execution and a later replay must hand the workflow the *same* value. Replay
    reads the result back from PostgreSQL as plain JSON types, so the first execution returns
    the JSON-normalized value too (e.g. a tuple becomes a list in both cases).
    """
    try:
        return json.loads(json.dumps(value, allow_nan=False))
    except (TypeError, ValueError) as exc:
        raise StepSerializationError(f"value is not JSON-serializable: {exc}") from exc
