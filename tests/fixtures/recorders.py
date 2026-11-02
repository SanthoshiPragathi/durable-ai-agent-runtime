"""Test doubles shared by unit tests."""

from typing import Any


class FakeRecorder:
    """Collects what the context reports, in order.

    `calls` holds (kind, step_name, detail) tuples: started/completed/failed/unknown/retry.
    `keys` remembers the idempotency key passed with each call, `reconciled` the steps that were
    completed by reconciliation instead of by running.
    """

    def __init__(self) -> None:
        self.calls: list[tuple[str, str, Any]] = []
        self.keys: dict[str, str | None] = {}
        self.reconciled: list[str] = []

    async def step_started(
        self, step_name: str, input_hash: str, idempotency_key: str | None = None
    ) -> None:
        self.keys[step_name] = idempotency_key
        self.calls.append(("started", step_name, input_hash))

    async def step_completed(
        self,
        step_name: str,
        result: Any,
        *,
        idempotency_key: str | None = None,
        reconciled: bool = False,
    ) -> None:
        if reconciled:
            self.reconciled.append(step_name)
        self.calls.append(("completed", step_name, result))

    async def step_failed(
        self, step_name: str, error: BaseException, *, idempotency_key: str | None = None
    ) -> None:
        self.calls.append(("failed", step_name, type(error).__name__))

    async def step_outcome_unknown(
        self,
        step_name: str,
        reason: str,
        *,
        idempotency_key: str | None = None,
        already_recorded: bool = False,
    ) -> None:
        self.calls.append(("unknown_replayed" if already_recorded else "unknown", step_name, reason))

    async def effect_attempt(self, step_name: str, idempotency_key: str) -> None:
        self.calls.append(("retry", step_name, idempotency_key))
