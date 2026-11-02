"""Test doubles shared by unit tests."""

from typing import Any


class FakeRecorder:
    """Collects what ctx.step reports, in order."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, str, Any]] = []

    async def step_started(self, step_name: str, input_hash: str) -> None:
        self.calls.append(("started", step_name, input_hash))

    async def step_completed(self, step_name: str, result: Any) -> None:
        self.calls.append(("completed", step_name, result))

    async def step_failed(self, step_name: str, error: BaseException) -> None:
        self.calls.append(("failed", step_name, type(error).__name__))
