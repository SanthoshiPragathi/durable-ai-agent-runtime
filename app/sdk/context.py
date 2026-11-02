"""The context object handed to every workflow.

The SDK knows nothing about databases. ctx.step() reports to a StepRecorder, which the engine
implements on top of PostgreSQL (engine/execution_context.py). That keeps this module unit-testable
and keeps durability concerns out of user-facing code.
"""

import inspect
import uuid
from collections.abc import Callable
from typing import Any, Protocol

from app.engine.errors import DuplicateStepNameError
from app.engine.hashing import hash_step_input, normalize_json


class StepRecorder(Protocol):
    async def step_started(self, step_name: str, input_hash: str) -> None: ...

    async def step_completed(self, step_name: str, result: Any) -> None: ...

    async def step_failed(self, step_name: str, error: BaseException) -> None: ...


class WorkflowContext:
    """Phase 1: records every step. Replay of recorded results arrives in Phase 2, and
    ctx.llm/now/random/sleep/wait_for_signal/fork in later phases."""

    def __init__(self, run_id: uuid.UUID, recorder: StepRecorder) -> None:
        self.run_id = run_id
        self._recorder = recorder
        self._used_step_names: set[str] = set()

    async def step(self, name: str, fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        """Run `fn(*args, **kwargs)` as a named, recorded step and return its JSON result.

        Arguments and the result must be JSON-serializable. Step names must be unique within
        a run (in a loop, put the index in the name: f"fetch_{i}").
        """
        if name in self._used_step_names:
            raise DuplicateStepNameError(f"step name {name!r} was already used in this run")
        self._used_step_names.add(name)

        input_hash = hash_step_input(args, kwargs)
        await self._recorder.step_started(name, input_hash)
        try:
            result = fn(*args, **kwargs)
            if inspect.isawaitable(result):
                result = await result
            result = normalize_json(result)
        except Exception as exc:
            await self._recorder.step_failed(name, exc)
            raise
        await self._recorder.step_completed(name, result)
        return result
