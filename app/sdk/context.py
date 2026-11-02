"""The context object handed to every workflow.

The SDK holds no database connection. ctx.step() reports to a StepRecorder, which the engine
implements on top of PostgreSQL (engine/execution_context.py), and consults a ReplayHistory that
the engine loads when a run is resumed. That keeps this module unit-testable without a database.
"""

import inspect
import random as _random
import uuid
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any, Protocol

from app.engine.errors import (
    DuplicateStepNameError,
    NonDeterminismError,
    StepFailedError,
)
from app.engine.hashing import hash_step_input, normalize_json
from app.engine.replay import RecordedStep, ReplayHistory, StepStatus


class StepRecorder(Protocol):
    async def step_started(self, step_name: str, input_hash: str) -> None: ...

    async def step_completed(self, step_name: str, result: Any) -> None: ...

    async def step_failed(self, step_name: str, error: BaseException) -> None: ...


class WorkflowContext:
    """What a workflow uses to do durable work.

    On a first run every step executes and is recorded. On a resumed run the steps that already
    finished are answered from the recorded history instead of being executed again.
    """

    def __init__(
        self,
        run_id: uuid.UUID,
        recorder: StepRecorder,
        replay: ReplayHistory | None = None,
        *,
        clock: Callable[[], datetime] | None = None,
        rng: Callable[[], float] | None = None,
    ) -> None:
        self.run_id = run_id
        self._recorder = recorder
        self._replay = replay or ReplayHistory()
        self._clock = clock or (lambda: datetime.now(UTC))
        self._rng = rng or _random.random
        self._auto_counters: dict[str, int] = {}
        self._used_step_names: set[str] = set()
        self._step_index = 0
        self._divergence: NonDeterminismError | None = None

    @property
    def divergence(self) -> NonDeterminismError | None:
        """Set once replay found the workflow code no longer matches its recorded history."""
        return self._divergence

    async def step(self, name: str, fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        """Run `fn(*args, **kwargs)` as a named, recorded step and return its JSON result.

        Arguments and the result must be JSON-serializable. Step names must be unique within
        a run (in a loop, put the index in the name: f"fetch_{i}").

        If `fn` raises, the failure is recorded and StepFailedError is raised (see its docs).
        When resuming, a step that already completed returns its recorded result without calling
        `fn`; a recorded failure raises StepFailedError again.
        """
        input_hash = hash_step_input(args, kwargs)
        return await self._execute(name, input_hash, lambda: fn(*args, **kwargs))

    async def now(self) -> datetime:
        """The current UTC time, recorded. A replay returns the originally recorded instant.

        Never call datetime.now() directly in a workflow: replay would see a different time and
        could take a different branch.
        """
        recorded = await self._execute(
            self._auto_name("now"),
            hash_step_input((), {}),
            lambda: self._clock().astimezone(UTC).isoformat(),
        )
        return datetime.fromisoformat(recorded)

    async def random(self) -> float:
        """A random float in [0, 1), recorded. A replay returns the originally drawn value."""
        return await self._execute(self._auto_name("random"), hash_step_input((), {}), self._rng)

    def _auto_name(self, kind: str) -> str:
        # Deterministic names, so replay matches the n-th now()/random() call with its recording.
        self._auto_counters[kind] = self._auto_counters.get(kind, 0) + 1
        return f"__{kind}_{self._auto_counters[kind]}"

    async def _execute(self, name: str, input_hash: str, call: Callable[[], Any]) -> Any:
        if self._divergence is not None:
            # Workflow code swallowed a NonDeterminismError and tried to carry on. Doing real work
            # from a state that no longer matches the history is unsafe, so refuse.
            raise self._divergence
        if name in self._used_step_names:
            raise DuplicateStepNameError(f"step name {name!r} was already used in this run")
        self._used_step_names.add(name)

        index = self._step_index
        self._step_index += 1

        recorded = self._replay.step_at(index)
        if recorded is not None:
            self._verify(index, recorded, name, input_hash)
            if recorded.status == StepStatus.COMPLETED:
                return recorded.result
            if recorded.status == StepStatus.FAILED:
                raise StepFailedError(
                    name, recorded.error_type or "Error", recorded.error_message or ""
                )
            # IN_DOUBT: the previous attempt died mid-step; fall through and run it again.

        await self._recorder.step_started(name, input_hash)
        try:
            result = call()
            if inspect.isawaitable(result):
                result = await result
            result = normalize_json(result)
        except Exception as exc:
            await self._recorder.step_failed(name, exc)
            raise StepFailedError(name, type(exc).__name__, str(exc)) from exc
        await self._recorder.step_completed(name, result)
        return result

    def _verify(self, index: int, recorded: RecordedStep, name: str, input_hash: str) -> None:
        problem: str | None = None
        if recorded.name != name:
            problem = (
                f"step #{index + 1} was {recorded.name!r} in the recorded history, "
                f"but the workflow now calls {name!r}"
            )
        elif recorded.input_hash != input_hash:
            problem = (
                f"step {name!r} is now called with different input than the recorded run "
                f"(recorded hash {str(recorded.input_hash)[:12]}, now {input_hash[:12]})"
            )
        if problem is not None:
            self._divergence = NonDeterminismError(problem)
            raise self._divergence

    def verify_replay_complete(self) -> None:
        """Called by the runtime when the workflow function returns.

        Fails if the history holds steps the workflow never reached, e.g. new code removed a step
        that had already run.
        """
        if self._divergence is not None:
            raise self._divergence
        if self._step_index < len(self._replay):
            missing = [s.name for s in self._replay.steps[self._step_index :]]
            self._divergence = NonDeterminismError(
                f"the workflow finished after {self._step_index} step(s), but the recorded history "
                f"also contains {missing}"
            )
            raise self._divergence
