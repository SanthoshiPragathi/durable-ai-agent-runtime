"""The context object handed to every workflow.

The SDK holds no database connection. Steps report to a StepRecorder, which the engine implements on
top of PostgreSQL (engine/execution_context.py), and consult a ReplayHistory that the engine loads
when a run is resumed. That keeps this module unit-testable without a database.

Two kinds of step:

  ctx.step(...)    for work that is safe to repeat (reads, pure computation). If the worker dies
                   mid-step it is simply run again.
  ctx.effect(...)  for calls with real-world effects (payments, emails). The runtime never repeats
                   one blindly; see effect() below.
"""

import asyncio
import hashlib
import inspect
import random as _random
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol

from app.engine.errors import (
    AmbiguousOutcomeError,
    DuplicateStepNameError,
    DurableRuntimeError,
    LLMConfigurationError,
    NonDeterminismError,
    OutcomeUnknownError,
    StepFailedError,
)
from app.engine.hashing import hash_step_input, normalize_json
from app.engine.replay import RecordedStep, ReplayHistory, StepStatus
from app.integrations.llm.base import LLMProvider


class StepRecorder(Protocol):
    async def step_started(
        self, step_name: str, input_hash: str, idempotency_key: str | None = None
    ) -> None: ...

    async def step_completed(
        self,
        step_name: str,
        result: Any,
        *,
        idempotency_key: str | None = None,
        reconciled: bool = False,
    ) -> None: ...

    async def step_failed(
        self, step_name: str, error: BaseException, *, idempotency_key: str | None = None
    ) -> None: ...

    async def step_outcome_unknown(
        self,
        step_name: str,
        reason: str,
        *,
        idempotency_key: str | None = None,
        already_recorded: bool = False,
    ) -> None: ...

    async def effect_attempt(self, step_name: str, idempotency_key: str) -> None: ...


@dataclass(frozen=True)
class EffectSpec:
    idempotent: bool
    key: str
    verify: Callable[..., Any] | None
    args: tuple[Any, ...]
    kwargs: dict[str, Any]
    max_attempts: int


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
        llm: LLMProvider | None = None,
        retry_delay: float = 0.05,
    ) -> None:
        self.run_id = run_id
        self._recorder = recorder
        self._replay = replay or ReplayHistory()
        self._clock = clock or (lambda: datetime.now(UTC))
        self._rng = rng or _random.random
        self._llm = llm
        self._retry_delay = retry_delay
        self._auto_counters: dict[str, int] = {}
        self._used_step_names: set[str] = set()
        self._step_index = 0
        self._halt: DurableRuntimeError | None = None

    # ---- state the runtime inspects ---------------------------------------------------------

    @property
    def halt(self) -> DurableRuntimeError | None:
        """Set once the run must stop: replay diverged (NonDeterminismError) or a step's outcome
        is unknown (OutcomeUnknownError). Every later ctx call re-raises it, so workflow code that
        swallows the exception cannot carry on and do real work from a state it cannot trust."""
        return self._halt

    @property
    def divergence(self) -> NonDeterminismError | None:
        return self._halt if isinstance(self._halt, NonDeterminismError) else None

    # ---- public API -------------------------------------------------------------------------

    def idempotency_key(self, step_name: str) -> str:
        """The stable key for a step's external operation: "<run_id>:<step_name>"."""
        return f"{self.run_id}:{step_name}"

    async def step(self, name: str, fn: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        """Run `fn(*args, **kwargs)` as a named, recorded step and return its JSON result.

        Use for work that is safe to repeat. Arguments and the result must be JSON-serializable.
        Step names must be unique within a run (in a loop: f"fetch_{i}"). If `fn` raises, the
        failure is recorded and StepFailedError is raised. When resuming, a step that already
        completed returns its recorded result without calling `fn`; a recorded failure raises
        StepFailedError again.
        """
        input_hash = hash_step_input(args, kwargs)
        return await self._execute(name, input_hash, lambda: fn(*args, **kwargs))

    async def effect(
        self,
        name: str,
        fn: Callable[..., Any],
        *args: Any,
        idempotent: bool,
        verify: Callable[..., Any] | None = None,
        max_attempts: int = 3,
        **kwargs: Any,
    ) -> Any:
        """Run a call with a real-world side effect (payment, email, ...) as a recorded step.

        idempotent=True: the tool honors idempotency keys. `fn` is called with an extra keyword
            argument `idempotency_key="<run_id>:<name>"`, the same on every attempt, so a retry
            after a crash or an ambiguous timeout returns the original result instead of acting
            twice. Ambiguous failures (AmbiguousOutcomeError) are retried up to `max_attempts`
            with backoff.

        idempotent=False: the tool cannot dedupe, so the runtime never retries it blindly. When the
            outcome is in doubt (the worker died mid-call, or the tool raised
            AmbiguousOutcomeError) it asks `verify(*args, **kwargs)`: return the recorded result
            if the effect happened, or None if it provably did not. With no verify hook, or if
            verify fails, the step is recorded as STEP_OUTCOME_UNKNOWN, the run moves to
            NEEDS_REVIEW and OutcomeUnknownError is raised. Pass `ctx.idempotency_key(name)` to the
            tool as a reference if you want verify to be able to find the operation later.

        A definitive tool error (e.g. a declined payment) is recorded as a failed step and raised
        as StepFailedError, like ctx.step. `idempotent` and `verify` are consumed here, so the tool
        cannot use keyword arguments with those names. `fn`'s result must be JSON and not None.
        """
        spec = EffectSpec(idempotent, self.idempotency_key(name), verify, args, kwargs, max_attempts)
        call_kwargs = {**kwargs, "idempotency_key": spec.key} if idempotent else kwargs
        input_hash = hash_step_input(args, kwargs)  # the key is derived, so it is not part of it
        return await self._execute(name, input_hash, lambda: fn(*args, **call_kwargs), spec)

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

    async def llm(self, name: str, prompt: str, *, model: str | None = None, **params: Any) -> str:
        """Ask the LLM and return its text answer. The call is recorded as a step named `name`.

        Recorded: output, model, a hash of the prompt (not the prompt itself), input/output tokens,
        estimated cost (None if unknown), latency in ms and a timestamp. A replay returns the
        recorded output and never calls the model again, so a different answer on a re-run cannot
        change the workflow's path. `params` (e.g. temperature) must be JSON-serializable.
        """
        provider = self._llm
        if provider is None:
            raise LLMConfigurationError("this run has no LLM provider configured")
        chosen_model = model or provider.default_model
        input_hash = hash_step_input((), {"prompt": prompt, "model": chosen_model, "params": params})

        async def call() -> dict[str, Any]:
            started = time.perf_counter()
            response = await provider.complete(prompt, model=chosen_model, **params)
            latency_ms = (time.perf_counter() - started) * 1000
            return {
                "output": response.output,
                "model": response.model,
                "prompt_hash": hashlib.sha256(prompt.encode("utf-8")).hexdigest(),
                "input_tokens": response.input_tokens,
                "output_tokens": response.output_tokens,
                "cost_usd": response.cost_usd,
                "latency_ms": round(latency_ms, 3),
                "timestamp": self._clock().astimezone(UTC).isoformat(),
            }

        record = await self._execute(name, input_hash, call)
        return record["output"]

    def verify_replay_complete(self) -> None:
        """Called by the runtime when the workflow function returns.

        Fails if the run was halted, or if the history holds steps the workflow never reached, e.g.
        new code removed a step that had already run.
        """
        if self._halt is not None:
            raise self._halt
        if self._step_index < len(self._replay):
            missing = [s.name for s in self._replay.steps[self._step_index :]]
            self._halt = NonDeterminismError(
                f"the workflow finished after {self._step_index} step(s), but the recorded history "
                f"also contains {missing}"
            )
            raise self._halt

    # ---- internals --------------------------------------------------------------------------

    def _auto_name(self, kind: str) -> str:
        # Deterministic names, so replay matches the n-th now()/random() call with its recording.
        self._auto_counters[kind] = self._auto_counters.get(kind, 0) + 1
        return f"__{kind}_{self._auto_counters[kind]}"

    async def _execute(
        self,
        name: str,
        input_hash: str,
        call: Callable[[], Any],
        effect: EffectSpec | None = None,
    ) -> Any:
        if self._halt is not None:
            raise self._halt
        if name in self._used_step_names:
            raise DuplicateStepNameError(f"step name {name!r} was already used in this run")
        self._used_step_names.add(name)

        index = self._step_index
        self._step_index += 1
        key = effect.key if effect else None

        recorded = self._replay.step_at(index)
        if recorded is not None:
            self._verify(index, recorded, name, input_hash)
            if recorded.status == StepStatus.COMPLETED:
                return recorded.result
            if recorded.status == StepStatus.FAILED:
                raise StepFailedError(
                    name, recorded.error_type or "Error", recorded.error_message or ""
                )
            if recorded.status == StepStatus.UNKNOWN:
                raise await self._unknown(
                    name, key, recorded.error_message or "unresolved", already_recorded=True
                )
            # IN_DOUBT: the previous attempt died mid-step.
            if effect is not None and not recorded.confirmed_not_executed:
                happened, result = await self._reconcile(name, effect)
                if happened:
                    await self._recorder.step_completed(
                        name, result, idempotency_key=key, reconciled=True
                    )
                    return result
            # A plain step, an idempotent effect, or an effect proven not to have happened:
            # safe to run again.

        await self._recorder.step_started(name, input_hash, key)
        return await self._run_live(name, call, effect)

    async def _run_live(self, name: str, call: Callable[[], Any], effect: EffectSpec | None) -> Any:
        key = effect.key if effect else None
        attempt = 1
        while True:
            try:
                result = call()
                if inspect.isawaitable(result):
                    result = await result
                result = normalize_json(result)
            except AmbiguousOutcomeError as exc:
                if effect is None:
                    await self._recorder.step_failed(name, exc)
                    raise StepFailedError(name, type(exc).__name__, str(exc)) from exc

                if not effect.idempotent:
                    # May or may not have happened, and calling again could repeat it.
                    happened, found = await self._reconcile(name, effect)
                    if happened:
                        await self._recorder.step_completed(
                            name, found, idempotency_key=key, reconciled=True
                        )
                        return found
                    # Verified: it did not happen, so another attempt is safe.

                if attempt >= effect.max_attempts:
                    await self._recorder.step_failed(name, exc, idempotency_key=key)
                    raise StepFailedError(name, type(exc).__name__, str(exc)) from exc
                await asyncio.sleep(self._retry_delay * 2 ** (attempt - 1))
                attempt += 1
                await self._recorder.effect_attempt(name, effect.key)
                continue
            except Exception as exc:
                await self._recorder.step_failed(name, exc, idempotency_key=key)
                raise StepFailedError(name, type(exc).__name__, str(exc)) from exc

            await self._recorder.step_completed(name, result, idempotency_key=key)
            return result

    async def _reconcile(self, name: str, effect: EffectSpec) -> tuple[bool, Any]:
        """Settle a step whose outcome is in doubt. Returns (True, result) if the effect happened,
        (False, None) if it is safe to call again, or raises OutcomeUnknownError."""
        if effect.idempotent:
            return False, None  # same key again: the tool returns the original result if it acted
        if effect.verify is None:
            raise await self._unknown(
                name,
                effect.key,
                "the call was in flight when the worker died or timed out, the tool cannot "
                "deduplicate, and no verify hook was provided",
            )
        try:
            found = effect.verify(*effect.args, **effect.kwargs)
            if inspect.isawaitable(found):
                found = await found
            found = None if found is None else normalize_json(found)
        except Exception as exc:
            raise await self._unknown(
                name, effect.key, f"verification failed: {type(exc).__name__}: {exc}"
            ) from exc
        return found is not None, found

    async def _unknown(
        self, name: str, key: str | None, reason: str, *, already_recorded: bool = False
    ) -> OutcomeUnknownError:
        await self._recorder.step_outcome_unknown(
            name, reason, idempotency_key=key, already_recorded=already_recorded
        )
        error = OutcomeUnknownError(name, reason)
        self._halt = error
        return error

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
            self._halt = NonDeterminismError(problem)
            raise self._halt
