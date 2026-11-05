"""Forking a run and diffing two runs.

fork_run() creates a NEW run that starts from a prefix of an existing run's history. The steps before
the fork point are copied as already-completed events, so when a worker claims the fork it replays
them (their functions are not called again) and runs live from the fork point onward. Optionally the
fork step itself is given a different result, which is the "what if the LLM had said REJECT?" question.

Rules, so a fork is always a valid, replayable history:
  * every step before the fork point must be COMPLETED in the source (a failed or unknown step has no
    result to reuse);
  * the source run is left untouched: the fork has its own run id, its own events, parent_run_id set;
  * the fork records where it came from in its WORKFLOW_STARTED event.

What a fork does NOT do: it does not undo the source run's real-world effects, and an effect step that
runs again in the fork uses the fork's run id in its idempotency key ("<fork_id>:<step>"), so it is a
NEW operation, not a duplicate of the source's. That is what you want for "run this again from step 4",
and it is why forking a run that already refunded someone will refund again if the fork reaches the
refund step. Pass an override_result for that step to skip it.
"""

import uuid
from dataclasses import dataclass
from typing import Any

from app.database.models import EventType, RunStatus, WorkflowRun
from app.database.repositories import EventRepository, RunRepository
from app.database.session import SessionFactory
from app.engine.errors import ForkError, RunNotFoundError
from app.engine.hashing import normalize_json
from app.engine.replay import RecordedStep, ReplayHistory, StepStatus

_UNSET: Any = object()
_COPIED = (EventType.STEP_STARTED.value, EventType.STEP_COMPLETED.value)


async def fork_run(
    session_factory: SessionFactory,
    source_run_id: uuid.UUID,
    from_step: str,
    *,
    override_result: Any = _UNSET,
    input_overrides: dict[str, Any] | None = None,
) -> WorkflowRun:
    """Create a claimable fork of `source_run_id` that re-executes from `from_step` onward.

    override_result: if given, `from_step` is recorded as already completed with this result instead
        of being executed (must be JSON). Omit it to re-execute the step.
    input_overrides: replaces workflow arguments in the fork. Steps before the fork point that were
        called with the old arguments will then fail replay with NonDeterminismError, which is
        correct: changing an input that an earlier recorded step depended on is not a valid fork.
    """
    async with session_factory() as session, session.begin():
        runs, events = RunRepository(session), EventRepository(session)
        source = await runs.get_run(source_run_id)
        if source is None:
            raise RunNotFoundError(f"run {source_run_id} does not exist")
        if source.status == RunStatus.PENDING.value:
            raise ForkError(f"run {source_run_id} has not started; there is no history to fork")

        source_events = await events.list_events(source_run_id)
        history = ReplayHistory.from_events(source_events)
        names = [s.name for s in history.steps]
        if from_step not in names:
            raise ForkError(f"run {source_run_id} has no step named {from_step!r} (steps: {names})")
        index = names.index(from_step)
        before = history.steps[:index]
        bad = [s.name for s in before if s.status != StepStatus.COMPLETED]
        if bad:
            raise ForkError(f"cannot fork at {from_step!r}: earlier step(s) {bad} did not complete")
        target: RecordedStep = history.steps[index]

        new_input = {**source.input_data, **(input_overrides or {})}
        fork = await runs.create_run(
            source.workflow_name, source.workflow_version, new_input, parent_run_id=source_run_id
        )
        await events.append(
            fork.run_id,
            EventType.WORKFLOW_STARTED,
            result_data={
                "forked_from": str(source_run_id),
                "from_step": from_step,
                "overridden": override_result is not _UNSET,
                "input_overrides": sorted(input_overrides or {}),
            },
        )

        keep = {s.name for s in before}
        for event in source_events:
            if event.event_type in _COPIED and event.step_name in keep:
                if event.event_type == EventType.STEP_STARTED.value and any(
                    e.event_type == EventType.STEP_STARTED.value
                    and e.step_name == event.step_name
                    and e.sequence_number < event.sequence_number
                    for e in source_events
                ):
                    continue  # a retried start; one STEP_STARTED per step is enough in the copy
                await events.append(
                    fork.run_id,
                    EventType(event.event_type),
                    step_name=event.step_name,
                    input_hash=event.input_hash,
                    result_data=event.result_data,
                )
        if override_result is not _UNSET:
            await events.append(
                fork.run_id, EventType.STEP_STARTED, step_name=from_step, input_hash=target.input_hash
            )
            await events.append(
                fork.run_id,
                EventType.STEP_COMPLETED,
                step_name=from_step,
                result_data=normalize_json(override_result),
            )
        await runs.start_claimable(fork.run_id)
        await session.refresh(fork)
        return fork


@dataclass(frozen=True)
class StepDiff:
    step_name: str
    status_a: str | None  # None: the run has no such step
    status_b: str | None
    same_input: bool
    same_result: bool
    result_a: Any = None
    result_b: Any = None

    @property
    def identical(self) -> bool:
        return self.status_a == self.status_b and self.same_input and self.same_result


@dataclass(frozen=True)
class RunDiff:
    run_a: uuid.UUID
    run_b: uuid.UUID
    steps: list[StepDiff]
    first_divergence: str | None  # first step (in order) that differs, or None if the steps match
    status_a: str
    status_b: str
    result_a: Any
    result_b: Any


async def diff_runs(session_factory: SessionFactory, run_a: uuid.UUID, run_b: uuid.UUID) -> RunDiff:
    """Compare two runs step by step (by step name, in the order of run A then B's extra steps)."""
    async with session_factory() as session:
        runs, events = RunRepository(session), EventRepository(session)
        a, b = await runs.get_run(run_a), await runs.get_run(run_b)
        for run, rid in ((a, run_a), (b, run_b)):
            if run is None:
                raise RunNotFoundError(f"run {rid} does not exist")
        assert a is not None and b is not None
        hist_a = ReplayHistory.from_events(await events.list_events(run_a))
        hist_b = ReplayHistory.from_events(await events.list_events(run_b))

    by_a = {s.name: s for s in hist_a.steps}
    by_b = {s.name: s for s in hist_b.steps}
    ordered = [s.name for s in hist_a.steps] + [s.name for s in hist_b.steps if s.name not in by_a]
    steps: list[StepDiff] = []
    for name in ordered:
        sa, sb = by_a.get(name), by_b.get(name)
        steps.append(
            StepDiff(
                step_name=name,
                status_a=sa.status.value if sa else None,
                status_b=sb.status.value if sb else None,
                same_input=bool(sa and sb and sa.input_hash == sb.input_hash),
                same_result=bool(sa and sb and sa.result == sb.result),
                result_a=sa.result if sa else None,
                result_b=sb.result if sb else None,
            )
        )
    first = next((d.step_name for d in steps if not d.identical), None)
    return RunDiff(run_a, run_b, steps, first, a.status, b.status, a.result_data, b.result_data)
