import uuid
from types import SimpleNamespace
from typing import Any

import pytest

from app.engine.errors import (
    CorruptHistoryError,
    NonDeterminismError,
    StepFailedError,
)
from app.engine.hashing import hash_step_input
from app.engine.replay import RecordedStep, ReplayHistory, StepStatus
from app.sdk import WorkflowContext
from tests.fixtures.recorders import FakeRecorder


def ev(event_type: str, step: str | None = None, input_hash: str | None = None, result: Any = None):
    return SimpleNamespace(
        event_type=event_type, step_name=step, input_hash=input_hash, result_data=result
    )


def completed(name: str, args: tuple, result: Any) -> RecordedStep:
    return RecordedStep(
        name, hash_step_input(args, {}), status=StepStatus.COMPLETED, result=result
    )


def make_ctx(history: ReplayHistory) -> tuple[WorkflowContext, FakeRecorder]:
    recorder = FakeRecorder()
    return WorkflowContext(uuid.uuid4(), recorder, history), recorder


# ---- building a history from events -------------------------------------------------------


def test_history_from_events_classifies_each_step() -> None:
    history = ReplayHistory.from_events(
        [
            ev("WORKFLOW_STARTED"),
            ev("STEP_STARTED", "a", "h1"),
            ev("STEP_COMPLETED", "a", result={"x": 1}),
            ev("STEP_STARTED", "b", "h2"),
            ev("STEP_FAILED", "b", result={"error_type": "ValueError", "message": "bad"}),
            ev("STEP_STARTED", "c", "h3"),  # died in doubt
        ]
    )
    a, b, c = history.steps
    assert (a.status, a.result) == (StepStatus.COMPLETED, {"x": 1})
    assert (b.status, b.error_type, b.error_message) == (StepStatus.FAILED, "ValueError", "bad")
    assert (c.status, c.attempts) == (StepStatus.IN_DOUBT, 1)
    assert history.count(StepStatus.COMPLETED) == 1 and len(history) == 3


def test_second_start_of_an_in_doubt_step_counts_as_another_attempt() -> None:
    history = ReplayHistory.from_events(
        [
            ev("STEP_STARTED", "a", "h"),
            ev("WORKFLOW_RESUMED"),
            ev("STEP_STARTED", "a", "h"),
            ev("STEP_COMPLETED", "a", result=1),
        ]
    )
    assert len(history) == 1
    assert (history.steps[0].status, history.steps[0].attempts) == (StepStatus.COMPLETED, 2)


@pytest.mark.parametrize(
    "events",
    [
        [ev("STEP_COMPLETED", "ghost", result=1)],
        [ev("STEP_STARTED", "a", "h"), ev("STEP_COMPLETED", "a", result=1), ev("STEP_COMPLETED", "a")],
        [ev("STEP_STARTED", "a", "h"), ev("STEP_COMPLETED", "a", result=1), ev("STEP_STARTED", "a", "h")],
    ],
)
def test_inconsistent_history_is_reported(events: list) -> None:
    with pytest.raises(CorruptHistoryError):
        ReplayHistory.from_events(events)


# ---- replaying through ctx.step -----------------------------------------------------------


async def test_completed_steps_return_recorded_results_without_running() -> None:
    history = ReplayHistory([completed("fetch", ("A1",), {"amount": 2500}), completed("calc", (2500,), 1250)])
    ctx, recorder = make_ctx(history)
    ran: list[str] = []

    def fetch(order_id: str) -> dict:
        ran.append("fetch")
        return {"amount": 999}

    def calc(amount: int) -> int:
        ran.append("calc")
        return 0

    assert await ctx.step("fetch", fetch, "A1") == {"amount": 2500}
    assert await ctx.step("calc", calc, 2500) == 1250
    assert ran == [] and recorder.calls == []  # nothing executed, nothing re-recorded
    ctx.verify_replay_complete()


async def test_steps_after_the_history_run_live_and_are_recorded() -> None:
    ctx, recorder = make_ctx(ReplayHistory([completed("a", (), 1)]))
    assert await ctx.step("a", lambda: 111) == 1  # replayed
    assert await ctx.step("b", lambda: 2) == 2  # live
    assert [c[:2] for c in recorder.calls] == [("started", "b"), ("completed", "b")]
    ctx.verify_replay_complete()


async def test_changed_input_is_detected_instead_of_reusing_the_old_result() -> None:
    """The refund example: recorded with 2000, workflow now passes 3000."""
    ctx, recorder = make_ctx(ReplayHistory([completed("refund", (2000,), {"refund_id": "r1"})]))

    with pytest.raises(NonDeterminismError, match="different input"):
        await ctx.step("refund", lambda amount: {"refund_id": "r2"}, 3000)

    assert recorder.calls == []
    assert isinstance(ctx.divergence, NonDeterminismError)


async def test_renamed_or_reordered_step_is_detected() -> None:
    ctx, _ = make_ctx(ReplayHistory([completed("fetch_order", (), 1)]))
    with pytest.raises(NonDeterminismError, match="'fetch_order'.*'load_order'"):
        await ctx.step("load_order", lambda: 1)


async def test_workflow_that_stops_early_is_detected_at_the_end() -> None:
    ctx, _ = make_ctx(ReplayHistory([completed("a", (), 1), completed("b", (), 2)]))
    await ctx.step("a", lambda: 1)
    with pytest.raises(NonDeterminismError, match=r"\['b'\]"):
        ctx.verify_replay_complete()


async def test_swallowing_the_divergence_does_not_let_the_workflow_continue() -> None:
    ctx, recorder = make_ctx(ReplayHistory([completed("a", (1,), 1)]))
    ran: list[str] = []

    with pytest.raises(NonDeterminismError):
        await ctx.step("a", lambda x: 1, 2)
    try:  # careless workflow code: catches everything and carries on
        await ctx.step("a", lambda x: 1, 2)
    except Exception:
        pass

    with pytest.raises(NonDeterminismError):
        await ctx.step("next", lambda: ran.append("side effect"))
    assert ran == [] and recorder.calls == []
    with pytest.raises(NonDeterminismError):
        ctx.verify_replay_complete()


async def test_recorded_failure_is_raised_again_without_running_the_step() -> None:
    history = ReplayHistory(
        [
            RecordedStep(
                "charge",
                hash_step_input((), {}),
                status=StepStatus.FAILED,
                error_type="RuntimeError",
                error_message="card declined",
            )
        ]
    )
    ctx, recorder = make_ctx(history)
    ran = False

    def charge() -> None:
        nonlocal ran
        ran = True

    with pytest.raises(StepFailedError, match="card declined") as caught:
        await ctx.step("charge", charge)
    assert (caught.value.error_type, ran, recorder.calls) == ("RuntimeError", False, [])


async def test_workflow_that_handles_a_failure_takes_the_same_branch_on_replay() -> None:
    history = ReplayHistory(
        [
            RecordedStep(
                "primary",
                hash_step_input((), {}),
                status=StepStatus.FAILED,
                error_type="TimeoutError",
                error_message="slow",
            ),
            completed("fallback", (), "from-fallback"),
        ]
    )
    ctx, recorder = make_ctx(history)

    async def workflow() -> str:
        try:
            return await ctx.step("primary", lambda: "from-primary")
        except StepFailedError:
            return await ctx.step("fallback", lambda: "NOT USED")

    assert await workflow() == "from-fallback"
    assert recorder.calls == []
    ctx.verify_replay_complete()


async def test_in_doubt_step_is_run_again_and_recorded() -> None:
    in_doubt = RecordedStep("charge", hash_step_input((5,), {}))  # STARTED, no outcome
    ctx, recorder = make_ctx(ReplayHistory([in_doubt]))

    assert await ctx.step("charge", lambda amount: amount * 2, 5) == 10
    assert [c[0] for c in recorder.calls] == ["started", "completed"]


async def test_in_doubt_step_with_changed_input_is_still_a_divergence() -> None:
    ctx, _ = make_ctx(ReplayHistory([RecordedStep("charge", hash_step_input((5,), {}))]))
    with pytest.raises(NonDeterminismError):
        await ctx.step("charge", lambda amount: amount, 6)
