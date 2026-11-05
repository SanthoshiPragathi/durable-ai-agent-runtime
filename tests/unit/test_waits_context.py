"""ctx.sleep / ctx.wait_for_signal against a fake recorder (no database)."""

import uuid

import pytest

from app.engine.errors import DuplicateStepNameError, NonDeterminismError, RunSuspendedError
from app.engine.hashing import hash_step_input
from app.engine.replay import RecordedStep, ReplayHistory, StepStatus
from app.sdk import WorkflowContext
from app.sdk.context import WaitOutcome
from tests.fixtures.recorders import FakeRecorder


def make(recorder: FakeRecorder, history: ReplayHistory | None = None) -> WorkflowContext:
    return WorkflowContext(uuid.uuid4(), recorder, history)


async def test_a_wait_that_is_over_returns_its_result_and_the_workflow_continues() -> None:
    rec = FakeRecorder()
    rec.wait_script = [WaitOutcome(True, {"received": True, "payload": {"ok": 1}, "signal_id": 7})]
    ctx = make(rec)

    out = await ctx.wait_for_signal("approval", signal="decision", timeout=60)

    assert out == {"received": True, "payload": {"ok": 1}, "signal_id": 7}
    assert [c[0] for c in rec.calls] == ["started", "wait"]
    assert rec.calls[1] == ("wait", "approval", ("signal", "decision", 60))
    assert ctx.halt is None


async def test_a_wait_that_is_not_over_suspends_and_halts_the_context() -> None:
    rec = FakeRecorder()
    rec.wait_script = [WaitOutcome(False)]
    ctx = make(rec)

    with pytest.raises(RunSuspendedError) as caught:
        await ctx.sleep("pause", 30)
    assert caught.value.step_name == "pause"
    assert ctx.halt is caught.value

    # Workflow code that swallows the exception cannot carry on and do real work.
    ran = []
    with pytest.raises(RunSuspendedError):
        await ctx.step("after", lambda: ran.append(1))
    assert ran == []


async def test_a_completed_wait_is_replayed_without_touching_the_recorder() -> None:
    rec = FakeRecorder()
    history = ReplayHistory(
        [
            RecordedStep(
                "approval",
                hash_step_input((), {"signal": "approval", "timeout": None}),
                StepStatus.COMPLETED,
                {"received": True, "payload": "yes", "signal_id": 3},
            )
        ]
    )
    ctx = make(rec, history)

    out = await ctx.wait_for_signal("approval")

    assert out["payload"] == "yes"
    assert rec.calls == []


async def test_an_interrupted_wait_is_evaluated_again_without_a_second_step_started() -> None:
    rec = FakeRecorder()
    history = ReplayHistory(
        [RecordedStep("nap", hash_step_input((), {"seconds": 5}), StepStatus.IN_DOUBT)]
    )
    ctx = make(rec, history)

    await ctx.sleep("nap", 5)

    assert [c[0] for c in rec.calls] == ["wait"]  # STEP_STARTED is already in the log


async def test_changing_a_wait_after_it_was_recorded_is_non_determinism() -> None:
    history = ReplayHistory(
        [
            RecordedStep(
                "nap", hash_step_input((), {"seconds": 5}), StepStatus.COMPLETED, {"fire_at": "x"}
            )
        ]
    )
    ctx = make(FakeRecorder(), history)

    with pytest.raises(NonDeterminismError):
        await ctx.sleep("nap", 6)


async def test_wait_names_share_the_step_namespace() -> None:
    ctx = make(FakeRecorder())
    await ctx.sleep("x", 1)
    with pytest.raises(DuplicateStepNameError):
        await ctx.wait_for_signal("x")


async def test_a_negative_sleep_is_rejected() -> None:
    with pytest.raises(ValueError):
        await make(FakeRecorder()).sleep("x", -1)
