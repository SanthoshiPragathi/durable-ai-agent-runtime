import uuid
from datetime import UTC, datetime, timedelta, timezone

import pytest

from app.engine.errors import NonDeterminismError
from app.engine.hashing import hash_step_input
from app.engine.replay import RecordedStep, ReplayHistory, StepStatus
from app.sdk import WorkflowContext
from tests.fixtures.recorders import FakeRecorder

T0 = datetime(2026, 11, 2, 10, 30, tzinfo=UTC)


def recorded(name: str, result) -> RecordedStep:
    return RecordedStep(name, hash_step_input((), {}), status=StepStatus.COMPLETED, result=result)


def make_ctx(history=None, *, clock=None, rng=None):
    recorder = FakeRecorder()
    ctx = WorkflowContext(uuid.uuid4(), recorder, history, clock=clock, rng=rng)
    return ctx, recorder


async def test_now_is_recorded_as_a_utc_timestamp() -> None:
    ctx, recorder = make_ctx(clock=lambda: T0)
    value = await ctx.now()
    assert value == T0 and value.tzinfo is not None
    assert recorder.calls[-1] == ("completed", "__now_1", "2026-11-02T10:30:00+00:00")


async def test_now_converts_other_timezones_to_utc() -> None:
    ist = timezone(timedelta(hours=5, minutes=30))
    ctx, _ = make_ctx(clock=lambda: datetime(2026, 11, 2, 16, 0, tzinfo=ist))
    assert (await ctx.now()) == datetime(2026, 11, 2, 10, 30, tzinfo=UTC)


async def test_replayed_now_ignores_the_current_clock() -> None:
    history = ReplayHistory([recorded("__now_1", "2026-11-02T10:30:00+00:00")])
    ctx, recorder = make_ctx(history, clock=lambda: T0 + timedelta(days=3))
    assert await ctx.now() == T0  # 10:30 AM, not three days later
    assert recorder.calls == []


async def test_random_is_recorded_and_replayed() -> None:
    first, _ = make_ctx(rng=lambda: 0.123456789)
    assert await first.random() == 0.123456789

    history = ReplayHistory([recorded("__random_1", 0.123456789)])
    replay, recorder = make_ctx(history, rng=lambda: 0.999)
    assert await replay.random() == 0.123456789
    assert recorder.calls == []


async def test_successive_calls_get_numbered_names() -> None:
    values = iter([0.1, 0.2])
    ctx, recorder = make_ctx(clock=lambda: T0, rng=lambda: next(values))
    await ctx.now()
    await ctx.random()
    await ctx.random()
    await ctx.now()
    started = [c[1] for c in recorder.calls if c[0] == "started"]
    assert started == ["__now_1", "__random_1", "__random_2", "__now_2"]


async def test_changing_the_call_order_is_detected_on_replay() -> None:
    history = ReplayHistory([recorded("__now_1", T0.isoformat())])
    ctx, _ = make_ctx(history)
    with pytest.raises(NonDeterminismError):
        await ctx.random()  # the history says the first call was now()
