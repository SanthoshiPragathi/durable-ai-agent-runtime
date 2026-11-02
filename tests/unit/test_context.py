import uuid
from typing import Any

import pytest

from app.engine.errors import DuplicateStepNameError, StepFailedError, StepSerializationError
from app.engine.hashing import hash_step_input
from app.sdk import WorkflowContext


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


@pytest.fixture
def recorder() -> FakeRecorder:
    return FakeRecorder()


@pytest.fixture
def ctx(recorder: FakeRecorder) -> WorkflowContext:
    return WorkflowContext(uuid.uuid4(), recorder)


async def test_step_records_started_then_completed(ctx: WorkflowContext, recorder: FakeRecorder) -> None:
    async def add(a: int, b: int) -> int:
        return a + b

    result = await ctx.step("add", add, 2, b=3)

    assert result == 5
    assert recorder.calls == [
        ("started", "add", hash_step_input((2,), {"b": 3})),
        ("completed", "add", 5),
    ]


async def test_sync_functions_are_supported(ctx: WorkflowContext) -> None:
    assert await ctx.step("upper", str.upper, "abc") == "ABC"


async def test_result_is_json_normalized(ctx: WorkflowContext, recorder: FakeRecorder) -> None:
    result = await ctx.step("pair", lambda: (1, 2))
    assert result == [1, 2]
    assert recorder.calls[-1] == ("completed", "pair", [1, 2])


async def test_failing_step_raises_step_failed(ctx: WorkflowContext, recorder: FakeRecorder) -> None:
    async def boom() -> None:
        raise ValueError("payment declined")

    with pytest.raises(StepFailedError, match="payment declined") as caught:
        await ctx.step("charge", boom)

    assert (caught.value.step_name, caught.value.error_type) == ("charge", "ValueError")
    assert isinstance(caught.value.__cause__, ValueError)  # original chained on the first run
    assert [c[0] for c in recorder.calls] == ["started", "failed"]
    assert recorder.calls[-1] == ("failed", "charge", "ValueError")


async def test_unserializable_result_fails_the_step(ctx: WorkflowContext, recorder: FakeRecorder) -> None:
    with pytest.raises(StepFailedError) as caught:
        await ctx.step("bad", lambda: object())
    assert caught.value.error_type == "StepSerializationError"
    assert [c[0] for c in recorder.calls] == ["started", "failed"]


async def test_unserializable_input_fails_before_the_step_starts(
    ctx: WorkflowContext, recorder: FakeRecorder
) -> None:
    called = False

    def fn(_arg: object) -> None:
        nonlocal called
        called = True

    with pytest.raises(StepSerializationError):
        await ctx.step("bad_input", fn, object())
    assert recorder.calls == [] and not called


async def test_step_names_must_be_unique_within_a_run(ctx: WorkflowContext, recorder: FakeRecorder) -> None:
    await ctx.step("once", lambda: 1)
    with pytest.raises(DuplicateStepNameError):
        await ctx.step("once", lambda: 2)
    assert len(recorder.calls) == 2  # only the first call was recorded


async def test_distinct_names_work_in_a_loop(ctx: WorkflowContext) -> None:
    results = [await ctx.step(f"item_{i}", lambda i=i: i * 10) for i in range(3)]
    assert results == [0, 10, 20]
