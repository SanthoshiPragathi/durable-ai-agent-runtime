"""ctx.effect: how the runtime treats calls with real-world side effects."""

import uuid
from typing import Any

import pytest

from app.engine.errors import (
    AmbiguousOutcomeError,
    CorruptHistoryError,
    OutcomeUnknownError,
    StepFailedError,
)
from app.engine.hashing import hash_step_input
from app.engine.replay import RecordedStep, ReplayHistory, StepStatus
from app.sdk import WorkflowContext
from tests.fixtures.recorders import FakeRecorder
from tests.unit.test_replay import ev

RUN_ID = uuid.UUID("00000000-0000-0000-0000-000000000101")
KEY = f"{RUN_ID}:process_refund"


class Tool:
    """A fake external tool that remembers what it was asked to do."""

    def __init__(self, *, results: list[Any] | None = None) -> None:
        self.calls: list[dict[str, Any]] = []
        self._results = list(results or [])

    async def refund(self, order_id: str, amount: int, **kwargs: Any) -> Any:
        self.calls.append({"order_id": order_id, "amount": amount, **kwargs})
        if self._results:
            outcome = self._results.pop(0)
            if isinstance(outcome, BaseException):
                raise outcome
            return outcome
        return {"refund_id": "rf_1", "amount": amount}


def make_ctx(history: ReplayHistory | None = None, **kwargs: Any) -> tuple[WorkflowContext, FakeRecorder]:
    recorder = FakeRecorder()
    ctx = WorkflowContext(RUN_ID, recorder, history, retry_delay=0, **kwargs)
    return ctx, recorder


def in_doubt(*args: Any, **kwargs: Any) -> RecordedStep:
    return RecordedStep("process_refund", hash_step_input(args, kwargs))


# ---- keys ---------------------------------------------------------------------------------


def test_the_idempotency_key_is_stable_and_readable() -> None:
    ctx, _ = make_ctx()
    assert ctx.idempotency_key("process_refund") == KEY


async def test_idempotent_effect_passes_the_key_and_records_it() -> None:
    ctx, recorder = make_ctx()
    tool = Tool()

    result = await ctx.effect("process_refund", tool.refund, "ORD-1", 2500, idempotent=True)

    assert result == {"refund_id": "rf_1", "amount": 2500}
    assert tool.calls == [{"order_id": "ORD-1", "amount": 2500, "idempotency_key": KEY}]
    assert recorder.keys["process_refund"] == KEY
    assert [c[0] for c in recorder.calls] == ["started", "completed"]


async def test_non_idempotent_effect_is_not_given_a_key() -> None:
    ctx, recorder = make_ctx()
    tool = Tool()
    await ctx.effect("process_refund", tool.refund, "ORD-1", 2500, idempotent=False)
    assert tool.calls == [{"order_id": "ORD-1", "amount": 2500}]
    assert recorder.keys["process_refund"] == KEY  # still tracked, just not sent to the tool


async def test_the_input_hash_does_not_include_the_derived_key() -> None:
    ctx, recorder = make_ctx()
    await ctx.effect("process_refund", Tool().refund, "ORD-1", 2500, idempotent=True)
    assert recorder.calls[0][2] == hash_step_input(("ORD-1", 2500), {})


# ---- crash recovery: idempotent effects ---------------------------------------------------


async def test_in_doubt_idempotent_effect_is_called_again_with_the_same_key() -> None:
    """THE Phase 2 gap: the refund may already have happened. Same key => the tool dedupes."""
    ctx, recorder = make_ctx(ReplayHistory([in_doubt("ORD-1", 2500)]))
    tool = Tool()

    result = await ctx.effect("process_refund", tool.refund, "ORD-1", 2500, idempotent=True)

    assert tool.calls == [{"order_id": "ORD-1", "amount": 2500, "idempotency_key": KEY}]
    assert result["refund_id"] == "rf_1"
    assert [c[0] for c in recorder.calls] == ["started", "completed"]


async def test_completed_effect_is_never_called_again() -> None:
    done = RecordedStep(
        "process_refund",
        hash_step_input(("ORD-1", 2500), {}),
        status=StepStatus.COMPLETED,
        result={"refund_id": "rf_ORIGINAL"},
    )
    ctx, recorder = make_ctx(ReplayHistory([done]))
    tool = Tool()

    result = await ctx.effect("process_refund", tool.refund, "ORD-1", 2500, idempotent=True)

    assert result == {"refund_id": "rf_ORIGINAL"} and tool.calls == [] and recorder.calls == []


# ---- ambiguous failures: idempotent -------------------------------------------------------


async def test_ambiguous_timeout_on_an_idempotent_effect_is_retried_with_the_same_key() -> None:
    ctx, recorder = make_ctx()
    tool = Tool(results=[AmbiguousOutcomeError("timeout"), AmbiguousOutcomeError("timeout")])

    result = await ctx.effect("process_refund", tool.refund, "ORD-1", 2500, idempotent=True)

    assert result["refund_id"] == "rf_1"
    assert [c.get("idempotency_key") for c in tool.calls] == [KEY, KEY, KEY]  # never a new key
    assert [c[0] for c in recorder.calls] == ["started", "retry", "retry", "completed"]


async def test_retries_are_bounded_and_the_step_then_fails() -> None:
    ctx, recorder = make_ctx()
    tool = Tool(results=[AmbiguousOutcomeError("t")] * 5)

    with pytest.raises(StepFailedError) as caught:
        await ctx.effect(
            "process_refund", tool.refund, "ORD-1", 2500, idempotent=True, max_attempts=3
        )

    assert caught.value.error_type == "AmbiguousOutcomeError" and len(tool.calls) == 3
    assert [c[0] for c in recorder.calls] == ["started", "retry", "retry", "failed"]


async def test_a_definitive_error_is_not_retried() -> None:
    ctx, recorder = make_ctx()
    tool = Tool(results=[ValueError("card declined")])

    with pytest.raises(StepFailedError, match="card declined"):
        await ctx.effect("process_refund", tool.refund, "ORD-1", 2500, idempotent=True)

    assert len(tool.calls) == 1 and [c[0] for c in recorder.calls] == ["started", "failed"]


# ---- crash recovery: non-idempotent effects -----------------------------------------------


async def test_in_doubt_non_idempotent_effect_without_verify_becomes_outcome_unknown() -> None:
    ctx, recorder = make_ctx(ReplayHistory([in_doubt("ORD-1", 2500)]))
    tool = Tool()

    with pytest.raises(OutcomeUnknownError, match="process_refund"):
        await ctx.effect("process_refund", tool.refund, "ORD-1", 2500, idempotent=False)

    assert tool.calls == []  # NOT blindly repeated
    assert [c[:2] for c in recorder.calls] == [("unknown", "process_refund")]
    assert isinstance(ctx.halt, OutcomeUnknownError)


async def test_verify_finding_the_effect_completes_the_step_without_calling_the_tool() -> None:
    ctx, recorder = make_ctx(ReplayHistory([in_doubt("ORD-1", 2500)]))
    tool = Tool()
    asked: list[tuple] = []

    def verify(order_id: str, amount: int) -> dict:
        asked.append((order_id, amount))
        return {"refund_id": "rf_FOUND", "amount": amount}

    result = await ctx.effect(
        "process_refund", tool.refund, "ORD-1", 2500, idempotent=False, verify=verify
    )

    assert result == {"refund_id": "rf_FOUND", "amount": 2500}
    assert asked == [("ORD-1", 2500)] and tool.calls == []
    assert recorder.calls == [("completed", "process_refund", result)]
    assert recorder.reconciled == ["process_refund"]


async def test_verify_proving_the_effect_did_not_happen_allows_a_real_call() -> None:
    ctx, recorder = make_ctx(ReplayHistory([in_doubt("ORD-1", 2500)]))
    tool = Tool()

    async def verify(order_id: str, amount: int) -> None:  # async hooks work too
        return None

    result = await ctx.effect(
        "process_refund", tool.refund, "ORD-1", 2500, idempotent=False, verify=verify
    )

    assert len(tool.calls) == 1 and result["refund_id"] == "rf_1"
    assert [c[0] for c in recorder.calls] == ["started", "completed"]


async def test_a_failing_verify_hook_means_outcome_unknown() -> None:
    ctx, recorder = make_ctx(ReplayHistory([in_doubt("ORD-1", 2500)]))
    tool = Tool()

    def verify(order_id: str, amount: int) -> dict:
        raise ConnectionError("provider API is down")

    with pytest.raises(OutcomeUnknownError, match="verification failed"):
        await ctx.effect(
            "process_refund", tool.refund, "ORD-1", 2500, idempotent=False, verify=verify
        )
    assert tool.calls == [] and recorder.calls[0][0] == "unknown"


async def test_ambiguous_error_from_a_non_idempotent_tool_without_verify_is_unknown() -> None:
    ctx, recorder = make_ctx()
    tool = Tool(results=[AmbiguousOutcomeError("connection dropped after sending")])

    with pytest.raises(OutcomeUnknownError):
        await ctx.effect("process_refund", tool.refund, "ORD-1", 2500, idempotent=False)

    assert len(tool.calls) == 1  # one attempt only: never retried blindly
    assert [c[0] for c in recorder.calls] == ["started", "unknown"]


async def test_ambiguous_error_resolved_by_verify_found() -> None:
    ctx, recorder = make_ctx()
    tool = Tool(results=[AmbiguousOutcomeError("reply lost")])

    result = await ctx.effect(
        "process_refund",
        tool.refund,
        "ORD-1",
        2500,
        idempotent=False,
        verify=lambda order_id, amount: {"refund_id": "rf_FOUND"},
    )

    assert result == {"refund_id": "rf_FOUND"} and len(tool.calls) == 1
    assert recorder.reconciled == ["process_refund"]


async def test_ambiguous_error_resolved_by_verify_not_found_retries_once_proven_safe() -> None:
    ctx, recorder = make_ctx()
    tool = Tool(results=[AmbiguousOutcomeError("request lost")])

    result = await ctx.effect(
        "process_refund", tool.refund, "ORD-1", 2500, idempotent=False, verify=lambda o, a: None
    )

    assert result["refund_id"] == "rf_1" and len(tool.calls) == 2
    assert [c[0] for c in recorder.calls] == ["started", "retry", "completed"]


# ---- unresolved unknowns, resolutions, and halting ----------------------------------------


async def test_replaying_an_unresolved_unknown_step_stops_again_without_new_records() -> None:
    unknown = RecordedStep(
        "process_refund",
        hash_step_input(("ORD-1", 2500), {}),
        status=StepStatus.UNKNOWN,
        error_message="no verify hook",
    )
    ctx, recorder = make_ctx(ReplayHistory([unknown]))
    tool = Tool()

    with pytest.raises(OutcomeUnknownError):
        await ctx.effect("process_refund", tool.refund, "ORD-1", 2500, idempotent=False)

    assert tool.calls == [] and [c[0] for c in recorder.calls] == ["unknown_replayed"]


async def test_a_workflow_cannot_swallow_outcome_unknown_and_carry_on() -> None:
    ctx, recorder = make_ctx(ReplayHistory([in_doubt("ORD-1", 2500)]))
    emailed: list[str] = []

    with pytest.raises(OutcomeUnknownError):
        await ctx.effect("process_refund", Tool().refund, "ORD-1", 2500, idempotent=False)
    try:  # careless code: "refund status unclear, send the email anyway"
        await ctx.step("email", lambda: emailed.append("sent"))
    except Exception:
        pass

    assert emailed == []
    with pytest.raises(OutcomeUnknownError):
        ctx.verify_replay_complete()


async def test_a_step_a_person_confirmed_was_not_executed_runs_without_reconciling() -> None:
    step = RecordedStep(
        "process_refund", hash_step_input(("ORD-1", 2500), {}), confirmed_not_executed=True
    )
    ctx, recorder = make_ctx(ReplayHistory([step]))
    tool = Tool()
    verify_calls: list[int] = []

    result = await ctx.effect(
        "process_refund",
        tool.refund,
        "ORD-1",
        2500,
        idempotent=False,
        verify=lambda o, a: verify_calls.append(1),
    )

    assert len(tool.calls) == 1 and verify_calls == [] and result["refund_id"] == "rf_1"


# ---- replay history built from the new events ---------------------------------------------


def test_history_marks_an_unresolved_unknown_step() -> None:
    history = ReplayHistory.from_events(
        [
            ev("STEP_STARTED", "process_refund", "h"),
            ev("STEP_OUTCOME_UNKNOWN", "process_refund", result={"reason": "no verify hook"}),
        ]
    )
    step = history.steps[0]
    assert (step.status, step.error_message) == (StepStatus.UNKNOWN, "no verify hook")


@pytest.mark.parametrize(
    ("resolution", "status", "result", "safe_to_run"),
    [
        (
            {"outcome": "completed", "result": {"refund_id": "rf_9"}},
            StepStatus.COMPLETED,
            {"refund_id": "rf_9"},
            False,
        ),
        ({"outcome": "failed", "message": "refund never arrived"}, StepStatus.FAILED, None, False),
        ({"outcome": "not_executed"}, StepStatus.IN_DOUBT, None, True),
    ],
)
def test_history_applies_a_reviewers_resolution(resolution, status, result, safe_to_run) -> None:
    history = ReplayHistory.from_events(
        [
            ev("STEP_STARTED", "process_refund", "h"),
            ev("STEP_OUTCOME_UNKNOWN", "process_refund", result={"reason": "r"}),
            ev("STEP_RESOLVED", "process_refund", result=resolution),
        ]
    )
    step = history.steps[0]
    assert (step.status, step.result, step.confirmed_not_executed) == (status, result, safe_to_run)


@pytest.mark.parametrize(
    "events",
    [
        [ev("STEP_RESOLVED", "x", result={"outcome": "completed"})],  # nothing to resolve
        [ev("STEP_OUTCOME_UNKNOWN", "x")],  # never started
        [
            ev("STEP_STARTED", "x", "h"),
            ev("STEP_OUTCOME_UNKNOWN", "x"),
            ev("STEP_RESOLVED", "x", result={"outcome": "something-else"}),
        ],
        [ev("STEP_STARTED", "x", "h"), ev("STEP_OUTCOME_UNKNOWN", "x"), ev("STEP_STARTED", "x", "h")],
    ],
)
def test_inconsistent_review_history_is_reported(events: list) -> None:
    with pytest.raises(CorruptHistoryError):
        ReplayHistory.from_events(events)


def test_a_step_run_again_after_being_resolved_not_executed_counts_a_new_attempt() -> None:
    history = ReplayHistory.from_events(
        [
            ev("STEP_STARTED", "x", "h"),
            ev("STEP_OUTCOME_UNKNOWN", "x"),
            ev("STEP_RESOLVED", "x", result={"outcome": "not_executed"}),
            ev("STEP_STARTED", "x", "h"),
            ev("STEP_COMPLETED", "x", result=1),
        ]
    )
    assert (history.steps[0].status, history.steps[0].attempts) == (StepStatus.COMPLETED, 2)


def test_a_reviewers_not_executed_answer_expires_once_a_new_attempt_has_started() -> None:
    """Regression (found by the Hypothesis property test): 'not executed' only covers the attempt
    that directly follows it. If that attempt starts and the worker dies again, the step is in
    doubt again and must be reconciled again."""
    history = ReplayHistory.from_events(
        [
            ev("STEP_STARTED", "process_refund", "h"),
            ev("STEP_OUTCOME_UNKNOWN", "process_refund", result={"reason": "r"}),
            ev("STEP_RESOLVED", "process_refund", result={"outcome": "not_executed"}),
            ev("STEP_STARTED", "process_refund", "h"),  # the attempt the reviewer cleared... died
        ]
    )
    step = history.steps[0]
    assert (step.status, step.attempts, step.confirmed_not_executed) == (StepStatus.IN_DOUBT, 2, False)


async def test_a_second_crash_after_a_cleared_attempt_is_not_blindly_retried() -> None:
    step = RecordedStep(
        "process_refund", hash_step_input(("ORD-1", 2500), {}), attempts=2, confirmed_not_executed=False
    )
    ctx, recorder = make_ctx(ReplayHistory([step]))
    tool = Tool()

    with pytest.raises(OutcomeUnknownError):
        await ctx.effect("process_refund", tool.refund, "ORD-1", 2500, idempotent=False)
    assert tool.calls == []
