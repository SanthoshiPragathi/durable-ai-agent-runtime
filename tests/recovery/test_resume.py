"""Crash and resume, simulated in-process. Every test crashes a run mid-way, resumes it from the
recorded history, and checks what was (and was not) executed again."""

import uuid
from collections import Counter
from typing import Any

import pytest

from app.database.repositories import EventRepository, RunRepository
from app.database.session import SessionFactory
from app.engine import runtime
from app.engine.errors import (
    InvalidRunStateError,
    NonDeterminismError,
    RunNotFoundError,
    StepFailedError,
    WorkflowVersionMismatchError,
)
from app.integrations.llm import MockLLMProvider
from app.sdk import workflow
from app.sdk.workflow import registry
from tests.fixtures.crash import SimulatedCrash

pytestmark = [
    pytest.mark.integration,
    pytest.mark.recovery,
    pytest.mark.usefixtures("isolated_registry"),
]


class Pipeline:
    """Four steps; counts how often each step function really ran; can crash inside any step."""

    def __init__(self) -> None:
        self.counts: Counter[str] = Counter()
        self.crash_at: str | None = None

    def _enter(self, name: str) -> None:
        self.counts[name] += 1
        if self.crash_at == name:
            raise SimulatedCrash(name)

    def fetch_order(self, order_id: str) -> dict:
        self._enter("fetch_order")
        return {"order_id": order_id, "amount": 2500}

    def check_eligibility(self, order: dict) -> bool:
        self._enter("check_eligibility")
        return order["amount"] < 10_000

    def process_refund(self, amount: int) -> dict:
        self._enter("process_refund")
        return {"refund_id": "r-1", "amount": amount}

    def send_email(self, refund_id: str) -> str:
        self._enter("send_email")
        return f"sent:{refund_id}"


def register_pipeline(p: Pipeline) -> None:
    @workflow(name="refund_pipeline")
    async def refund_pipeline(ctx, order_id: str):
        order = await ctx.step("fetch_order", p.fetch_order, order_id)
        eligible = await ctx.step("check_eligibility", p.check_eligibility, order)
        refund = await ctx.step("process_refund", p.process_refund, order["amount"])
        await ctx.step("send_email", p.send_email, refund["refund_id"])
        return {"order_id": order_id, "refund_id": refund["refund_id"], "eligible": eligible}


async def events_of(sf: SessionFactory, run_id: uuid.UUID) -> list:
    async with sf() as session:
        return await EventRepository(session).list_events(run_id)


async def run_of(sf: SessionFactory, run_id: uuid.UUID):
    async with sf() as session:
        return await RunRepository(session).get_run(run_id)


def summary(events: list) -> list[tuple[str, str | None]]:
    return [(e.event_type, e.step_name) for e in events]


async def start(sf: SessionFactory, name: str = "refund_pipeline") -> uuid.UUID:
    input_data: dict[str, Any] = {"order_id": "A1"} if name == "refund_pipeline" else {}
    run = await runtime.create_run(sf, name, input_data)
    return run.run_id


# ---- the core promise -----------------------------------------------------------------------


async def test_resume_skips_completed_steps_and_finishes_the_run(session_factory: SessionFactory) -> None:
    p = Pipeline()
    register_pipeline(p)
    run_id = await start(session_factory)

    p.crash_at = "process_refund"
    with pytest.raises(SimulatedCrash):
        await runtime.execute_run(session_factory, run_id)

    crashed = await run_of(session_factory, run_id)
    assert crashed.status == "RUNNING" and crashed.current_step == "process_refund"
    assert summary(await events_of(session_factory, run_id))[-1] == ("STEP_STARTED", "process_refund")

    p.crash_at = None
    final = await runtime.resume_run(session_factory, run_id)

    assert final.status == "COMPLETED"
    assert final.result_data == {"order_id": "A1", "refund_id": "r-1", "eligible": True}
    # Steps 1 and 2 had completed, so they were NOT executed again.
    assert p.counts == {"fetch_order": 1, "check_eligibility": 1, "process_refund": 2, "send_email": 1}

    events = await events_of(session_factory, run_id)
    assert [e.sequence_number for e in events] == list(range(1, len(events) + 1))
    assert summary(events) == [
        ("WORKFLOW_STARTED", None),
        ("STEP_STARTED", "fetch_order"),
        ("STEP_COMPLETED", "fetch_order"),
        ("STEP_STARTED", "check_eligibility"),
        ("STEP_COMPLETED", "check_eligibility"),
        ("STEP_STARTED", "process_refund"),  # first attempt: died here
        ("WORKFLOW_RESUMED", None),
        ("STEP_STARTED", "process_refund"),  # second attempt
        ("STEP_COMPLETED", "process_refund"),
        ("STEP_STARTED", "send_email"),
        ("STEP_COMPLETED", "send_email"),
        ("WORKFLOW_COMPLETED", None),
    ]
    resumed = next(e for e in events if e.event_type == "WORKFLOW_RESUMED")
    assert resumed.result_data == {"recorded_steps": 3, "completed": 2, "failed": 0, "in_doubt": 1}


async def test_resumed_run_ends_with_the_same_result_as_an_uninterrupted_run(
    session_factory: SessionFactory,
) -> None:
    p = Pipeline()
    register_pipeline(p)

    clean = await runtime.execute_run(session_factory, await start(session_factory))

    crashed_id = await start(session_factory)
    p.crash_at = "send_email"
    with pytest.raises(SimulatedCrash):
        await runtime.execute_run(session_factory, crashed_id)
    p.crash_at = None
    resumed = await runtime.resume_run(session_factory, crashed_id)

    assert resumed.result_data == clean.result_data and resumed.status == clean.status == "COMPLETED"


async def test_crash_between_steps_replays_everything_without_rerunning_anything(
    session_factory: SessionFactory,
) -> None:
    calls: Counter[str] = Counter()
    crash = {"now": True}

    @workflow(name="two_steps")
    async def two_steps(ctx):
        a = await ctx.step("a", lambda: calls.update(["a"]) or 1)
        if crash["now"]:
            raise SimulatedCrash("between steps, outside any step")
        b = await ctx.step("b", lambda: calls.update(["b"]) or 2)
        return a + b

    run_id = await start(session_factory, "two_steps")
    with pytest.raises(SimulatedCrash):
        await runtime.execute_run(session_factory, run_id)

    crash["now"] = False
    final = await runtime.resume_run(session_factory, run_id)

    assert final.result_data == 3 and calls == {"a": 1, "b": 1}
    resumed = next(e for e in await events_of(session_factory, run_id) if e.event_type == "WORKFLOW_RESUMED")
    assert resumed.result_data["in_doubt"] == 0


async def test_crash_before_the_first_step_resumes_from_an_empty_history(
    session_factory: SessionFactory,
) -> None:
    crash = {"now": True}

    @workflow(name="early")
    async def early(ctx):
        if crash["now"]:
            raise SimulatedCrash("before any step")
        return await ctx.step("only", lambda: "done")

    run_id = await start(session_factory, "early")
    with pytest.raises(SimulatedCrash):
        await runtime.execute_run(session_factory, run_id)
    crash["now"] = False
    assert (await runtime.resume_run(session_factory, run_id)).result_data == "done"


async def test_a_run_can_crash_and_be_resumed_repeatedly(session_factory: SessionFactory) -> None:
    p = Pipeline()
    register_pipeline(p)
    run_id = await start(session_factory)

    for crash_at in ("process_refund", "send_email"):
        p.crash_at = crash_at
        with pytest.raises(SimulatedCrash):
            if crash_at == "process_refund":
                await runtime.execute_run(session_factory, run_id)
            else:
                await runtime.resume_run(session_factory, run_id)

    p.crash_at = None
    final = await runtime.resume_run(session_factory, run_id)

    assert final.status == "COMPLETED"
    # Each crash re-ran only the step it died in.
    assert p.counts == {"fetch_order": 1, "check_eligibility": 1, "process_refund": 2, "send_email": 2}
    events = await events_of(session_factory, run_id)
    assert [e.event_type for e in events].count("WORKFLOW_RESUMED") == 2
    assert [e.event_type for e in events].count("WORKFLOW_COMPLETED") == 1
    assert [e.sequence_number for e in events] == list(range(1, len(events) + 1))


# ---- recorded failures, time, randomness, LLM answers --------------------------------------


async def test_a_handled_step_failure_takes_the_same_branch_after_resume(
    session_factory: SessionFactory,
) -> None:
    calls: Counter[str] = Counter()
    crash = {"now": True}

    def primary() -> str:
        calls["primary"] += 1
        raise ValueError("primary service is down")

    @workflow(name="with_fallback")
    async def with_fallback(ctx):
        try:
            value = await ctx.step("primary", primary)
        except StepFailedError:
            value = await ctx.step("fallback", lambda: calls.update(["fallback"]) or "fallback-value")
        if crash["now"]:
            raise SimulatedCrash("after the handled failure")
        return value

    run_id = await start(session_factory, "with_fallback")
    with pytest.raises(SimulatedCrash):
        await runtime.execute_run(session_factory, run_id)
    crash["now"] = False
    final = await runtime.resume_run(session_factory, run_id)

    assert final.result_data == "fallback-value"
    assert calls == {"primary": 1, "fallback": 1}  # the failing step was not retried on replay


async def test_time_and_randomness_are_identical_after_resume(session_factory: SessionFactory) -> None:
    crash = {"now": True}

    @workflow(name="clocked")
    async def clocked(ctx):
        t = await ctx.now()
        r = await ctx.random()

        def flaky() -> str:
            if crash["now"]:
                raise SimulatedCrash("dies after now() and random() were recorded")
            return "ok"

        await ctx.step("flaky", flaky)
        return {"t": t.isoformat(), "r": r}

    run_id = await start(session_factory, "clocked")
    with pytest.raises(SimulatedCrash):
        await runtime.execute_run(session_factory, run_id)
    recorded = {
        e.step_name: e.result_data
        for e in await events_of(session_factory, run_id)
        if e.event_type == "STEP_COMPLETED"
    }

    crash["now"] = False
    final = await runtime.resume_run(session_factory, run_id)

    assert final.result_data == {"t": recorded["__now_1"], "r": recorded["__random_1"]}


async def test_llm_answer_is_not_asked_again_after_resume(session_factory: SessionFactory) -> None:
    llm = MockLLMProvider(["APPROVE", "REJECT"])  # a second call WOULD give a different answer
    crash = {"now": True}

    @workflow(name="decider")
    async def decider(ctx):
        decision = await ctx.llm("refund_decision", prompt="Should we refund order A1?")
        if crash["now"]:
            raise SimulatedCrash("after the model answered")
        return decision

    run_id = await start(session_factory, "decider")
    with pytest.raises(SimulatedCrash):
        await runtime.execute_run(session_factory, run_id, llm_provider=llm)
    crash["now"] = False
    final = await runtime.resume_run(session_factory, run_id, llm_provider=llm)

    assert final.result_data == "APPROVE" and len(llm.calls) == 1

    recorded = next(
        e.result_data
        for e in await events_of(session_factory, run_id)
        if e.event_type == "STEP_COMPLETED" and e.step_name == "refund_decision"
    )
    assert recorded["output"] == "APPROVE" and recorded["model"] == "mock-model-a"
    assert recorded["input_tokens"] == 5 and recorded["output_tokens"] == 1
    assert recorded["cost_usd"] is not None and recorded["latency_ms"] >= 0


# ---- the code changed under a recorded run --------------------------------------------------


async def _crash_a_refund_run(session_factory: SessionFactory, amount: int) -> tuple[uuid.UUID, Counter]:
    calls: Counter[str] = Counter()

    @workflow(name="refund_flow")
    async def refund_flow(ctx):
        await ctx.step("refund", lambda a: calls.update(["refund"]) or {"paid": a}, amount)
        raise SimulatedCrash("after the refund completed")

    run_id = await start(session_factory, "refund_flow")
    with pytest.raises(SimulatedCrash):
        await runtime.execute_run(session_factory, run_id)
    registry.clear()
    return run_id, calls


async def test_changed_step_input_is_detected_and_nothing_executes(session_factory: SessionFactory) -> None:
    run_id, calls = await _crash_a_refund_run(session_factory, 2000)
    events_before = len(await events_of(session_factory, run_id))

    @workflow(name="refund_flow")
    async def refund_flow(ctx):  # same name, but the amount changed from 2000 to 3000
        await ctx.step("refund", lambda a: calls.update(["refund"]) or {"paid": a}, 3000)
        return "done"

    with pytest.raises(NonDeterminismError, match="different input"):
        await runtime.resume_run(session_factory, run_id)

    assert calls == {"refund": 1}  # the 3000 refund was never executed
    run = await run_of(session_factory, run_id)
    assert run.status == "RUNNING"  # not silently failed: fix the code and resume again
    new_events = (await events_of(session_factory, run_id))[events_before:]
    assert summary(new_events) == [("WORKFLOW_RESUMED", None)]


async def test_renamed_step_is_detected(session_factory: SessionFactory) -> None:
    run_id, _ = await _crash_a_refund_run(session_factory, 2000)

    @workflow(name="refund_flow")
    async def refund_flow(ctx):
        await ctx.step("issue_refund", lambda a: {"paid": a}, 2000)

    with pytest.raises(NonDeterminismError, match="'refund'.*'issue_refund'"):
        await runtime.resume_run(session_factory, run_id)


async def test_removed_step_is_detected_when_the_workflow_ends_early(
    session_factory: SessionFactory,
) -> None:
    run_id, _ = await _crash_a_refund_run(session_factory, 2000)

    @workflow(name="refund_flow")
    async def refund_flow(ctx):
        return "skipped the refund step entirely"

    with pytest.raises(NonDeterminismError, match="recorded history"):
        await runtime.resume_run(session_factory, run_id)
    run = await run_of(session_factory, run_id)
    assert run.status == "RUNNING" and run.result_data is None


async def test_a_workflow_cannot_swallow_a_divergence_and_keep_going(
    session_factory: SessionFactory,
) -> None:
    run_id, calls = await _crash_a_refund_run(session_factory, 2000)
    side_effects: list[str] = []

    @workflow(name="refund_flow")
    async def refund_flow(ctx):
        try:
            await ctx.step("refund", lambda a: {"paid": a}, 9999)  # diverges
        except Exception:
            pass  # careless code that hides the problem...
        await ctx.step("email_customer", lambda: side_effects.append("email sent"))  # ...must not run
        return "finished"

    with pytest.raises(NonDeterminismError):
        await runtime.resume_run(session_factory, run_id)
    assert side_effects == []
    assert (await run_of(session_factory, run_id)).status == "RUNNING"


async def test_new_steps_added_after_the_recorded_point_are_allowed(session_factory: SessionFactory) -> None:
    run_id, calls = await _crash_a_refund_run(session_factory, 2000)

    @workflow(name="refund_flow")
    async def refund_flow(ctx):
        paid = await ctx.step("refund", lambda a: calls.update(["refund"]) or {"paid": a}, 2000)
        await ctx.step("audit_log", lambda: "logged")  # new code, after the recorded history
        return paid

    final = await runtime.resume_run(session_factory, run_id)
    assert final.status == "COMPLETED" and final.result_data == {"paid": 2000}
    assert calls == {"refund": 1}


# ---- when resume is refused -----------------------------------------------------------------


async def test_resume_is_refused_for_runs_that_are_not_running(session_factory: SessionFactory) -> None:
    p = Pipeline()
    register_pipeline(p)

    pending = await start(session_factory)
    with pytest.raises(InvalidRunStateError, match="PENDING"):
        await runtime.resume_run(session_factory, pending)

    done = await runtime.execute_run(session_factory, await start(session_factory))
    with pytest.raises(InvalidRunStateError, match="COMPLETED"):
        await runtime.resume_run(session_factory, done.run_id)

    p.crash_at = "send_email"
    with pytest.raises(SimulatedCrash):
        await runtime.execute_run(session_factory, await start(session_factory))
    p.crash_at = None

    with pytest.raises(RunNotFoundError):
        await runtime.resume_run(session_factory, uuid.uuid4())


async def test_resume_is_refused_when_the_workflow_version_changed(session_factory: SessionFactory) -> None:
    crash = {"now": True}

    @workflow(name="versioned", version=1)
    async def versioned_v1(ctx):
        await ctx.step("a", lambda: 1)
        if crash["now"]:
            raise SimulatedCrash("x")

    run_id = await start(session_factory, "versioned")
    with pytest.raises(SimulatedCrash):
        await runtime.execute_run(session_factory, run_id)

    registry.clear()

    @workflow(name="versioned", version=2)
    async def versioned_v2(ctx):
        await ctx.step("a", lambda: 1)

    with pytest.raises(WorkflowVersionMismatchError, match="v1.*v2"):
        await runtime.resume_run(session_factory, run_id)
    assert (await run_of(session_factory, run_id)).status == "RUNNING"
