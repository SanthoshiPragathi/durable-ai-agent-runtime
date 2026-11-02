import asyncio

import pytest

from app.database.repositories import EventRepository
from app.database.session import SessionFactory
from app.engine import runtime
from app.engine.errors import (
    InvalidRunStateError,
    InvalidWorkflowInputError,
    RunNotFoundError,
    WorkflowNotFoundError,
)
from app.engine.hashing import hash_step_input
from app.sdk import workflow

pytestmark = [pytest.mark.integration, pytest.mark.usefixtures("isolated_registry")]


async def _events(session_factory: SessionFactory, run_id):
    async with session_factory() as session:
        return await EventRepository(session).list_events(run_id)


def _register_order_workflow() -> None:
    def fetch_order(order_id: str) -> dict:
        return {"order_id": order_id, "amount": 2500}

    async def compute_refund(amount: int) -> int:
        return amount // 2

    @workflow
    async def refund(ctx, order_id: str):
        order = await ctx.step("fetch_order", fetch_order, order_id)
        refund_amount = await ctx.step("compute_refund", compute_refund, order["amount"])
        return {"order_id": order_id, "refund": refund_amount}


async def test_successful_run_is_recorded_end_to_end(session_factory: SessionFactory) -> None:
    _register_order_workflow()

    run = await runtime.create_run(session_factory, "refund", {"order_id": "A1"})
    assert run.status == "PENDING" and run.started_at is None

    final = await runtime.execute_run(session_factory, run.run_id)

    assert final.status == "COMPLETED"
    assert final.result_data == {"order_id": "A1", "refund": 1250}
    assert final.started_at is not None and final.completed_at is not None
    assert final.current_step == "compute_refund"
    assert final.error_message is None

    events = await _events(session_factory, run.run_id)
    assert [(e.sequence_number, e.event_type, e.step_name) for e in events] == [
        (1, "WORKFLOW_STARTED", None),
        (2, "STEP_STARTED", "fetch_order"),
        (3, "STEP_COMPLETED", "fetch_order"),
        (4, "STEP_STARTED", "compute_refund"),
        (5, "STEP_COMPLETED", "compute_refund"),
        (6, "WORKFLOW_COMPLETED", None),
    ]
    assert events[1].input_hash == hash_step_input(("A1",), {})
    assert events[2].result_data == {"order_id": "A1", "amount": 2500}
    assert events[4].result_data == 1250
    assert events[5].result_data == {"order_id": "A1", "refund": 1250}


async def test_failing_step_marks_the_run_failed(session_factory: SessionFactory) -> None:
    async def charge(amount: int) -> None:
        raise RuntimeError("card declined")

    @workflow
    async def pay(ctx, amount: int):
        await ctx.step("prepare", lambda: "ok")
        await ctx.step("charge", charge, amount)
        await ctx.step("never_reached", lambda: "no")

    run = await runtime.create_run(session_factory, "pay", {"amount": 10})
    final = await runtime.execute_run(session_factory, run.run_id)

    assert final.status == "FAILED"
    assert final.error_message == "RuntimeError: card declined"
    assert final.result_data is None and final.completed_at is not None

    events = await _events(session_factory, run.run_id)
    assert [(e.event_type, e.step_name) for e in events] == [
        ("WORKFLOW_STARTED", None),
        ("STEP_STARTED", "prepare"),
        ("STEP_COMPLETED", "prepare"),
        ("STEP_STARTED", "charge"),
        ("STEP_FAILED", "charge"),
        ("WORKFLOW_FAILED", None),
    ]
    assert events[4].result_data == {"error_type": "RuntimeError", "message": "card declined"}
    assert "Traceback" not in str(events[5].result_data)  # no stack traces are stored


async def test_error_raised_outside_a_step_also_fails_the_run(session_factory: SessionFactory) -> None:
    @workflow
    async def sloppy(ctx):
        raise KeyError("oops")

    run = await runtime.create_run(session_factory, "sloppy", {})
    final = await runtime.execute_run(session_factory, run.run_id)
    assert final.status == "FAILED" and final.error_message.startswith("KeyError")


async def test_run_cannot_be_executed_twice(session_factory: SessionFactory) -> None:
    _register_order_workflow()
    run = await runtime.create_run(session_factory, "refund", {"order_id": "A1"})
    await runtime.execute_run(session_factory, run.run_id)
    before = len(await _events(session_factory, run.run_id))

    with pytest.raises(InvalidRunStateError):
        await runtime.execute_run(session_factory, run.run_id)

    assert len(await _events(session_factory, run.run_id)) == before


async def test_concurrent_executions_of_one_run_have_a_single_winner(
    session_factory: SessionFactory,
) -> None:
    _register_order_workflow()
    run = await runtime.create_run(session_factory, "refund", {"order_id": "A1"})

    outcomes = await asyncio.gather(
        *(runtime.execute_run(session_factory, run.run_id) for _ in range(5)),
        return_exceptions=True,
    )

    assert sum(1 for o in outcomes if not isinstance(o, Exception)) == 1
    assert all(isinstance(o, InvalidRunStateError) for o in outcomes if isinstance(o, Exception))
    events = await _events(session_factory, run.run_id)
    assert [e.event_type for e in events].count("WORKFLOW_STARTED") == 1
    assert [e.sequence_number for e in events] == list(range(1, len(events) + 1))


async def test_step_returning_none_is_recorded(session_factory: SessionFactory) -> None:
    @workflow
    async def quiet(ctx):
        return await ctx.step("noop", lambda: None)

    run = await runtime.create_run(session_factory, "quiet", {})
    final = await runtime.execute_run(session_factory, run.run_id)
    assert final.status == "COMPLETED" and final.result_data is None


async def test_create_run_validates_workflow_and_input(session_factory: SessionFactory) -> None:
    _register_order_workflow()
    with pytest.raises(WorkflowNotFoundError):
        await runtime.create_run(session_factory, "missing", {})
    with pytest.raises(InvalidWorkflowInputError):
        await runtime.create_run(session_factory, "refund", {"wrong_name": 1})


async def test_execute_unknown_run_raises(session_factory: SessionFactory) -> None:
    import uuid

    with pytest.raises(RunNotFoundError):
        await runtime.execute_run(session_factory, uuid.uuid4())


async def test_many_runs_execute_concurrently_without_mixing_events(
    session_factory: SessionFactory,
) -> None:
    _register_order_workflow()
    runs = [
        await runtime.create_run(session_factory, "refund", {"order_id": f"O{i}"}) for i in range(8)
    ]
    finals = await asyncio.gather(*(runtime.execute_run(session_factory, r.run_id) for r in runs))

    for i, final in enumerate(finals):
        assert final.status == "COMPLETED"
        assert final.result_data == {"order_id": f"O{i}", "refund": 1250}
        events = await _events(session_factory, final.run_id)
        assert [e.sequence_number for e in events] == [1, 2, 3, 4, 5, 6]
