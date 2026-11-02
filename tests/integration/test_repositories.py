import asyncio
import uuid

import pytest

from app.database.models import EventType, RunStatus
from app.database.repositories import EventRepository, RunRepository
from app.database.session import SessionFactory
from app.engine.errors import DuplicateEventError, InvalidRunStateError, RunNotFoundError

pytestmark = pytest.mark.integration


async def _new_run(session_factory: SessionFactory, input_data=None) -> uuid.UUID:
    async with session_factory() as session, session.begin():
        repo = RunRepository(session)
        await repo.ensure_workflow("wf", 1)
        run = await repo.create_run("wf", 1, input_data or {})
        return run.run_id


async def test_create_and_get_run(session_factory: SessionFactory) -> None:
    run_id = await _new_run(session_factory, {"order_id": "A1"})
    async with session_factory() as session:
        run = await RunRepository(session).get_run(run_id)
    assert run is not None
    assert run.status == RunStatus.PENDING.value
    assert run.input_data == {"order_id": "A1"}
    assert run.created_at is not None and run.started_at is None


async def test_ensure_workflow_is_idempotent(session_factory: SessionFactory) -> None:
    async with session_factory() as session, session.begin():
        repo = RunRepository(session)
        await repo.ensure_workflow("wf", 1)
        await repo.ensure_workflow("wf", 1)


async def test_events_get_consecutive_sequence_numbers(session_factory: SessionFactory) -> None:
    run_id = await _new_run(session_factory)
    for event_type in (EventType.WORKFLOW_STARTED, EventType.STEP_STARTED, EventType.STEP_COMPLETED):
        async with session_factory() as session, session.begin():
            await EventRepository(session).append(run_id, event_type, step_name="s")
    async with session_factory() as session:
        events = await EventRepository(session).list_events(run_id)
    assert [e.sequence_number for e in events] == [1, 2, 3]
    assert [e.event_type for e in events] == [
        "WORKFLOW_STARTED",
        "STEP_STARTED",
        "STEP_COMPLETED",
    ]


async def test_concurrent_appends_never_collide(session_factory: SessionFactory) -> None:
    """25 writers append to the same run at the same time. Sequence numbers must be 1..25."""
    run_id = await _new_run(session_factory)

    async def writer(i: int) -> None:
        async with session_factory() as session, session.begin():
            await EventRepository(session).append(
                run_id, EventType.STEP_STARTED, step_name=f"step-{i}"
            )

    await asyncio.gather(*(writer(i) for i in range(25)))

    async with session_factory() as session:
        events = await EventRepository(session).list_events(run_id)
    assert [e.sequence_number for e in events] == list(range(1, 26))
    assert len({e.step_name for e in events}) == 25


async def test_sequences_are_independent_per_run(session_factory: SessionFactory) -> None:
    run_a, run_b = await _new_run(session_factory), await _new_run(session_factory)
    for run_id in (run_a, run_a, run_b):
        async with session_factory() as session, session.begin():
            await EventRepository(session).append(run_id, EventType.STEP_STARTED, step_name="s")
    async with session_factory() as session:
        events_a = await EventRepository(session).list_events(run_a)
        events_b = await EventRepository(session).list_events(run_b)
    assert [e.sequence_number for e in events_a] == [1, 2]
    assert [e.sequence_number for e in events_b] == [1]


async def test_append_to_unknown_run_raises(session_factory: SessionFactory) -> None:
    with pytest.raises(RunNotFoundError):
        async with session_factory() as session, session.begin():
            await EventRepository(session).append(uuid.uuid4(), EventType.WORKFLOW_STARTED)


async def test_second_completion_of_a_step_is_rejected_and_rolled_back(
    session_factory: SessionFactory,
) -> None:
    run_id = await _new_run(session_factory)
    async with session_factory() as session, session.begin():
        await EventRepository(session).append(
            run_id, EventType.STEP_COMPLETED, step_name="charge", result_data={"ok": True}
        )
    with pytest.raises(DuplicateEventError):
        async with session_factory() as session, session.begin():
            await EventRepository(session).append(
                run_id, EventType.STEP_COMPLETED, step_name="charge", result_data={"ok": False}
            )
    async with session_factory() as session:
        events = await EventRepository(session).list_events(run_id)
    assert len(events) == 1
    assert events[0].result_data == {"ok": True}


async def test_mark_running_succeeds_only_once(session_factory: SessionFactory) -> None:
    run_id = await _new_run(session_factory)
    results = []
    for _ in range(2):
        async with session_factory() as session, session.begin():
            results.append(await RunRepository(session).mark_running(run_id))
    assert results == [True, False]


async def test_concurrent_mark_running_has_a_single_winner(session_factory: SessionFactory) -> None:
    run_id = await _new_run(session_factory)

    async def attempt() -> bool:
        async with session_factory() as session, session.begin():
            return await RunRepository(session).mark_running(run_id)

    outcomes = await asyncio.gather(*(attempt() for _ in range(10)))
    assert outcomes.count(True) == 1


async def test_finish_requires_running_state(session_factory: SessionFactory) -> None:
    run_id = await _new_run(session_factory)
    with pytest.raises(InvalidRunStateError):
        async with session_factory() as session, session.begin():
            await RunRepository(session).mark_completed(run_id, {"x": 1})

    async with session_factory() as session, session.begin():
        repo = RunRepository(session)
        assert await repo.mark_running(run_id)
        await repo.mark_completed(run_id, {"x": 1})
    async with session_factory() as session:
        run = await RunRepository(session).get_run(run_id)
    assert run is not None
    assert run.status == "COMPLETED" and run.result_data == {"x": 1} and run.completed_at


async def test_list_runs_filters_by_status_and_workflow(session_factory: SessionFactory) -> None:
    first, _second = await _new_run(session_factory), await _new_run(session_factory)
    async with session_factory() as session, session.begin():
        await RunRepository(session).mark_running(first)
    async with session_factory() as session:
        repo = RunRepository(session)
        assert len(await repo.list_runs()) == 2
        running = await repo.list_runs(status="RUNNING")
        assert [r.run_id for r in running] == [first]
        assert await repo.list_runs(workflow_name="other") == []
        assert len(await repo.list_runs(limit=1)) == 1
