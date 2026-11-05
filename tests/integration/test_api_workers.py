"""EXECUTION_MODE=workers: the API records runs, worker processes execute them."""

from collections.abc import AsyncIterator

import httpx
import pytest
import pytest_asyncio

from app.api.deps import get_session_factory
from app.api.main import app
from app.config import get_settings
from app.database.models import EventType
from app.database.repositories import EventRepository, RunRepository
from app.database.session import SessionFactory
from app.engine import runtime
from app.engine.hashing import hash_step_input
from app.engine.worker import Worker
from app.sdk import workflow

pytestmark = [pytest.mark.integration, pytest.mark.usefixtures("isolated_registry")]


@pytest_asyncio.fixture
async def client(
    session_factory: SessionFactory, monkeypatch: pytest.MonkeyPatch
) -> AsyncIterator[httpx.AsyncClient]:
    monkeypatch.setenv("EXECUTION_MODE", "workers")
    get_settings.cache_clear()
    app.dependency_overrides[get_session_factory] = lambda: session_factory
    transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c
    app.dependency_overrides.clear()
    monkeypatch.undo()
    get_settings.cache_clear()


def worker(session_factory: SessionFactory, name: str = "w1") -> Worker:
    return Worker(session_factory, name, lease_seconds=30, heartbeat_seconds=10, poll_seconds=0.05)


async def test_the_api_only_records_the_run_and_a_worker_executes_it(
    client: httpx.AsyncClient, session_factory: SessionFactory
) -> None:
    @workflow
    async def hello(ctx, name: str):
        return await ctx.step("greet", lambda: f"hello {name}")

    created = await client.post("/workflows/hello/runs", json={"input": {"name": "asha"}})
    run_id = created.json()["run_id"]
    assert (await client.get(f"/runs/{run_id}")).json()["status"] == "PENDING"  # nobody ran it

    assert await worker(session_factory).run_until_idle() == 1

    body = (await client.get(f"/runs/{run_id}")).json()
    assert body["status"] == "COMPLETED" and body["result_data"] == "hello asha"
    assert body["fencing_token"] == 1 and body["lease_owner"] is None


async def test_run_output_shows_the_lease_while_a_worker_holds_it(
    client: httpx.AsyncClient, session_factory: SessionFactory
) -> None:
    @workflow
    async def hello(ctx):
        return await ctx.step("s", lambda: 1)

    run_id = (await client.post("/workflows/hello/runs", json={"input": {}})).json()["run_id"]
    claim = await worker(session_factory, "w7").claim_one()
    assert claim is not None

    body = (await client.get(f"/runs/{run_id}")).json()
    assert body["status"] == "RUNNING" and body["lease_owner"] == "w7" and body["fencing_token"] == 1
    assert body["lease_expires_at"] is not None


async def test_workers_endpoint_lists_registered_workers(
    client: httpx.AsyncClient, session_factory: SessionFactory
) -> None:
    w = worker(session_factory, "w-list")
    await w.register()

    (row,) = (await client.get("/workers")).json()
    assert row["worker_id"] == "w-list" and row["status"] == "ACTIVE" and row["alive"] is True

    await w.deregister()
    (row,) = (await client.get("/workers")).json()
    assert row["status"] == "STOPPED" and row["alive"] is False


async def test_resolving_a_review_run_hands_it_to_a_worker(
    client: httpx.AsyncClient, session_factory: SessionFactory
) -> None:
    sent: list[str] = []

    @workflow
    async def pay(ctx):
        paid = await ctx.effect("charge", lambda: sent.append("charged") or "new", idempotent=False)
        return {"paid": paid}

    run_id = (await client.post("/workflows/pay/runs", json={"input": {}})).json()["run_id"]
    # Put the run into NEEDS_REVIEW the way a crashed non-idempotent charge would.
    async with session_factory() as session, session.begin():
        runs, events = RunRepository(session), EventRepository(session)
        import uuid

        rid = uuid.UUID(run_id)
        assert await runs.mark_running(rid)
        await events.append(rid, EventType.WORKFLOW_STARTED)
        await events.append(
            rid, EventType.STEP_STARTED, step_name="charge", input_hash=hash_step_input((), {})
        )
        await events.append(
            rid, EventType.STEP_OUTCOME_UNKNOWN, step_name="charge", result_data={"reason": "x"}
        )
        await runs.mark_needs_review(rid)

    response = await client.post(
        f"/runs/{run_id}/steps/charge/resolve",
        json={"outcome": "completed", "result": "charged-by-person", "resolved_by": "asha"},
    )
    assert response.status_code == 202
    assert (await client.get(f"/runs/{run_id}")).json()["status"] == "RUNNING"  # waiting for a worker

    assert await worker(session_factory).run_until_idle() == 1

    body = (await client.get(f"/runs/{run_id}")).json()
    assert body["status"] == "COMPLETED" and body["result_data"] == {"paid": "charged-by-person"}
    assert sent == []  # the reviewer's answer was used; the charge was never run again


async def test_resume_run_is_refused_for_a_run_a_worker_holds(
    client: httpx.AsyncClient, session_factory: SessionFactory
) -> None:
    @workflow
    async def hello(ctx):
        return await ctx.step("s", lambda: 1)

    run_id = (await client.post("/workflows/hello/runs", json={"input": {}})).json()["run_id"]
    await worker(session_factory).claim_one()
    import uuid

    from app.engine.errors import InvalidRunStateError

    with pytest.raises(InvalidRunStateError):
        await runtime.resume_run(session_factory, uuid.UUID(run_id))
