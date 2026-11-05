"""Signals and timers through the HTTP API (EXECUTION_MODE=workers)."""

from collections.abc import AsyncIterator

import httpx
import pytest
import pytest_asyncio

from app.api.deps import get_session_factory
from app.api.main import app
from app.config import get_settings
from app.database.session import SessionFactory
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


def worker(sf: SessionFactory, name: str = "w1") -> Worker:
    return Worker(sf, name, lease_seconds=30, heartbeat_seconds=10, poll_seconds=0.05)


def register() -> None:
    @workflow
    async def approval(ctx):
        got = await ctx.wait_for_signal("wait", signal="decision", timeout=3600)
        return got


async def test_signal_endpoint_wakes_a_waiting_run(
    client: httpx.AsyncClient, session_factory: SessionFactory
) -> None:
    register()
    run_id = (await client.post("/workflows/approval/runs", json={"input": {}})).json()["run_id"]
    await worker(session_factory).run_until_idle()

    waiting = (await client.get(f"/runs/{run_id}")).json()
    assert waiting["status"] == "WAITING" and waiting["waiting_signal"] == "decision"
    assert waiting["lease_owner"] is None
    timers = (await client.get(f"/runs/{run_id}/timers")).json()
    assert [(t["kind"], t["status"]) for t in timers] == [("SIGNAL_TIMEOUT", "PENDING")]

    sent = await client.post(
        f"/runs/{run_id}/signals/decision", json={"payload": {"approved": True}, "idempotency_key": "a"}
    )
    assert sent.status_code == 202 and sent.json()["woke_run"] is True
    await worker(session_factory, "w2").run_until_idle()

    done = (await client.get(f"/runs/{run_id}")).json()
    assert done["status"] == "COMPLETED" and done["result_data"]["payload"] == {"approved": True}
    signals = (await client.get(f"/runs/{run_id}/signals")).json()
    assert signals[0]["consumed_by_step"] == "wait"


async def test_a_redelivered_signal_returns_200_and_duplicate(
    client: httpx.AsyncClient, session_factory: SessionFactory
) -> None:
    register()
    run_id = (await client.post("/workflows/approval/runs", json={"input": {}})).json()["run_id"]
    await worker(session_factory).run_until_idle()

    body = {"payload": 1, "idempotency_key": "same"}
    first = await client.post(f"/runs/{run_id}/signals/decision", json=body)
    again = await client.post(f"/runs/{run_id}/signals/decision", json=body)

    assert first.status_code == 202 and again.status_code == 200
    assert again.json() == {**first.json(), "duplicate": True, "woke_run": False}
    assert len((await client.get(f"/runs/{run_id}/signals")).json()) == 1


async def test_signal_errors_are_404_and_409(
    client: httpx.AsyncClient, session_factory: SessionFactory
) -> None:
    @workflow
    async def quick(ctx):
        return 1

    missing = await client.post(
        "/runs/00000000-0000-0000-0000-000000000000/signals/x", json={"payload": None}
    )
    assert missing.status_code == 404

    run_id = (await client.post("/workflows/quick/runs", json={"input": {}})).json()["run_id"]
    await worker(session_factory).run_until_idle()
    finished = await client.post(f"/runs/{run_id}/signals/x", json={"payload": None})
    assert finished.status_code == 409
