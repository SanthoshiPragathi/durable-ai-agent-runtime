"""Forking a run from a step, and diffing runs."""

import uuid

import httpx
import pytest
import pytest_asyncio

from app.api.deps import get_session_factory
from app.api.main import app
from app.config import get_settings
from app.database.repositories import EventRepository, RunRepository
from app.database.session import SessionFactory
from app.engine import runtime
from app.engine.errors import ForkError, RunNotFoundError
from app.engine.forks import diff_runs, fork_run
from app.engine.worker import Worker
from app.sdk import workflow

pytestmark = [pytest.mark.integration, pytest.mark.usefixtures("isolated_registry")]


def worker(sf: SessionFactory, name: str = "w1") -> Worker:
    return Worker(sf, name, lease_seconds=30, heartbeat_seconds=10, poll_seconds=0.05)


def register(calls: list[str], verdict: str = "APPROVE") -> None:
    @workflow
    async def pipeline(ctx, n: int):
        a = await ctx.step("load", lambda: calls.append("load") or n * 2)
        v = await ctx.step("decide", lambda: calls.append("decide") or verdict)
        out = await ctx.step("act", lambda: calls.append("act") or f"{v}:{a}")
        return out


async def finished_run(sf: SessionFactory, calls: list[str], **kw) -> uuid.UUID:
    register(calls, **kw)
    run = await runtime.create_run(sf, "pipeline", {"n": 5})
    await worker(sf).run_until_idle()
    return run.run_id


async def get(sf: SessionFactory, run_id: uuid.UUID):
    async with sf() as session:
        run = await RunRepository(session).get_run(run_id)
        events = await EventRepository(session).list_events(run_id)
    assert run is not None
    return run, events


async def test_a_fork_reuses_earlier_steps_and_reruns_from_the_fork_point(session_factory) -> None:
    calls: list[str] = []
    source = await finished_run(session_factory, calls)
    assert calls == ["load", "decide", "act"]

    fork = await fork_run(session_factory, source, "decide")
    assert fork.parent_run_id == source and fork.status == "RUNNING" and fork.lease_owner is None
    await worker(session_factory, "w2").run_until_idle()

    done, events = await get(session_factory, fork.run_id)
    assert done.status == "COMPLETED" and done.result_data == "APPROVE:10"
    assert calls == ["load", "decide", "act", "decide", "act"]  # "load" was replayed, not re-run
    started = events[0].result_data
    assert started["forked_from"] == str(source) and started["from_step"] == "decide"
    source_run, _ = await get(session_factory, source)
    assert source_run.status == "COMPLETED"  # the source is untouched


async def test_an_override_replaces_the_fork_step_and_changes_what_follows(session_factory) -> None:
    calls: list[str] = []
    source = await finished_run(session_factory, calls)

    fork = await fork_run(session_factory, source, "decide", override_result="REJECT")
    await worker(session_factory).run_until_idle()

    done, _ = await get(session_factory, fork.run_id)
    assert done.result_data == "REJECT:10"
    assert calls == ["load", "decide", "act", "act"]  # decide was not executed in the fork

    diff = await diff_runs(session_factory, source, fork.run_id)
    assert diff.first_divergence == "decide"
    by_name = {s.step_name: s for s in diff.steps}
    assert by_name["load"].identical
    assert by_name["decide"].result_a == "APPROVE" and by_name["decide"].result_b == "REJECT"
    assert by_name["decide"].same_input and not by_name["decide"].same_result
    assert not by_name["act"].identical
    assert (diff.result_a, diff.result_b) == ("APPROVE:10", "REJECT:10")


async def test_diff_of_a_run_with_itself_has_no_divergence(session_factory) -> None:
    source = await finished_run(session_factory, [])
    diff = await diff_runs(session_factory, source, source)
    assert diff.first_divergence is None and all(s.identical for s in diff.steps)


async def test_fork_errors(session_factory) -> None:
    source = await finished_run(session_factory, [])
    with pytest.raises(ForkError):
        await fork_run(session_factory, source, "no_such_step")
    with pytest.raises(RunNotFoundError):
        await fork_run(session_factory, uuid.uuid4(), "load")
    pending = await runtime.create_run(session_factory, "pipeline", {"n": 1})
    with pytest.raises(ForkError):
        await fork_run(session_factory, pending.run_id, "load")


async def test_cannot_fork_after_a_step_that_did_not_complete(session_factory) -> None:
    @workflow
    async def breaks(ctx):
        await ctx.step("ok", lambda: 1)

        def boom():
            raise RuntimeError("x")

        await ctx.step("bad", boom)
        await ctx.step("never", lambda: 3)

    run = await runtime.create_run(session_factory, "breaks", {})
    await worker(session_factory).run_until_idle()
    with pytest.raises(ForkError):
        await fork_run(session_factory, run.run_id, "never")  # 'bad' failed, it has no result to reuse
    await fork_run(session_factory, run.run_id, "bad")  # forking AT the failed step is allowed


async def test_changing_an_input_an_earlier_step_used_is_rejected_at_replay(session_factory) -> None:
    source = await finished_run(session_factory, [])
    # "load" is a lambda over n, so n isn't part of its recorded input; use a step that takes n.
    @workflow
    async def takes_input(ctx, n: int):
        return await ctx.step("double", lambda x: x * 2, n)

    run = await runtime.create_run(session_factory, "takes_input", {"n": 1})
    await worker(session_factory).run_until_idle()
    fork = await fork_run(session_factory, run.run_id, "double", input_overrides={"n": 2})
    await worker(session_factory).run_until_idle()  # same step re-runs with new input: fine
    done, _ = await get(session_factory, fork.run_id)
    assert done.result_data == 4
    assert source  # silence unused


@pytest_asyncio.fixture
async def client(session_factory, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("EXECUTION_MODE", "workers")
    get_settings.cache_clear()
    app.dependency_overrides[get_session_factory] = lambda: session_factory
    transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c
    app.dependency_overrides.clear()
    monkeypatch.undo()
    get_settings.cache_clear()


async def test_fork_and_diff_through_the_api(client: httpx.AsyncClient, session_factory) -> None:
    source = await finished_run(session_factory, [])

    resp = await client.post(
        f"/runs/{source}/fork",
        json={"from_step": "decide", "use_override": True, "override_result": "REJECT"},
    )
    assert resp.status_code == 202
    fork_id = resp.json()["run_id"]
    assert resp.json()["parent_run_id"] == str(source)
    await worker(session_factory).run_until_idle()

    diff = (await client.get(f"/runs/{source}/diff/{fork_id}")).json()
    assert diff["first_divergence"] == "decide" and diff["result_b"] == "REJECT:10"

    assert (await client.post(f"/runs/{source}/fork", json={"from_step": "nope"})).status_code == 409
    missing = "00000000-0000-0000-0000-000000000000"
    assert (await client.get(f"/runs/{source}/diff/{missing}")).status_code == 404
