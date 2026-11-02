"""The manual-review API: finding runs that need a person, and resolving their unknown steps."""

import uuid
from collections.abc import AsyncIterator

import httpx
import pytest
import pytest_asyncio

from app.api.deps import get_session_factory
from app.api.main import app
from app.database.session import SessionFactory
from app.engine import runtime
from app.integrations.tools import NonIdempotentPaymentService
from app.sdk import workflow
from tests.fixtures.crash import CrashPlan, SimulatedCrash

pytestmark = [pytest.mark.integration, pytest.mark.usefixtures("isolated_registry")]


@pytest_asyncio.fixture
async def client(session_factory: SessionFactory) -> AsyncIterator[httpx.AsyncClient]:
    app.dependency_overrides[get_session_factory] = lambda: session_factory
    transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c
    app.dependency_overrides.clear()


@pytest_asyncio.fixture
async def review_run(
    session_factory: SessionFactory, services_session_factory: SessionFactory
) -> tuple[uuid.UUID, NonIdempotentPaymentService]:
    """A run parked in NEEDS_REVIEW: a non-idempotent refund whose worker died after it succeeded."""
    legacy = NonIdempotentPaymentService(services_session_factory)
    crash = CrashPlan()
    crash.after.add("process_refund")

    @workflow(name="legacy_refund")
    async def legacy_refund(ctx):
        refund = await ctx.effect(
            "process_refund", crash.wrap("process_refund", legacy.refund), "ORD-1", 2500, idempotent=False
        )
        await ctx.step("record", lambda: "recorded")
        return refund["refund_id"]

    run_id = (await runtime.create_run(session_factory, "legacy_refund", {})).run_id
    with pytest.raises(SimulatedCrash):
        await runtime.execute_run(session_factory, run_id)
    assert (await runtime.resume_run(session_factory, run_id)).status == "NEEDS_REVIEW"
    return run_id, legacy


async def test_runs_needing_review_can_be_listed(
    client: httpx.AsyncClient, review_run: tuple[uuid.UUID, NonIdempotentPaymentService]
) -> None:
    run_id, _ = review_run
    listed = (await client.get("/runs", params={"status": "NEEDS_REVIEW"})).json()
    assert [r["run_id"] for r in listed] == [str(run_id)]
    assert listed[0]["current_step"] == "process_refund" and listed[0]["completed_at"] is None


async def test_the_unknown_step_is_visible_in_events_and_external_operations(
    client: httpx.AsyncClient, review_run: tuple[uuid.UUID, NonIdempotentPaymentService]
) -> None:
    run_id, _ = review_run
    events = (await client.get(f"/runs/{run_id}/events")).json()
    assert events[-1]["event_type"] == "STEP_OUTCOME_UNKNOWN"
    assert events[-1]["step_name"] == "process_refund"

    ops = (await client.get(f"/runs/{run_id}/external-operations")).json()
    # One attempt: the resuming worker never called the provider again, it parked the run instead.
    assert [(o["step_name"], o["status"], o["attempts"]) for o in ops] == [("process_refund", "UNKNOWN", 1)]
    assert ops[0]["idempotency_key"] == f"{run_id}:process_refund"


async def test_resolving_as_completed_resumes_and_finishes_the_run(
    client: httpx.AsyncClient,
    review_run: tuple[uuid.UUID, NonIdempotentPaymentService],
) -> None:
    run_id, legacy = review_run
    refund = (await legacy.list_refunds())[0]

    response = await client.post(
        f"/runs/{run_id}/steps/process_refund/resolve",
        json={"outcome": "completed", "result": refund, "resolved_by": "asha", "message": "checked"},
    )

    assert response.status_code == 202 and response.json()["status"] == "RUNNING"
    run = (await client.get(f"/runs/{run_id}")).json()  # the background resume has run
    assert run["status"] == "COMPLETED" and run["result_data"] == refund["refund_id"]
    assert await legacy.count_refunds() == 1

    events = (await client.get(f"/runs/{run_id}/events")).json()
    resolved = next(e for e in events if e["event_type"] == "STEP_RESOLVED")
    assert resolved["result_data"]["resolved_by"] == "asha"
    ops = (await client.get(f"/runs/{run_id}/external-operations")).json()
    assert ops[0]["status"] == "RECONCILED"


async def test_resolving_as_not_executed_runs_the_step(
    client: httpx.AsyncClient, review_run: tuple[uuid.UUID, NonIdempotentPaymentService]
) -> None:
    run_id, legacy = review_run
    # (In real life the reviewer would have checked the dashboard; here the refund DID happen, so
    # saying "not_executed" is a wrong human answer, which makes the ledger show the consequence.)
    response = await client.post(
        f"/runs/{run_id}/steps/process_refund/resolve", json={"outcome": "not_executed"}
    )
    assert response.status_code == 202
    assert (await client.get(f"/runs/{run_id}")).json()["status"] == "COMPLETED"
    assert await legacy.count_refunds() == 2  # the system trusts the reviewer; that is their call


async def test_resolve_validation_and_errors(
    client: httpx.AsyncClient, review_run: tuple[uuid.UUID, NonIdempotentPaymentService]
) -> None:
    run_id, _ = review_run
    url = f"/runs/{run_id}/steps/process_refund/resolve"

    assert (await client.post(url, json={"outcome": "shrug"})).status_code == 422
    assert (await client.post(url, json={"outcome": "completed"})).status_code == 422  # needs a result
    unknown_step = await client.post(f"/runs/{run_id}/steps/nope/resolve", json={"outcome": "not_executed"})
    assert unknown_step.status_code == 422
    missing_run = await client.post(
        f"/runs/{uuid.uuid4()}/steps/process_refund/resolve", json={"outcome": "not_executed"}
    )
    assert missing_run.status_code == 404
    assert (await client.get(f"/runs/{uuid.uuid4()}/external-operations")).status_code == 404

    ok = await client.post(url, json={"outcome": "failed", "message": "reversed"})
    assert ok.status_code == 202
    again = await client.post(url, json={"outcome": "failed"})  # no longer NEEDS_REVIEW
    assert again.status_code == 409
