import uuid
from collections.abc import AsyncIterator

import httpx
import pytest
import pytest_asyncio

from app.api.deps import get_session_factory
from app.api.main import app
from app.database.session import SessionFactory

pytestmark = pytest.mark.integration


@pytest_asyncio.fixture
async def client(session_factory: SessionFactory) -> AsyncIterator[httpx.AsyncClient]:
    app.dependency_overrides[get_session_factory] = lambda: session_factory
    transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c
    app.dependency_overrides.clear()


async def test_list_workflows_includes_the_example(client: httpx.AsyncClient) -> None:
    response = await client.get("/workflows")
    assert response.status_code == 200
    assert {"name": "order_summary", "version": 1} in response.json()


async def test_start_run_executes_and_records_history(client: httpx.AsyncClient) -> None:
    created = await client.post("/workflows/order_summary/runs", json={"input": {"order_id": "A-100"}})
    assert created.status_code == 202
    body = created.json()
    assert body["status"] == "PENDING" and body["workflow_name"] == "order_summary"
    run_id = body["run_id"]

    # The background task has finished by the time the ASGI call returns in this test transport.
    run = (await client.get(f"/runs/{run_id}")).json()
    assert run["status"] == "COMPLETED"
    assert run["result_data"] == {"order_id": "A-100", "total": 6900, "summary": "2 item(s), total 69.00"}

    events = (await client.get(f"/runs/{run_id}/events")).json()
    assert [e["sequence_number"] for e in events] == list(range(1, 9))
    assert events[0]["event_type"] == "WORKFLOW_STARTED"
    assert events[-1]["event_type"] == "WORKFLOW_COMPLETED"
    assert [e["step_name"] for e in events if e["event_type"] == "STEP_COMPLETED"] == [
        "fetch_order",
        "calculate_total",
        "format_summary",
    ]


async def test_workflow_failure_is_visible_through_the_api(client: httpx.AsyncClient) -> None:
    created = await client.post("/workflows/order_summary/runs", json={"input": {"order_id": "NOPE"}})
    run = (await client.get(f"/runs/{created.json()['run_id']}")).json()
    assert run["status"] == "FAILED"
    assert "not found" in run["error_message"]
    assert "Traceback" not in run["error_message"]


async def test_unknown_workflow_is_404(client: httpx.AsyncClient) -> None:
    response = await client.post("/workflows/does_not_exist/runs", json={"input": {}})
    assert response.status_code == 404
    assert "not registered" in response.json()["detail"]


async def test_wrong_input_is_422_and_creates_no_run(client: httpx.AsyncClient) -> None:
    response = await client.post("/workflows/order_summary/runs", json={"input": {"bogus": 1}})
    assert response.status_code == 422
    assert (await client.get("/runs")).json() == []


async def test_malformed_body_is_rejected_by_validation(client: httpx.AsyncClient) -> None:
    response = await client.post("/workflows/order_summary/runs", json={"input": "not-an-object"})
    assert response.status_code == 422


async def test_unknown_run_is_404(client: httpx.AsyncClient) -> None:
    missing = uuid.uuid4()
    assert (await client.get(f"/runs/{missing}")).status_code == 404
    assert (await client.get(f"/runs/{missing}/events")).status_code == 404
    assert (await client.get("/runs/not-a-uuid")).status_code == 422


async def test_list_runs_with_filters_and_paging(client: httpx.AsyncClient) -> None:
    for order in ("A-100", "A-200", "NOPE"):
        await client.post("/workflows/order_summary/runs", json={"input": {"order_id": order}})

    assert len((await client.get("/runs")).json()) == 3
    completed = (await client.get("/runs", params={"status": "COMPLETED"})).json()
    failed = (await client.get("/runs", params={"status": "FAILED"})).json()
    assert (len(completed), len(failed)) == (2, 1)
    assert len((await client.get("/runs", params={"limit": 2})).json()) == 2
    assert (await client.get("/runs", params={"status": "BOGUS"})).status_code == 422
    assert (await client.get("/runs", params={"limit": 0})).status_code == 422


async def test_unexpected_errors_do_not_leak_internals(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    from app.engine import runtime

    async def explode(*_args, **_kwargs):
        raise RuntimeError("secret connection string postgres://user:pw@host")

    monkeypatch.setattr(runtime, "create_run", explode)
    response = await client.post("/workflows/order_summary/runs", json={"input": {"order_id": "A-100"}})
    assert response.status_code == 500
    assert response.json() == {"detail": "internal error"}
    assert "secret" not in response.text
