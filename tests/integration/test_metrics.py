# ruff: noqa: F811  (the payments/mail fixtures are imported, then named as test arguments)
"""/metrics: Prometheus text computed from the database; checked against what really happened."""

from collections.abc import AsyncIterator

import httpx
import pytest
import pytest_asyncio

from app.api.deps import get_session_factory
from app.api.main import app
from app.database.repositories import RunRepository, WorkerRepository
from app.database.session import SessionFactory
from app.engine import runtime
from app.engine.signals import send_signal
from app.engine.worker import Worker
from app.integrations.llm import MockLLMProvider
from tests.integration.test_refund_workflow import mail, payments  # noqa: F401  (fixtures)

pytestmark = pytest.mark.integration


@pytest_asyncio.fixture
async def client(session_factory: SessionFactory) -> AsyncIterator[httpx.AsyncClient]:
    app.dependency_overrides[get_session_factory] = lambda: session_factory
    transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c
    app.dependency_overrides.clear()


def samples(body: str) -> dict[tuple[str, tuple], float]:
    """Parse with the reference Prometheus parser, so a malformed exposition fails the test."""
    parser = pytest.importorskip("prometheus_client.parser")
    out: dict[tuple[str, tuple], float] = {}
    for family in parser.text_string_to_metric_families(body):
        for s in family.samples:
            out[(s.name, tuple(sorted(s.labels.items())))] = s.value
    return out


def value(m: dict, name: str, **labels: str) -> float:
    return m[(name, tuple(sorted(labels.items())))]


async def test_an_empty_system_exposes_zeroes_in_valid_prometheus_format(client: httpx.AsyncClient) -> None:
    response = await client.get("/metrics")
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/plain; version=0.0.4")
    m = samples(response.text)
    assert value(m, "durable_runs", status="PENDING") == 0
    assert value(m, "durable_queue_depth") == 0
    assert value(m, "durable_llm_calls_total") == 0


async def test_metrics_reflect_runs_workers_llm_and_effects(
    client: httpx.AsyncClient,
    session_factory: SessionFactory,
    payments,
    mail,
) -> None:
    import app.examples.refund_workflow  # noqa: F401, I001
    from app.sdk.workflow import registry  # noqa: F401

    # one COMPLETED refund (an LLM call and two effects), one WAITING (unclear AI answer), two PENDING
    done = await runtime.create_run(session_factory, "refund_workflow", {"order_id": "ORD-1001"})
    await runtime.execute_run(session_factory, done.run_id, llm_provider=MockLLMProvider("APPROVE"))
    parked = await runtime.create_run(session_factory, "refund_workflow", {"order_id": "ORD-1001"})
    await runtime.execute_run(session_factory, parked.run_id, llm_provider=MockLLMProvider("hmm"))
    for _ in range(2):
        await runtime.create_run(session_factory, "refund_workflow", {"order_id": "ORD-1001"})
    async with session_factory() as session, session.begin():
        await WorkerRepository(session).register("w-metrics", "host", 1)

    m = samples((await client.get("/metrics")).text)
    assert value(m, "durable_runs", status="COMPLETED") == 1
    assert value(m, "durable_runs", status="WAITING") == 1
    assert value(m, "durable_runs", status="PENDING") == 2
    assert value(m, "durable_queue_depth") == 2
    assert value(m, "durable_oldest_pending_age_seconds") >= 0
    assert value(m, "durable_workers", state="alive") == 1
    assert value(m, "durable_llm_calls_total") == 2  # the two runs that reached the model
    assert value(m, "durable_llm_cost_usd_total") > 0
    assert value(m, "durable_llm_tokens_total", direction="input") > 0
    assert value(m, "durable_external_operations", status="SUCCEEDED") == 2  # refund + confirmation
    assert value(m, "durable_external_operation_attempts_total") == 2
    assert value(m, "durable_events_total", event_type="WORKFLOW_COMPLETED") == 1
    assert value(m, "durable_events_total", event_type="WORKFLOW_WAITING") == 1
    assert value(m, "durable_timers", status="PENDING") == 1  # the review wait's timeout
    assert value(m, "durable_step_duration_samples") >= 5  # the waiting step is excluded
    assert (
        value(m, "durable_step_duration_seconds", quantile="0.99")
        >= value(m, "durable_step_duration_seconds", quantile="0.5")
        >= 0
    )


async def test_resumes_count_recoveries_and_wakes(
    client: httpx.AsyncClient,
    session_factory: SessionFactory,
    payments,
    mail,
) -> None:
    import app.examples.refund_workflow  # noqa: F401, I001

    parked = await runtime.create_run(session_factory, "refund_workflow", {"order_id": "ORD-1001"})
    await runtime.execute_run(session_factory, parked.run_id, llm_provider=MockLLMProvider("hmm"))
    await send_signal(session_factory, parked.run_id, "refund_review", {"decision": "REJECT"})
    await Worker(
        session_factory, "m-w", lease_seconds=30.0, heartbeat_seconds=10.0, poll_seconds=0.05
    ).run_until_idle()
    async with session_factory() as session:
        run = await RunRepository(session).get_run(parked.run_id)
    assert run is not None and run.status == "COMPLETED"

    m = samples((await client.get("/metrics")).text)
    assert value(m, "durable_workflow_resumes_total") == 1  # the wake-up after the signal


async def test_metrics_need_a_key_when_auth_is_on_and_a_read_only_key_suffices(
    client: httpx.AsyncClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    from app.config import get_settings

    monkeypatch.setenv("API_OPERATOR_KEYS", "op")
    monkeypatch.setenv("API_READONLY_KEYS", "ro")
    get_settings.cache_clear()
    try:
        assert (await client.get("/metrics")).status_code == 401
        assert (await client.get("/metrics", headers={"X-API-Key": "ro"})).status_code == 200
    finally:
        monkeypatch.undo()
        get_settings.cache_clear()
