from collections.abc import AsyncIterator

import httpx
import pytest
import pytest_asyncio

from app.api.deps import get_services_session_factory_dep
from app.api.main import app
from app.database.session import SessionFactory
from app.integrations.tools import EmailService, IdempotentPaymentService

pytestmark = pytest.mark.integration


@pytest_asyncio.fixture
async def client(services_session_factory: SessionFactory) -> AsyncIterator[httpx.AsyncClient]:
    app.dependency_overrides[get_services_session_factory_dep] = lambda: services_session_factory
    transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c
    app.dependency_overrides.clear()


async def test_refund_ledger_can_be_inspected_and_filtered(
    client: httpx.AsyncClient, services_session_factory: SessionFactory
) -> None:
    payments = IdempotentPaymentService(services_session_factory)
    await payments.refund("ORD-1", 100, idempotency_key="k1")
    await payments.refund("ORD-2", 200, idempotency_key="k2")

    everything = (await client.get("/fake-services/payments/refunds")).json()
    assert [r["order_id"] for r in everything] == ["ORD-1", "ORD-2"]
    assert everything[0]["idempotency_key"] == "k1" and everything[0]["status"] == "SUCCEEDED"

    one = (await client.get("/fake-services/payments/refunds", params={"order_id": "ORD-2"})).json()
    assert [r["amount"] for r in one] == [200]


async def test_email_ledger_can_be_inspected(
    client: httpx.AsyncClient, services_session_factory: SessionFactory
) -> None:
    await EmailService(services_session_factory).send("a@example.com", "Refund", "done")
    emails = (await client.get("/fake-services/emails")).json()
    assert [(e["to"], e["subject"]) for e in emails] == [("a@example.com", "Refund")]
    assert (await client.get("/fake-services/emails", params={"to": "x@example.com"})).json() == []


async def test_empty_ledgers_return_empty_lists(client: httpx.AsyncClient) -> None:
    assert (await client.get("/fake-services/payments/refunds")).json() == []
    assert (await client.get("/fake-services/emails")).json() == []
