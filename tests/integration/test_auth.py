"""API-key authentication: off by default, two roles when on, and the always-open endpoints."""

import uuid
from collections.abc import AsyncIterator

import httpx
import pytest
import pytest_asyncio

from app.api.deps import get_session_factory
from app.api.main import app
from app.config import get_settings
from app.database.session import SessionFactory

pytestmark = pytest.mark.integration

OPERATOR, READER = "op-secret-1", "read-secret-1"
MISSING = uuid.uuid4()


@pytest_asyncio.fixture
async def client(session_factory: SessionFactory) -> AsyncIterator[httpx.AsyncClient]:
    app.dependency_overrides[get_session_factory] = lambda: session_factory
    transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c
    app.dependency_overrides.clear()


@pytest.fixture
def auth_on(monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[None]:
    monkeypatch.setenv("API_OPERATOR_KEYS", f"old-op-key, {OPERATOR}")  # two keys: rotation
    monkeypatch.setenv("API_READONLY_KEYS", READER)
    get_settings.cache_clear()
    yield
    monkeypatch.undo()
    get_settings.cache_clear()


async def test_authentication_is_off_by_default(client: httpx.AsyncClient) -> None:
    assert (await client.get("/runs")).status_code == 200


@pytest.mark.usefixtures("auth_on")
async def test_requests_without_a_key_are_rejected(client: httpx.AsyncClient) -> None:
    for method, path in [
        ("GET", "/runs"),
        ("GET", "/workers"),
        ("GET", "/workflows"),
        ("POST", f"/runs/{MISSING}/signals/x"),
        ("GET", "/fake-services/emails"),
    ]:
        response = await client.request(method, path)
        assert response.status_code == 401, (method, path)
        assert response.headers["www-authenticate"] == "Bearer"


@pytest.mark.usefixtures("auth_on")
async def test_a_wrong_key_is_rejected(client: httpx.AsyncClient) -> None:
    assert (await client.get("/runs", headers={"X-API-Key": "nope"})).status_code == 401
    assert (await client.get("/runs", headers={"Authorization": "Bearer nope"})).status_code == 401
    assert (await client.get("/runs", headers={"Authorization": f"Basic {OPERATOR}"})).status_code == 401


@pytest.mark.usefixtures("auth_on")
async def test_a_read_only_key_can_read_but_not_write(client: httpx.AsyncClient) -> None:
    headers = {"X-API-Key": READER}
    assert (await client.get("/runs", headers=headers)).status_code == 200
    assert (await client.get("/workers", headers=headers)).status_code == 200
    for path in (f"/runs/{MISSING}/signals/x", f"/runs/{MISSING}/fork", f"/runs/{MISSING}/steps/s/resolve"):
        response = await client.post(path, json={}, headers=headers)
        assert response.status_code == 403, path
        assert response.json() == {"detail": "this key is read-only"}
    body = {"input": {"order_id": "A"}}
    assert (await client.post("/workflows/order_summary/runs", json=body, headers=headers)).status_code == 403


@pytest.mark.usefixtures("auth_on")
async def test_an_operator_key_can_write_with_either_header_and_after_rotation(
    client: httpx.AsyncClient,
) -> None:
    styles = ({"X-API-Key": OPERATOR}, {"Authorization": f"Bearer {OPERATOR}"}, {"X-API-Key": "old-op-key"})
    for headers in styles:
        # Auth passed: the request reaches the handler, which says the run does not exist.
        response = await client.post(f"/runs/{MISSING}/signals/x", json={}, headers=headers)
        assert response.status_code == 404, headers
    body = {"input": {"order_id": "A-1"}}
    created = await client.post("/workflows/order_summary/runs", json=body, headers={"X-API-Key": OPERATOR})
    assert created.status_code == 202


@pytest.mark.usefixtures("auth_on")
async def test_health_dashboard_and_docs_stay_open(client: httpx.AsyncClient) -> None:
    for path in ("/health", "/dashboard", "/openapi.json"):
        assert (await client.get(path)).status_code == 200, path


def test_keys_are_not_exposed_by_the_settings_repr(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("API_OPERATOR_KEYS", "very-secret-value")
    monkeypatch.setenv("OPENAI_API_KEY", "sk-very-secret")
    get_settings.cache_clear()
    try:
        text = repr(get_settings()) + str(get_settings())
    finally:
        monkeypatch.undo()
        get_settings.cache_clear()
    assert "very-secret-value" not in text and "sk-very-secret" not in text
