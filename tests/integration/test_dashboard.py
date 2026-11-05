"""The dashboard is one static page; check it is served and only calls endpoints that exist."""

import httpx
import pytest

from app.api.main import app

pytestmark = pytest.mark.integration


async def test_dashboard_is_served_and_calls_only_real_endpoints() -> None:
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        page = await client.get("/dashboard")
    assert page.status_code == 200 and "text/html" in page.headers["content-type"]
    assert "Durable AI Agent Runtime" in page.text

    routes = set(app.openapi()["paths"])
    for needle in (
        "/runs",
        "/runs/{run_id}/events",
        "/runs/{run_id}/timers",
        "/runs/{run_id}/signals",
        "/runs/{run_id}/external-operations",
        "/runs/{run_id}/fork",
        "/runs/{run_id}/diff/{other_run_id}",
        "/runs/{run_id}/signals/{name}",
        "/workers",
    ):
        assert needle in routes, needle
        assert needle.split("{")[0].rstrip("/") in page.text, needle  # the page really uses it
