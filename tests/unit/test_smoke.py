"""Phase 0 smoke tests: the package imports and the API process starts."""

from fastapi.testclient import TestClient

from app.api.main import app
from app.config import get_settings


def test_health_endpoint() -> None:
    response = TestClient(app).get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_settings_have_sane_lease_defaults() -> None:
    settings = get_settings()
    # A heartbeat slower than the lease would let healthy workers lose their leases.
    assert settings.heartbeat_interval_seconds < settings.lease_duration_seconds
