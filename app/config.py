"""Central configuration, loaded from environment variables (and an optional .env file).

No secrets are hardcoded here. Defaults are safe for local development only.
"""

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql+asyncpg://durable:durable@localhost:5432/durable"
    services_database_url: str = (
        "postgresql+asyncpg://durable:durable@localhost:5432/fake_services"
    )

    # A worker must renew its lease more often than it expires.
    lease_duration_seconds: float = 15.0
    heartbeat_interval_seconds: float = 5.0
    worker_poll_interval_seconds: float = 1.0

    llm_provider: str = "mock"  # "mock" or "openai"
    openai_api_key: str = ""
    openai_model: str = "gpt-4o-mini"

    log_level: str = "INFO"


def to_sync_url(async_url: str) -> str:
    """Alembic runs synchronously, so it uses psycopg instead of asyncpg."""
    return async_url.replace("+asyncpg", "+psycopg")


@lru_cache
def get_settings() -> Settings:
    return Settings()
