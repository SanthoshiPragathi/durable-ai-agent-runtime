"""Central configuration, loaded from environment variables (and an optional .env file).

No secrets are hardcoded here. Defaults are safe for local development only.
"""

from functools import lru_cache
from typing import Literal

from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql+asyncpg://durable:durable@localhost:5432/durable"
    services_database_url: str = (
        "postgresql+asyncpg://durable:durable@localhost:5432/fake_services"
    )

    # "inline": the API process executes runs itself (single process, simplest to try out).
    # "workers": the API only records runs; worker processes (python -m app.engine.worker) execute them.
    execution_mode: Literal["inline", "workers"] = "inline"

    # A worker must renew its lease more often than it expires.
    lease_duration_seconds: float = 15.0
    heartbeat_interval_seconds: float = 5.0
    worker_poll_interval_seconds: float = 1.0

    llm_provider: str = "mock"  # "mock" or "openai"
    openai_api_key: SecretStr = SecretStr("")
    openai_model: str = "gpt-4o-mini"
    # Optional. Without both prices the recorded cost is None (unknown), not a guess.
    openai_input_price_per_1m: float | None = None
    openai_output_price_per_1m: float | None = None

    # Retries for plain steps and LLM calls on transient failures (never for external effects).
    retry_max_attempts: int = 3
    retry_base_delay_seconds: float = 0.5
    retry_max_delay_seconds: float = 30.0
    retry_jitter: float = 0.2
    step_timeout_seconds: float | None = None  # per-attempt limit for plain steps and LLM calls

    # API authentication. Comma-separated keys. Both empty = auth OFF (local development only).
    #   operator keys may read and write; read-only keys may only issue GET requests.
    api_operator_keys: SecretStr = SecretStr("")
    api_readonly_keys: SecretStr = SecretStr("")

    # Idle workers poll with exponential backoff (capped) and jitter; they write their heartbeat
    # row at most this often instead of on every poll.
    worker_poll_backoff_factor: float = 1.5
    worker_poll_max_interval_seconds: float = 5.0
    worker_poll_jitter: float = 0.2
    worker_touch_interval_seconds: float = 5.0

    log_level: str = "INFO"


def to_sync_url(async_url: str) -> str:
    """Alembic runs synchronously, so it uses psycopg instead of asyncpg."""
    return async_url.replace("+asyncpg", "+psycopg")


@lru_cache
def get_settings() -> Settings:
    return Settings()
