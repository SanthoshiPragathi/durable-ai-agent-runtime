"""Shared fixtures.

Integration tests need a running PostgreSQL. Point TEST_DATABASE_URL at it (the database name
must end in "_test"; it is created if missing and its schema is rebuilt at the start of every
test session). If PostgreSQL is unreachable, integration tests are SKIPPED, never faked.
"""

import os
from collections.abc import AsyncIterator, Iterator
from pathlib import Path

import pytest
import pytest_asyncio
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.pool import NullPool

from app.config import to_sync_url
from app.database.session import SessionFactory, create_session_factory
from app.sdk.workflow import registry

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_TEST_URL = "postgresql+asyncpg://durable:durable@localhost:5432/durable_test"


@pytest.fixture(scope="session")
def test_database_url() -> str:
    return os.environ.get("TEST_DATABASE_URL", DEFAULT_TEST_URL)


@pytest.fixture(scope="session")
def migrated_database(test_database_url: str) -> str:
    """Create the test database if needed and apply all migrations to a clean schema."""
    sync_url = make_url(to_sync_url(test_database_url))
    assert sync_url.database and sync_url.database.endswith("_test"), (
        "Refusing to wipe a database whose name does not end in '_test'"
    )

    try:
        admin = create_engine(sync_url.set(database="postgres"), isolation_level="AUTOCOMMIT")
        with admin.connect() as conn:
            exists = conn.execute(
                text("SELECT 1 FROM pg_database WHERE datname = :n"), {"n": sync_url.database}
            ).scalar()
            if not exists:
                conn.execute(text(f'CREATE DATABASE "{sync_url.database}"'))
        admin.dispose()
    except OperationalError as exc:
        pytest.skip(f"PostgreSQL is not reachable ({exc.orig}); integration tests skipped")

    engine = create_engine(sync_url, isolation_level="AUTOCOMMIT")
    with engine.connect() as conn:
        conn.execute(text("DROP SCHEMA public CASCADE"))
        conn.execute(text("CREATE SCHEMA public"))
    engine.dispose()

    cfg = alembic_config(sync_url.render_as_string(hide_password=False))
    command.upgrade(cfg, "head")
    return test_database_url


def alembic_config(sync_url: str) -> Config:
    cfg = Config(str(ROOT / "alembic.ini"))
    cfg.set_main_option("script_location", str(ROOT / "migrations"))
    cfg.set_main_option("sqlalchemy.url", sync_url.replace("%", "%%"))
    return cfg


@pytest.fixture
def clean_db(migrated_database: str) -> Iterator[None]:
    """Empty all tables before each integration test (TRUNCATE is allowed on the event log)."""
    engine = create_engine(to_sync_url(migrated_database), isolation_level="AUTOCOMMIT")
    with engine.connect() as conn:
        conn.execute(
            text("TRUNCATE workflow_events, workflow_runs, workflows RESTART IDENTITY CASCADE")
        )
    engine.dispose()
    yield


@pytest_asyncio.fixture
async def session_factory(clean_db: None, migrated_database: str) -> AsyncIterator[SessionFactory]:
    # NullPool: each test gets fresh connections, no cross-event-loop reuse.
    engine = create_async_engine(migrated_database, poolclass=NullPool)
    yield create_session_factory(engine)
    await engine.dispose()



@pytest.fixture
def isolated_registry() -> Iterator[None]:
    """Give a test an empty workflow registry and restore the previous one afterwards."""
    saved = registry.snapshot()
    registry.clear()
    yield
    registry.restore(saved)
