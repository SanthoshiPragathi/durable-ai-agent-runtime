"""A database that accepts the TCP connection but never answers must produce a bounded error.

This is the exact shape of the failure seen on Windows with Docker Desktop: the port is open, the
PostgreSQL handshake never completes, and `psycopg.waiting.wait_conn` (sync) or asyncpg's connect (async)
waits forever. The tests use a local "black hole" socket, so they need neither PostgreSQL nor Docker and
behave the same on Windows and Linux.
"""

import socket
import threading
import time
from collections.abc import Iterator

import pytest
from sqlalchemy import text
from sqlalchemy.exc import OperationalError

from app.config import Settings, get_settings
from app.database.session import (
    async_connect_args,
    create_engine,
    create_session_factory,
    create_sync_engine,
    sync_connect_args,
)
from app.engine.errors import is_infrastructure_error
from app.engine.worker import Worker

BOUND = 1.0  # seconds, set through the settings under test
SLACK = 4.0  # a generous ceiling: the point is "bounded", not "exact"


@pytest.fixture
def black_hole() -> Iterator[int]:
    """Listens, accepts, and never says a word (as a stalled port proxy or a half-started server does)."""
    server = socket.socket()
    server.bind(("127.0.0.1", 0))
    server.listen(64)
    held: list[socket.socket] = []
    stop = threading.Event()

    def accept_loop() -> None:
        server.settimeout(0.2)
        while not stop.is_set():
            try:
                conn, _ = server.accept()
                held.append(conn)
            except (TimeoutError, OSError):
                continue

    thread = threading.Thread(target=accept_loop, daemon=True)
    thread.start()
    yield server.getsockname()[1]
    stop.set()
    thread.join(timeout=2)
    for conn in held:
        conn.close()
    server.close()


@pytest.fixture(autouse=True)
def short_bounds(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DB_CONNECT_TIMEOUT_SECONDS", str(BOUND))
    monkeypatch.setenv("DB_COMMAND_TIMEOUT_SECONDS", "5")
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def test_defaults_are_finite() -> None:
    settings = Settings()
    assert 0 < settings.db_connect_timeout_seconds <= 30
    assert settings.db_command_timeout_seconds is None or settings.db_command_timeout_seconds > 0


def test_connect_args_carry_the_bounds() -> None:
    assert async_connect_args() == {"timeout": BOUND, "command_timeout": 5.0}
    assert sync_connect_args() == {"connect_timeout": 1}


def test_sync_connect_timeout_is_never_zero(monkeypatch: pytest.MonkeyPatch) -> None:
    """libpq reads connect_timeout=0 as 'wait forever', so a tiny setting must still round up to 1."""
    monkeypatch.setenv("DB_CONNECT_TIMEOUT_SECONDS", "0.2")
    get_settings.cache_clear()
    assert sync_connect_args() == {"connect_timeout": 1}


def test_sync_engine_gives_up_on_a_silent_server(black_hole: int) -> None:
    engine = create_sync_engine(f"postgresql+psycopg://u:p@127.0.0.1:{black_hole}/db")
    started = time.monotonic()
    with pytest.raises(OperationalError):
        engine.connect()
    assert time.monotonic() - started < SLACK


def test_a_sync_engine_without_the_bound_would_hang(black_hole: int) -> None:
    """Pins down the failure the bound prevents: the same connect, unbounded, is still waiting after
    several seconds (it is abandoned in a thread, which is daemonic)."""
    from sqlalchemy import create_engine as unbounded

    outcome: list[object] = []

    def attempt() -> None:
        try:
            unbounded(f"postgresql+psycopg://u:p@127.0.0.1:{black_hole}/db").connect()
        except Exception as exc:  # noqa: BLE001
            outcome.append(exc)

    thread = threading.Thread(target=attempt, daemon=True)
    thread.start()
    thread.join(timeout=SLACK)
    assert thread.is_alive() and not outcome


async def test_async_engine_gives_up_on_a_silent_server(black_hole: int) -> None:
    engine = create_engine(f"postgresql+asyncpg://u:p@127.0.0.1:{black_hole}/db")
    started = time.monotonic()
    with pytest.raises(Exception) as caught:  # noqa: PT011
        async with engine.connect() as conn:
            await conn.execute(text("select 1"))
    assert time.monotonic() - started < SLACK
    assert is_infrastructure_error(caught.value), repr(caught.value)  # so the worker retries, not fails
    await engine.dispose()


async def test_worker_survives_a_silent_database_and_stays_responsive(black_hole: int) -> None:
    """One poll against a database that never answers returns within the bound, counts an
    infrastructure error, and leaves the worker able to try again."""
    engine = create_engine(f"postgresql+asyncpg://u:p@127.0.0.1:{black_hole}/db")
    worker = Worker(create_session_factory(engine), "w-silent", lease_seconds=1.0, heartbeat_seconds=0.2)
    for attempt in (1, 2):
        started = time.monotonic()
        claimed = await worker._survive(worker.run_once)
        assert claimed is False
        assert time.monotonic() - started < SLACK
        assert worker.infra_errors == attempt
    await engine.dispose()
