"""Chaos test: the database becomes unreachable, and acknowledgements get lost, while workers run.

Two kinds of fault, both injected where they hurt:
  * outage windows: for a few hundred milliseconds every statement on the runtime's connections fails
    (what a PostgreSQL restart or a network partition looks like to a worker);
  * lost acknowledgements: a recorder write is committed, then the caller is told it failed (the worst
    case: the database has the event, the worker believes it does not).

The judges are independent of the runtime: the payment and email ledgers (a separate database) and
the event log's own invariants. A run must never be recorded as FAILED because of an outage, never
refund or email twice, and always complete once the database is back.

CHAOS_SEEDS (default 4) controls how many random schedules run on the direct link. The "slow-link"
case runs one schedule through a proxy that delays every packet, modelling Docker Desktop on Windows
(see tests/fixtures/slow_link.py): lease and heartbeat timings are scaled to the measured round trip,
and the engine is the production pooled one, so a slow route does not turn the same faults into a
takeover storm. The assertions are identical in both cases.
"""

import asyncio
import os
import random
import time
from collections.abc import AsyncIterator

import pytest
import pytest_asyncio
from sqlalchemy import event, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncEngine

import app.examples.refund_workflow  # noqa: F401  (registers "refund_workflow")
from app.database.repositories import RunRepository
from app.database.session import SessionFactory, create_engine, create_session_factory
from app.engine import runtime
from app.engine.execution_context import DbStepRecorder
from app.engine.worker import Worker
from app.integrations.llm import MockLLMProvider
from tests.chaos import test_lease_chaos as lease_chaos
from tests.chaos.test_lease_chaos import RUNS, check_history, reset
from tests.fixtures.latency_proxy import latency_proxy
from tests.fixtures.slow_link import Timings, measure_round_trip, scaled_timings

payments = lease_chaos.payments  # the same fixtures, reused
mail = lease_chaos.mail

pytestmark = [pytest.mark.chaos, pytest.mark.integration]

SEEDS = int(os.environ.get("CHAOS_SEEDS", "4"))
WORKERS = 4


class Outage:
    def __init__(self) -> None:
        self.active = False
        self.rejected = 0

    def attach(self, session_factory: SessionFactory) -> None:
        engine = session_factory.kw["bind"].sync_engine

        @event.listens_for(engine, "before_cursor_execute")
        def _fail(conn, cursor, statement, parameters, context, executemany):  # noqa: ANN001
            if self.active:
                self.rejected += 1
                raise OperationalError(statement, parameters, ConnectionResetError("simulated outage"))


CHAOS_SECONDS = 8.0  # outages stop after this; the system must then drain on its own


async def outages(outage: Outage, rng: random.Random, stop: asyncio.Event) -> int:
    """Random outage windows for CHAOS_SECONDS, then calm. The bound makes the test independent of
    machine speed: a slow machine gets the same amount of chaos, then as long as it needs to finish
    (found on Windows, where unbounded outages starved the runs of any clear time)."""
    windows = 0
    started = time.monotonic()
    while not stop.is_set() and time.monotonic() - started < CHAOS_SECONDS:
        await asyncio.sleep(rng.uniform(0.15, 0.5))
        outage.active = True
        windows += 1
        await asyncio.sleep(rng.uniform(0.05, 0.4))
        outage.active = False
    return windows


def lose_acks(monkeypatch: pytest.MonkeyPatch, rng: random.Random) -> list[int]:
    """Make 8% of step_started / step_completed calls commit and then raise (ack lost)."""
    lost = [0]
    for name in ("step_started", "step_completed"):
        original = getattr(DbStepRecorder, name)

        async def wrapper(self, *args, _original=original, **kwargs):
            result = await _original(self, *args, **kwargs)
            if rng.random() < 0.08:
                lost[0] += 1
                raise OperationalError("COMMIT", {}, ConnectionResetError("ack lost"))
            return result

        monkeypatch.setattr(DbStepRecorder, name, wrapper)
    return lost


SLOW_LINK_DELAY = float(os.environ.get("CHAOS_SLOW_LINK_DELAY", "0.05"))


POOL = {"pool_size": 16, "max_overflow": 16}  # production uses a pool; see chaos_session_factory


def counting(engine: AsyncEngine) -> AsyncEngine:
    """Count the connections the engine opens (kept on its sync engine as `.opened`)."""
    sync_engine = engine.sync_engine
    sync_engine.opened = 0  # type: ignore[attr-defined]

    @event.listens_for(sync_engine, "connect")
    def _count(dbapi_connection, record):  # noqa: ANN001, ARG001
        sync_engine.opened += 1  # type: ignore[attr-defined]

    return engine


@pytest.fixture(params=[0.0, SLOW_LINK_DELAY], ids=["direct", "slow-link"])
def link_delay(request: pytest.FixtureRequest) -> float:
    """Seconds of delay the proxy adds to every packet (0 = connect straight to PostgreSQL)."""
    return request.param


@pytest_asyncio.fixture
async def chaos_session_factory(
    clean_db: None, migrated_database: str, link_delay: float
) -> AsyncIterator[SessionFactory]:
    """The production engine configuration (pooled, pre-ping, bounded connects), optionally behind the
    latency proxy. The shared fixture uses NullPool, which opens a new connection for every session:
    cheap on a Linux host, but about a hundred connections a second here, and on Docker Desktop for
    Windows each one is slow enough to dominate the run."""
    url = make_url(migrated_database)
    if not link_delay:
        engine = counting(create_engine(url.render_as_string(hide_password=False), **POOL))
        yield create_session_factory(engine)
        await engine.dispose()
        return
    with latency_proxy(url.host or "127.0.0.1", url.port or 5432, link_delay) as proxy:
        via_proxy = url.set(host="127.0.0.1", port=proxy.port)
        engine = counting(create_engine(via_proxy.render_as_string(hide_password=False), **POOL))
        yield create_session_factory(engine)
        await engine.dispose()


async def describe_stall(session_factory: SessionFactory, workers: list[Worker], outage: Outage) -> str:
    """What a timed-out drain looked like, so a failure says why instead of just 'TimeoutError'."""
    try:
        async with session_factory() as session:
            rows = (
                await session.execute(
                    text(
                        "SELECT status, count(*), max(fencing_token), "
                        "count(*) FILTER (WHERE lease_expires_at < now()) "
                        "FROM workflow_runs GROUP BY status ORDER BY status"
                    )
                )
            ).all()
        runs = "; ".join(f"{r[0]}: {r[1]} (max fencing token {r[2]}, {r[3]} lease(s) expired)" for r in rows)
    except Exception as exc:  # the database may be the problem; say so instead of hiding it
        runs = f"could not read run states: {exc!r}"
    per_worker = ", ".join(
        f"{w.worker_id}: infra_errors={w.infra_errors} lost_leases={w.lost_leases}" for w in workers
    )
    return f"runs -> {runs}. workers -> {per_worker}. outage statements rejected so far: {outage.rejected}"


async def test_outages_and_lost_acks_never_fail_a_run_or_duplicate_an_effect(
    chaos_session_factory: SessionFactory,
    link_delay: float,
    migrated_database: str,
    services_database_url: str,
    payments,
    mail,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session_factory = chaos_session_factory
    outage = Outage()
    outage.attach(session_factory)
    total_windows = total_infra = total_lost = takeovers = 0
    seeds = SEEDS if not link_delay else 1  # the slow-link case is about timing, one schedule is enough
    timings: Timings = scaled_timings(await measure_round_trip(session_factory))

    for seed in range(seeds):
        outage.active = False
        # reset() is synchronous; run it off the event loop so a slow database cannot freeze the
        # workers' heartbeats (and be misread as lost leases).
        await asyncio.to_thread(reset, migrated_database, services_database_url)
        rng = random.Random(seed)
        run_ids = [
            (await runtime.create_run(session_factory, "refund_workflow", {"order_id": "ORD-1001"})).run_id
            for _ in range(RUNS)
        ]
        with monkeypatch.context() as m:
            lost = lose_acks(m, rng)
            workers = [
                Worker(
                    session_factory,
                    f"w{i}",
                    lease_seconds=timings.lease,
                    heartbeat_seconds=timings.heartbeat,
                    poll_seconds=timings.poll,
                    llm_provider=MockLLMProvider("APPROVE"),
                )
                for i in range(WORKERS)
            ]
            stop = asyncio.Event()

            async def run_workers(pool: list[Worker] = workers, done: asyncio.Event = stop) -> None:
                await asyncio.gather(*(w.run_forever(drain=True) for w in pool))
                done.set()

            try:
                windows, _ = await asyncio.wait_for(
                    asyncio.gather(outages(outage, rng, stop), run_workers()), timeout=timings.drain_timeout
                )
            except TimeoutError:
                outage.active = False
                pytest.fail(
                    f"seed {seed}: the runs did not drain within {timings.drain_timeout:.0f}s "
                    f"(link delay {link_delay}s, measured round trip {timings.round_trip * 1000:.0f} ms, "
                    f"lease {timings.lease:.2f}s, heartbeat {timings.heartbeat:.2f}s). "
                    + await describe_stall(session_factory, workers, outage)
                )
        outage.active = False

        total_windows += windows
        total_lost += lost[0]
        total_infra += sum(w.infra_errors for w in workers)

        # Independent judges.
        assert await payments.count_refunds() == RUNS, f"seed {seed}: refunds"
        assert len(await mail.list_emails()) == RUNS, f"seed {seed}: emails"
        for run_id in run_ids:
            await check_history(session_factory, run_id, seed)
            async with session_factory() as session:
                run = await RunRepository(session).get_run(run_id)
            takeovers += run.fencing_token - 1

    # Meaningful only if the faults really happened.
    assert outage.rejected > 0 and total_lost > 0 and total_infra > 0, (
        outage.rejected,
        total_lost,
        total_infra,
    )
    # Root cause of the Windows timeout: one new connection per operation (NullPool). With the
    # production pool the whole test needs a few dozen connections at most, however long it runs.
    opened = session_factory.kw["bind"].sync_engine.opened
    assert opened <= 2 * (POOL["pool_size"] + POOL["max_overflow"]), (
        f"{opened} connections opened: the engine is not reusing connections, so every operation pays "
        f"a full connect (slow behind Docker Desktop) and renewals outlast their leases"
    )

    link = f"slow link {link_delay}s" if link_delay else "direct"
    print(
        f"\noutage chaos ({link}, round trip {timings.round_trip * 1000:.0f} ms, "
        f"lease {timings.lease:.2f}s): "
        f"{seeds} seeds, {total_windows} outage windows, {outage.rejected} statements rejected, "
        f"{total_lost} lost acks, {total_infra} abandoned runs/loops, {takeovers} takeovers, "
        f"{opened} connections opened"
    )
