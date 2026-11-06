"""Timing that scales with how slow the route to PostgreSQL is.

The chaos tests use short leases (0.8 s) and heartbeats (0.1 s) so that expiry and takeover happen
quickly. Those numbers silently assume a database a few milliseconds away. Behind Docker Desktop on
Windows every round trip costs far more, a renewal can take longer than the lease itself, and the same
fault schedule then causes many times more takeovers (each one a replay over the same slow link).

`scaled_timings` keeps the original numbers on a fast link and stretches them in proportion to the
measured cost of one lease-renewal-shaped transaction on a slow one. Assertions are not touched: only how
long the system gets to react.
"""

import statistics
import time
from dataclasses import dataclass

from sqlalchemy import text

from app.database.session import SessionFactory

LEASE_ROUND_TRIPS = 8.0  # a lease must outlast this many renewal-shaped transactions
TIMEOUT_ROUND_TRIPS = 1500.0  # the drain budget, in renewal-shaped transactions


@dataclass(frozen=True)
class Timings:
    round_trip: float
    lease: float
    heartbeat: float
    poll: float
    drain_timeout: float


def scaled_timings(
    round_trip: float,
    *,
    lease: float = 0.8,
    heartbeat: float = 0.1,
    poll: float = 0.02,
    drain_timeout: float = 300.0,
) -> Timings:
    """The defaults are the values the chaos tests always used; they are floors, never reduced."""
    scaled_lease = max(lease, LEASE_ROUND_TRIPS * round_trip)
    return Timings(
        round_trip=round_trip,
        lease=scaled_lease,
        heartbeat=max(heartbeat, scaled_lease / LEASE_ROUND_TRIPS),
        poll=max(poll, round_trip / 4),
        drain_timeout=max(drain_timeout, TIMEOUT_ROUND_TRIPS * round_trip),
    )


async def measure_round_trip(session_factory: SessionFactory, samples: int = 5) -> float:
    """Median seconds for one transaction shaped like a lease renewal: three statements. The first
    sample is discarded (it pays for opening the connection)."""
    durations: list[float] = []
    for _ in range(samples + 1):
        started = time.monotonic()
        async with session_factory() as session, session.begin():
            for _ in range(3):
                await session.execute(text("SELECT 1"))
        durations.append(time.monotonic() - started)
    return statistics.median(durations[1:])
