"""Measure how slow your route to PostgreSQL is, so slow tests can be explained with numbers.

    python scripts/db_latency.py        # DATABASE_URL (or the default); also tries 127.0.0.1 and localhost
    python scripts/db_latency.py postgresql+asyncpg://durable:durable@127.0.0.1:5432/durable

Prints, per host: how long opening a connection takes, how long a query takes on an open connection,
and what a lease renewal costs when it has to open a connection first (the pre-fix test setup did that
for every operation). Every attempt has a 5 s limit, so a stuck address shows up as FAIL, not a hang.
"""

import asyncio
import statistics
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import asyncpg  # noqa: E402
from sqlalchemy.engine import make_url  # noqa: E402

from app.config import get_settings  # noqa: E402

TIMEOUT = 5.0


async def measure(dsn: str, label: str) -> None:
    connects: list[float] = []
    try:
        for _ in range(8):
            started = time.monotonic()
            conn = await asyncpg.connect(dsn, timeout=TIMEOUT)
            connects.append(time.monotonic() - started)
            await conn.close()
        conn = await asyncpg.connect(dsn, timeout=TIMEOUT)
        queries: list[float] = []
        for _ in range(20):
            started = time.monotonic()
            await conn.fetchval("SELECT 1")
            queries.append(time.monotonic() - started)
        await conn.close()
    except Exception as exc:  # noqa: BLE001
        print(f"{label:<12} FAIL  {type(exc).__name__}: {str(exc)[:100]}")
        return
    connect, query = statistics.median(connects) * 1000, statistics.median(queries) * 1000
    print(
        f"{label:<12} ok    connect {connect:7.1f} ms   query {query:6.1f} ms   "
        f"renewal with a new connection ~{connect + 3 * query:7.0f} ms"
    )


async def main() -> None:
    given = sys.argv[1] if len(sys.argv) > 1 else None
    url = make_url((given or get_settings().database_url).replace("+psycopg", "+asyncpg"))
    configured = url.host or "localhost"
    hosts = [configured] if given else sorted({configured, "127.0.0.1", "localhost"})
    print(f"port {url.port or 5432}, database {url.database}\n")
    for host in hosts:
        dsn = url.set(host=host, drivername="postgresql").render_as_string(hide_password=False)
        await measure(dsn, host)
    print(
        "\nReading it: on a Linux host expect connect ~5-20 ms and query ~0.2-1 ms. The chaos tests' 0.8 s\n"
        "leases assume a renewal costs a few ms. If 'renewal with a new connection' is several hundred ms\n"
        "or more, a slow route (often the Docker Desktop port proxy) is why; if one of the two hosts is\n"
        "much faster than the other, use that one in DATABASE_URL / TEST_DATABASE_URL."
    )


if __name__ == "__main__":
    asyncio.run(main())
