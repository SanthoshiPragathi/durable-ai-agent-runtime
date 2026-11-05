"""Idle workers back off with jitter instead of hammering the database every poll interval."""

from typing import Any

import pytest

from app.engine.worker import Worker


def make(**kwargs: Any) -> Worker:
    defaults: dict[str, Any] = {"lease_seconds": 30.0, "heartbeat_seconds": 10.0, "poll_seconds": 1.0}
    return Worker(None, "w", **{**defaults, **kwargs})  # type: ignore[arg-type]  # no database needed


def test_the_wait_grows_exponentially_and_is_capped(monkeypatch: pytest.MonkeyPatch) -> None:
    worker = make()
    worker.poll_jitter = 0.0
    worker.poll_backoff, worker.poll_max_seconds = 2.0, 8.0
    assert [worker.idle_delay(n) for n in range(6)] == [1.0, 2.0, 4.0, 8.0, 8.0, 8.0]


def test_jitter_keeps_workers_from_polling_in_lockstep() -> None:
    worker = make()
    worker.poll_jitter, worker.poll_backoff, worker.poll_max_seconds = 0.2, 1.0, 10.0
    delays = [worker.idle_delay(0) for _ in range(300)]
    assert all(0.8 <= d <= 1.2 for d in delays)
    assert len({round(d, 5) for d in delays}) > 100


def test_an_explicit_poll_interval_keeps_the_cap_close_to_it() -> None:
    assert make(poll_seconds=0.05).poll_max_seconds == pytest.approx(0.2)
    assert make(poll_seconds=0.5).poll_max_seconds == pytest.approx(2.0)


def test_the_idle_heartbeat_write_is_never_slower_than_the_lease_heartbeat() -> None:
    assert make(heartbeat_seconds=2.0, lease_seconds=10.0).touch_seconds <= 2.0
