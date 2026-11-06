from tests.fixtures.slow_link import LEASE_ROUND_TRIPS, scaled_timings


def test_a_fast_link_keeps_the_original_numbers() -> None:
    t = scaled_timings(0.004)
    assert (t.lease, t.heartbeat, t.poll, t.drain_timeout) == (0.8, 0.1, 0.02, 300.0)


def test_a_slow_link_stretches_the_lease_beyond_a_renewal() -> None:
    t = scaled_timings(0.5)
    assert t.lease >= LEASE_ROUND_TRIPS * 0.5 > 0.8  # a renewal must fit many times inside a lease
    assert t.heartbeat < t.lease  # still renews several times per lease
    assert t.heartbeat == t.lease / LEASE_ROUND_TRIPS
    assert t.drain_timeout >= 300.0


def test_timings_never_shrink_below_the_original_floors() -> None:
    for round_trip in (0.0, 0.001, 0.05, 1.0, 5.0):
        t = scaled_timings(round_trip)
        assert t.lease >= 0.8 and t.heartbeat >= 0.1 and t.poll >= 0.02 and t.drain_timeout >= 300.0


def test_scaling_is_monotonic() -> None:
    previous = scaled_timings(0.0)
    for round_trip in (0.1, 0.3, 1.0, 3.0):
        t = scaled_timings(round_trip)
        assert t.lease >= previous.lease and t.drain_timeout >= previous.drain_timeout
        previous = t
