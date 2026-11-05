"""The credentials a worker presents with every write to a run it executes."""

from dataclasses import dataclass


@dataclass(frozen=True)
class Fence:
    """(worker_id, fencing_token) as handed out when the worker claimed the run.

    The token increases by one on every claim of a run. The database accepts a write only while the
    run's current token and owner still equal these values, so a worker that was paused, partitioned
    or slow and whose lease was given to someone else can no longer change the run's history.
    """

    worker_id: str
    token: int
