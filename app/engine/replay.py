"""Replay history: what a previous execution of a run already did.

When a run is resumed, the workflow function is executed again from the top. Every ctx.step call
is matched, in order, against the recorded steps:

    recorded step #i      what ctx.step does
    ------------------    -------------------------------------------------------------
    COMPLETED             return the saved result; do NOT call the function again
    FAILED                raise StepFailedError again, as the first run did
    IN_DOUBT              STEP_STARTED but no outcome: the worker died mid-step. The step is
                          run again (at-least-once). Phase 3 replaces "run again" with
                          idempotency keys / OUTCOME_UNKNOWN for steps with side effects.
    (no record)           history exhausted: run live and record as usual

Name or input mismatches mean the workflow code changed since the history was written; that is
reported as NonDeterminismError instead of silently reusing an unrelated result.
"""

import enum
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any, Protocol

from app.database.models import EventType
from app.engine.errors import CorruptHistoryError


class StepStatus(enum.StrEnum):
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    IN_DOUBT = "IN_DOUBT"


@dataclass
class RecordedStep:
    name: str
    input_hash: str | None
    status: StepStatus = StepStatus.IN_DOUBT
    result: Any = None
    error_type: str | None = None
    error_message: str | None = None
    attempts: int = 1  # how many times STEP_STARTED was recorded for this step


class EventLike(Protocol):
    event_type: str
    step_name: str | None
    input_hash: str | None
    result_data: Any


@dataclass
class ReplayHistory:
    steps: list[RecordedStep] = field(default_factory=list)

    def __len__(self) -> int:
        return len(self.steps)

    def step_at(self, index: int) -> RecordedStep | None:
        return self.steps[index] if index < len(self.steps) else None

    def count(self, status: StepStatus) -> int:
        return sum(1 for s in self.steps if s.status == status)

    @classmethod
    def from_events(cls, events: Iterable[EventLike]) -> "ReplayHistory":
        """Build the history from a run's events, which must be ordered by sequence number."""
        steps: list[RecordedStep] = []
        by_name: dict[str, RecordedStep] = {}

        for event in events:
            kind = event.event_type
            name = event.step_name

            if kind == EventType.STEP_STARTED:
                assert name is not None
                existing = by_name.get(name)
                if existing is None:
                    record = RecordedStep(name=name, input_hash=event.input_hash)
                    by_name[name] = record
                    steps.append(record)
                else:
                    # A second STEP_STARTED means the first attempt died in doubt and the step
                    # was run again. That is only legal while it still has no outcome.
                    if existing.status != StepStatus.IN_DOUBT:
                        raise CorruptHistoryError(f"step {name!r} started again after it finished")
                    existing.attempts += 1

            elif kind in (EventType.STEP_COMPLETED, EventType.STEP_FAILED):
                record = by_name.get(name or "")
                if record is None or record.status != StepStatus.IN_DOUBT:
                    raise CorruptHistoryError(f"{kind} for step {name!r} without a matching start")
                if kind == EventType.STEP_COMPLETED:
                    record.status = StepStatus.COMPLETED
                    record.result = event.result_data
                else:
                    record.status = StepStatus.FAILED
                    details = event.result_data or {}
                    record.error_type = details.get("error_type")
                    record.error_message = details.get("message")

        return cls(steps)
