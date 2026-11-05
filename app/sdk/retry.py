"""Retry policy for plain steps and LLM calls (never for external effects).

An LLM call or a read-only step has no side effect to protect, so a transient failure can simply be
tried again. Delays grow exponentially up to a cap, with random jitter so many workers that failed
together do not retry together. An attempt that exceeds `timeout` counts as a transient failure.

External effects (ctx.effect) do NOT use this: their retries follow the idempotency-key and
reconciliation rules, because retrying a payment whose outcome is unknown can charge twice.
"""

import random
from dataclasses import dataclass


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 3  # total tries, including the first
    base_delay: float = 0.5  # seconds before the 2nd try
    max_delay: float = 30.0  # cap on any single wait
    multiplier: float = 2.0
    jitter: float = 0.2  # +/- fraction of the delay, e.g. 0.2 means 80%..120%
    timeout: float | None = None  # per-attempt limit in seconds; None means no limit

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if self.base_delay < 0 or self.max_delay < 0 or self.multiplier < 1:
            raise ValueError("delays must be >= 0 and multiplier >= 1")
        if not 0 <= self.jitter <= 1:
            raise ValueError("jitter must be between 0 and 1")
        if self.timeout is not None and self.timeout <= 0:
            raise ValueError("timeout must be > 0")

    def delay_for(self, failed_attempt: int, rng: random.Random | None = None) -> float:
        """Seconds to wait after attempt number `failed_attempt` (1-based) failed."""
        raw = min(self.max_delay, self.base_delay * self.multiplier ** (failed_attempt - 1))
        if self.jitter == 0:
            return raw
        draw = (rng or random).uniform(1 - self.jitter, 1 + self.jitter)
        return min(self.max_delay, raw * draw)

    @classmethod
    def from_settings(cls, settings: "object") -> "RetryPolicy":
        return cls(
            max_attempts=settings.retry_max_attempts,  # type: ignore[attr-defined]
            base_delay=settings.retry_base_delay_seconds,  # type: ignore[attr-defined]
            max_delay=settings.retry_max_delay_seconds,  # type: ignore[attr-defined]
            jitter=settings.retry_jitter,  # type: ignore[attr-defined]
            timeout=settings.step_timeout_seconds,  # type: ignore[attr-defined]
        )


NO_RETRY = RetryPolicy(max_attempts=1)
