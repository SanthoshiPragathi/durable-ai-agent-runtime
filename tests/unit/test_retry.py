"""Retries for plain steps and LLM calls: backoff, jitter, timeout, and never for external effects."""

import asyncio
import random
import uuid
from typing import Any

import pytest

from app.engine.errors import (
    AmbiguousOutcomeError,
    LLMTransientError,
    StepFailedError,
    TransientError,
)
from app.integrations.llm import MockLLMProvider
from app.integrations.llm.openai_provider import OpenAIProvider
from app.sdk import WorkflowContext
from app.sdk.retry import RetryPolicy
from tests.fixtures.recorders import FakeRecorder

FAST = RetryPolicy(max_attempts=4, base_delay=0.0, max_delay=0.0, jitter=0.0)


def make(policy: RetryPolicy = FAST, llm: Any = None) -> tuple[WorkflowContext, FakeRecorder]:
    recorder = FakeRecorder()
    return WorkflowContext(uuid.uuid4(), recorder, llm=llm, retry_policy=policy), recorder


# ---- the policy itself ---------------------------------------------------------------------


def test_delays_grow_exponentially_up_to_the_cap_without_jitter() -> None:
    policy = RetryPolicy(max_attempts=9, base_delay=1.0, max_delay=5.0, multiplier=2.0, jitter=0.0)
    assert [policy.delay_for(n) for n in (1, 2, 3, 4, 5)] == [1.0, 2.0, 4.0, 5.0, 5.0]


def test_jitter_stays_inside_its_band_and_varies() -> None:
    policy = RetryPolicy(base_delay=10.0, max_delay=100.0, jitter=0.2)
    rng = random.Random(7)
    delays = [policy.delay_for(1, rng) for _ in range(200)]
    assert all(8.0 <= d <= 12.0 for d in delays)
    assert len({round(d, 6) for d in delays}) > 50


@pytest.mark.parametrize(
    "kwargs",
    [{"max_attempts": 0}, {"jitter": 1.5}, {"timeout": 0}, {"multiplier": 0.5}, {"base_delay": -1}],
)
def test_invalid_policies_are_rejected(kwargs: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        RetryPolicy(**kwargs)


# ---- plain steps ---------------------------------------------------------------------------


async def test_a_transient_failure_is_retried_until_it_succeeds() -> None:
    ctx, recorder = make()
    calls = 0

    async def flaky() -> str:
        nonlocal calls
        calls += 1
        if calls < 3:
            raise TransientError("blip")
        return "ok"

    assert await ctx.step("read", flaky) == "ok"
    assert calls == 3
    assert [c[0] for c in recorder.calls] == ["started", "completed"]  # one step, one record


async def test_transient_failures_beyond_max_attempts_fail_the_step() -> None:
    ctx, recorder = make(RetryPolicy(max_attempts=2, base_delay=0, max_delay=0, jitter=0))
    calls = 0

    def always() -> None:
        nonlocal calls
        calls += 1
        raise TransientError("down")

    with pytest.raises(StepFailedError) as info:
        await ctx.step("read", always)
    assert calls == 2 and info.value.error_type == "TransientError"
    assert recorder.calls[-1][0] == "failed"


async def test_a_permanent_error_is_not_retried() -> None:
    ctx, _ = make()
    calls = 0

    def broken() -> None:
        nonlocal calls
        calls += 1
        raise KeyError("days_since_purchase")

    with pytest.raises(StepFailedError):
        await ctx.step("read", broken)
    assert calls == 1


async def test_a_slow_attempt_times_out_and_is_retried() -> None:
    ctx, _ = make(RetryPolicy(max_attempts=3, base_delay=0, max_delay=0, jitter=0, timeout=0.05))
    calls = 0

    async def slow_then_fast() -> str:
        nonlocal calls
        calls += 1
        if calls == 1:
            await asyncio.sleep(1)
        return "done"

    assert await ctx.step("read", slow_then_fast) == "done"
    assert calls == 2


async def test_retry_waits_use_the_backoff_delay(monkeypatch: pytest.MonkeyPatch) -> None:
    slept: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        slept.append(seconds)

    monkeypatch.setattr("app.sdk.context.asyncio.sleep", fake_sleep)
    ctx, _ = make(RetryPolicy(max_attempts=4, base_delay=1.0, max_delay=30.0, jitter=0.0))
    calls = 0

    def flaky() -> int:
        nonlocal calls
        calls += 1
        if calls < 4:
            raise TransientError("x")
        return calls

    assert await ctx.step("read", flaky) == 4
    assert slept == [1.0, 2.0, 4.0]


# ---- LLM calls -----------------------------------------------------------------------------


async def test_llm_call_survives_two_rate_limits_and_records_the_attempts() -> None:
    attempts = {"n": 0}

    class RateLimited(MockLLMProvider):
        async def complete(self, prompt: str, *, model: str | None = None, **params: Any):
            attempts["n"] += 1
            if attempts["n"] <= 2:
                raise LLMTransientError("429 rate limit")
            return await super().complete(prompt, model=model, **params)

    ctx, recorder = make(llm=RateLimited("APPROVE"))
    assert await ctx.llm("classify", "is this fine?") == "APPROVE"
    completed = next(c for c in recorder.calls if c[0] == "completed")
    assert completed[2]["attempts"] == 3 and attempts["n"] == 3


async def test_llm_configuration_errors_are_not_retried() -> None:
    llm = MockLLMProvider(fail_with=ValueError("bad request"))
    ctx, _ = make(llm=llm)
    with pytest.raises(StepFailedError):
        await ctx.llm("classify", "x")
    assert len(llm.calls) == 1


async def test_a_per_call_policy_overrides_the_default() -> None:
    llm = MockLLMProvider(fail_with=LLMTransientError("overloaded"))
    ctx, _ = make(RetryPolicy(max_attempts=5, base_delay=0, max_delay=0, jitter=0), llm=llm)
    with pytest.raises(StepFailedError):
        await ctx.llm("classify", "x", retry=RetryPolicy(max_attempts=2, base_delay=0, jitter=0))
    assert len(llm.calls) == 2


# ---- external effects keep their own rules -------------------------------------------------


async def test_a_transient_error_inside_an_effect_is_not_retried_by_the_step_policy() -> None:
    ctx, _ = make()
    calls = 0

    def charge(idempotency_key: str) -> dict[str, Any]:
        nonlocal calls
        calls += 1
        raise TransientError("looks retryable, but this is a payment")

    with pytest.raises(StepFailedError):
        await ctx.effect("pay", charge, idempotent=True)
    assert calls == 1


async def test_ambiguous_effects_still_follow_the_idempotency_rules() -> None:
    ctx, recorder = make()
    calls = 0

    def charge(idempotency_key: str) -> dict[str, Any]:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise AmbiguousOutcomeError("timed out after sending")
        return {"refund_id": "rf_1"}

    result = await ctx.effect("pay", charge, idempotent=True)
    assert result == {"refund_id": "rf_1"} and calls == 2
    assert ("retry", "pay", ctx.idempotency_key("pay")) in recorder.calls


# ---- OpenAI error mapping ------------------------------------------------------------------


class RateLimitError(Exception):  # same class name the openai SDK uses
    pass


class _StatusError(Exception):
    def __init__(self, status: int) -> None:
        super().__init__(f"status {status}")
        self.status_code = status


class _FailingClient:
    def __init__(self, exc: Exception) -> None:
        self.chat = self
        self.completions = self
        self._exc = exc

    async def create(self, **_: Any) -> Any:
        raise self._exc


@pytest.mark.parametrize("exc", [RateLimitError("slow down"), _StatusError(503), _StatusError(429)])
async def test_openai_provider_marks_rate_limits_and_server_errors_transient(exc: Exception) -> None:
    with pytest.raises(LLMTransientError):
        await OpenAIProvider(client=_FailingClient(exc)).complete("hi")


@pytest.mark.parametrize("exc", [_StatusError(400), _StatusError(401), ValueError("nope")])
async def test_openai_provider_does_not_mark_client_errors_transient(exc: Exception) -> None:
    with pytest.raises(type(exc)):
        await OpenAIProvider(client=_FailingClient(exc)).complete("hi")
