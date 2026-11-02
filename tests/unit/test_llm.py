import hashlib
import uuid
from types import SimpleNamespace

import pytest

from app.config import Settings
from app.engine.errors import LLMConfigurationError, NonDeterminismError, StepFailedError
from app.engine.hashing import hash_step_input
from app.engine.replay import RecordedStep, ReplayHistory, StepStatus
from app.integrations.llm import MockLLMProvider, OpenAIProvider, get_llm_provider
from app.sdk import WorkflowContext
from tests.fixtures.recorders import FakeRecorder

# ---- MockLLMProvider ----------------------------------------------------------------------


async def test_mock_returns_a_fixed_answer_with_token_counts_and_cost() -> None:
    llm = MockLLMProvider("APPROVE")
    response = await llm.complete("should we refund this order please")
    assert response.output == "APPROVE" and response.model == "mock-model-a"
    assert (response.input_tokens, response.output_tokens) == (6, 1)
    assert response.cost_usd == pytest.approx(7e-6)
    assert [(c["prompt"], c["model"]) for c in llm.calls] == [
        ("should we refund this order please", "mock-model-a")
    ]


async def test_mock_consumes_a_list_and_repeats_the_last_answer() -> None:
    llm = MockLLMProvider(["REJECT", "APPROVE"])
    answers = [(await llm.complete("p")).output for _ in range(3)]
    assert answers == ["REJECT", "APPROVE", "APPROVE"]


async def test_mock_can_answer_differently_per_model() -> None:
    llm = MockLLMProvider(lambda prompt, model: "APPROVE" if model == "model-b" else "REJECT")
    assert (await llm.complete("p", model="model-a")).output == "REJECT"
    assert (await llm.complete("p", model="model-b")).output == "APPROVE"


async def test_mock_can_fail() -> None:
    llm = MockLLMProvider(fail_with=TimeoutError("model timed out"))
    with pytest.raises(TimeoutError):
        await llm.complete("p")


# ---- OpenAIProvider (fake client, no network) ---------------------------------------------


class FakeCompletions:
    def __init__(self) -> None:
        self.kwargs: dict = {}

    async def create(self, **kwargs):
        self.kwargs = kwargs
        return SimpleNamespace(
            choices=[SimpleNamespace(message=SimpleNamespace(content="APPROVE"))],
            usage=SimpleNamespace(prompt_tokens=12, completion_tokens=3),
        )


def fake_client() -> tuple[SimpleNamespace, FakeCompletions]:
    completions = FakeCompletions()
    return SimpleNamespace(chat=SimpleNamespace(completions=completions)), completions


async def test_openai_provider_maps_request_and_usage() -> None:
    client, completions = fake_client()
    provider = OpenAIProvider(client=client, default_model="gpt-test")
    response = await provider.complete("hello", temperature=0)

    assert completions.kwargs == {
        "model": "gpt-test",
        "messages": [{"role": "user", "content": "hello"}],
        "temperature": 0,
    }
    assert (response.output, response.model) == ("APPROVE", "gpt-test")
    assert (response.input_tokens, response.output_tokens) == (12, 3)
    assert response.cost_usd is None  # no prices configured: unknown, not guessed


async def test_openai_provider_estimates_cost_only_when_prices_are_configured() -> None:
    client, _ = fake_client()
    provider = OpenAIProvider(
        client=client, input_price_per_1m=2.0, output_price_per_1m=10.0
    )
    response = await provider.complete("hello")
    assert response.cost_usd == pytest.approx((12 * 2.0 + 3 * 10.0) / 1_000_000)


def test_openai_provider_requires_a_key_when_it_builds_its_own_client() -> None:
    with pytest.raises(LLMConfigurationError, match="OPENAI_API_KEY"):
        OpenAIProvider(api_key="")


def test_factory_picks_the_provider_from_settings() -> None:
    assert isinstance(get_llm_provider(Settings(llm_provider="mock")), MockLLMProvider)
    with pytest.raises(LLMConfigurationError):
        get_llm_provider(Settings(llm_provider="openai", openai_api_key=""))
    with pytest.raises(LLMConfigurationError, match="unknown"):
        get_llm_provider(Settings(llm_provider="bogus"))


# ---- ctx.llm ------------------------------------------------------------------------------


def make_ctx(llm=None, history=None):
    recorder = FakeRecorder()
    return WorkflowContext(uuid.uuid4(), recorder, history, llm=llm), recorder


async def test_ctx_llm_records_everything_and_returns_the_text() -> None:
    llm = MockLLMProvider("APPROVE")
    ctx, recorder = make_ctx(llm)

    answer = await ctx.llm("refund_decision", prompt="refund order A1 ?")

    assert answer == "APPROVE"
    kind, name, record = recorder.calls[-1]
    assert (kind, name) == ("completed", "refund_decision")
    assert record["output"] == "APPROVE" and record["model"] == "mock-model-a"
    assert record["prompt_hash"] == hashlib.sha256(b"refund order A1 ?").hexdigest()
    assert (record["input_tokens"], record["output_tokens"]) == (4, 1)
    assert record["cost_usd"] == pytest.approx(5e-6)
    assert record["latency_ms"] >= 0 and record["timestamp"].endswith("+00:00")
    assert "refund order A1" not in str(record)  # the prompt itself is not stored


async def test_replay_returns_the_recorded_answer_and_never_calls_the_model() -> None:
    first_llm = MockLLMProvider("APPROVE")
    first_ctx, first_recorder = make_ctx(first_llm)
    await first_ctx.llm("decide", prompt="p")
    recorded_result = first_recorder.calls[-1][2]
    input_hash = first_recorder.calls[0][2]

    history = ReplayHistory(
        [RecordedStep("decide", input_hash, status=StepStatus.COMPLETED, result=recorded_result)]
    )
    second_llm = MockLLMProvider("REJECT")  # a re-run would give a different answer
    ctx, recorder = make_ctx(second_llm, history)

    assert await ctx.llm("decide", prompt="p") == "APPROVE"
    assert second_llm.calls == [] and recorder.calls == []


async def test_changed_prompt_or_model_is_a_divergence() -> None:
    llm = MockLLMProvider("APPROVE")
    recorded_hash = hash_step_input(
        (), {"prompt": "original", "model": "mock-model-a", "params": {}}
    )
    history = ReplayHistory(
        [RecordedStep("decide", recorded_hash, status=StepStatus.COMPLETED, result={"output": "X"})]
    )

    ctx, _ = make_ctx(llm, history)
    with pytest.raises(NonDeterminismError):
        await ctx.llm("decide", prompt="edited prompt")

    ctx, _ = make_ctx(llm, history)
    with pytest.raises(NonDeterminismError):
        await ctx.llm("decide", prompt="original", model="other-model")
    assert llm.calls == []


async def test_llm_failure_is_recorded_as_a_failed_step() -> None:
    ctx, recorder = make_ctx(MockLLMProvider(fail_with=TimeoutError("model timed out")))
    with pytest.raises(StepFailedError, match="model timed out") as caught:
        await ctx.llm("decide", prompt="p")
    assert caught.value.error_type == "TimeoutError"
    assert [c[0] for c in recorder.calls] == ["started", "failed"]


async def test_ctx_llm_without_a_provider_is_a_clear_error() -> None:
    ctx, recorder = make_ctx(llm=None)
    with pytest.raises(LLMConfigurationError):
        await ctx.llm("decide", prompt="p")
    assert recorder.calls == []
