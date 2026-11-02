"""A free, offline LLM for tests and demos. Nothing here calls a real model.

Responses can be a fixed string, a list consumed one per call (the last one repeats), or a function
`(prompt, model) -> str`, which is handy for showing "Model A rejects, Model B approves".
"""

from collections.abc import Callable, Sequence
from typing import Any

from app.integrations.llm.base import LLMResponse

Responses = str | Sequence[str] | Callable[[str, str], str]


class MockLLMProvider:
    # Made-up price so cost tracking can be exercised: 1 micro-dollar per token.
    COST_PER_TOKEN_USD = 1e-6

    def __init__(
        self,
        responses: Responses = "APPROVE",
        *,
        default_model: str = "mock-model-a",
        fail_with: Exception | None = None,
    ) -> None:
        self.default_model = default_model
        self._responses = responses
        self._fail_with = fail_with
        self.calls: list[dict[str, Any]] = []  # tests inspect this to count real "model calls"

    async def complete(self, prompt: str, *, model: str | None = None, **params: Any) -> LLMResponse:
        chosen = model or self.default_model
        self.calls.append({"prompt": prompt, "model": chosen, "params": params})
        if self._fail_with is not None:
            raise self._fail_with

        output = self._pick_output(prompt, chosen)
        input_tokens = len(prompt.split())
        output_tokens = max(1, len(output.split()))
        return LLMResponse(
            output=output,
            model=chosen,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            cost_usd=(input_tokens + output_tokens) * self.COST_PER_TOKEN_USD,
        )

    def _pick_output(self, prompt: str, model: str) -> str:
        if callable(self._responses):
            return self._responses(prompt, model)
        if isinstance(self._responses, str):
            return self._responses
        index = min(len(self.calls) - 1, len(self._responses) - 1)
        return self._responses[index]
