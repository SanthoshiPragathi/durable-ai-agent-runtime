"""OpenAI chat-completions provider.

Tests never call it for real: they inject a fake client. The estimated cost is computed only if
you configure per-million-token prices (OPENAI_INPUT_PRICE_PER_1M / OPENAI_OUTPUT_PRICE_PER_1M);
otherwise cost_usd is None rather than a guess from stale built-in prices.
"""

from typing import Any

from app.engine.errors import LLMConfigurationError
from app.integrations.llm.base import LLMResponse


class OpenAIProvider:
    def __init__(
        self,
        api_key: str | None = None,
        default_model: str = "gpt-4o-mini",
        *,
        client: Any = None,
        input_price_per_1m: float | None = None,
        output_price_per_1m: float | None = None,
    ) -> None:
        if client is None:
            if not api_key:
                raise LLMConfigurationError("OPENAI_API_KEY is not set")
            from openai import AsyncOpenAI  # imported lazily so the mock path needs no key

            client = AsyncOpenAI(api_key=api_key)
        self._client = client
        self.default_model = default_model
        self._input_price = input_price_per_1m
        self._output_price = output_price_per_1m

    async def complete(self, prompt: str, *, model: str | None = None, **params: Any) -> LLMResponse:
        chosen = model or self.default_model
        response = await self._client.chat.completions.create(
            model=chosen, messages=[{"role": "user", "content": prompt}], **params
        )
        usage = getattr(response, "usage", None)
        input_tokens = int(getattr(usage, "prompt_tokens", 0) or 0)
        output_tokens = int(getattr(usage, "completion_tokens", 0) or 0)
        return LLMResponse(
            output=response.choices[0].message.content or "",
            model=chosen,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            cost_usd=self._estimate_cost(input_tokens, output_tokens),
        )

    def _estimate_cost(self, input_tokens: int, output_tokens: int) -> float | None:
        if self._input_price is None or self._output_price is None:
            return None
        return (input_tokens * self._input_price + output_tokens * self._output_price) / 1_000_000
