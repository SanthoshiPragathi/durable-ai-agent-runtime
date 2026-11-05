"""OpenAI chat-completions provider.

Tests never call it for real: they inject a fake client. The estimated cost is computed only if
you configure per-million-token prices (OPENAI_INPUT_PRICE_PER_1M / OPENAI_OUTPUT_PRICE_PER_1M);
otherwise cost_usd is None rather than a guess from stale built-in prices.
"""

from typing import Any

from app.engine.errors import LLMConfigurationError, LLMTransientError
from app.integrations.llm.base import LLMResponse

_TRANSIENT_CLASS_NAMES = {"APIConnectionError", "APITimeoutError", "RateLimitError", "InternalServerError"}


def _is_transient(exc: BaseException) -> bool:
    """Rate limits, overload and network trouble are worth retrying; a bad request or key is not."""
    if any(cls.__name__ in _TRANSIENT_CLASS_NAMES for cls in type(exc).__mro__):
        return True
    status = getattr(exc, "status_code", None)
    return isinstance(status, int) and (status in (408, 409, 429) or status >= 500)


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
        try:
            response = await self._client.chat.completions.create(
                model=chosen, messages=[{"role": "user", "content": prompt}], **params
            )
        except Exception as exc:
            if _is_transient(exc):
                raise LLMTransientError(f"{type(exc).__name__}: {exc}") from exc
            raise
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
