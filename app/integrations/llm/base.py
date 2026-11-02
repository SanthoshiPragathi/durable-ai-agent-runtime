"""The interface every LLM provider implements."""

from dataclasses import dataclass
from typing import Any, Protocol


@dataclass(frozen=True)
class LLMResponse:
    output: str
    model: str
    input_tokens: int
    output_tokens: int
    cost_usd: float | None = None  # None means "unknown", never a guess


class LLMProvider(Protocol):
    default_model: str

    async def complete(
        self, prompt: str, *, model: str | None = None, **params: Any
    ) -> LLMResponse: ...
