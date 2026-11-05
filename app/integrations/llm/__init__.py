from app.config import Settings, get_settings
from app.engine.errors import LLMConfigurationError
from app.integrations.llm.base import LLMProvider, LLMResponse
from app.integrations.llm.mock_provider import MockLLMProvider
from app.integrations.llm.openai_provider import OpenAIProvider


def get_llm_provider(settings: Settings | None = None) -> LLMProvider:
    """Build the provider named by LLM_PROVIDER ("mock" by default, which is free and offline)."""
    settings = settings or get_settings()
    if settings.llm_provider == "mock":
        return MockLLMProvider()
    if settings.llm_provider == "openai":
        return OpenAIProvider(
            api_key=settings.openai_api_key.get_secret_value(),
            default_model=settings.openai_model,
            input_price_per_1m=settings.openai_input_price_per_1m,
            output_price_per_1m=settings.openai_output_price_per_1m,
        )
    raise LLMConfigurationError(f"unknown LLM_PROVIDER {settings.llm_provider!r}")


__all__ = ["LLMProvider", "LLMResponse", "MockLLMProvider", "OpenAIProvider", "get_llm_provider"]
