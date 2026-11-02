"""FastAPI entrypoint. Phase 0 exposes only a health check; routes arrive in later phases."""

from fastapi import FastAPI

app = FastAPI(
    title="Durable AI Agent Runtime",
    description="Fault-tolerant execution engine for AI workflows.",
    version="0.1.0",
)


@app.get("/health", tags=["meta"])
def health() -> dict[str, str]:
    return {"status": "ok"}
