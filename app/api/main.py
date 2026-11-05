"""FastAPI entrypoint."""

import logging
from pathlib import Path

from fastapi import FastAPI, Request, status
from fastapi.responses import FileResponse, JSONResponse

import app.examples.leave_approval_workflow  # noqa: F401
import app.examples.refund_workflow  # noqa: F401  (importing registers the example workflows)
import app.examples.simple_workflow  # noqa: F401
import app.examples.slow_workflow  # noqa: F401
from app.api.routes import runs, services, workers, workflows
from app.engine.errors import (
    DurableRuntimeError,
    InvalidResolutionError,
    InvalidRunStateError,
    InvalidWorkflowInputError,
    RunNotFoundError,
    StepSerializationError,
    WorkflowNotFoundError,
)

logger = logging.getLogger(__name__)

app = FastAPI(
    title="Durable AI Agent Runtime",
    description="Fault-tolerant execution engine for AI workflows.",
    version="0.1.0",
)
app.include_router(workflows.router)
app.include_router(runs.router)
app.include_router(services.router)
app.include_router(workers.router)

_STATUS_BY_ERROR: dict[type[DurableRuntimeError], int] = {
    WorkflowNotFoundError: status.HTTP_404_NOT_FOUND,
    RunNotFoundError: status.HTTP_404_NOT_FOUND,
    InvalidWorkflowInputError: 422,
    StepSerializationError: 422,
    InvalidRunStateError: status.HTTP_409_CONFLICT,
    InvalidResolutionError: 422,
}


@app.exception_handler(DurableRuntimeError)
async def runtime_error_handler(_request: Request, exc: DurableRuntimeError) -> JSONResponse:
    code = next(
        (c for cls, c in _STATUS_BY_ERROR.items() if isinstance(exc, cls)),
        status.HTTP_500_INTERNAL_SERVER_ERROR,
    )
    if code >= 500:
        logger.exception("unhandled runtime error")
        return JSONResponse({"detail": "internal error"}, status_code=code)
    return JSONResponse({"detail": str(exc)}, status_code=code)


@app.exception_handler(Exception)
async def unexpected_error_handler(_request: Request, _exc: Exception) -> JSONResponse:
    # Never leak stack traces or internals to API clients; details go to the server log.
    logger.exception("unhandled error")
    return JSONResponse({"detail": "internal error"}, status_code=500)


@app.get("/dashboard", include_in_schema=False)
def dashboard() -> FileResponse:
    """A dependency-free single-page dashboard (plain HTML and JavaScript, no build step)."""
    return FileResponse(Path(__file__).resolve().parent.parent / "static" / "dashboard.html")


@app.get("/health", tags=["meta"])
def health() -> dict[str, str]:
    return {"status": "ok"}
