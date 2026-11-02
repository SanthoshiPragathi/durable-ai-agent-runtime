"""Exception hierarchy for the runtime. Public API handlers map these to HTTP status codes."""

from typing import Any


class DurableRuntimeError(Exception):
    """Base class for all errors raised deliberately by the runtime."""


class WorkflowNotFoundError(DurableRuntimeError):
    pass


class WorkflowAlreadyRegisteredError(DurableRuntimeError):
    pass


class InvalidWorkflowInputError(DurableRuntimeError):
    pass


class RunNotFoundError(DurableRuntimeError):
    pass


class InvalidRunStateError(DurableRuntimeError):
    """A run was not in the state required for the requested transition."""


class DuplicateEventError(DurableRuntimeError):
    """An event would violate an event-log uniqueness rule (e.g. a step completing twice)."""


class DuplicateStepNameError(DurableRuntimeError):
    """Step names must be unique within a run. In loops, include the index in the name."""


class StepSerializationError(DurableRuntimeError):
    """A step input or result cannot be stored as JSON."""


class StepFailedError(DurableRuntimeError):
    """A step raised. This is what workflow code sees, on the first run AND on every replay.

    The original exception object cannot be rebuilt from the database, so replay could only
    raise a different type than the first run did. To keep both runs identical, ctx.step always
    raises StepFailedError (the original is chained as __cause__ on the first run only).
    Workflow code that handles step failures should catch StepFailedError.
    """

    def __init__(self, step_name: str, error_type: str, message: str) -> None:
        super().__init__(f"step {step_name!r} failed: {error_type}: {message}")
        self.step_name = step_name
        self.error_type = error_type
        self.message = message


class NonDeterminismError(DurableRuntimeError):
    """Replay found that the workflow code no longer matches the recorded history."""


class WorkflowVersionMismatchError(DurableRuntimeError):
    """The registered workflow version differs from the version the run was started with."""


class CorruptHistoryError(DurableRuntimeError):
    """The recorded event history is internally inconsistent."""


def format_error(exc: BaseException) -> dict[str, Any]:
    """A compact, safe description of an exception for storage. No stack trace."""
    if isinstance(exc, StepFailedError):
        # Report the underlying failure, not the wrapper.
        return {"error_type": exc.error_type, "message": exc.message}
    return {"error_type": type(exc).__name__, "message": str(exc)}
