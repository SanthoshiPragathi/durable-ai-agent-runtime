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


def format_error(exc: BaseException) -> dict[str, Any]:
    """A compact, safe description of an exception for storage. No stack trace."""
    return {"error_type": type(exc).__name__, "message": str(exc)}
