"""Workflow definitions and the in-process registry."""

import inspect
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from app.engine.errors import (
    InvalidWorkflowInputError,
    WorkflowAlreadyRegisteredError,
    WorkflowNotFoundError,
)
from app.engine.hashing import canonical_json

WorkflowFunction = Callable[..., Awaitable[Any]]


@dataclass(frozen=True)
class WorkflowDefinition:
    name: str
    version: int
    fn: WorkflowFunction


class WorkflowRegistry:
    def __init__(self) -> None:
        self._workflows: dict[str, WorkflowDefinition] = {}

    def register(self, definition: WorkflowDefinition) -> None:
        existing = self._workflows.get(definition.name)
        if existing is not None and existing.fn is not definition.fn:
            raise WorkflowAlreadyRegisteredError(
                f"a different workflow is already registered as {definition.name!r}"
            )
        self._workflows[definition.name] = definition

    def get(self, name: str) -> WorkflowDefinition:
        try:
            return self._workflows[name]
        except KeyError:
            raise WorkflowNotFoundError(f"workflow {name!r} is not registered") from None

    def list(self) -> list[WorkflowDefinition]:
        return sorted(self._workflows.values(), key=lambda d: d.name)

    def validate_input(self, name: str, input_data: dict[str, Any]) -> None:
        """Check the input is JSON and matches the workflow's parameters (after `ctx`)."""
        definition = self.get(name)
        canonical_json(input_data)  # raises StepSerializationError if not JSON
        try:
            inspect.signature(definition.fn).bind(None, **input_data)
        except TypeError as exc:
            raise InvalidWorkflowInputError(f"invalid input for workflow {name!r}: {exc}") from exc

    # Test helpers
    def clear(self) -> None:
        self._workflows.clear()

    def snapshot(self) -> dict[str, WorkflowDefinition]:
        return dict(self._workflows)

    def restore(self, snapshot: dict[str, WorkflowDefinition]) -> None:
        self._workflows = dict(snapshot)


registry = WorkflowRegistry()
