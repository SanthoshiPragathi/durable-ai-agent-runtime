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
    """Workflow definitions by (name, version), so a deploy can carry v1 and v2 side by side.

    New runs always start on the highest registered version. A run keeps the version it was created
    with: a worker that has that version's code replays it with exactly that code, so a runs waiting
    for a signal for days is not broken by shipping v2.
    """

    def __init__(self) -> None:
        self._workflows: dict[tuple[str, int], WorkflowDefinition] = {}

    def register(self, definition: WorkflowDefinition) -> None:
        key = (definition.name, definition.version)
        existing = self._workflows.get(key)
        if existing is not None and existing.fn is not definition.fn:
            raise WorkflowAlreadyRegisteredError(
                f"a different workflow is already registered as {definition.name!r} v{definition.version}"
            )
        self._workflows[key] = definition

    def versions(self, name: str) -> list[int]:
        return sorted(version for (n, version) in self._workflows if n == name)

    def get(self, name: str, version: int | None = None) -> WorkflowDefinition:
        """The definition for `name`: the latest version, or exactly `version` when given."""
        versions = self.versions(name)
        if not versions:
            raise WorkflowNotFoundError(f"workflow {name!r} is not registered")
        chosen = versions[-1] if version is None else version
        try:
            return self._workflows[(name, chosen)]
        except KeyError:
            raise WorkflowNotFoundError(
                f"workflow {name!r} v{chosen} is not registered (registered: {versions})"
            ) from None

    def list(self) -> list[WorkflowDefinition]:
        return [self._workflows[key] for key in sorted(self._workflows)]

    def validate_input(self, name: str, input_data: dict[str, Any], version: int | None = None) -> None:
        """Check the input is JSON and matches the workflow's parameters (after `ctx`)."""
        definition = self.get(name, version)
        canonical_json(input_data)  # raises StepSerializationError if not JSON
        try:
            inspect.signature(definition.fn).bind(None, **input_data)
        except TypeError as exc:
            raise InvalidWorkflowInputError(f"invalid input for workflow {name!r}: {exc}") from exc

    # Test helpers
    def clear(self) -> None:
        self._workflows.clear()

    def snapshot(self) -> dict[tuple[str, int], WorkflowDefinition]:
        return dict(self._workflows)

    def restore(self, snapshot: dict[tuple[str, int], WorkflowDefinition]) -> None:
        self._workflows = dict(snapshot)


registry = WorkflowRegistry()
