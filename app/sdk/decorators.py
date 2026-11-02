"""The @workflow decorator."""

import inspect
from typing import Any

from app.sdk.workflow import WorkflowDefinition, WorkflowFunction, registry


def workflow(
    fn: WorkflowFunction | None = None, *, name: str | None = None, version: int = 1
) -> Any:
    """Register an async function as a durable workflow.

    Usage::

        @workflow
        async def refund_workflow(ctx, order_id: str): ...

        @workflow(name="refunds", version=2)
        async def refund_v2(ctx, order_id: str): ...

    The function receives a WorkflowContext first; the run's input is passed as keyword
    arguments. Bump `version` when you change the workflow's logic.
    """

    def decorator(func: WorkflowFunction) -> WorkflowFunction:
        if not inspect.iscoroutinefunction(func):
            raise TypeError(f"workflow {func.__name__!r} must be defined with 'async def'")
        registry.register(WorkflowDefinition(name=name or func.__name__, version=version, fn=func))
        return func

    return decorator(fn) if fn is not None else decorator
