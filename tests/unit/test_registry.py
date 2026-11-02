import pytest

from app.engine.errors import (
    InvalidWorkflowInputError,
    StepSerializationError,
    WorkflowAlreadyRegisteredError,
    WorkflowNotFoundError,
)
from app.sdk import workflow
from app.sdk.workflow import registry

pytestmark = pytest.mark.usefixtures("isolated_registry")


def test_decorator_registers_with_defaults() -> None:
    @workflow
    async def greet(ctx, name: str):
        return name

    definition = registry.get("greet")
    assert (definition.name, definition.version) == ("greet", 1)
    assert definition.fn is greet  # decorator returns the original function


def test_decorator_accepts_name_and_version() -> None:
    @workflow(name="refunds", version=3)
    async def refund_v3(ctx):
        return None

    definition = registry.get("refunds")
    assert (definition.name, definition.version) == ("refunds", 3)


def test_non_async_function_is_rejected() -> None:
    with pytest.raises(TypeError, match="async def"):

        @workflow
        def not_async(ctx):
            return 1


def test_two_different_functions_cannot_share_a_name() -> None:
    @workflow(name="dup")
    async def first(ctx):
        return 1

    with pytest.raises(WorkflowAlreadyRegisteredError):

        @workflow(name="dup")
        async def second(ctx):
            return 2


def test_unknown_workflow_raises() -> None:
    with pytest.raises(WorkflowNotFoundError):
        registry.get("nope")


def test_list_is_sorted_by_name() -> None:
    @workflow
    async def b_flow(ctx):
        return 1

    @workflow
    async def a_flow(ctx):
        return 1

    assert [d.name for d in registry.list()] == ["a_flow", "b_flow"]


def test_validate_input_accepts_matching_arguments() -> None:
    @workflow
    async def refund(ctx, order_id: str, amount: int = 5):
        return 1

    registry.validate_input("refund", {"order_id": "A1"})
    registry.validate_input("refund", {"order_id": "A1", "amount": 9})


def test_validate_input_rejects_missing_and_unexpected_arguments() -> None:
    @workflow
    async def refund(ctx, order_id: str):
        return 1

    with pytest.raises(InvalidWorkflowInputError):
        registry.validate_input("refund", {})
    with pytest.raises(InvalidWorkflowInputError):
        registry.validate_input("refund", {"order_id": "A1", "typo": 1})


def test_validate_input_rejects_non_json() -> None:
    @workflow
    async def refund(ctx, order_id):
        return 1

    with pytest.raises(StepSerializationError):
        registry.validate_input("refund", {"order_id": object()})
