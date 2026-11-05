"""Workflow imported by the worker SUBPROCESS (via DURABLE_WORKFLOW_MODULES).

Every step appends a line to a ledger file. The file lives outside the database and outside the
process, so it is an independent record of which step functions really executed, the same role the
fake payment ledger plays in Phase 3.
"""

import os

from app.sdk import workflow

LEDGER_ENV = "DURABLE_TEST_LEDGER"


def _record(effect: str) -> None:
    with open(os.environ[LEDGER_ENV], "a", encoding="utf-8") as ledger:
        ledger.write(effect + "\n")


def fetch_order(order_id: str) -> dict:
    _record("fetch_order")
    return {"order_id": order_id, "amount": 2500}


def check_eligibility(order: dict) -> bool:
    _record("check_eligibility")
    return order["amount"] < 10_000


def process_refund(amount: int) -> dict:
    _record("process_refund")
    return {"refund_id": "r-1", "amount": amount}


def send_email(refund_id: str) -> str:
    _record("send_email")
    return f"sent:{refund_id}"


@workflow(name="process_refund_pipeline")
async def process_refund_pipeline(ctx, order_id: str):
    order = await ctx.step("fetch_order", fetch_order, order_id)
    eligible = await ctx.step("check_eligibility", check_eligibility, order)
    refund = await ctx.step("process_refund", process_refund, order["amount"])
    await ctx.step("send_email", send_email, refund["refund_id"])
    return {"order_id": order_id, "refund_id": refund["refund_id"], "eligible": eligible}


@workflow(name="legacy_refund_pipeline")
async def legacy_refund_pipeline(ctx, order_id: str):
    """A refund through a provider that cannot dedupe and has no verify hook."""
    from app.integrations.tools import NonIdempotentPaymentService
    from app.integrations.tools.fake_ledger import get_services_session_factory

    payments = NonIdempotentPaymentService(get_services_session_factory())
    reference = ctx.idempotency_key("process_refund")
    refund = await ctx.effect(
        "process_refund", payments.refund, order_id, 2500, reference, idempotent=False
    )
    return {"refund_id": refund["refund_id"]}


@workflow(name="waiting_pipeline")
async def waiting_pipeline(ctx, order_id: str):
    """Do some work, wait for a signal, do the rest. Every step writes to the ledger file."""
    order = await ctx.step("fetch_order", fetch_order, order_id)
    answer = await ctx.wait_for_signal("wait", signal="go")
    refund = await ctx.step("process_refund", process_refund, order["amount"])
    await ctx.step("send_email", send_email, refund["refund_id"])
    return {"order_id": order_id, "payload": answer["payload"]}
