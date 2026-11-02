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
