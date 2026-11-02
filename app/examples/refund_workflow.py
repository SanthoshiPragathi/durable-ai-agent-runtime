"""Refund processing: the flagship example.

Read-only work uses ctx.step; the two calls with real-world effects (the refund and the email) use
ctx.effect with idempotency keys, so a crash or timeout can never refund or email twice.
"""

from typing import Any

from app.integrations.tools import emails, payments
from app.sdk import workflow

# A tiny fake order database. Amounts are in cents.
FAKE_ORDERS: dict[str, dict[str, Any]] = {
    "ORD-1001": {
        "amount": 4999,
        "days_since_purchase": 5,
        "customer_email": "asha@example.com",
        "complaint": "The headphones arrived with a cracked left ear cup.",
    },
    "ORD-2002": {
        "amount": 12999,
        "days_since_purchase": 90,
        "customer_email": "ravi@example.com",
        "complaint": "I changed my mind about the monitor.",
    },
    "ORD-3003": {
        "amount": 250000,
        "days_since_purchase": 3,
        "customer_email": "meera@example.com",
        "complaint": "The laptop never powered on.",
    },
}

REFUND_WINDOW_DAYS = 30
AUTO_REFUND_LIMIT_CENTS = 100_000


def fetch_order(order_id: str) -> dict[str, Any]:
    if order_id not in FAKE_ORDERS:
        raise LookupError(f"order {order_id!r} not found")
    return {"order_id": order_id, **FAKE_ORDERS[order_id]}


def check_eligibility(order: dict[str, Any]) -> dict[str, Any]:
    """Deterministic business rules (no LLM involved)."""
    if order["days_since_purchase"] > REFUND_WINDOW_DAYS:
        return {"eligible": False, "reason": f"outside the {REFUND_WINDOW_DAYS}-day refund window"}
    if order["amount"] > AUTO_REFUND_LIMIT_CENTS:
        return {"eligible": False, "reason": "amount above the automatic refund limit"}
    return {"eligible": True, "reason": "within policy"}


@workflow(name="refund_workflow")
async def refund_workflow(ctx, order_id: str) -> dict[str, Any]:
    order = await ctx.step("fetch_order", fetch_order, order_id)
    eligibility = await ctx.step("check_eligibility", check_eligibility, order)
    if not eligibility["eligible"]:
        return {"order_id": order_id, "status": "REJECTED", "reason": eligibility["reason"]}

    verdict = await ctx.llm(
        "classify_complaint",
        prompt=(
            "You review refund requests. Is this complaint justified? "
            f"Answer exactly APPROVE or REJECT.\nComplaint: {order['complaint']}"
        ),
    )
    if verdict.strip().upper() != "APPROVE":
        return {"order_id": order_id, "status": "REJECTED", "reason": "complaint not justified"}

    refund = await ctx.effect(
        "process_refund", payments().refund, order_id, order["amount"], idempotent=True
    )
    await ctx.effect(
        "send_confirmation",
        emails().send,
        order["customer_email"],
        "Your refund has been processed",
        f"We refunded {order['amount'] / 100:.2f} for order {order_id} (refund {refund['refund_id']}).",
        idempotent=True,
    )
    return {
        "order_id": order_id,
        "status": "REFUNDED",
        "refund_id": refund["refund_id"],
        "amount": refund["amount"],
    }
