"""A small three-step workflow used for the Phase 1 demo. The business logic is fake on purpose:
the point is the recorded event history, not the order data."""

from typing import Any

from app.sdk import workflow

FAKE_ORDERS: dict[str, list[dict[str, Any]]] = {
    "A-100": [{"sku": "keyboard", "price": 4500, "qty": 1}, {"sku": "mouse", "price": 1200, "qty": 2}],
    "A-200": [{"sku": "monitor", "price": 15000, "qty": 1}],
}


def fetch_order(order_id: str) -> list[dict[str, Any]]:
    if order_id not in FAKE_ORDERS:
        raise LookupError(f"order {order_id!r} not found")
    return FAKE_ORDERS[order_id]


def calculate_total(items: list[dict[str, Any]]) -> int:
    return sum(item["price"] * item["qty"] for item in items)


@workflow
async def order_summary(ctx, order_id: str) -> dict[str, Any]:
    items = await ctx.step("fetch_order", fetch_order, order_id)
    total = await ctx.step("calculate_total", calculate_total, items)
    summary = await ctx.step("format_summary", lambda: f"{len(items)} item(s), total {total / 100:.2f}")
    return {"order_id": order_id, "total": total, "summary": summary}
