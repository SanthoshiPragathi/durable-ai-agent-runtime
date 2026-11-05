"""A workflow whose steps take real time, so several workers visibly share a batch of runs."""

import asyncio
from typing import Any

from app.sdk import workflow

STEP_SECONDS = 0.4


async def _work(label: str, seconds: float) -> str:
    await asyncio.sleep(seconds)
    return label


@workflow
async def slow_report(ctx, report_id: int) -> dict[str, Any]:
    await ctx.step("gather", _work, f"gathered-{report_id}", STEP_SECONDS)
    await ctx.step("analyse", _work, f"analysed-{report_id}", STEP_SECONDS)
    await ctx.step("publish", _work, f"published-{report_id}", STEP_SECONDS)
    return {"report_id": report_id, "status": "published"}
