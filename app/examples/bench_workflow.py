"""Workflow used only by scripts/benchmark.py: n trivial steps, so the numbers measure the runtime
(database writes, claiming, replay) and not the work."""

from app.sdk import workflow


@workflow
async def bench_flow(ctx, steps: int = 5):
    total = 0
    for i in range(steps):
        total = await ctx.step(f"s{i}", lambda t, k: t + k, total, i)
    return total
