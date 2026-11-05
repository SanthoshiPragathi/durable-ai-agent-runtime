"""A leave request that waits for a manager, possibly for days, without holding a process.

    request -> email the manager (idempotent) -> WAIT for the manager's decision (3 days)
            -> approved: notify the employee
            -> rejected: notify the employee
            -> no answer in time: escalate to the manager's manager and auto-reject

While it waits the run is WAITING: no lease, no worker, nothing in memory. The decision arrives as a
signal (POST /runs/{id}/signals/manager_decision) and any worker continues the run.
"""

from typing import Any

from app.sdk import workflow

DECISION_TIMEOUT_SECONDS = 3 * 24 * 3600


async def _send(outbox: list[dict[str, Any]], to: str, subject: str, *, idempotency_key: str) -> dict:
    # Stands in for an email API that honours idempotency keys: same key, same result, sent once.
    for mail in outbox:
        if mail["key"] == idempotency_key:
            return mail
    mail = {"to": to, "subject": subject, "key": idempotency_key}
    outbox.append(mail)
    return mail


# The demo and tests can swap this for a recording outbox; the default keeps the example self-contained.
OUTBOX: list[dict[str, Any]] = []


@workflow
async def leave_approval(
    ctx,
    employee: str,
    manager: str,
    days: int,
    timeout_seconds: float = DECISION_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    await ctx.effect(
        "email_manager",
        _send,
        OUTBOX,
        manager,
        f"{employee} requests {days} day(s) of leave",
        idempotent=True,
    )

    answer = await ctx.wait_for_signal(
        "await_decision", signal="manager_decision", timeout=timeout_seconds
    )

    if not answer["received"]:
        await ctx.effect(
            "escalate", _send, OUTBOX, f"{manager}-boss", f"Unanswered leave request from {employee}",
            idempotent=True,
        )
        outcome = "auto_rejected"
    else:
        outcome = "approved" if (answer["payload"] or {}).get("approved") else "rejected"

    await ctx.effect(
        "notify_employee", _send, OUTBOX, employee, f"Your leave request was {outcome}", idempotent=True
    )
    return {"employee": employee, "days": days, "outcome": outcome}
