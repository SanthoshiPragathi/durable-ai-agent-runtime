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


# Stands in for the email provider's records. It is deliberately NOT passed to ctx.effect: effect
# arguments are hashed to detect non-determinism, and this list grows when the effect runs, so passing it
# made the replayed call hash differently inside a process that had already sent the email.
OUTBOX: list[dict[str, Any]] = []


async def _send(to: str, subject: str, *, idempotency_key: str) -> dict:
    # Stands in for an email API that honours idempotency keys: same key, same result, sent once.
    # OUTBOX is looked up here, at call time, so tests and demos can swap or clear it.
    for mail in OUTBOX:
        if mail["key"] == idempotency_key:
            return mail
    mail = {"to": to, "subject": subject, "key": idempotency_key}
    OUTBOX.append(mail)
    return mail


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
        manager,
        f"{employee} requests {days} day(s) of leave",
        idempotent=True,
    )

    answer = await ctx.wait_for_signal(
        "await_decision", signal="manager_decision", timeout=timeout_seconds
    )

    if not answer["received"]:
        await ctx.effect(
            "escalate", _send, f"{manager}-boss", f"Unanswered leave request from {employee}",
            idempotent=True,
        )
        outcome = "auto_rejected"
    else:
        outcome = "approved" if (answer["payload"] or {}).get("approved") else "rejected"

    await ctx.effect(
        "notify_employee", _send, employee, f"Your leave request was {outcome}", idempotent=True
    )
    return {"employee": employee, "days": days, "outcome": outcome}
