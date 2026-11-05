"""Failpoints: deliberately kill the process at an exact moment, to test crash recovery.

Random kills rarely land in the dangerous windows, so code calls failpoint("name", key) at the
interesting moments and a test selects one through the DURABLE_FAILPOINT environment variable:

    DURABLE_FAILPOINT=before_step_completed:process_refund

Format: "<name>" (any step) or "<name>:<key>" (only that step). When it matches, the process ends
immediately with exit code 77 and no cleanup, like SIGKILL: no `finally` blocks, no flushing.
If the variable is unset (always the case in production) failpoint() does nothing.

DURABLE_STALL freezes the whole process instead of killing it, to imitate a worker that is paused
(a long garbage collection, a VM freeze, a network partition) and wakes up later:

    DURABLE_STALL=before_step_completed:process_refund/8     # same name syntax, then /<seconds>

The stall is a blocking time.sleep, so the event loop stops, including the lease heartbeat, exactly
as it would in a frozen process. It works on every operating system (no SIGSTOP needed).

Names used so far:
    after_workflow_started        right after PENDING->RUNNING committed, before any step
    before_step_started           key=step name, before STEP_STARTED is recorded
    after_step_started            key=step name, STEP_STARTED recorded, step not yet run
    before_step_completed         key=step name, step function RAN, result not yet recorded
    after_step_completed          key=step name, STEP_COMPLETED recorded
    before_wait_recorded          key=step name, a sleep/signal wait is about to be evaluated
    after_wait_recorded           key=step name, the wait was recorded (completed, or run parked)
    before_workflow_completed     all steps done, WORKFLOW_COMPLETED not yet recorded
"""

import os
import sys
import time

ENV_VAR = "DURABLE_FAILPOINT"
STALL_ENV_VAR = "DURABLE_STALL"
CRASH_EXIT_CODE = 77


def failpoint(name: str, key: str | None = None) -> None:
    stall = os.environ.get(STALL_ENV_VAR)
    if stall:
        target, _, seconds = stall.rpartition("/")
        if target == name or (key is not None and target == f"{name}:{key}"):
            sys.stderr.write(f"STALL {stall}: freezing the process\n")
            sys.stderr.flush()
            time.sleep(float(seconds))
            sys.stderr.write("STALL over: process resumes\n")
            sys.stderr.flush()
    spec = os.environ.get(ENV_VAR)
    if not spec:
        return
    if spec == name or (key is not None and spec == f"{name}:{key}"):
        sys.stderr.write(f"FAILPOINT {spec}: simulating a crash\n")
        sys.stderr.flush()
        os._exit(CRASH_EXIT_CODE)
