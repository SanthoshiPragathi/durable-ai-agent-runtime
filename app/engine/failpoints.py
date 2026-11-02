"""Failpoints: deliberately kill the process at an exact moment, to test crash recovery.

Random kills rarely land in the dangerous windows, so code calls failpoint("name", key) at the
interesting moments and a test selects one through the DURABLE_FAILPOINT environment variable:

    DURABLE_FAILPOINT=before_step_completed:process_refund

Format: "<name>" (any step) or "<name>:<key>" (only that step). When it matches, the process ends
immediately with exit code 77 and no cleanup, like SIGKILL: no `finally` blocks, no flushing.
If the variable is unset (always the case in production) failpoint() does nothing.

Names used so far:
    after_workflow_started        right after PENDING->RUNNING committed, before any step
    before_step_started           key=step name, before STEP_STARTED is recorded
    after_step_started            key=step name, STEP_STARTED recorded, step not yet run
    before_step_completed         key=step name, step function RAN, result not yet recorded
    after_step_completed          key=step name, STEP_COMPLETED recorded
    before_workflow_completed     all steps done, WORKFLOW_COMPLETED not yet recorded
"""

import os
import sys

ENV_VAR = "DURABLE_FAILPOINT"
CRASH_EXIT_CODE = 77


def failpoint(name: str, key: str | None = None) -> None:
    spec = os.environ.get(ENV_VAR)
    if not spec:
        return
    if spec == name or (key is not None and spec == f"{name}:{key}"):
        sys.stderr.write(f"FAILPOINT {spec}: simulating a crash\n")
        sys.stderr.flush()
        os._exit(CRASH_EXIT_CODE)
