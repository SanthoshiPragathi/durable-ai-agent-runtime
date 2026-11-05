"""Start worker processes that run or resume one workflow run, optionally killed by a failpoint."""

import os
import subprocess
import sys
import uuid

from app.engine.failpoints import ENV_VAR, STALL_ENV_VAR
from tests.conftest import ROOT


def run_worker(
    action: str,
    run_id: uuid.UUID,
    *,
    db_url: str,
    services_url: str,
    failpoint: str | None = None,
    modules: str = "",
) -> subprocess.CompletedProcess:
    env = {
        **os.environ,
        "DATABASE_URL": db_url,
        "SERVICES_DATABASE_URL": services_url,
        "DURABLE_WORKFLOW_MODULES": modules,
        "LLM_PROVIDER": "mock",
    }
    env.pop(ENV_VAR, None)
    if failpoint:
        env[ENV_VAR] = failpoint
    return subprocess.run(
        [sys.executable, "-m", "app.engine.runner", action, str(run_id)],
        cwd=ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=120,
    )


def start_worker(
    worker_id: str,
    *,
    db_url: str,
    services_url: str,
    failpoint: str | None = None,
    stall: str | None = None,
    lease: float = 1.5,
    heartbeat: float = 0.3,
    poll: float = 0.2,
    modules: str = "",
) -> subprocess.Popen:
    """Start a long-lived worker process (python -m app.engine.worker --drain). Short lease timings
    keep takeover tests quick. Use .communicate(timeout=...) to wait for it."""
    env = {
        **os.environ,
        "DATABASE_URL": db_url,
        "SERVICES_DATABASE_URL": services_url,
        "DURABLE_WORKFLOW_MODULES": modules,
        "LLM_PROVIDER": "mock",
    }
    env.pop(ENV_VAR, None)
    env.pop(STALL_ENV_VAR, None)
    if failpoint:
        env[ENV_VAR] = failpoint
    if stall:
        env[STALL_ENV_VAR] = stall
    return subprocess.Popen(
        [
            sys.executable,
            "-m",
            "app.engine.worker",
            "--id",
            worker_id,
            "--lease",
            str(lease),
            "--heartbeat",
            str(heartbeat),
            "--poll",
            str(poll),
            "--drain",
        ],
        cwd=ROOT,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
