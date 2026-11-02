"""Start worker processes that run or resume one workflow run, optionally killed by a failpoint."""

import os
import subprocess
import sys
import uuid

from app.engine.failpoints import ENV_VAR
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
