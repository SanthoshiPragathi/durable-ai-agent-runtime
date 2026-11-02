"""Run or resume one workflow run from the command line, in its own process.

    python -m app.engine.runner execute <run_id>
    python -m app.engine.runner resume  <run_id>

Workflow modules to import are listed in DURABLE_WORKFLOW_MODULES (comma separated); the example
workflows are always loaded. Prints one JSON line. Exit codes: 0 COMPLETED, 1 FAILED workflow, 2 runtime error
(e.g. NonDeterminismError), 3 NEEDS_REVIEW (a step's outcome is unknown). A failpoint crash exits 77.
"""

import argparse
import asyncio
import importlib
import json
import os
import sys
import uuid

from app.database.session import get_default_session_factory
from app.engine import runtime
from app.engine.errors import DurableRuntimeError


def load_workflow_modules() -> None:
    modules = ["app.examples.simple_workflow", "app.examples.refund_workflow"]
    modules += [m.strip() for m in os.environ.get("DURABLE_WORKFLOW_MODULES", "").split(",") if m.strip()]
    for module in modules:
        importlib.import_module(module)


async def _run(action: str, run_id: uuid.UUID) -> int:
    session_factory = get_default_session_factory()
    try:
        if action == "execute":
            run = await runtime.execute_run(session_factory, run_id)
        else:
            run = await runtime.resume_run(session_factory, run_id)
    except DurableRuntimeError as exc:
        print(json.dumps({"run_id": str(run_id), "error": type(exc).__name__, "message": str(exc)}))
        return 2
    finally:
        await session_factory.kw["bind"].dispose()
    print(json.dumps({"run_id": str(run.run_id), "status": run.status, "result": run.result_data}))
    return {"COMPLETED": 0, "NEEDS_REVIEW": 3}.get(run.status, 1)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Execute or resume one workflow run.")
    parser.add_argument("action", choices=["execute", "resume"])
    parser.add_argument("run_id", type=uuid.UUID)
    args = parser.parse_args(argv)
    load_workflow_modules()
    return asyncio.run(_run(args.action, args.run_id))


if __name__ == "__main__":
    sys.exit(main())
