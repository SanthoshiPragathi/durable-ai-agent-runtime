"""In-process stand-in for a killed worker.

SimulatedCrash derives from BaseException, so neither ctx.step nor the runtime's `except
Exception` handlers touch it: it tears straight through like a SIGKILL would, leaving the run
RUNNING with whatever was already committed. (Real process kills are tested in tests/recovery
through failpoints.)
"""


class SimulatedCrash(BaseException):
    pass


class CrashPlan:
    """Wraps a tool call so the 'worker' can die just before or just after the tool acts.

    plan.before.add("process_refund")  -> raise SimulatedCrash BEFORE the tool is called
    plan.after.add("process_refund")   -> call the tool (its effect happens), THEN raise SimulatedCrash

    Each crash fires once, so the resumed run gets through.
    """

    def __init__(self) -> None:
        self.before: set[str] = set()
        self.after: set[str] = set()

    def wrap(self, name: str, fn):
        async def wrapped(*args, **kwargs):
            if name in self.before:
                self.before.discard(name)
                raise SimulatedCrash(f"before {name}")
            result = await fn(*args, **kwargs)
            if name in self.after:
                self.after.discard(name)
                raise SimulatedCrash(f"after {name}")
            return result

        return wrapped
