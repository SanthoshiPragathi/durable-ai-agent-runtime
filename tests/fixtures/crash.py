"""In-process stand-in for a killed worker.

SimulatedCrash derives from BaseException, so neither ctx.step nor the runtime's `except
Exception` handlers touch it: it tears straight through like a SIGKILL would, leaving the run
RUNNING with whatever was already committed. (Real process kills are tested in tests/recovery
through failpoints.)
"""


class SimulatedCrash(BaseException):
    pass
