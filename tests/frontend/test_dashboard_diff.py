"""The dashboard rebuilds its detail panel every few seconds; a Diff result must survive that.

Runs the real dashboard.html in jsdom (Node) against a stubbed API. Skipped when Node or the jsdom npm
package is not available (install with: npm install jsdom).
"""

import os
import shutil
import subprocess
from pathlib import Path

import pytest

HERE = Path(__file__).parent
DASHBOARD = HERE.parent.parent / "app" / "static" / "dashboard.html"
NODE_SKIPPED = 77  # the script's own "jsdom is not installed" exit code


def test_diff_result_survives_refresh_updates_and_does_not_leak_between_runs() -> None:
    node = shutil.which("node")
    if node is None:
        pytest.skip("node is not installed")
    result = subprocess.run(
        [node, str(HERE / "dashboard_diff_check.mjs"), str(DASHBOARD)],
        capture_output=True,
        text=True,
        timeout=60,
        env=os.environ.copy(),
    )
    if result.returncode == NODE_SKIPPED:
        pytest.skip("the jsdom npm package is not installed (npm install jsdom)")
    assert result.returncode == 0, result.stdout + result.stderr
