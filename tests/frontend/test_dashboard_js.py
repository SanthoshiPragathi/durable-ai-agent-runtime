"""Behaviour of the dashboard's JavaScript, checked by loading the real dashboard.html in jsdom (Node)
against a stubbed API. Skipped when Node or the jsdom npm package is not available (npm install jsdom).

  dashboard_diff_check.mjs  a Diff result survives the periodic refresh, updates, and never leaks to
                            another run.
  dashboard_auth_check.mjs  the API-key prompt: asked once, remembered for the tab, not repeated.
"""

import os
import shutil
import subprocess
from pathlib import Path

import pytest

HERE = Path(__file__).parent
DASHBOARD = HERE.parent.parent / "app" / "static" / "dashboard.html"
NODE_SKIPPED = 77  # the scripts' own "jsdom is not installed" exit code


@pytest.mark.parametrize("script", ["dashboard_diff_check.mjs", "dashboard_auth_check.mjs"])
def test_dashboard_javascript_behaviour(script: str) -> None:
    node = shutil.which("node")
    if node is None:
        pytest.skip("node is not installed")
    result = subprocess.run(
        [node, str(HERE / script), str(DASHBOARD)],
        capture_output=True,
        text=True,
        timeout=60,
        env=os.environ.copy(),
    )
    if result.returncode == NODE_SKIPPED:
        pytest.skip("the jsdom npm package is not installed (npm install jsdom)")
    assert result.returncode == 0, result.stdout + result.stderr
