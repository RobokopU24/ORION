"""Confirm `uv build` succeeds against the current pyproject.toml.

Catches malformed version strings (PEP 440), broken build-system requires, missing
package data, etc. — the class of bug that turns the pypi.yml workflow red after a
benign-looking edit.

The actual build is run into a temp output dir so this leaves nothing behind.
"""

import shutil
import subprocess
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parent.parent


@pytest.mark.skipif(shutil.which('uv') is None, reason='uv is not installed in this environment')
def test_uv_build_succeeds(tmp_path):
    result = subprocess.run(
        ['uv', 'build', '--out-dir', str(tmp_path)],
        cwd=str(REPO_ROOT),
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, (
        f'`uv build` failed with exit code {result.returncode}.\n'
        f'stdout:\n{result.stdout}\n'
        f'stderr:\n{result.stderr}'
    )
    artifacts = list(tmp_path.glob('*'))
    sdists = [a for a in artifacts if a.suffix == '.gz']
    wheels = [a for a in artifacts if a.suffix == '.whl']
    assert sdists, f'`uv build` did not produce an sdist. Artifacts: {artifacts}'
    assert wheels, f'`uv build` did not produce a wheel. Artifacts: {artifacts}'