"""``import datanika_cloud.plugin`` must work as the FIRST import (core#832).

[core#772] put ``from datanika_cloud.plugin import bootstrap_cloud`` at module
scope in ``celery_app.py``. The plugin's own module body imports back into
core's UI state, which reaches ``celery_app`` again:

    datanika_cloud.plugin
      -> datanika.ui.state.auth_state
      -> datanika.services.email_verification
      -> datanika.tasks.email_tasks
      -> datanika.tasks.celery_app
      -> datanika_cloud.plugin      (partially initialised)

    ImportError: cannot import name 'bootstrap_cloud' from partially
    initialized module 'datanika_cloud.plugin'

Both production entrypoints happen to import in an order that avoids it —
``celery -A datanika.tasks.celery_app`` and ``datanika.datanika`` — so this was
latent rather than broken. But it is luck, not design, and it **did** break the
prod-verification command that `CLAUDE.md` and `WORKFLOW_RULES` §3 both
prescribe: ``docker exec datanika-app /app/.venv/bin/python -c "from
datanika_cloud import plugin; ..."``. Infra ran exactly that against
``master b1a5fc25`` and, for a few minutes, a healthy deploy read as a broken
one — and the error names the *plugin*, while the edge that closed the loop is
core's.

The real fix is in the graph, not in the call: ``services/email_verification.py``
imported ``tasks/email_tasks.py`` at module scope, which is a layering
inversion (`models -> services -> tasks -> ui/state`) as well as the edge that
closes this cycle.

⚠️ **Core's venv has no ``datanika_cloud``**, so this writes a stand-in package
and puts it on ``sys.path``. The stand-in reproduces the ONE edge that closes
the loop — its module body imports ``datanika.ui.state.auth_state`` *before*
binding ``bootstrap_cloud`` — because everything else about the cycle is a
property of **core's** import graph, which is the thing under test.

🔑 That is also why ``test_celery_worker_bootstraps_cloud.py`` could not have
caught this: its stub is a bare ``types.ModuleType`` that imports nothing, so it
models a plugin with no edge back into core, and the cycle shipped green
underneath it. A stub is only as good as the edge it reproduces.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent.parent

#: The stand-in writes this marker as its FIRST statement, then rewrites it once
#: the core import returns.
#:
#: 🔑 It exists because the obvious control does not work. Asserting
#: ``"datanika.tasks.celery_app" in sys.modules`` looks like a check that the
#: chain was traversed, and it is not: Python **removes** a module from
#: ``sys.modules`` when its import raises, so the whole chain unwinds and the
#: control reports "the harness never ran" for the one case where it ran and
#: found the bug. A control that fails in exactly the state under test is
#: indistinguishable from a broken harness, and the natural response to it —
#: loosening the stand-in — deletes the test.
#:
#: The marker survives the exception, so "body-entered" is proof the stand-in
#: was imported whatever happened next.
_BODY_ENTERED = "body-entered"
_CORE_IMPORTED = "core-imported"

_PLUGIN_BODY = f"""\
# Stand-in for datanika_cloud.plugin. It reproduces the single edge that
# matters: the real module body reaches into core's UI state BEFORE binding its
# own public names.
import os
import pathlib

_marker = pathlib.Path(os.environ["DATANIKA_STANDIN_MARKER"])
_marker.write_text("{_BODY_ENTERED}", encoding="utf-8")

from datanika.ui.state.auth_state import AuthState  # noqa: E402, F401

_marker.write_text("{_CORE_IMPORTED}", encoding="utf-8")


def bootstrap_cloud():
    pass


def init_cloud(app):
    pass
"""

_SCRIPT = """\
import sys
import traceback

ok = True
bound = False
try:
    import datanika_cloud.plugin as p
    bound = hasattr(p, "bootstrap_cloud")
except Exception:
    ok = False
    traceback.print_exc()
print("IMPORT_OK", ok)
print("BOOTSTRAP_BOUND", bound)
"""


def _import_plugin_first(tmp_path: Path) -> tuple[subprocess.CompletedProcess[str], str]:
    pkg = tmp_path / "datanika_cloud"
    pkg.mkdir(exist_ok=True)
    (pkg / "__init__.py").write_bytes(b"")
    (pkg / "plugin.py").write_bytes(_PLUGIN_BODY.encode("utf-8"))
    marker = tmp_path / "standin.marker"

    env = {
        **os.environ,
        "DATANIKA_EDITION": "cloud",
        "PYTHONPATH": str(tmp_path),
        "PYTHONIOENCODING": "utf-8",
        "DATANIKA_STANDIN_MARKER": str(marker),
    }
    proc = subprocess.run(
        [sys.executable, "-c", _SCRIPT],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        timeout=300,
        env=env,
    )
    seen = marker.read_text(encoding="utf-8") if marker.exists() else "<no marker>"
    return proc, seen


class TestThePluginMayBeImportedFirst:
    def test_the_stand_in_is_actually_imported(self, tmp_path):
        """Control. It has to hold in BOTH states or nothing below means anything.

        A stand-in that is never imported — a PYTHONPATH that does not take, a
        name shadowed by a real package — makes the assertion below pass while
        exercising nothing, which is precisely how the bare-``ModuleType`` stub
        in ``test_celery_worker_bootstraps_cloud.py`` sat over this cycle.
        """
        _, seen = _import_plugin_first(tmp_path)
        assert seen in (_BODY_ENTERED, _CORE_IMPORTED), (
            f"the stand-in's module body never ran (marker={seen!r}), so this file "
            "is not testing the thing it claims to"
        )

    def test_importing_the_plugin_first_does_not_raise(self, tmp_path):
        proc, seen = _import_plugin_first(tmp_path)
        assert "IMPORT_OK True" in proc.stdout, (
            "importing datanika_cloud.plugin first raised — that is the documented "
            f"prod-verification command.\nmarker={seen!r}\nstdout={proc.stdout}\n"
            f"stderr={proc.stderr[-2000:]}"
        )
        assert seen == _CORE_IMPORTED, (
            f"the stand-in's import of core's UI state did not return (marker={seen!r})"
        )
        assert "BOOTSTRAP_BOUND True" in proc.stdout, proc.stdout
