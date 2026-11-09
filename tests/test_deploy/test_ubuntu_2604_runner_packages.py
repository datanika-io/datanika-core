"""Ubuntu 26.04 removed the apt package CI installs before xmlsec will build.

PR 1620's lint job (Actions run 40660378168, 2026-10-28) exited 100 in about
11 seconds: ``libxmlsec1-openssl`` has no installation candidate on that image.
The replacement package is ``libxmlsec1-openssl1``. Ubuntu 24.04 and the Debian
image the Dockerfile builds on still ship the old name, so the install step has
to ask apt which name is installable.

The wiring assertion reads ``ci.yml`` only. ``nightly-connector-smoke.yml`` is
Infra's under core#1444 and is outside this check on purpose.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest
import yaml

_ROOT = Path(__file__).resolve().parents[2]
_CI = _ROOT / ".github" / "workflows" / "ci.yml"
_SCRIPT = _ROOT / "scripts" / "apt_xmlsec_openssl_package.py"

_POLICY_NONE = "libxmlsec1-openssl:\n  Installed: (none)\n  Candidate: (none)\n"
_POLICY_OLD = "libxmlsec1-openssl:\n  Installed: (none)\n  Candidate: 1.2.39-5build2\n"
_POLICY_NEW = "libxmlsec1-openssl1:\n  Installed: (none)\n  Candidate: 1.3.7-1\n"


def _load():
    spec = importlib.util.spec_from_file_location("apt_xmlsec_openssl_package", _SCRIPT)
    assert spec is not None and spec.loader is not None, _SCRIPT
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _runs_installing(text: str, needle: str) -> list[str]:
    """Run scripts, across every job, whose shell contains ``needle``."""
    doc = yaml.safe_load(text) or {}
    found: list[str] = []
    for job in (doc.get("jobs") or {}).values():
        if not isinstance(job, dict):
            continue
        for step in job.get("steps") or []:
            if not isinstance(step, dict):
                continue
            run = step.get("run")
            if isinstance(run, str) and needle in run:
                found.append(run)
    return found


def test_ubuntu_2604_selects_the_renamed_package() -> None:
    """The policy shape apt printed when the old package name had no candidate."""
    chosen = _load().select_package(
        {
            "libxmlsec1-openssl": _POLICY_NONE,
            "libxmlsec1-openssl1": _POLICY_NEW,
        }
    )
    assert chosen == "libxmlsec1-openssl1"


def test_ubuntu_2404_keeps_the_historical_package() -> None:
    chosen = _load().select_package(
        {
            "libxmlsec1-openssl": _POLICY_OLD,
            "libxmlsec1-openssl1": _POLICY_NONE,
        }
    )
    assert chosen == "libxmlsec1-openssl"


def test_both_candidates_prefer_the_historical_package() -> None:
    chosen = _load().select_package(
        {
            "libxmlsec1-openssl": _POLICY_OLD,
            "libxmlsec1-openssl1": _POLICY_NEW,
        }
    )
    assert chosen == "libxmlsec1-openssl"


def test_no_candidate_on_either_name_refuses() -> None:
    with pytest.raises(SystemExit, match="has an apt candidate"):
        _load().select_package(
            {
                "libxmlsec1-openssl": _POLICY_NONE,
                "libxmlsec1-openssl1": "libxmlsec1-openssl1:\n  Candidate: (none)\n",
            }
        )


def test_a_policy_with_no_candidate_line_is_not_a_version() -> None:
    assert _load().candidate("libxmlsec1-openssl:\n  Installed: (none)\n") is None


def test_ci_installs_xmlsec_openssl_by_candidate() -> None:
    runs = _runs_installing(_CI.read_text(encoding="utf-8"), "libxmlsec1-dev")
    assert runs, (
        "no ci.yml step installs libxmlsec1-dev, so this assertion cannot see "
        "a return to the hardcoded package name"
    )
    unguarded = [run for run in runs if "apt_xmlsec_openssl_package.py" not in run]
    assert not unguarded, (
        "a ci.yml install of libxmlsec1-dev still hardcodes libxmlsec1-openssl. "
        "On ubuntu-26.04 that package has no candidate and apt exits 100 "
        "(PR 1620, run 40660378168)."
    )


def test_cloud_suite_resolves_the_selector_beside_its_checkout() -> None:
    """cloud-suite checks core out at ``path: core`` and installs from the workspace root."""
    runs = _runs_installing(_CI.read_text(encoding="utf-8"), "libxmlsec1-dev")
    cloud = [run for run in runs if "cd core" in run]
    root = [run for run in runs if "cd core" not in run]
    assert cloud, "cloud-suite's install step is not visible to the wiring check"
    assert root, "the root-checkout jobs are not visible to the wiring check"
    assert all("python core/scripts/apt_xmlsec_openssl_package.py" in run for run in cloud)
    assert all("python scripts/apt_xmlsec_openssl_package.py" in run for run in root)
    assert all("python core/scripts/apt_xmlsec_openssl_package.py" not in run for run in root)


def test_a_hardcoded_install_is_classified_unguarded() -> None:
    """The same helper must reject the install line that failed on 2026-10-28.

    A helper that treated every script as guarded would make the ci.yml assertion
    pass the day the selector call was deleted.
    """
    sample = """
jobs:
  lint:
    steps:
      - run: |
          sudo apt-get install -y --no-install-recommends \\
            libxmlsec1-dev libxmlsec1-openssl
"""
    runs = _runs_installing(sample, "libxmlsec1-dev")
    assert len(runs) == 1
    assert "apt_xmlsec_openssl_package.py" not in runs[0]
