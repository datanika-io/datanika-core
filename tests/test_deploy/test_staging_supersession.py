"""A superseded `dev` run does not enter `staging-deploy` (core#975, the residual).

The defect
----------
`test_staging_concurrency.py` pins ONE group member per run, which closed the *intra*-run
displacement core#975 was filed about. It did not close *inter*-run displacement under a
burst. GitHub holds one running plus one PENDING entry per concurrency group, a newer waiter
cancels the pending one, and "newer" means **whichever run's tests finished later** -- not
whichever commit is newer. Measured 2026-09-16:

    commit     pushed      tests done (enters group)   staging
    3a2d4147   23:06:19Z   23:17:24Z                   pending, cancelled 23:17:41Z  <- the HEAD
    0cd5b471   23:02:44Z   23:17:40Z                   pending, then ran

The commit that became `dev`'s head got no staging verdict, and nothing anywhere was red.

The shape that fixes it
-----------------------
A `supersession` job reads `dev`'s head through the API when the caller would enter the group,
and the caller enters only if the run's own commit is still the head. `e2e-sso`, which reads
the same staging stack, is gated on the same answer.

Measured before it was built (run 35231903068 on a throwaway branch, since deleted): a `uses:`
caller skipped by `if:` does **not** enter its group and does **not** cancel a pending entry --
that entry ran when the holder released -- while the same caller with the condition true
cancelled the pending entry one second after its check finished. Without that fact this gate
would be decoration.

Invariants, each derived from what a job DOES rather than from its name
------------------------------------------------------------------------
1. **The caller of `staging.yml` is gated on the check's `is_head` output**, and needs the check.
2. 🚨 **The check needs exactly what the caller otherwise needs.** It must read the head at
   group-entry time, after the tests. A check with no `needs` runs at push time, eleven minutes
   too early, and every burst it exists for slips past it.
3. 🚨 **The check holds no concurrency group.** A second `staging-deploy` member is core#975
   itself.
4. **Every other job that needs the caller is gated on the same output**, so a superseded run
   does not start `e2e-sso` against somebody else's build.
5. **The script fails OPEN.** An unreadable head answers `true` -- the behaviour before the
   check existed. Answering `false` would drop the head's verdict silently, the one outcome
   core#975 exists to prevent.

What this does NOT claim
------------------------
That two overlapping `dev` pushes now always leave the head a complete verdict. That is core#975's
acceptance test and it needs a real burst, not a unit test. This file pins the shape and the
script; the burst is read on the issue when one occurs.
"""

from __future__ import annotations

import shutil
import stat
import subprocess
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "staging-supersession.sh"
CI = ROOT / ".github" / "workflows" / "ci.yml"
STAGING_WF = "staging.yml"
GROUP = "staging-deploy"
SCRIPT_REF = "scripts/staging-supersession.sh"

HEAD = "a" * 40
OTHER = "b" * 40
REPO = "datanika-io/datanika-core"


# ── behaviour: the script against a fake `gh` ────────────────────────────────────────────


def _bash() -> str:
    exe = shutil.which("bash")
    if exe is None:  # pragma: no cover - no bash means no CI either
        pytest.fail("bash not found; this suite must not silently stop testing the script")
    return exe


def _posix_path(path: Path) -> str:
    exe = shutil.which("cygpath")
    if exe:
        return subprocess.run(
            [exe, "-u", str(path)], capture_output=True, text=True, check=True
        ).stdout.strip()
    text = path.as_posix()
    if len(text) > 1 and text[1] == ":":
        return f"/{text[0].lower()}{text[2:]}"
    return text


_FAKE_GH = r"""#!/usr/bin/env bash
ST="__STATE__"
echo "gh $*" >> "$ST/calls.log"
mode=$(cat "$ST/mode")
case "$mode" in
  head:*)  echo "${mode#head:}" ;;
  fail)    echo "HTTP 502: Bad Gateway" >&2; exit 1 ;;
  garbage) echo "<html>rate limited</html>" ;;
  empty)   ;;
esac
"""


def _run(tmp: Path, mode: str) -> tuple[str, str, str]:
    """Run the script with `gh` faked; return (GITHUB_OUTPUT contents, stdout, gh call log)."""
    bindir = tmp / "bin"
    state = tmp / "state"
    bindir.mkdir(parents=True, exist_ok=True)
    state.mkdir(parents=True, exist_ok=True)
    (state / "mode").write_bytes(mode.encode())
    (state / "calls.log").write_bytes(b"")
    gh = bindir / "gh"
    gh.write_bytes(_FAKE_GH.replace("__STATE__", _posix_path(state)).encode())  # never CRLF
    gh.chmod(gh.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)
    out = tmp / "github_output"
    out.write_bytes(b"")
    proc = subprocess.run(
        [
            _bash(),
            "-lc",
            f'export PATH="{_posix_path(bindir)}:$PATH"; '
            f'export REPO="{REPO}" SHA="{HEAD}" GITHUB_OUTPUT="{_posix_path(out)}"; '
            f'exec bash "{SCRIPT.as_posix()}"',
        ],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert proc.returncode == 0, f"the check must never fail the job: {proc.stderr}"
    return (
        out.read_text(encoding="utf-8"),
        proc.stdout,
        (state / "calls.log").read_text(encoding="utf-8"),
    )


def _is_head(output: str) -> list[str]:
    return [ln for ln in output.splitlines() if ln.startswith("is_head=")]


def test_the_stub_is_what_the_script_calls(tmp_path: Path) -> None:
    """Control: without it every test below could pass against the real `gh` and prove nothing."""
    _, _, calls = _run(tmp_path, f"head:{HEAD}")
    assert f"api repos/{REPO}/commits/dev" in calls, f"the fake gh was not called: {calls!r}"


def test_the_head_enters(tmp_path: Path) -> None:
    output, _, _ = _run(tmp_path, f"head:{HEAD}")
    assert _is_head(output) == ["is_head=true"], output


def test_a_superseded_commit_does_not_enter(tmp_path: Path) -> None:
    output, stdout, _ = _run(tmp_path, f"head:{OTHER}")
    assert _is_head(output) == ["is_head=false"], output
    assert "::notice::" in stdout and HEAD[:8] in stdout and OTHER[:8] in stdout, (
        "a skip must say which commit superseded which, or it reads like a missing run"
    )


@pytest.mark.parametrize("mode", ["fail", "garbage", "empty"])
def test_an_unreadable_head_fails_open(tmp_path: Path, mode: str) -> None:
    """The one direction that must never be silent: a read failure never drops a verdict."""
    output, stdout, _ = _run(tmp_path, mode)
    assert _is_head(output) == ["is_head=true"], (
        f"gh {mode}: an unreadable head must answer true (the pre-check behaviour), got {output!r}"
    )
    assert "::warning::" in stdout, "a fail-open must be visible in the job log"


# ── shape: ci.yml ─────────────────────────────────────────────────────────────────────────


def _needs(job: dict) -> set[str]:
    n = job.get("needs") or []
    return {n} if isinstance(n, str) else set(n)


def _group(job: dict) -> str | None:
    c = job.get("concurrency")
    if isinstance(c, str):
        return c
    if isinstance(c, dict):
        return c.get("group")
    return None


def audit(doc: dict) -> dict[str, list[str]]:
    """ci.yml's parsed document -> named findings, each an empty list when clean."""
    jobs: dict[str, dict] = doc.get("jobs") or {}
    callers = [n for n, j in jobs.items() if str(j.get("uses") or "").endswith(f"/{STAGING_WF}")]
    checks = [
        n
        for n, j in jobs.items()
        if any(SCRIPT_REF in str(s.get("run") or "") for s in (j.get("steps") or []))
    ]
    findings: dict[str, list[str]] = {
        "no_caller": [] if callers else ["no job calls staging.yml"],
        "no_check": [] if checks else [f"no job runs {SCRIPT_REF}"],
        "caller_ungated": [],
        "check_runs_early": [],
        "check_holds_a_group": [],
        "reader_ungated": [],
        "output_unwired": [],
    }
    if not callers or not checks:
        return findings
    check = checks[0]
    gate = f"needs.{check}.outputs.is_head == 'true'"
    for c in callers:
        job = jobs[c]
        if check not in _needs(job) or gate not in str(job.get("if") or ""):
            findings["caller_ungated"].append(c)
        # 2. the check needs exactly what the caller needs besides the check itself
        if _needs(jobs[check]) != _needs(job) - {check}:
            findings["check_runs_early"].append(
                f"{check} needs {sorted(_needs(jobs[check]))}, the caller {c} needs "
                f"{sorted(_needs(job) - {check})}"
            )
        # 4. every other job that needs the caller is gated on the same answer
        for n, j in jobs.items():
            if c in _needs(j) and (check not in _needs(j) or gate not in str(j.get("if") or "")):
                findings["reader_ungated"].append(n)
    if _group(jobs[check]):
        findings["check_holds_a_group"].append(f"{check} ({_group(jobs[check])})")
    outputs = (jobs[check].get("outputs") or {}).get("is_head", "")
    step_ids = [
        s.get("id") for s in jobs[check].get("steps") or [] if SCRIPT_REF in str(s.get("run"))
    ]
    if not step_ids or f"steps.{step_ids[0]}.outputs.is_head" not in str(outputs):
        findings["output_unwired"].append(check)
    return findings


@pytest.fixture(scope="module")
def real() -> dict[str, list[str]]:
    return audit(yaml.safe_load(CI.read_text(encoding="utf-8")))


def test_the_auditor_found_the_caller_and_the_check(real) -> None:
    """A selector that matches nothing reports a clean bill of health."""
    assert real["no_caller"] == [] and real["no_check"] == [], real


SHAPE_FINDINGS = [
    "caller_ungated",
    "check_runs_early",
    "check_holds_a_group",
    "reader_ungated",
    "output_unwired",
]


@pytest.mark.parametrize("finding", SHAPE_FINDINGS)
def test_ci_yml_satisfies(real, finding: str) -> None:
    assert real[finding] == [], f"{finding}: {real[finding]}"


def _shape(**overrides: dict) -> dict:
    step = {"id": "head", "run": f"bash {SCRIPT_REF}"}
    jobs = {
        "lint": {"steps": [{"run": "true"}]},
        "test": {"steps": [{"run": "true"}]},
        "supersession": {
            "needs": ["lint", "test"],
            "outputs": {"is_head": "${{ steps.head.outputs.is_head }}"},
            "steps": [step],
        },
        "staging": {
            "needs": ["lint", "test", "supersession"],
            "if": "github.ref == 'refs/heads/dev' && needs.supersession.outputs.is_head == 'true'",
            "concurrency": {"group": GROUP, "cancel-in-progress": False},
            "uses": "./.github/workflows/staging.yml",
        },
        "e2e-sso": {
            "needs": ["staging", "supersession"],
            "if": "!cancelled() && needs.supersession.outputs.is_head == 'true'",
            "steps": [{"run": "true"}],
        },
    }
    for name, job in overrides.items():
        jobs[name] = job
    return {"jobs": jobs}


def test_the_auditor_passes_the_shipped_shape() -> None:
    """Positive control: without it every red below could be a broken auditor."""
    assert all(v == [] for v in audit(_shape()).values()), audit(_shape())


def test_auditor_rejects_an_ungated_caller() -> None:
    caller = dict(_shape()["jobs"]["staging"], **{"if": "github.ref == 'refs/heads/dev'"})
    assert audit(_shape(staging=caller))["caller_ungated"] == ["staging"]


def test_auditor_rejects_a_check_that_runs_at_push_time() -> None:
    check = dict(_shape()["jobs"]["supersession"], needs=[])
    assert audit(_shape(supersession=check))["check_runs_early"]


def test_auditor_rejects_a_check_holding_the_lock() -> None:
    check = dict(_shape()["jobs"]["supersession"], concurrency={"group": GROUP})
    assert audit(_shape(supersession=check))["check_holds_a_group"]


def test_auditor_rejects_a_reader_left_ungated() -> None:
    sso = dict(_shape()["jobs"]["e2e-sso"], needs=["staging"], **{"if": "!cancelled()"})
    assert audit(_shape(**{"e2e-sso": sso}))["reader_ungated"] == ["e2e-sso"]


def test_auditor_rejects_an_unwired_output() -> None:
    check = dict(_shape()["jobs"]["supersession"], outputs={"is_head": "true"})
    assert audit(_shape(supersession=check))["output_unwired"] == ["supersession"]
