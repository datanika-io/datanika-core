"""core#1166 — the scheduled CVE scan, and the five traps its own issue listed.

Each test below corresponds to a trap recorded on the issue before implementation.
They are here because every one of them fails *silently* in production: a schedule
that never fires, a token outage that reads as green, an issue-filer that cannot
file. None would turn anything red on its own.
"""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

REPO = Path(__file__).resolve().parents[2]
WF = REPO / ".github" / "workflows" / "cve-watch.yml"
SCRIPT = REPO / ".github" / "scripts" / "cve_report.py"


def _wf() -> dict:
    return yaml.safe_load(WF.read_text(encoding="utf-8"))


def _on(doc: dict) -> dict:
    # PyYAML parses a bare `on:` key as the boolean True.
    return doc.get("on") or doc.get(True) or {}


def test_workflow_exists() -> None:
    assert WF.is_file(), "cve-watch.yml is missing"


def test_it_has_a_schedule_which_is_what_registers_it_with_the_watchdog() -> None:
    """Trap 2: an unregistered schedule inherits the silent-death mode.

    `scheduled-workflow-watchdog.yml` auto-discovers by reading `schedule:` keys,
    so the presence of this key IS the registration. Remove it and the workflow
    stops being watched at the same moment it stops running.
    """
    sched = _on(_wf()).get("schedule")
    assert sched, "no `schedule:` — the watchdog cannot discover this workflow"
    assert any(entry.get("cron") for entry in sched)


def test_cron_is_off_the_top_of_the_hour() -> None:
    """Trap 3: measured skew at :00 on this org has been 3h22m-6h05m."""
    for entry in _on(_wf())["schedule"]:
        minute = entry["cron"].split()[0]
        assert minute not in ("0", "00"), (
            f"cron {entry['cron']!r} fires at the top of the hour, which is the "
            f"high-load window this org has measured hours of skew in"
        )


def test_cron_does_not_collide_with_the_existing_schedules() -> None:
    """A pile-up at one minute is the load window this avoids."""
    mine = {e["cron"] for e in _on(_wf())["schedule"]}
    others: set[str] = set()
    for path in (REPO / ".github" / "workflows").glob("*.yml"):
        if path.name == WF.name:
            continue
        doc = yaml.safe_load(path.read_text(encoding="utf-8"))
        if not isinstance(doc, dict):
            continue
        for entry in _on(doc).get("schedule") or []:
            others.add(entry.get("cron"))
    assert mine & others == set(), f"cron collides with an existing schedule: {mine & others}"


def test_it_can_write_issues_because_that_is_the_whole_delivery_mechanism() -> None:
    perms = _wf().get("permissions") or {}
    assert perms.get("issues") == "write", (
        "without `issues: write` the filer cannot file, and the run still goes green"
    )


def test_a_missing_cloud_token_fails_rather_than_scanning_nothing() -> None:
    """Trap 4: image-cve warns and continues; a scheduled run must not.

    On a PR from a fork, warning is right — nothing can be built and the PR is not
    at fault. On the default branch the token is always supposed to be there, so
    its absence is a real fault and a green would be a scan of nothing.
    """
    steps = _wf()["jobs"]["scan"]["steps"]
    guard = next(s for s in steps if s.get("id") == "token")
    body = str(guard.get("run", ""))
    assert "exit 1" in body, "a token outage must fail, not warn-and-continue"


def test_it_asserts_the_report_exists_before_believing_it_is_clean() -> None:
    """An empty report is indistinguishable from a clean scan."""
    runs = " ".join(str(s.get("run", "")) for s in _wf()["jobs"]["scan"]["steps"])
    assert "is NOT a clean scan" in runs


def test_the_scan_is_fixed_only_and_critical_high() -> None:
    """Volume was measured before this was built: the unfixed set never gates."""
    step = next(
        s for s in _wf()["jobs"]["scan"]["steps"] if "trivy-action" in str(s.get("uses", ""))
    )
    with_ = step["with"]
    assert with_["severity"] == "CRITICAL,HIGH"
    assert with_["ignore-unfixed"] is True


def test_it_does_not_go_red_on_a_finding() -> None:
    """The delivery is an issue. A red tick here would page nobody and mute fast."""
    step = next(
        s for s in _wf()["jobs"]["scan"]["steps"] if "trivy-action" in str(s.get("uses", ""))
    )
    assert str(step["with"]["exit-code"]) == "0"


# --------------------------------------------------------------------------
# cve_report.py behaviour — the edge-triggering logic
# --------------------------------------------------------------------------


def _report(vulns: list[dict]) -> dict:
    return {"Results": [{"Target": "img", "Vulnerabilities": vulns}]}


def _v(vid: str, sev: str = "HIGH", fixed: str | None = "2.0") -> dict:
    d = {"VulnerabilityID": vid, "Severity": sev, "PkgName": "pkg", "InstalledVersion": "1.0"}
    if fixed:
        d["FixedVersion"] = fixed
    return d


def _run(report: dict, tmp_path: Path) -> str:
    p = tmp_path / "trivy.json"
    p.write_text(json.dumps(report), encoding="utf-8")
    res = subprocess.run(
        [sys.executable, str(SCRIPT), "--report", str(p), "--repo", "o/r", "--dry-run"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert res.returncode == 0, res.stderr
    return res.stdout


def test_unfixed_advisories_are_never_filed(tmp_path: Path) -> None:
    """The 130 CRITICAL+HIGH on master are unfixed; filing them would be noise."""
    out = _run(_report([_v("CVE-1", fixed=None), _v("CVE-2")]), tmp_path)
    assert "fixable CRITICAL/HIGH advisories: 1" in out
    assert "CVE-2" in out
    assert "CVE-1" not in out


def test_low_and_medium_are_never_filed(tmp_path: Path) -> None:
    out = _run(_report([_v("CVE-LOW", sev="LOW"), _v("CVE-MED", sev="MEDIUM")]), tmp_path)
    assert "fixable CRITICAL/HIGH advisories: 0" in out


def test_the_same_advisory_across_targets_is_one_finding(tmp_path: Path) -> None:
    report = {
        "Results": [
            {"Target": "a", "Vulnerabilities": [_v("CVE-9")]},
            {"Target": "b", "Vulnerabilities": [_v("CVE-9")]},
        ]
    }
    assert "fixable CRITICAL/HIGH advisories: 1" in _run(report, tmp_path)


def test_a_clean_report_files_nothing(tmp_path: Path) -> None:
    assert "nothing to file." in _run(_report([]), tmp_path)


def test_critical_sorts_before_high(tmp_path: Path) -> None:
    out = _run(_report([_v("CVE-H", sev="HIGH"), _v("CVE-C", sev="CRITICAL")]), tmp_path)
    assert out.index("CVE-C") < out.index("CVE-H")


@pytest.mark.parametrize(
    "title,expected",
    [
        ("[Infra] CRITICAL CVE-2026-1 in libx (1.0 -> 2.0)", {"CVE-2026-1"}),
        ("[Infra] HIGH GHSA-8g6f-qw9x-4q6q in mcp (1.0 -> 2.0)", {"GHSA-8g6f-qw9x-4q6q"}),
        ("[Infra] A file deleted in git is never deleted", set()),
    ],
)
def test_advisory_ids_are_recoverable_from_issue_titles(title: str, expected: set) -> None:
    """Dedup reads the id back out of the title, so the format is the contract.

    ⚠️ This imports the REAL parser. An earlier version of this test reimplemented
    the same expression inline, which meant it asserted that my copy agreed with
    itself and could not fail if the script's parser changed.
    """
    sys.path.insert(0, str(SCRIPT.parent))
    import cve_report

    assert cve_report.advisory_ids_in(title) == expected


def test_a_title_this_script_writes_round_trips() -> None:
    """The end-to-end contract: what we file must be what dedup later recognises."""
    sys.path.insert(0, str(SCRIPT.parent))
    import cve_report

    f = {
        "id": "GHSA-8g6f-qw9x-4q6q",
        "pkg": "mcp",
        "installed": "1.0",
        "fixed": "2.0",
        "severity": "HIGH",
        "title": "t",
        "target": "img",
    }
    title = f"[Infra] {f['severity']} {f['id']} in {f['pkg']} ({f['installed']} -> {f['fixed']})"
    assert f["id"] in cve_report.advisory_ids_in(title), (
        "an advisory we file would not be recognised as already-tracked, so it "
        "would be re-filed on every scheduled run"
    )


def test_trivy_pin_matches_the_one_ci_already_proves_works() -> None:
    """The mistake this catches, made on the first dispatch of this workflow.

    `aquasecurity/trivy-action@0.33.1` — invented version, missing `v` prefix —
    failed at `Set up job` with `Unable to resolve action`. Every test in this
    file passed, because they validate the workflow's SHAPE and cannot know
    whether an action version exists on the marketplace.

    ci.yml's image-cve runs this action on every PR, so its pin is continuously
    proven to resolve. Matching it is the cheapest available guarantee.

    🚨 A wrong pin here is uniquely nasty: a scheduled workflow that fails at job
    setup produces a red tick nobody reads, and the watchdog only asks whether a
    schedule FIRED, never whether it succeeded — so it would report healthy
    forever. See core#1193.
    """
    import re

    pins: set[str] = set()
    for path in (REPO / ".github" / "workflows").glob("*.yml"):
        for m in re.finditer(
            r"^\s*uses:\s*(aquasecurity/trivy-action@\S+)",
            path.read_text(encoding="utf-8"),
            re.MULTILINE,
        ):
            pins.add(m.group(1))
    assert len(pins) == 1, (
        f"trivy-action is pinned inconsistently across workflows: {sorted(pins)}. "
        f"The scheduled scan must use the pin CI already proves resolves."
    )
