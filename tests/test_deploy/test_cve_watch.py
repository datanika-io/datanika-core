"""core#1166 — the scheduled CVE scan, and the five traps its own issue listed.

Each test below corresponds to a trap recorded on the issue before implementation.
They are here because every one of them fails *silently* in production: a schedule
that never fires, a token outage that reads as green, an issue-filer that cannot
file. None would turn anything red on its own.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import urllib.parse
from pathlib import Path

import pytest
import yaml

REPO = Path(__file__).resolve().parents[2]
WF = REPO / ".github" / "workflows" / "cve-watch.yml"
SCRIPT = REPO / ".github" / "scripts" / "cve_report.py"

#: Founder, 2026-09-17 (refs #1166): findings are filed into the private tracker.
PRIVATE_TRACKER = "datanika-io/datanika-cloud"
LABELS = ("core", "security scan")


def _wf() -> dict:
    return yaml.safe_load(WF.read_text(encoding="utf-8"))


def _on(doc: dict) -> dict:
    # PyYAML parses a bare `on:` key as the boolean True.
    return doc.get("on") or doc.get(True) or {}


def _steps() -> list[dict]:
    return _wf()["jobs"]["scan"]["steps"]


def _filing_steps() -> list[dict]:
    """Every step that runs the filer, dry or not -- never the reach check."""
    return [
        s
        for s in _steps()
        if "cve_report.py" in str(s.get("run", "")) and "--check-reach" not in str(s.get("run", ""))
    ]


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


def test_the_filer_holds_a_token_that_can_reach_the_tracker_it_files_into() -> None:
    """Repointed, not deleted (WORKFLOW_RULES §5a), 2026-09-17.

    This test used to assert `permissions: issues: write`. The invariant was never that
    line: it is that the filer can file where it files. While the target was this
    repository, the job's own token with `issues: write` was how. The target is now the
    private tracker, and a job's own `GITHUB_TOKEN` cannot reach another repository at all
    -- a filer still holding it would fail on the first advisory, and on a clean day it
    would fail on nothing and read green.
    """
    steps = _filing_steps()
    assert steps, "no step runs the filer"
    for step in steps:
        token = str((step.get("env") or {}).get("CVE_FILING_TOKEN", ""))
        assert "secrets." in token, f"{step.get('name')}: the filer's token is not a secret"
        assert "GITHUB_TOKEN" not in token, (
            f"{step.get('name')}: the job's own GITHUB_TOKEN cannot reach {PRIVATE_TRACKER}"
        )


def test_findings_are_filed_into_the_private_tracker_not_this_public_repo() -> None:
    """Founder, 2026-09-17: findings go to the private tracker; the daily scan keeps running."""
    for step in _filing_steps():
        run = str(step["run"])
        assert f"--repo {PRIVATE_TRACKER}" in run, f"{step.get('name')} files somewhere else"
        assert "github.repository" not in run, f"{step.get('name')} still targets this repo"


def test_filed_issues_carry_both_labels() -> None:
    for step in _filing_steps():
        run = f"{step['run']} "
        for label in LABELS:
            assert f'--label "{label}"' in run or f"--label {label} " in run, (
                f"{step.get('name')} does not apply the `{label}` label"
            )


REACH_WF = REPO / ".github" / "workflows" / "cve-watch-reach.yml"


def test_the_filer_workflow_never_runs_on_a_pull_request() -> None:
    """A pull request must never be able to file an issue."""
    triggers = _on(_wf())
    for event in ("pull_request", "pull_request_target"):
        assert event not in triggers, f"cve-watch.yml runs on {event}, so a PR could file"


def test_reach_is_proven_on_a_pull_request_by_a_workflow_the_pause_does_not_disable() -> None:
    """🔴 Measured on PR #1418, 2026-09-17: a `disabled_manually` workflow runs on NO trigger.

    The first version of this change put a `pull_request` trigger on cve-watch.yml itself, to
    prove the filing token's reach before merging. The PR produced no CVE-watch run at all,
    because the workflow is disabled, and enabling it to get the reading would have re-armed
    the default branch's PUBLIC filer on its schedule. So the proof lives in its own workflow,
    which the pause does not cover, and it must prove the same thing the filer depends on: the
    same token, the same tracker and the same labels.
    """
    assert REACH_WF.is_file(), "cve-watch-reach.yml is missing"
    doc = yaml.safe_load(REACH_WF.read_text(encoding="utf-8"))
    triggers = _on(doc)
    assert "schedule" not in triggers, "the reach proof must not become a second schedule"
    paths = set((triggers.get("pull_request") or {}).get("paths") or [])
    for needed in (
        ".github/workflows/cve-watch.yml",
        ".github/workflows/cve-watch-reach.yml",
        ".github/scripts/cve_report.py",
    ):
        assert needed in paths, f"a change to {needed} does not re-prove the reach"

    steps = [s for job in doc["jobs"].values() for s in job.get("steps", [])]
    runs = [s for s in steps if "cve_report.py" in str(s.get("run", ""))]
    assert runs, "the reach workflow never runs cve_report.py"
    for step in runs:
        run = str(step["run"])
        assert "--check-reach" in run, f"{step.get('name')} runs the FILER on a pull request"
        assert f"--repo {PRIVATE_TRACKER}" in run
        for label in LABELS:
            assert f'--label "{label}"' in run or f"--label {label} " in f"{run} "

    filer_token = {(s.get("env") or {}).get("CVE_FILING_TOKEN") for s in _filing_steps()}
    reach_token = {(s.get("env") or {}).get("CVE_FILING_TOKEN") for s in runs}
    assert len(filer_token) == 1 and filer_token == reach_token, (
        f"the PR proves {reach_token} but the scheduled run files with {filer_token}"
    )


def test_the_filing_tokens_reach_is_checked_before_the_image_is_built() -> None:
    """A token that lost its reach must fail in seconds, not after a ten-minute build.

    And on a clean day there is nothing to file, so without this a token that can no
    longer write reads green until the first advisory it cannot file.
    """
    steps = _steps()
    reach = [i for i, s in enumerate(steps) if "--check-reach" in str(s.get("run", ""))]
    build = [i for i, s in enumerate(steps) if "build-push-action" in str(s.get("uses", ""))]
    assert reach and build, "no reach check, or no build"
    assert reach[0] < build[0], "the reach check runs after the build"
    reach_token = (steps[reach[0]].get("env") or {}).get("CVE_FILING_TOKEN")
    for step in _filing_steps():
        assert (step.get("env") or {}).get("CVE_FILING_TOKEN") == reach_token, (
            "the reach check proves a different token from the one that files"
        )


def test_a_missing_cloud_token_fails_rather_than_scanning_nothing() -> None:
    """Trap 4: image-cve warns and continues; a scheduled run must not.

    On a PR from a fork, warning is right — nothing can be built and the PR is not
    at fault. On the default branch the token is always supposed to be there, so
    its absence is a real fault and a green would be a scan of nothing.
    """
    guard = next(s for s in _steps() if s.get("id") == "token")
    body = str(guard.get("run", ""))
    assert "exit 1" in body, "a token outage must fail, not warn-and-continue"


def test_it_asserts_the_report_exists_before_believing_it_is_clean() -> None:
    """An empty report is indistinguishable from a clean scan."""
    runs = " ".join(str(s.get("run", "")) for s in _steps())
    assert "is NOT a clean scan" in runs


def test_the_scan_is_fixed_only_and_critical_high() -> None:
    """Volume was measured before this was built: the unfixed set never gates."""
    step = next(s for s in _steps() if "trivy-action" in str(s.get("uses", "")))
    with_ = step["with"]
    assert with_["severity"] == "CRITICAL,HIGH"
    assert with_["ignore-unfixed"] is True


def test_it_does_not_go_red_on_a_finding() -> None:
    """The delivery is an issue. A red tick here would page nobody and mute fast."""
    step = next(s for s in _steps() if "trivy-action" in str(s.get("uses", "")))
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


def _run(report: dict, tmp_path: Path, *extra: str) -> str:
    p = tmp_path / "trivy.json"
    p.write_text(json.dumps(report), encoding="utf-8")
    env = {k: v for k, v in os.environ.items() if k != "CVE_FILING_TOKEN"}
    res = subprocess.run(
        [sys.executable, str(SCRIPT), "--report", str(p), "--repo", "o/r", "--dry-run", *extra],
        capture_output=True,
        text=True,
        check=False,
        env=env,
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


# --------------------------------------------------------------------------
# The private tracker (founder, 2026-09-17, refs #1166)
# --------------------------------------------------------------------------


def _mod():
    sys.path.insert(0, str(SCRIPT.parent))
    import cve_report

    return cve_report


class _FakeTracker:
    """Answers the calls the reach check makes, and records them."""

    def __init__(self, private=True, labels=LABELS, write_status=200, read_status=200):
        self.private = private
        self.labels = labels
        self.write_status = write_status
        self.read_status = read_status
        self.calls: list[tuple] = []

    def __call__(self, url, token, method="GET", body=None):
        self.calls.append((method, url, body))
        if "/labels/" in url:
            name = urllib.parse.unquote(url.rsplit("/labels/", 1)[1])
            if name not in self.labels:
                return 404, {}, {"message": "Not Found"}
            if method == "GET":
                return 200, {}, {"name": name, "color": "abcdef", "description": "d"}
            if self.write_status != 200:
                return self.write_status, {"X-Accepted-GitHub-Permissions": "issues=write"}, {}
            return 200, {}, {"name": name}
        if url.endswith("/repos/o/private") and method == "GET":
            return self.read_status, {}, {"private": self.private}
        raise AssertionError(f"unexpected call: {method} {url}")


def _reach(**fake):
    return _mod().check_reach("o/private", list(LABELS), "t", request=_FakeTracker(**fake))


def test_reach_passes_on_a_private_tracker_with_both_labels_and_write_access() -> None:
    fake = _FakeTracker()
    assert _mod().check_reach("o/private", list(LABELS), "t", request=fake) == []
    patches = [c for c in fake.calls if c[0] == "PATCH"]
    assert len(patches) == 1, "write reach is proven by exactly one no-op write"
    assert patches[0][2] == {"color": "abcdef", "description": "d"}, (
        "the write probe must re-send the label's own values, so it changes nothing"
    )


def test_reach_refuses_a_public_tracker() -> None:
    """Filing into a public tracker is the exact thing this change exists to stop."""
    problems = _reach(private=False)
    assert any("not private" in p for p in problems), problems


def test_reach_refuses_an_unreadable_tracker() -> None:
    problems = _reach(read_status=404)
    assert any("cannot read" in p for p in problems), problems


def test_reach_refuses_a_missing_label() -> None:
    problems = _reach(labels=("core",))
    assert any("security scan" in p for p in problems), problems


def test_reach_refuses_a_token_that_can_read_but_not_write() -> None:
    problems = _reach(write_status=403)
    assert any("cannot write" in p for p in problems), problems
    assert any("issues=write" in p for p in problems), "say what GitHub reports the token lacks"


def test_dedupe_reads_every_tracker_and_ignores_pull_requests(monkeypatch) -> None:
    """The existing public issues stay open (founder, 2026-09-17), so they still count.

    A pull request is not a tracking issue: a bump PR naming the id in its title must not
    stop the finding from being filed.
    """
    mod = _mod()
    pages = {
        "a/private": [{"title": "[Infra] HIGH CVE-1 in x (1 -> 2)"}],
        "b/public": [
            {"title": "[Infra] HIGH CVE-2 in y (1 -> 2)"},
            {"title": "[Engineering] Bump z for CVE-3", "pull_request": {"url": "u"}},
        ],
    }

    def fake_req(url, token, method="GET", body=None):
        for repo, items in pages.items():
            if f"/repos/{repo}/issues" in url:
                return items if "page=1" in url else []
        raise AssertionError(url)

    monkeypatch.setattr(mod, "_req", fake_req)
    assert mod.known_advisories(["a/private", "b/public"], "t") == {"CVE-1", "CVE-2"}


def test_the_body_names_core_issues_so_they_resolve_from_another_repository() -> None:
    body = _mod().body_for(
        {
            "id": "CVE-1",
            "pkg": "p",
            "installed": "1",
            "fixed": "2",
            "severity": "HIGH",
            "title": "t",
            "target": "img",
        }
    )
    assert re.findall(r"(?<![\w/.-])core#\d+", body) == [], "a bare core#N resolves to nothing"
    assert "datanika-io/datanika-core#1166" in body


def test_a_dry_run_shows_the_labels_it_would_apply(tmp_path: Path) -> None:
    out = _run(_report([_v("CVE-7")]), tmp_path, "--label", "core", "--label", "security scan")
    assert "would file: [Infra] HIGH CVE-7 in pkg" in out
    assert "labels: core, security scan" in out


def test_no_token_is_a_failure_not_a_silent_skip(tmp_path: Path) -> None:
    p = tmp_path / "trivy.json"
    p.write_text(json.dumps(_report([_v("CVE-8")])), encoding="utf-8")
    env = {k: v for k, v in os.environ.items() if k != "CVE_FILING_TOKEN"}
    res = subprocess.run(
        [sys.executable, str(SCRIPT), "--report", str(p), "--repo", "o/r"],
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )
    assert res.returncode == 1, (res.returncode, res.stderr)
    assert "CVE_FILING_TOKEN" in res.stderr, res.stderr
