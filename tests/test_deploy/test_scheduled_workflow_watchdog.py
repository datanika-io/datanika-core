"""The scheduled-workflow watchdog's own logic, tested (core#628).

`scripts/check_scheduled_workflows.py` exists because a cron that stops firing
is **silent** — `daily-rebuild.yml` in datanika-landing sat in state
`disabled_inactivity` for 70 days and nothing said so.

Which makes the watchdog itself the highest-risk piece of the fix: if its
comparator returned "all fine" unconditionally, the job would go green every
morning and we would be exactly where we started, only now with a reassuring
tick. So every case below is written in **both** directions — it must flag the
faults that actually happened, and must not invent faults on a healthy repo.

The `TestAgainstTheRealWorkflows` class is the part that catches the failure
this design is most exposed to: `cron_period_seconds` models a deliberately
narrow set of cron shapes and returns `None` for anything else, and `None`
means *staleness is silently not checked*. A cron shape we cannot model is
therefore a hole, and the only way to know is to run the parser against the
crons we really ship.
"""

from __future__ import annotations

import base64
import importlib.util
import json
import re
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
import yaml

_REPO_ROOT = Path(__file__).resolve().parents[2]
_MODULE_PATH = _REPO_ROOT / "scripts" / "check_scheduled_workflows.py"
_WORKFLOW_DIR = _REPO_ROOT / ".github" / "workflows"
_WATCHDOG = _WORKFLOW_DIR / "scheduled-workflow-watchdog.yml"


def _load():
    name = "check_scheduled_workflows"
    spec = importlib.util.spec_from_file_location(name, _MODULE_PATH)
    module = importlib.util.module_from_spec(spec)
    # Register before exec: @dataclass resolves annotations via
    # sys.modules[cls.__module__], which is None for an unregistered module and
    # raises AttributeError rather than anything that names the real cause.
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


wd = _load()

NOW = datetime(2026, 8, 30, 16, 41, tzinfo=UTC)


def _wf(**kwargs):
    """A healthy daily workflow that fired two hours ago, unless overridden."""
    defaults = dict(
        repo="datanika-io/datanika-landing",
        name="Daily rebuild",
        path=".github/workflows/daily-rebuild.yml",
        state="active",
        created_at=NOW - timedelta(days=200),
        crons=["0 6 * * *"],
        last_schedule_run=NOW - timedelta(hours=2),
    )
    defaults.update(kwargs)
    return wd.WorkflowState(**defaults)


def test_the_script_exists_where_the_workflow_expects_it():
    """The workflow invokes this path by string; a rename would 404 silently."""
    assert _MODULE_PATH.is_file(), f"{_MODULE_PATH} is missing"
    invoked = _WATCHDOG.read_text(encoding="utf-8")
    assert "scripts/check_scheduled_workflows.py" in invoked


class TestCronPeriod:
    @pytest.mark.parametrize(
        "expr,expected",
        [
            ("0 6 * * *", wd.DAY),  # landing daily-rebuild
            ("30 6 * * *", wd.DAY),  # landing connector-count-parity
            ("41 16 * * *", wd.DAY),  # this watchdog
            ("0 3 * * 1", 7 * wd.DAY),
            ("15 4 * * MON", 7 * wd.DAY),
            ("0 * * * *", wd.HOUR),
            ("0 */6 * * *", 6 * wd.HOUR),
            ("*/15 * * * *", 15 * 60),
        ],
    )
    def test_models_the_shapes_we_use(self, expr, expected):
        assert wd.cron_period_seconds(expr) == expected

    @pytest.mark.parametrize(
        "expr",
        [
            "0 6 1 * *",  # day-of-month restriction
            "0 6 * 3 *",  # month restriction
            "0 6 * * 1,4",  # two days a week
            "* * * * *",  # every minute
            "0 6 * *",  # four fields
            "nonsense",
            "",
        ],
    )
    def test_returns_none_rather_than_guessing(self, expr):
        """`None` is honest. A wrong period is an unfalsifiable check."""
        assert wd.cron_period_seconds(expr) is None

    def test_an_unmodelled_cron_is_a_note_and_never_a_problem(self):
        problems, notes = wd.find_problems([_wf(crons=["0 6 1 * *"])], NOW)
        assert problems == []
        assert len(notes) == 1
        assert "NOT checked" in notes[0]


class TestTheFaultsThatActuallyHappened:
    def test_disabled_inactivity_is_flagged(self):
        """The exact state landing's daily-rebuild was found in."""
        problems, _ = wd.find_problems([_wf(state="disabled_inactivity")], NOW)
        assert len(problems) == 1
        assert "disabled_inactivity" in problems[0]

    def test_disabled_manually_is_flagged_too(self):
        problems, _ = wd.find_problems([_wf(state="disabled_manually")], NOW)
        assert len(problems) == 1

    def test_a_daily_cron_silent_for_seventy_days_is_flagged(self):
        """State can be `active` and the thing still not be running."""
        problems, _ = wd.find_problems([_wf(last_schedule_run=NOW - timedelta(days=70))], NOW)
        assert len(problems) == 1
        assert "70.0 days ago" in problems[0]
        assert "NOT the 60-day disable" in problems[0]

    def test_a_cron_that_never_registered_is_flagged(self):
        """Active, on the default branch, and has simply never fired."""
        problems, _ = wd.find_problems(
            [_wf(created_at=NOW - timedelta(days=7), last_schedule_run=None)], NOW
        )
        assert len(problems) == 1
        assert "never" in problems[0]
        assert "DEFAULT branch" in problems[0]


class TestItDoesNotInventFaults:
    def test_a_healthy_workflow_is_clean(self):
        problems, notes = wd.find_problems([_wf()], NOW)
        assert problems == []
        assert notes == []

    def test_a_brand_new_cron_is_not_yet_judgeable(self):
        """The real case: connector-count-parity, 15h old and not yet fired.

        Landed on `main` 2026-08-29, cron `30 6 * * *`, still zero scheduled
        runs the next morning. That is not a fault -- core's own nightlies were
        running ~6h late the same day. Flagging it would have sent someone to
        debug a schedule that was merely waiting its turn.
        """
        problems, notes = wd.find_problems(
            [_wf(created_at=NOW - timedelta(hours=15), last_schedule_run=None)], NOW
        )
        assert problems == []
        assert len(notes) == 1
        assert "Too new to judge" in notes[0]

    def test_one_missed_day_is_tolerated(self):
        """GitHub documents that queued jobs may be dropped under load."""
        problems, _ = wd.find_problems([_wf(last_schedule_run=NOW - timedelta(hours=30))], NOW)
        assert problems == []

    def test_two_missed_days_are_not(self):
        problems, _ = wd.find_problems([_wf(last_schedule_run=NOW - timedelta(hours=54))], NOW)
        assert len(problems) == 1

    def test_the_measured_github_skew_does_not_trip_it(self):
        """Real skew, measured 2026-08-30 across both repos.

        landing `0 6 * * *` fired 09:22Z-11:03Z (3h22m-5h03m late); core
        `0 3 * * *` fired 09:05Z and `30 3 * * *` fired 09:22Z, both ~6h late.
        A grace tighter than that would be a check that cannot pass -- the same
        fault as the alert family in core#604, pointing the other way.
        """
        fired_late = NOW.replace(hour=11, minute=3) - timedelta(days=1)
        problems, _ = wd.find_problems([_wf(last_schedule_run=fired_late)], NOW)
        assert problems == []
        assert wd.GRACE_SECONDS > 6 * wd.HOUR

    def test_the_grace_covers_the_skew_range_not_just_the_skew(self):
        """The gap between consecutive runs is what staleness actually sees.

        A healthy daily cron that fires 0h late one day and 12h late the next
        leaves a 36h gap. If the grace only covered the *worst* skew rather than
        its *spread*, that healthy pattern would page.
        """
        worst_observed_skew = 6 * wd.HOUR + 5 * 60
        assert 2 * worst_observed_skew <= wd.GRACE_SECONDS
        problems, _ = wd.find_problems([_wf(last_schedule_run=NOW - timedelta(hours=35))], NOW)
        assert problems == []

    def test_a_workflow_with_no_cron_is_ignored_entirely(self):
        problems, notes = wd.find_problems([_wf(crons=[])], NOW)
        assert problems == []
        assert notes == []


class TestAgainstTheRealWorkflows:
    """The parser must handle the crons we actually ship, not just the nice ones.

    `cron_period_seconds` returning `None` means staleness is silently not
    checked for that workflow. That is the one way this watchdog can go quietly
    blind, so it is asserted against the real files rather than against
    fixtures.
    """

    @staticmethod
    def _crons(path: Path) -> list[str]:
        doc = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        triggers = doc.get("on", doc.get(True)) or {}
        if not isinstance(triggers, dict):
            return []
        return [
            entry["cron"]
            for entry in (triggers.get("schedule") or [])
            if isinstance(entry, dict) and entry.get("cron")
        ]

    def test_every_cron_in_this_repo_can_be_modelled(self):
        unmodelled = {}
        for path in sorted(_WORKFLOW_DIR.glob("*.yml")):
            for cron in self._crons(path):
                if wd.cron_period_seconds(cron) is None:
                    unmodelled.setdefault(path.name, []).append(cron)
        assert not unmodelled, (
            "These crons parse to no period, so the watchdog will report them as "
            "`active` and never check whether they are still firing:\n"
            f"{unmodelled}\n\n"
            "Either widen cron_period_seconds() to model the shape, or change "
            "the cron. Do not leave it -- an unmodelled cron is exactly the "
            "silent failure core#628 exists to stop."
        )

    def test_the_watchdog_is_itself_scheduled_and_modelled(self):
        crons = self._crons(_WATCHDOG)
        assert crons, "the watchdog has no schedule of its own"
        assert all(wd.cron_period_seconds(c) is not None for c in crons)

    def test_the_watchdog_does_not_run_at_the_top_of_an_hour(self):
        """GitHub names the start of the hour as its high-load window."""
        for cron in self._crons(_WATCHDOG):
            minute = cron.split()[0]
            assert minute != "0", (
                f"{cron!r} fires at the top of the hour, the window GitHub "
                "documents as most delayed. Landing's `0 6 * * *` was measured "
                "3h22m-5h03m late every day."
            )

    def test_the_watchdog_watches_both_public_repos(self):
        body = _WATCHDOG.read_text(encoding="utf-8")
        for repo in ("datanika-io/datanika-core", "datanika-io/datanika-landing"):
            assert repo in body, f"{repo} is not being watched"

    def test_the_watchdog_can_write_issues(self):
        """It reports by filing an issue; without the scope it fails silently."""
        doc = yaml.safe_load(_WATCHDOG.read_text(encoding="utf-8"))
        assert doc["permissions"]["issues"] == "write"

    def test_the_reporting_step_runs_when_the_check_fails(self):
        """`continue-on-error` on the check, or the report step never runs."""
        body = _WATCHDOG.read_text(encoding="utf-8")
        assert "continue-on-error: true" in body
        assert re.search(r"steps\.check\.outcome\s*==\s*'failure'", body)


# ---------------------------------------------------------------------------
# core#691 -- the collection layer. Everything above this line tests the
# comparator, and the comparator was never the thing that broke.
# ---------------------------------------------------------------------------


# The exact API payload that killed the watchdog: core's real workflow list on
# 2026-09-02, ending in the entry GitHub synthesises for Dependabot. Ordering is
# load-bearing -- the synthesised entry is LAST, so every real workflow was
# collected before the crash and a total-count guard would have been satisfied.
_REAL_WORKFLOWS_PAYLOAD = {
    "workflows": [
        {
            "id": 1,
            "name": "CI",
            "path": ".github/workflows/ci.yml",
            "state": "active",
            "created_at": "2026-04-01T00:00:00Z",
        },
        {
            "id": 2,
            "name": "Nightly Connector Smoke",
            "path": ".github/workflows/nightly-connector-smoke.yml",
            "state": "active",
            "created_at": "2026-04-01T00:00:00Z",
        },
        {
            "id": 3,
            "name": "Scheduled workflow watchdog",
            "path": ".github/workflows/scheduled-workflow-watchdog.yml",
            "state": "active",
            "created_at": "2026-08-30T00:00:00Z",
        },
        # Not a file in the repository. `contents/dynamic/...` returns 404.
        {
            "id": 4,
            "name": "Dependabot Updates",
            "path": "dynamic/dependabot/update-graph",
            "state": "active",
            "created_at": "2026-08-31T00:00:00Z",
        },
        # core#982 -- a REAL `.github/workflows/` path whose file is on no branch.
        # GitHub keeps the record after the branch that carried it is deleted.
        # Observed live on 2026-09-03: `importtime-probe.yml`, listed `active`,
        # ABSENT from `git ls-tree` on both `master` and `dev`, from a throwaway
        # branch pushed and deleted the same session. It passes core#952's
        # whitelist and 404s anyway, which killed the run before any repo was
        # checked. Ordering is load-bearing here too: it sits after every healthy
        # workflow, so the crash it caused looked like a complete run.
        {
            "id": 5,
            "name": "importtime probe",
            "path": ".github/workflows/importtime-probe.yml",
            "state": "active",
            "created_at": "2026-09-02T00:00:00Z",
        },
    ]
}

# What the default branch actually contains. `importtime-probe.yml` is absent,
# which is the whole point -- the record exists and the file does not.
_DEFAULT_BRANCH_FILES = [
    ".github/workflows/ci.yml",
    ".github/workflows/nightly-connector-smoke.yml",
    ".github/workflows/scheduled-workflow-watchdog.yml",
]


def _fake_gh(
    *,
    missing: set[str] = frozenset(),
    listing: list[str] | None = None,
    schedule_runs: dict[str, list] | None = None,
):
    """A stand-in for `gh api` that 404s exactly like the real one.

    `missing` names `contents/` paths that do not exist; requesting one raises
    the same exception `subprocess.run(check=True)` raises, because that is what
    the real failure was.

    `listing` is what the default branch's `.github/workflows` directory contains
    (core#982). It is deliberately a SEPARATE input from `missing`: the two model
    different faults, and keeping them independent is what lets
    `test_a_404_on_a_real_workflow_file_is_still_fatal` keep its meaning — a file
    the listing says is present but which cannot be read is an anomaly, not an
    orphaned record.

    `schedule_runs` overrides the per-workflow-id schedule history; the default is
    one recent run for every workflow.
    """
    calls: list[str] = []
    files = _DEFAULT_BRANCH_FILES if listing is None else listing

    def fake(*args: str) -> str:
        endpoint = args[1] if len(args) > 1 else ""
        calls.append(endpoint)
        if endpoint.startswith("repos/") and endpoint.count("/") == 2:
            return "master\n"
        if endpoint.endswith("/actions/workflows"):
            return json.dumps(_REAL_WORKFLOWS_PAYLOAD)
        if "/contents/.github/workflows?" in endpoint:
            return "\n".join(files) + ("\n" if files else "")
        if "/contents/" in endpoint:
            path = endpoint.split("/contents/", 1)[1].split("?", 1)[0]
            # 🚨 A path absent from the listing ALSO 404s, because that is what
            # the real API does — the file is on no branch. Modelling the two
            # independently would make the core#982 control VACUOUS: removing the
            # fix would let `_parse_crons` happily return contents for a file that
            # does not exist, so the test could not reproduce the crash it exists
            # for. `missing` stays a separate input for the *other* fault: a file
            # the listing says is present that still cannot be read.
            if path in missing or (path.startswith(wd.WORKFLOW_DIR_PREFIX) and path not in files):
                raise subprocess.CalledProcessError(1, ["gh", *args])
            body = "on:\n  schedule:\n    - cron: '0 6 * * *'\n"
            return base64.b64encode(body.encode()).decode()
        if "/runs?event=schedule" in endpoint:
            wf_id = endpoint.split("/workflows/", 1)[1].split("/", 1)[0]
            if schedule_runs is not None and wf_id in schedule_runs:
                return json.dumps({"workflow_runs": schedule_runs[wf_id]})
            return json.dumps({"workflow_runs": [{"created_at": "2026-09-02T06:10:00Z"}]})
        raise AssertionError(f"unexpected gh call: {endpoint}")

    fake.calls = calls
    return fake


class TestCollectionSurvivesTheRealWorkflowList:
    """The regression that made this watchdog useless for three nights.

    GitHub lists workflows it synthesises (`dynamic/dependabot/update-graph`,
    `dynamic/pages/pages-build-deployment`) alongside real ones. They have no
    file in the repository, `contents/<path>` 404s, and `_gh` raises. Nothing
    here is a fixture invention -- the payload is core's real list.
    """

    def test_the_synthesised_path_is_not_a_repository_workflow(self):
        assert not wd.is_repository_workflow("dynamic/dependabot/update-graph")
        assert not wd.is_repository_workflow("dynamic/pages/pages-build-deployment")

    def test_real_workflow_paths_still_are(self):
        assert wd.is_repository_workflow(".github/workflows/ci.yml")
        assert wd.is_repository_workflow(".github/workflows/daily-rebuild.yml")

    def test_collect_completes_against_the_list_that_crashed_it(self, monkeypatch):
        """The whole bug, end to end. Before the fix this raised."""
        monkeypatch.setattr(wd, "_gh", _fake_gh(missing={"dynamic/dependabot/update-graph"}))
        found = wd.collect("datanika-io/datanika-core")
        assert [w.path for w in found if w.crons] == [
            ".github/workflows/ci.yml",
            ".github/workflows/nightly-connector-smoke.yml",
            ".github/workflows/scheduled-workflow-watchdog.yml",
        ]

    def test_it_never_even_asks_for_the_synthesised_file(self, monkeypatch):
        """Skipped by construction, not by swallowing the error it raises."""
        fake = _fake_gh(missing={"dynamic/dependabot/update-graph"})
        monkeypatch.setattr(wd, "_gh", fake)
        wd.collect("datanika-io/datanika-core")
        assert not [c for c in fake.calls if "dynamic/" in c], (
            "the fix must skip synthesised workflows before requesting their "
            "contents; asking and catching would also hide a real 404"
        )

    def test_a_404_on_a_real_workflow_file_is_still_fatal(self, monkeypatch):
        """The control that stops the fix becoming 'ignore all read errors'.

        If this ever passes-by-skipping, the watchdog will report a workflow as
        `active, all fine` having never read it -- the exact silent-success
        defect core#628 exists to detect, moved inside the detector.

        ⚠️ core#982 kept this intact on purpose. The file is present in the
        default branch listing and still 404s on read, which is an anomaly (a
        token scope, a race, an API change) and not an orphaned record. Only the
        listing decides "orphan"; the read failure keeps its original meaning.
        """
        monkeypatch.setattr(
            wd, "_gh", _fake_gh(missing={".github/workflows/nightly-connector-smoke.yml"})
        )
        with pytest.raises(subprocess.CalledProcessError):
            wd.collect("datanika-io/datanika-core")


class TestAWorkflowRecordWhoseFileIsOnNoBranch:
    """core#982 -- the third state, and the fifth night it would have cost.

    Two hours after core#952's fix reached `master`, a live rehearsal died on
    `importtime-probe.yml`: a workflow RECORD GitHub kept after the throwaway
    branch carrying its file was deleted. Its path is under `.github/workflows/`,
    so core#952's whitelist passes it -- correctly, because that whitelist
    discriminates on *synthesised vs repository*, and this is a repository path.
    The discriminator here is different: the file is simply not on the default
    branch.

    Believed as "core#952's fix does not work", this is a wasted session. It is a
    new instance of the same class, and the fix has to be a third state rather
    than a swallowed 404.

    ⚠️ The live half of the control is already measured -- the crash above was
    observed against the real repositories on 2026-09-03, not predicted. What
    needs proving here is that the code now handles it, which is what these
    exercise, through the real `collect()` and the real `find_problems()`.
    """

    NOW = datetime(2026, 9, 3, 12, 0, tzinfo=UTC)
    ORPHAN = ".github/workflows/importtime-probe.yml"

    def _collect(self, monkeypatch, schedule_runs=None):
        monkeypatch.setattr(
            wd,
            "_gh",
            _fake_gh(missing={"dynamic/dependabot/update-graph"}, schedule_runs=schedule_runs),
        )
        return wd.collect("datanika-io/datanika-core")

    def test_collection_completes_instead_of_crashing(self, monkeypatch):
        """Before the fix this raised CalledProcessError and checked nothing."""
        found = self._collect(monkeypatch, schedule_runs={"5": []})
        assert self.ORPHAN in [w.path for w in found], (
            "the orphaned record must be collected and reported, not skipped -- a "
            "silent skip is how a schedule that stopped firing disappears"
        )

    def test_it_is_never_counted_as_a_scheduled_workflow(self, monkeypatch):
        """It has no crons, so it must not vouch for the repo being watched.

        `main()` counts `w.crons` rather than `len(found)` for exactly this: a
        repo whose workflow files had all vanished would otherwise satisfy the
        per-repo emptiness guard with a page of findings about workflows that
        nothing is running.
        """
        found = self._collect(monkeypatch, schedule_runs={"5": []})
        orphan = next(w for w in found if w.path == self.ORPHAN)
        assert orphan.file_missing is True
        assert orphan.crons == []
        assert len([w for w in found if w.crons]) == 3

    def test_a_record_that_never_fired_is_a_note_naming_both_readings(self, monkeypatch):
        """Litter or a stopped schedule -- the API cannot tell, so do not guess."""
        found = self._collect(monkeypatch, schedule_runs={"5": []})
        problems, notes = wd.find_problems(found, self.NOW)
        assert not problems, f"an unfired orphan must not fail the run: {problems}"
        hit = [n for n in notes if self.ORPHAN in n]
        assert len(hit) == 1, f"expected exactly one note about the orphan, got {notes}"
        assert "(a)" in hit[0] and "(b)" in hit[0], (
            "the note must carry BOTH readings. Auto-classifying is worse than a "
            "noisy line, because reading (b) is the fault this watchdog exists for."
        )

    def test_a_record_that_did_fire_is_a_problem(self, monkeypatch):
        """The discriminator, and the half that earns the feature.

        A workflow that has run on a `schedule` event and whose file is now gone
        from the default branch is a schedule that has stopped. That is not
        litter, and it is precisely core#628's fault class arriving by a new
        route.
        """
        found = self._collect(monkeypatch)  # default: every workflow has a run
        problems, _notes = wd.find_problems(found, self.NOW)
        hit = [p for p in problems if self.ORPHAN in p]
        assert len(hit) == 1, f"expected the fired orphan to be a problem, got {problems}"
        assert "schedule that has stopped" in hit[0]

    def test_the_two_branches_disagree_on_the_same_record(self, monkeypatch):
        """Discrimination, not just two passing assertions.

        Both tests above could pass against a check that always returns whatever
        it was handed. This pins that the SAME workflow lands in different
        buckets purely on its schedule history.
        """
        never = self._collect(monkeypatch, schedule_runs={"5": []})
        fired = self._collect(monkeypatch)
        p_never, n_never = wd.find_problems(never, self.NOW)
        p_fired, n_fired = wd.find_problems(fired, self.NOW)
        assert not [p for p in p_never if self.ORPHAN in p]
        assert [n for n in n_never if self.ORPHAN in n]
        assert [p for p in p_fired if self.ORPHAN in p]
        assert not [n for n in n_fired if self.ORPHAN in n]

    def test_an_unreadable_workflow_directory_refuses_a_verdict(self, monkeypatch):
        """Anti-vacuity on the new call.

        An empty listing makes EVERY workflow look file-missing. That would turn
        one unreadable directory into a page of confident findings about healthy
        workflows -- a check whose own breakage produces the alarming answer,
        which is as useless as one that produces the reassuring one.
        """
        monkeypatch.setattr(
            wd, "_gh", _fake_gh(missing={"dynamic/dependabot/update-graph"}, listing=[])
        )
        with pytest.raises(RuntimeError, match="lists no files"):
            wd.collect("datanika-io/datanika-core")

    def test_the_listing_is_read_once_per_repo(self, monkeypatch):
        """One extra API call, not one per workflow."""
        fake = _fake_gh(missing={"dynamic/dependabot/update-graph"}, schedule_runs={"5": []})
        monkeypatch.setattr(wd, "_gh", fake)
        wd.collect("datanika-io/datanika-core")
        listings = [c for c in fake.calls if "/contents/.github/workflows?" in c]
        assert len(listings) == 1, f"expected one directory listing, got {listings}"

    def test_the_orphans_file_is_never_requested(self, monkeypatch):
        """Skipped by construction. Asking and catching would hide a real 404."""
        fake = _fake_gh(missing={"dynamic/dependabot/update-graph"}, schedule_runs={"5": []})
        monkeypatch.setattr(wd, "_gh", fake)
        wd.collect("datanika-io/datanika-core")
        assert not [c for c in fake.calls if "importtime-probe.yml" in c and "/contents/" in c]


class TestEmptinessIsCountedPerRepo:
    """Core is collected first; its four workflows must not vouch for landing."""

    @staticmethod
    def _fresh(repo):
        """Healthy relative to the real clock -- main() calls datetime.now()."""
        real_now = datetime.now(UTC)
        return _wf(
            repo=repo,
            created_at=real_now - timedelta(days=200),
            last_schedule_run=real_now - timedelta(hours=2),
        )

    def test_a_repo_contributing_nothing_is_an_error(self, monkeypatch, capsys):
        monkeypatch.setattr(
            wd, "collect", lambda repo: [self._fresh(repo)] if repo.endswith("core") else []
        )
        monkeypatch.setattr(
            sys, "argv", ["x", "datanika-io/datanika-core", "datanika-io/datanika-landing"]
        )
        assert wd.main() == 1
        out = capsys.readouterr().out
        named = [ln for ln in out.splitlines() if "No scheduled workflows found in" in ln]
        assert len(named) == 1, f"expected exactly one repo named, got {named}"
        assert "datanika-io/datanika-landing" in named[0]

    def test_a_healthy_two_repo_run_reports_both_counts(self, monkeypatch, capsys):
        monkeypatch.setattr(wd, "collect", lambda repo: [self._fresh(repo)])
        monkeypatch.setattr(
            sys, "argv", ["x", "datanika-io/datanika-core", "datanika-io/datanika-landing"]
        )
        assert wd.main() == 0
        out = capsys.readouterr().out
        assert "datanika-io/datanika-core=1" in out
        assert "datanika-io/datanika-landing=1" in out


class TestTheWatchdogReportsItsOwnBreakage:
    """A crash and a finding must not be the same signal.

    Filing a finding ends in `exit 1`, so a red scheduled run is the *designed*
    outcome of the watchdog working. That camouflaged three nights of it being
    dead. The crash path therefore has to reach the issue tracker too, under a
    title that is distinguishable at a glance.
    """

    # Asserted as a CONSTRUCT (`--title "<x>"`), never as the bare token, and
    # never with the surrounding indentation baked in. A guard pinned to layout
    # goes red on a reindent, and a guard pinned to a bare word goes red on the
    # comment that explains the hazard -- both teach people to delete the thing
    # that made the file readable.
    _BROKEN_TITLE = "Scheduled workflow watchdog is failing to run"
    _FINDING_TITLE = "Scheduled workflow stopped firing"

    def test_the_no_problems_branch_opens_a_differently_titled_issue(self):
        body = _WATCHDOG.read_text(encoding="utf-8")
        assert re.search(rf'--title\s+"{re.escape(self._BROKEN_TITLE)}"', body), (
            "the crash path must file an issue, not just print ::error:: lines "
            "into a log nobody reads"
        )

    def test_the_two_titles_are_distinct(self):
        body = _WATCHDOG.read_text(encoding="utf-8")
        assert re.search(rf'--title\s+"{re.escape(self._FINDING_TITLE)}"', body)
        assert self._BROKEN_TITLE != self._FINDING_TITLE
        # Once to dedupe against, once to create with.
        assert body.count(self._BROKEN_TITLE) >= 2, (
            "expected both the dedupe search and the create to use this title"
        )

    def test_the_breakage_report_says_nothing_was_verified(self):
        """Otherwise a reader takes 'no findings issue' as 'crons are healthy'."""
        body = _WATCHDOG.read_text(encoding="utf-8")
        assert "nothing was verified" in body
        assert "the detector is down" in body
