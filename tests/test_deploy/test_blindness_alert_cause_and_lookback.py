"""The E2E blindness alert: say WHY a run carried no reading; read every push since the last look.

Three defects in the detector `scheduled-workflow-watchdog.yml` runs daily, all found on 2026-09-17
by reading its first real blackout (eight `e2e-staging` runs that died in the seed, core#1437):

* **core#1447 — one cause for every non-reading.** The reader collapsed each run's own classifier
  token (`wrong_build`, `no_verdict`, `cancelled`, ...) into the class `UNMEASURED` and then printed
  core#876's cause — *"a newer push had redeployed staging out from under it — the guard working"* —
  for all of them. All eight runs had been on the right build and printed `verdict: no_verdict`. The
  sentence it chose is the one that asks nobody to act, and the right response was the opposite:
  fix the harness.
* **core#1448 — a count-sized look-back on a daily cadence.** The reader asked for the 40 newest
  push runs on `dev`. A push starts two push workflows, so that is about 20 pushes, and on 09-16/17
  there were 23 between two detector runs: `ba7289b`, `4a7830a` and `6bbdc91` were read by no run,
  ever, and the output could not say so.
* **core#1273 AC2/AC3 — the permission had no guard.** `actions: read` on the job is what lets the
  reader download a job log, and nothing failed when it was absent: that is how the detector ran
  for days without ever reading one.

The GitHub layer is faked at the reader's own seam (`_gh`, `_gh_log_or_none`), so the tests drive
`main()` exactly as the workflow does, on logs shaped like the real ones.
"""

from __future__ import annotations

import copy
import re
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "scripts"))

import scripts.e2e_tier_streak as streak  # noqa: E402

WATCHDOG = REPO_ROOT / ".github" / "workflows" / "scheduled-workflow-watchdog.yml"
T0 = datetime(2026, 9, 17, 8, 0, tzinfo=UTC)
#: The core#876 sentence. Pinned only where it MUST appear (the control), and where a run that
#: was on the right build must not be told it was not.
CORE_876_CLAIM = "redeployed staging out from under it"


def _iso(t: datetime) -> str:
    return t.strftime("%Y-%m-%dT%H:%M:%SZ")


def _staging_log(info: str, gating: str) -> str:
    """The two executed classifier lines of a real `staging / e2e-staging` log, stamp included."""
    return (
        f"2026-09-17T08:23:59.1030364Z INFORMATIONAL_RESULT={info}\n"
        "2026-09-17T08:24:03.0218611Z gating step outcome: failure / job status: failure / "
        f"verdict: {gating}\n"
    )


class FakeActions:
    """`actions/runs`, `.../jobs` and job logs, served the way the reader asks for them.

    Every commit gets TWO push runs, `CI` and `Build & push image`, because that is what a push to
    `dev` starts. The pair is what halves a count-sized page (core#1448), so a fake with one run
    per commit would hide the defect.
    """

    def __init__(self, commits: list[tuple[datetime, str, str, str]]) -> None:
        self.runs: list[dict] = []
        self.logs: dict[int, str] = {}
        self.list_calls: list[dict[str, str]] = []
        for i, (created, sha, info, gating) in enumerate(commits):
            ci = 1000 + 2 * i
            common = {
                "status": "completed",
                "created_at": _iso(created),
                "head_sha": sha.ljust(40, "0"),
            }
            self.runs.append({"id": ci, "path": ".github/workflows/ci.yml", **common})
            self.runs.append(
                {"id": ci + 1, "path": ".github/workflows/build-push-image.yml", **common}
            )
            self.logs[ci * 10] = _staging_log(info, gating)
        self.runs.sort(
            key=lambda r: (r["created_at"], r["id"]), reverse=True
        )  # newest first, as the API

    def gh(self, path: str, *, raw: bool = False) -> object:
        url = urlsplit(path)
        q = {k: v[0] for k, v in parse_qs(url.query).items()}
        jobs = re.fullmatch(r"repos/[^/]+/[^/]+/actions/runs/(\d+)/jobs", url.path)
        if jobs:
            run_id = int(jobs.group(1))
            job = {"id": run_id * 10, "name": "staging / e2e-staging", "status": "completed"}
            return {"jobs": [{**job, "conclusion": "failure"}]}
        if url.path.endswith("/actions/runs"):
            self.list_calls.append(q)
            runs = self.runs
            if "created" in q:
                runs = [r for r in runs if r["created_at"] >= q["created"].removeprefix(">=")]
            per_page, page = int(q.get("per_page", 30)), int(q.get("page", 1))
            return {"workflow_runs": runs[(page - 1) * per_page : page * per_page]}
        raise AssertionError(f"the reader made a call this fake does not serve: {path}")

    def log(self, repo: str, job_id: int) -> tuple[str | None, str]:
        return (self.logs[job_id], "") if job_id in self.logs else (None, "HTTP 404")


def _commits(*blocks: tuple[int, str, str]) -> list[tuple[datetime, str, str, str]]:
    """`(count, informational token, gating token)` blocks, 20 minutes apart, oldest first."""
    out, t, n = [], T0, 0
    for count, info, gating in blocks:
        for _ in range(count):
            out.append((t, f"{n:08x}", info, gating))
            t += timedelta(minutes=20)
            n += 1
    return out


def _main(monkeypatch, capsys, fake: FakeActions, *argv: str) -> tuple[int, str]:
    monkeypatch.setattr(streak, "_gh", fake.gh)
    monkeypatch.setattr(streak, "_gh_log_or_none", fake.log)
    code = streak.main(["--job", "e2e-staging", "--branch", "dev", "--max-unmeasured", "3", *argv])
    return code, capsys.readouterr().out


#: The 2026-09-17 blackout, reduced to its shape: the informational tier's usual red either side,
#: and eight runs whose informational step never ran because the gating step produced no verdict.
SEED_DEATH = _commits(
    (2, "failure", "clean"), (8, "unknown", "no_verdict"), (2, "failure", "clean")
)


# ── core#1447: the cause is the run's own ─────────────────────────────────────────────────────────


class TestTheCauseIsTheRunsOwn:
    def test_the_seed_death_window_names_the_harness_not_core_876(
        self, monkeypatch, capsys
    ) -> None:
        code, out = _main(monkeypatch, capsys, FakeActions(SEED_DEATH), "--runs", "40")
        assert code == 2, f"eight runs in a row carried no reading and nothing fired:\n{out}"
        assert "no_verdict" in out, f"the runs' own token never reached the alert:\n{out}"
        assert "fix the harness" in out, f"the alert does not say what these runs need:\n{out}"
        assert CORE_876_CLAIM not in out, (
            "runs that were on the right build were told staging ran another commit, and that "
            f"the guard was working (core#1447):\n{out}"
        )

    def test_each_unmeasured_run_carries_its_token(self, monkeypatch, capsys) -> None:
        _code, out = _main(monkeypatch, capsys, FakeActions(SEED_DEATH), "--runs", "40")
        tagged = [
            ln
            for ln in out.splitlines()
            if ln.rstrip().endswith("UNMEASURED (unknown, gating=no_verdict)")
        ]
        assert len(tagged) == 8, f"expected 8 tagged history lines, got {len(tagged)}:\n{out}"

    def test_control_a_wrong_build_stretch_still_names_core_876(self, monkeypatch, capsys) -> None:
        """The sentence is right for the case it was written for, and must survive the fix."""
        fake = FakeActions(
            _commits(
                (1, "failure", "clean"), (4, "unknown", "wrong_build"), (1, "failure", "clean")
            )
        )
        code, out = _main(monkeypatch, capsys, fake, "--runs", "40")
        assert code == 2
        assert "core#876" in out and CORE_876_CLAIM in out, out

    def test_a_mixed_stretch_names_both_causes(self, monkeypatch, capsys) -> None:
        fake = FakeActions(
            _commits(
                (1, "failure", "clean"), (2, "unknown", "no_verdict"), (2, "unknown", "wrong_build")
            )
        )
        code, out = _main(monkeypatch, capsys, fake, "--runs", "40")
        assert code == 2
        assert "fix the harness" in out and CORE_876_CLAIM in out, out

    def test_every_unmeasured_token_has_a_recorded_cause(self) -> None:
        """Derived from the vocabularies, so a new non-reading token cannot arrive unexplained.

        `gating_failed` is here too: an informational `unknown` defers to the gating token beside
        it, and a failed gating step is one of the reasons the informational step never runs.
        """
        tokens = {t for t, c in streak.VERDICT_CLASS.items() if c == streak.UNMEASURED}
        assert tokens >= {"wrong_build", "cancelled", "no_verdict", "unknown", "empty"}, tokens
        missing = sorted((tokens | {"gating_failed"}) - set(streak.UNMEASURED_CAUSES))
        assert not missing, f"tokens with no recorded cause: {missing}"

    def test_a_token_with_no_gating_beside_it_is_explained_by_itself(self) -> None:
        """The SSO tier's shape: `no_verdict` is the tier's own token, with no gating deferral."""
        run = streak.RunReading("2026-09-17T08:00:00Z", "5500aa00", streak.UNMEASURED, "no_verdict")
        text = "\n".join(streak.explain_blindness([[run, run, run]]))
        assert "3 x no_verdict" in text and "fix the harness" in text, text
        assert CORE_876_CLAIM not in text, text

    def test_an_unrecognised_token_is_named_rather_than_explained_away(self) -> None:
        run = streak.RunReading("2026-09-17T08:00:00Z", "deadbeef", streak.UNMEASURED, "brand_new")
        text = "\n".join(streak.explain_blindness([[run, run, run]]))
        assert "brand_new" in text, text


# ── core#1448: read every push since the last look ────────────────────────────────────────────────

#: 23 commits = 46 push runs: the count 09-16 19:41Z -> 09-17 13:39Z actually produced.
BUSY_DAY = _commits((23, "failure", "clean"))


class TestTheLookBackReachesThePreviousLook:
    def test_control_a_count_page_leaves_a_busy_days_earliest_pushes_unread(
        self, monkeypatch, capsys
    ) -> None:
        """What the watchdog used to ask for, kept as the documented shape of the defect.

        🔴 **Repointed 2026-09-25 (core#1567), WORKFLOW_RULES §5a.** This pinned the literal
        ``20`` and went red on a correct change: count mode now drops the boundary tie group of a
        FULL page, because which member of that group falls inside a ``per_page`` cut is not a
        function of the reader's arguments. So a 40-run page over this 46-run day reads **19**
        pushes rather than 20.
        The **invariant** is what this control has always been for and it is unchanged: a count
        page reads strictly fewer pushes than the day produced, while ``--since`` reads them all.
        Asserting that first means the guard survives the next honest change to the arithmetic;
        the exact figure is kept after it, because a silent jump to 23 would mean the page had
        quietly stopped truncating.
        """
        _code, out = _main(monkeypatch, capsys, FakeActions(BUSY_DAY), "--runs", "40")
        read = int(re.search(r"^runs read      : (\d+) ", out, re.M).group(1))
        assert read < 23, f"a 40-run page cannot cover a 23-push day, yet it read {read}:\n{out}"
        assert read == 19, f"40 runs = 20 pushes, less the cut boundary tie group; got {read}"

    def test_since_reads_every_push_since_the_previous_look(self, monkeypatch, capsys) -> None:
        _code, out = _main(monkeypatch, capsys, FakeActions(BUSY_DAY), "--since", _iso(T0))
        assert "runs read      : 23 " in out, f"a push since the last look was left unread:\n{out}"

    def test_since_pages_past_one_hundred_runs(self, monkeypatch, capsys) -> None:
        fake = FakeActions(_commits((70, "failure", "clean")))
        _code, out = _main(monkeypatch, capsys, fake, "--since", _iso(T0))
        assert "runs read      : 70 " in out, out
        assert len(fake.list_calls) >= 2, f"140 runs cannot arrive in one page: {fake.list_calls}"

    def test_the_output_says_how_far_back_it_read(self, monkeypatch, capsys) -> None:
        _code, out = _main(monkeypatch, capsys, FakeActions(BUSY_DAY), "--since", _iso(T0))
        assert f"since {_iso(T0)}" in out, out
        assert f"oldest run read: {_iso(T0)}" in out, out

    def test_a_stretch_already_reported_does_not_alert_again(
        self, monkeypatch, capsys, tmp_path
    ) -> None:
        """A 48-hour look-back sees each blackout on two consecutive days. Without this, every
        blackout is reported twice, and an alert that repeats itself gets ignored."""
        code1, out1 = _main(monkeypatch, capsys, FakeActions(SEED_DEATH), "--since", _iso(T0))
        assert code1 == 2 and "BLIND-STRETCH " in out1, out1
        reported = tmp_path / "reported.txt"
        reported.write_text(out1, encoding="utf-8")
        code2, out2 = _main(
            monkeypatch,
            capsys,
            FakeActions(SEED_DEATH),
            "--since",
            _iso(T0),
            "--already-reported",
            str(reported),
        )
        assert code2 != 2, f"the same stretch alerted twice:\n{out2}"
        assert "already reported" in out2, out2

    def test_control_a_new_stretch_alerts_even_after_an_old_one_was_reported(
        self, monkeypatch, capsys, tmp_path
    ) -> None:
        _code, first = _main(monkeypatch, capsys, FakeActions(SEED_DEATH), "--since", _iso(T0))
        reported = tmp_path / "reported.txt"
        reported.write_text(first, encoding="utf-8")
        later = SEED_DEATH + [
            (T0 + timedelta(hours=10, minutes=20 * i), f"f{i:07x}", "unknown", "wrong_build")
            for i in range(3)
        ]
        code, out = _main(
            monkeypatch,
            capsys,
            FakeActions(later),
            "--since",
            _iso(T0),
            "--already-reported",
            str(reported),
        )
        assert code == 2, (
            f"a new blackout was swallowed because an older one had been reported:\n{out}"
        )
        assert "f0000000..f0000002" in out, out


# ── the watchdog wiring ───────────────────────────────────────────────────────────────────────


def _strip_comments(shell: str) -> str:
    return "\n".join(line for line in shell.splitlines() if not line.lstrip().startswith("#"))


def _doc() -> dict:
    return yaml.safe_load(WATCHDOG.read_text(encoding="utf-8"))


def _gap_shell() -> str:
    steps = _doc()["jobs"]["e2e-measurement-gap"]["steps"]
    return _strip_comments(str(next(s for s in steps if s.get("id") == "gap").get("run", "")))


class TestTheWatchdogAsksTheRightQuestion:
    def test_it_reads_by_time_not_by_count(self) -> None:
        body = _gap_shell()
        assert "--since" in body and "hours ago" in body, body

    def test_it_hands_the_reader_what_it_already_reported(self) -> None:
        body = _gap_shell()
        assert "--already-reported" in body and "gh issue view" in body, body

    def test_the_issue_text_no_longer_asserts_one_cause(self) -> None:
        steps = _doc()["jobs"]["e2e-measurement-gap"]["steps"]
        filing = _strip_comments(
            str(next(s for s in steps if str(s.get("name", "")).startswith("File an issue"))["run"])
        )
        assert "core#1447" in filing, (
            f"the filed text does not point at the per-run causes:\n{filing}"
        )


# ── core#1273 AC2/AC3: the job holds the permission its script needs ──────────────────────────────

#: The job-log endpoint as a script spells it. Derived from the scripts, never a hardcoded name,
#: so the next script that downloads a log is covered by being written.
LOG_ENDPOINT = re.compile(r"actions/jobs/[^\"'\s]*/logs")


def log_reading_scripts() -> set[str]:
    return {
        p.name
        for p in (REPO_ROOT / "scripts").glob("*.py")
        if LOG_ENDPOINT.search(p.read_text(encoding="utf-8"))
    }


def actions_read_violations(doc: dict, scripts: set[str]) -> tuple[list[str], int]:
    """`(violations, jobs examined)`. A job that runs a log-reading script must grant `actions`."""
    top = doc.get("permissions")
    violations, examined = [], 0
    for name, job in (doc.get("jobs") or {}).items():
        shell = " ".join(_strip_comments(str(s.get("run", ""))) for s in job.get("steps") or [])
        used = sorted(s for s in scripts if re.search(rf"scripts/{re.escape(s)}(?![\w.])", shell))
        if not used:
            continue
        examined += 1
        perms = job.get("permissions", top)
        granted = perms in ("read-all", "write-all") or (
            isinstance(perms, dict) and perms.get("actions") in ("read", "write")
        )
        if not granted:
            violations.append(f"job `{name}` runs {used} without `actions: read`")
    return violations, examined


class TestTheJobCanReadWhatItsScriptReads:
    def test_every_job_running_a_log_reading_script_may_read_actions(self) -> None:
        scripts = log_reading_scripts()
        assert "e2e_tier_streak.py" in scripts, (
            f"the scan no longer finds the known reader: {scripts}"
        )
        violations, examined = [], 0
        for wf in sorted((REPO_ROOT / ".github" / "workflows").glob("*.yml")):
            v, n = actions_read_violations(yaml.safe_load(wf.read_text(encoding="utf-8")), scripts)
            violations += [f"{wf.name}: {x}" for x in v]
            examined += n
        assert examined >= 1, (
            "no workflow job runs a log-reading script: the guard examined nothing"
        )
        assert not violations, "\n".join(violations)

    def test_control_the_guard_refuses_the_gap_job_without_the_permission(self) -> None:
        doc = copy.deepcopy(_doc())
        doc["jobs"]["e2e-measurement-gap"]["permissions"].pop("actions")
        violations, examined = actions_read_violations(doc, log_reading_scripts())
        assert examined >= 1 and any("e2e-measurement-gap" in v for v in violations), violations

    def test_control_the_workflow_level_default_does_not_grant_it(self) -> None:
        """The file-level block reads `contents` and writes `issues` — deleting the job-level
        block must not fall back to something that quietly satisfies the guard."""
        doc = copy.deepcopy(_doc())
        doc["jobs"]["e2e-measurement-gap"].pop("permissions")
        violations, _ = actions_read_violations(doc, log_reading_scripts())
        assert any("e2e-measurement-gap" in v for v in violations), violations
