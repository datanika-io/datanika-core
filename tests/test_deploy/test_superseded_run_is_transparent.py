"""A run skipped by supersession ran NOTHING, and the reader must say so (core#1507).

`docs/QA_RULES.md` §10 draws the line that decides a graduation streak:

* a run **known** to have measured nothing is *transparent* — it neither advances nor resets;
* a run we **cannot read** *blocks* — we cannot rule out a red, and assuming otherwise is the
  reassuring assumption.

Since core#975's residual fix, a push that is no longer `dev`'s head skips `staging` and
`e2e-sso` outright. That population arrived **after** §10 was written, and the reader had no
branch for it: a skipped job has no log, GitHub answers **404**, `parse_verdict_line` returns
`None`, and `classify_verdict(None)` is `UNREADABLE`. So the most certain non-measurement in the
system — a job with zero steps that ran by design — was graded as the least certain one.

Measured on the scheduled watchdog's own run `35651904991` (2026-09-21):

    job       : e2e-sso on dev
    runs read : 24   a reading on 14 (14 pass / 0 fail)   no reading on 10
                (0 unmeasured, 10 unreadable)
    unmeasured: 0 of 24 runs (0%)   longest run of consecutive non-readings: 0

**Ten blocking non-readings, and the blindness alert reported zero**, because it counts only
`UNMEASURED`. Three of the ten, read job by job from the Actions API rather than relayed
(`7506c795` / `bbced75e` / `6da96531`): `supersession` **success**, `staging` **skipped**,
`e2e-sso` **skipped with 0 steps**, and no `staging / e2e-staging` job in the run at all.

Two consequences this file pins, and the second is the one that makes it S2 rather than tidiness:

1. the streak is reset by pushes that ran nothing, so **graduation is held by the merge rate**;
2. the two tiers disagree about the same push — `e2e-sso` blocks on it while `e2e-staging`
   drops it silently and does not even count it.

⚠️ **The fix must not become invisibility.** A tier whose job is *always* skipped — somebody adds
a `paths:` filter — must still block. So supersession is recognised only on POSITIVE evidence of
all three facts, never on the absence of a log.
"""

from __future__ import annotations

import re
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))
sys.path.insert(0, str(REPO_ROOT / "scripts"))

import scripts.e2e_tier_streak as streak  # noqa: E402

T0 = datetime(2026, 9, 21, 8, 0, tzinfo=UTC)


def _emittable_tokens(text: str) -> set[str]:
    """Every verdict token a workflow could actually put into a log the reader parses.

    Deliberately NOT "does the word appear". `verdict_states_in_workflow` is anchored to a
    `STATE=` **assignment**, so a comparison or a comment cannot enter the set; the other three
    are the marker forms the reader's own regexes look for.
    """
    tokens = set(streak.verdict_states_in_workflow(text))
    tokens |= set(re.findall(r"INFORMATIONAL_RESULT=([a-z_]+)", text))
    tokens |= set(re.findall(r"INFORMATIONAL_SPEC_RESULT=[^:\s]+:([a-z_]+)", text))
    tokens |= set(re.findall(r"verdict: ([a-z_]+)", text))
    return tokens


#: What a run's tier jobs looked like, as three shapes read off real runs.
HEAD = "head"  # this commit was dev's head: everything ran
SUPERSEDED_RUN = "superseded"  # a newer push had taken the head: staging + e2e-sso skipped
SKIPPED_NO_EVIDENCE = "skipped_no_evidence"  # skipped, and the gate did not decide it


def _iso(t: datetime) -> str:
    return t.strftime("%Y-%m-%dT%H:%M:%SZ")


def _sso_log(verdict: str, specs: str = "success") -> str:
    """The executed classifier line of a real `e2e-sso` log, runner stamp included."""
    return (
        f"2026-09-21T08:23:59.1030364Z SSO specs outcome: {specs} / job status: success / "
        f"verdict: {verdict}\n"
    )


def _staging_log(info: str, gating: str = "clean") -> str:
    return (
        f"2026-09-21T08:23:59.1030364Z INFORMATIONAL_RESULT={info}\n"
        f"2026-09-21T08:24:03.0218611Z gating step outcome: success / job status: success / "
        f"verdict: {gating}\n"
    )


def _jobs_for(shape: str, base_id: int) -> list[dict]:
    """The job records a run of each shape really carries.

    Read from the Actions API on 2026-09-23 for runs `35533616464`, `35534590675` and
    `35599284862` (superseded) and `35668870056` (head). The shapes are the measurement; the
    ids are made up.
    """
    if shape == HEAD:
        return [
            {"id": base_id + 1, "name": "lint", "status": "completed", "conclusion": "success"},
            {
                "id": base_id + 2,
                "name": "supersession",
                "status": "completed",
                "conclusion": "success",
            },
            {"id": base_id + 3, "name": "staging", "status": "completed", "conclusion": "success"},
            {
                "id": base_id + 4,
                "name": "staging / deploy-staging",
                "status": "completed",
                "conclusion": "success",
            },
            {
                "id": base_id + 5,
                "name": "staging / e2e-staging",
                "status": "completed",
                "conclusion": "success",
            },
            {"id": base_id + 6, "name": "e2e-sso", "status": "completed", "conclusion": "success"},
        ]
    # Both skip shapes are byte-identical in the tier jobs. ONLY the supersession job's own
    # conclusion tells them apart, which is the whole point of the predicate.
    gate = "success" if shape == SUPERSEDED_RUN else "skipped"
    return [
        {"id": base_id + 1, "name": "lint", "status": "completed", "conclusion": "success"},
        {"id": base_id + 2, "name": "supersession", "status": "completed", "conclusion": gate},
        {"id": base_id + 3, "name": "staging", "status": "completed", "conclusion": "skipped"},
        {"id": base_id + 6, "name": "e2e-sso", "status": "completed", "conclusion": "skipped"},
    ]


class FakeActions:
    """`actions/runs`, `.../jobs` and job logs, served the way the reader asks for them.

    A skipped job's log is a **404** — measured on job `106141900442`. That is modelled here
    rather than asserted about, because it is what turns a deliberate non-run into `UNREADABLE`.
    """

    def __init__(self, shapes: list[str]) -> None:
        self.runs: list[dict] = []
        self.jobs: dict[int, list[dict]] = {}
        self.logs: dict[int, str] = {}
        self.log_calls: list[int] = []
        for i, shape in enumerate(shapes):
            run_id = 1000 + 2 * i
            created = _iso(T0 + timedelta(minutes=20 * i))
            self.runs.append(
                {
                    "id": run_id,
                    "path": ".github/workflows/ci.yml",
                    "status": "completed",
                    "created_at": created,
                    "head_sha": f"{i:08x}".ljust(40, "0"),
                }
            )
            # A push to `dev` starts two push workflows; the reader filters on `path`.
            self.runs.append(
                {
                    "id": run_id + 1,
                    "path": ".github/workflows/build-push-image.yml",
                    "status": "completed",
                    "created_at": created,
                    "head_sha": f"{i:08x}".ljust(40, "0"),
                }
            )
            jobs = _jobs_for(shape, run_id * 10)
            self.jobs[run_id] = jobs
            if shape == HEAD:
                self.logs[run_id * 10 + 6] = _sso_log("clean")
                self.logs[run_id * 10 + 5] = _staging_log("success")
        self.runs.sort(key=lambda r: (r["created_at"], r["id"]), reverse=True)

    def gh(self, path: str, *, raw: bool = False) -> object:
        url = urlsplit(path)
        q = {k: v[0] for k, v in parse_qs(url.query).items()}
        if url.path.endswith("/jobs"):
            run_id = int(url.path.rsplit("/", 2)[-2])
            return {"jobs": self.jobs.get(run_id, [])}
        if url.path.endswith("/actions/runs"):
            runs = self.runs
            if "created" in q:
                runs = [r for r in runs if r["created_at"] >= q["created"].removeprefix(">=")]
            per_page, page = int(q.get("per_page", 30)), int(q.get("page", 1))
            return {"workflow_runs": runs[(page - 1) * per_page : page * per_page]}
        raise AssertionError(f"the reader made a call this fake does not serve: {path}")

    def log(self, repo: str, job_id: int) -> tuple[str | None, str]:
        self.log_calls.append(job_id)
        if job_id in self.logs:
            return self.logs[job_id], ""
        return None, "HTTP 404: Not Found"


def _run(monkeypatch, capsys, fake: FakeActions, *argv: str) -> tuple[int, str]:
    monkeypatch.setattr(streak, "_gh", fake.gh)
    monkeypatch.setattr(streak, "_gh_log_or_none", fake.log)
    try:
        code = streak.main(["--branch", "dev", "--runs", "60", *argv])
    except SystemExit as exc:  # the reader's instrument-failure guard
        return 99, f"SystemExit: {exc}\n" + capsys.readouterr().out
    return code, capsys.readouterr().out


# ── the predicate, driven with BOTH populations ──────────────────────────────────────────────
#
# Coordinator rule 10's sharper form: a guard that refuses everything is not discriminating.
# These rows must answer the two skip shapes DIFFERENTLY, and the rows that must stay False are
# the ones that keep the fix from becoming invisibility.


class TestSupersessionIsRecognisedOnPositiveEvidence:
    def test_the_three_facts_together_are_supersession(self) -> None:
        jobs = _jobs_for(SUPERSEDED_RUN, 10)
        tier = next(j for j in jobs if j["name"] == "e2e-sso")
        assert streak.skipped_by_supersession(jobs, tier) is True

    def test_the_tier_job_absent_is_supersession_too(self) -> None:
        """`staging / e2e-staging` is never CREATED when its caller is skipped — which is why
        the two tiers disagreed about the same push."""
        jobs = _jobs_for(SUPERSEDED_RUN, 10)
        assert streak.skipped_by_supersession(jobs, None) is True

    def test_a_gate_that_never_decided_is_not_evidence(self) -> None:
        """The `lint`-failed shape. `supersession` needs [lint, test, helm-lint], so a red one
        skips the gate, which skips `staging` and `e2e-sso` — byte-identical tier jobs, and
        nobody decided anything. This stays UNREADABLE."""
        jobs = _jobs_for(SKIPPED_NO_EVIDENCE, 10)
        tier = next(j for j in jobs if j["name"] == "e2e-sso")
        assert streak.skipped_by_supersession(jobs, tier) is False

    def test_a_gate_that_failed_is_not_evidence(self) -> None:
        """A gate that errored decided nothing either — and it fails in the safe direction."""
        jobs = _jobs_for(SUPERSEDED_RUN, 10)
        next(j for j in jobs if j["name"] == "supersession")["conclusion"] = "failure"
        assert streak.skipped_by_supersession(jobs, None) is False

    def test_a_caller_that_ran_is_not_evidence(self) -> None:
        """`staging` skipped is what the gate DOES to a non-head push. A caller that ran means
        the gate answered `true`, whatever else is skipped."""
        jobs = _jobs_for(SUPERSEDED_RUN, 10)
        next(j for j in jobs if j["name"] == "staging")["conclusion"] = "success"
        assert streak.skipped_by_supersession(jobs, None) is False

    def test_a_tier_job_that_ran_is_not_evidence(self) -> None:
        """The tier job produced a verdict, so there is a reading to grade. Read it."""
        jobs = _jobs_for(SUPERSEDED_RUN, 10)
        tier = next(j for j in jobs if j["name"] == "e2e-sso")
        tier["conclusion"] = "failure"
        assert streak.skipped_by_supersession(jobs, tier) is False

    def test_the_caller_is_matched_by_exact_name_not_by_substring(self) -> None:
        """🚨 `"staging" in "staging / e2e-staging"` is True. A substring match would read the
        reusable workflow's own job as its caller and manufacture the evidence it is looking
        for — the caller fact would then be satisfied by the thing it is supposed to explain."""
        jobs = [
            {"name": "supersession", "status": "completed", "conclusion": "success"},
            {"name": "staging / e2e-staging", "status": "completed", "conclusion": "skipped"},
        ]
        assert streak.skipped_by_supersession(jobs, None) is False

    def test_an_empty_jobs_list_is_not_evidence(self) -> None:
        """Fails closed: no jobs means nothing was established, not that nothing ran by design."""
        assert streak.skipped_by_supersession([], None) is False


# ── the streak: transparent, and the two tiers agree ─────────────────────────────────────────

#: Three head pushes, each with a superseded push between them. The real shape of a busy day.
INTERLEAVED = [HEAD, SUPERSEDED_RUN, HEAD, SUPERSEDED_RUN, HEAD]


class TestASupersededRunIsTransparentToTheStreak:
    def test_e2e_sso_graduates_across_superseded_pushes(self, monkeypatch, capsys) -> None:
        """RED before the fix: the two superseded runs grade UNREADABLE, which BLOCKS, so the
        trailing streak is 1 and the verdict is `not-yet`."""
        code, out = _run(monkeypatch, capsys, FakeActions(INTERLEAVED), "--job", "e2e-sso")
        assert "trailing streak: 3 / 3" in out, (
            f"three head pushes passed and two superseded pushes reset the streak:\n{out}"
        )
        assert "verdict        : graduate" in out, out
        assert code == 0, out

    def test_the_superseded_runs_are_classified_as_such_in_the_history(
        self, monkeypatch, capsys
    ) -> None:
        _code, out = _run(monkeypatch, capsys, FakeActions(INTERLEAVED), "--job", "e2e-sso")
        assert out.count(f" {streak.SUPERSEDED}") == 2, out
        assert f" {streak.UNREADABLE}" not in out, (
            f"a deliberate non-run is still being graded as one we could not read:\n{out}"
        )

    def test_e2e_staging_classifies_the_same_pushes_the_same_way(self, monkeypatch, capsys) -> None:
        """RED before the fix, in the OTHER direction: `staging / e2e-staging` does not exist in
        a superseded run, so the reader dropped it silently and did not even count it."""
        _code, out = _run(monkeypatch, capsys, FakeActions(INTERLEAVED), "--job", "e2e-staging")
        assert "runs read      : 5" in out, (
            f"superseded pushes are dropped without being counted:\n{out}"
        )
        assert out.count(f" {streak.SUPERSEDED}") == 2, out

    def test_the_two_tiers_agree_run_for_run(self, monkeypatch, capsys) -> None:
        """The issue's own acceptance criterion, asserted as a comparison rather than twice."""
        _c1, sso = _run(monkeypatch, capsys, FakeActions(INTERLEAVED), "--job", "e2e-sso")
        _c2, stg = _run(monkeypatch, capsys, FakeActions(INTERLEAVED), "--job", "e2e-staging")

        def classes(text: str) -> list[tuple[str, str]]:
            rows = []
            for line in text.splitlines():
                parts = line.split()
                if len(parts) >= 3 and parts[0].endswith("Z") and parts[2].isupper():
                    rows.append((parts[1], parts[2]))
            return rows

        assert classes(sso), f"no history rows parsed from the sso output:\n{sso}"
        assert classes(sso) == classes(stg), (
            f"the two tiers grade the same pushes differently:\n{classes(sso)}\n{classes(stg)}"
        )


class TestASkipWithoutEvidenceStillBlocks:
    """The false-positive controls. Without these, a fix that simply made every skip transparent
    would score identically on everything above — and a tier that stopped running entirely would
    become invisible instead of blocking."""

    def test_a_skip_the_gate_did_not_decide_still_blocks(self, monkeypatch, capsys) -> None:
        shapes = [HEAD, HEAD, HEAD, SKIPPED_NO_EVIDENCE]
        code, out = _run(monkeypatch, capsys, FakeActions(shapes), "--job", "e2e-sso")
        assert f" {streak.UNREADABLE}" in out, out
        assert "trailing streak: 0 / 3" in out, (
            f"a skip nobody explained was treated as transparent:\n{out}"
        )
        assert code != 0, out

    def test_the_two_shapes_are_answered_differently(self, monkeypatch, capsys) -> None:
        """Identical histories but for the supersession job's own conclusion. If these two agree,
        the predicate is not reading the thing it claims to read."""
        blocked = [HEAD, HEAD, HEAD, SKIPPED_NO_EVIDENCE]
        transparent = [HEAD, HEAD, HEAD, SUPERSEDED_RUN]
        code_b, out_b = _run(monkeypatch, capsys, FakeActions(blocked), "--job", "e2e-sso")
        code_t, out_t = _run(monkeypatch, capsys, FakeActions(transparent), "--job", "e2e-sso")
        assert (code_b, code_t) == (1, 0), f"blocked={code_b} transparent={code_t}"
        assert "verdict        : not-yet" in out_b, out_b
        assert "verdict        : graduate" in out_t, out_t


# ── the population is printed, and it is not blindness ───────────────────────────────────────


class TestTheSupersededPopulationIsPrinted:
    def test_the_count_is_printed_beside_the_verdict(self, monkeypatch, capsys) -> None:
        """`QA_RULES` §31 rule 1. A class that is transparent to the streak and invisible in the
        output is the previous defect with a friendlier class name."""
        _code, out = _run(monkeypatch, capsys, FakeActions(INTERLEAVED), "--job", "e2e-sso")
        assert "superseded     : 2 of 5 runs" in out, out

    def test_it_is_printed_even_when_there_are_none(self, monkeypatch, capsys) -> None:
        """Unconditional, for the same reason `unmeasured:` is: the number nobody asked for is
        the one that was invisible."""
        _code, out = _run(monkeypatch, capsys, FakeActions([HEAD, HEAD, HEAD]), "--job", "e2e-sso")
        assert "superseded     : 0 of 3 runs" in out, out

    def test_a_superseded_run_is_not_counted_as_unmeasured(self, monkeypatch, capsys) -> None:
        """They need opposite responses. `unmeasured` is a run that tried and failed to grade —
        somebody should look. `superseded` is a push that ran nothing by design — nobody should."""
        _code, out = _run(monkeypatch, capsys, FakeActions(INTERLEAVED), "--job", "e2e-sso")
        assert "unmeasured     : 0 of 5 runs (0%)" in out, out

    def test_the_blindness_alert_does_not_fire_on_the_merge_rate(self, monkeypatch, capsys) -> None:
        """A false-positive control, GREEN both before and after. Folding superseded runs into
        the blindness population would move the defect rather than fix it: the alert would then
        be governed by how fast `dev` moves instead of by the instrument's health."""
        shapes = [HEAD, SUPERSEDED_RUN, SUPERSEDED_RUN, SUPERSEDED_RUN, SUPERSEDED_RUN, HEAD]
        code, out = _run(
            monkeypatch, capsys, FakeActions(shapes), "--job", "e2e-sso", "--max-unmeasured", "3"
        )
        assert code != 2, f"four superseded pushes raised a blindness alert:\n{out}"
        assert "BLIND:" not in out, out


class TestTheInstrumentFailureGuardStillMeansWhatItSays:
    def test_a_window_of_superseded_runs_is_not_an_instrument_failure(
        self, monkeypatch, capsys
    ) -> None:
        """RED before the fix, and loudly: every superseded run costs a 404 log fetch, so a
        window of them trips *"could not fetch a single job log"* — the guard that exists to say
        the READER is broken, fired by a perfectly healthy burst of pushes."""
        fake = FakeActions([SUPERSEDED_RUN] * 4)
        code, out = _run(monkeypatch, capsys, fake, "--job", "e2e-sso")
        assert code != 99, f"the instrument-failure guard fired on a healthy window:\n{out}"
        assert "superseded     : 4 of 4 runs" in out, out

    def test_no_log_is_fetched_for_a_run_that_ran_nothing(self, monkeypatch, capsys) -> None:
        """The cheap half, and the reason the guard above stops misfiring: a skipped job has no
        log to ask for. Asking anyway inflates the failed-fetch count beside the genuinely
        unreachable ones, which is what made a real 404 harder to see."""
        fake = FakeActions([SUPERSEDED_RUN] * 4)
        _code, _out = _run(monkeypatch, capsys, fake, "--job", "e2e-sso")
        assert fake.log_calls == [], f"logs were requested for skipped jobs: {fake.log_calls}"

    def test_control_a_genuinely_unreachable_log_still_trips_it(self, monkeypatch, capsys) -> None:
        """The guard must keep firing for the case it was built for, or this fix has deleted it."""
        fake = FakeActions([HEAD, HEAD, HEAD])
        fake.logs.clear()  # every log 404s, and nothing explains it
        code, out = _run(monkeypatch, capsys, fake, "--job", "e2e-sso")
        assert code == 99 and "could not fetch a single job log" in out, out


# ── the subject must be IN the population (QA_RULES §31) ─────────────────────────────────────


class TestAFabricatedJobIsNotSuperseded:
    """core#1480's property, at the job level. Without it the fix hands a confident reading to
    anyone who mistypes `--job`: a fabricated name is absent from every run, and a superseded
    run's evidence is about the RUN, so it would grade every run `SUPERSEDED` and print a
    population that has nothing to do with the name asked about."""

    def test_a_fabricated_job_name_is_refused(self, monkeypatch, capsys) -> None:
        code, out = _run(
            monkeypatch, capsys, FakeActions(INTERLEAVED), "--job", "not-a-real-job-qa1507"
        )
        assert code == 2, f"a job that does not exist was answered, not refused:\n{out}"
        assert "not-a-real-job-qa1507" in out, out

    def test_the_refusal_is_not_byte_identical_to_a_real_reading(self, monkeypatch, capsys) -> None:
        """The core#1480 control restated: three invocations differing only in the subject
        produced byte-identical output, down to the percentage."""
        _c1, real = _run(monkeypatch, capsys, FakeActions(INTERLEAVED), "--job", "e2e-sso")
        _c2, fake = _run(
            monkeypatch, capsys, FakeActions(INTERLEAVED), "--job", "not-a-real-job-qa1507"
        )
        assert real != fake, real

    def test_a_fabricated_job_does_not_get_a_superseded_population(
        self, monkeypatch, capsys
    ) -> None:
        _code, out = _run(
            monkeypatch, capsys, FakeActions(INTERLEAVED), "--job", "not-a-real-job-qa1507"
        )
        assert "superseded     : 2 of" not in out, (
            f"the run's supersession evidence was attributed to a job that never existed:\n{out}"
        )

    def test_control_a_real_job_is_placed_and_says_so(self, monkeypatch, capsys) -> None:
        _code, out = _run(monkeypatch, capsys, FakeActions(INTERLEAVED), "--job", "e2e-sso")
        assert "e2e-sso" in out and "present in 5 of 5 runs" in out, out

    def test_a_window_that_only_ever_superseded_says_which_question_it_cannot_answer(
        self, monkeypatch, capsys
    ) -> None:
        """Two reasons a job was never seen, and they need opposite responses: *widen the
        window* versus *you mistyped the name*. One sentence for both is the defect §31 names."""
        code, out = _run(
            monkeypatch, capsys, FakeActions([SUPERSEDED_RUN] * 4), "--job", "e2e-staging"
        )
        assert code == 2, out
        assert "widen" in out.lower(), (
            f"a window of superseded pushes was reported as a missing job:\n{out}"
        )


# ── vocabulary hygiene ───────────────────────────────────────────────────────────────────────


class TestTheSupersededTokenCannotBeForged:
    def test_it_is_a_class_of_its_own_and_not_unmeasured(self) -> None:
        assert streak.SUPERSEDED not in (streak.UNMEASURED, streak.UNREADABLE, streak.PASS)

    def test_it_is_transparent_to_the_streak(self) -> None:
        assert streak.streak([streak.PASS, streak.SUPERSEDED, streak.PASS]) == 2

    def test_it_does_not_dilute_the_streaks_window(self) -> None:
        """Otherwise `sparse` becomes the new way the merge rate holds a graduation: ten
        superseded pushes between three greens would exceed `max_gaps` and refuse them."""
        classes = [streak.PASS, *[streak.SUPERSEDED] * 10, streak.PASS, streak.PASS]
        r = streak.Reading.from_classes(classes)
        assert r.streak == 3 and r.state == "graduate", r

    def test_control_real_unmeasured_dilution_is_still_refused(self) -> None:
        """The same window with runs that TRIED and failed to grade must still read `sparse`."""
        classes = [streak.PASS, *[streak.UNMEASURED] * 10, streak.PASS, streak.PASS]
        r = streak.Reading.from_classes(classes)
        assert r.state == "sparse", r

    def test_no_workflow_can_emit_the_token(self) -> None:
        """Its defining property, the same one `local_environment` has: it is DERIVED from the
        jobs API, never parsed from a log. A workflow that could print it could forge a run
        that is transparent to a graduation streak — a graduation nobody measured.

        🚨 **Asserted on what a workflow can EMIT, not on whether the word appears** — and the
        raw-text form is how this was first written and it went red immediately, correctly.
        `ci.yml`'s own comment reads *"A superseded run skips `staging`"*. That is `QA_RULES`
        §24a from the other side: a text ban is failed by the sentence that EXPLAINS the
        mechanism, so keeping it would have meant deleting the comment that documents the gate
        this whole class depends on. `local_environment` gets away with a raw-text scan only
        because nobody writes that string in prose; `superseded` is an ordinary English word.
        """
        for wf in sorted((REPO_ROOT / ".github" / "workflows").glob("*.yml")):
            emittable = _emittable_tokens(wf.read_text(encoding="utf-8", errors="replace"))
            assert streak.SUPERSEDED_VERDICT not in emittable, (
                f"{wf.name} can put `{streak.SUPERSEDED_VERDICT}` into a log where the reader "
                "parses it, which would let CI forge a run transparent to a graduation streak."
            )

    def test_control_the_emission_scan_sees_real_tokens(self) -> None:
        """Without this the assertion above is satisfied by a scanner that matches nothing —
        and a guard that finds no tokens anywhere passes forever."""
        ci = _emittable_tokens(
            (REPO_ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
        )
        assert {"clean", "specs_failed"} <= ci, f"the emission scan reads nothing real: {ci}"

    def test_control_the_word_is_still_present_as_prose(self) -> None:
        """Pins the reason the test above is shaped the way it is. If this ever goes red the
        comment was deleted, and a raw-text ban would then look correct again — which is how
        the weaker guard gets restored by someone tidying up."""
        text = (REPO_ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
        assert streak.SUPERSEDED_VERDICT in text and "supersession:" in text, (
            "ci.yml no longer documents the supersession gate in prose"
        )
