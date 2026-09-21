"""A reader must establish that its SUBJECT is in the population it measured (core#1480).

Three instruments violated this property in two rounds, each fixed as one reader's bug:
[core#1447] printed the *class* word for an unmeasured run where it needed the run's own cause
token; [core#1448] used a count-sized look-back on a time-based cadence, with a hole wherever the
count ran out first; [core#1468] graded twelve runs `UNREADABLE` while printing
``unmeasured: 0 of 12 runs (0%)``. The shared property is this file's subject.

**The fourth instance, and the one this file was written for.** ``classify_for_spec`` attributed a
whole TIER's verdict to whatever spec name it was handed. Its own docstring gives the reasoning —
*"a green tier means every spec in it was green"* — which is sound only for a spec that is IN the
tier, and nothing checked that. Measured in-process before the fix:

    classify_for_spec("not-a-real-spec.ts", {}, "success")  ->  PASS

That is the graduation instrument, and graduation moves a spec out of the informational tier and
into the **gating** tier that holds promotions.

**What makes the readings below evidence rather than assertions.** The discriminating control is a
spec that does not exist: before the fix, three CLI invocations differing only in ``--spec`` — a
real spec of the job, a real spec of a *different* job, and a fabricated name — produced
byte-identical output down to the percentage. A test that only checked the real spec would have
passed throughout.
"""

from __future__ import annotations

import ast
import re
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import pytest

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:  # pragma: no cover - import plumbing
    sys.path.insert(0, str(ROOT))

from scripts.e2e_tier_streak import (  # noqa: E402
    FAIL,
    MEMBERSHIP_UNKNOWN,
    PASS,
    SPEC_NOT_PRESENT,
    UNMEASURED,
    SpecCoverage,
    classify_for_spec,
)

from scripts import e2e_tier_streak as streak  # noqa: E402  isort:skip

INCUMBENT = "reflex-wire.spec.ts"
FOREIGN = "sso-oidc.spec.ts"
FABRICATED = "no-such-spec-qa-1480.spec.ts"
TIER = frozenset({INCUMBENT, "a11y-sweep.spec.ts"})

T0 = datetime(2026, 9, 17, 8, 0, tzinfo=UTC)


# ── the property, at the function ────────────────────────────────────────────────────────────


class TestATierVerdictIsNotAttributedToAStrangerSpec:
    """A tier-only log carries no membership, so the caller must supply what it established."""

    def test_a_spec_in_the_tier_still_takes_a_green_tiers_pass(self) -> None:
        """The control, and it must be green before and after the fix.

        `reflex-wire.spec.ts` lost seven greens once already (core#1221). A fix that protected
        against the stranger by refusing every tier-only attribution would destroy that history
        again, and would look like a stricter, safer instrument while doing it.
        """
        assert classify_for_spec(INCUMBENT, {}, "success", membership=TIER) == PASS

    def test_a_spec_from_another_job_is_not_credited(self) -> None:
        assert classify_for_spec(FOREIGN, {}, "success", membership=TIER) == UNMEASURED

    def test_a_spec_that_does_not_exist_is_not_credited(self) -> None:
        """The discriminating control: no real spec can be confused with this name."""
        assert classify_for_spec(FABRICATED, {}, "success", membership=TIER) == UNMEASURED

    def test_the_sso_vocabulary_is_covered_too_not_just_success(self) -> None:
        """`clean` and `infra_only` are PASS for the SSO tier, by a different code path."""
        for token in ("clean", "infra_only"):
            assert classify_for_spec(FABRICATED, {}, token, membership=TIER) == UNMEASURED

    def test_a_red_tier_is_unmeasured_for_a_stranger_as_it_already_was_for_a_member(self) -> None:
        assert classify_for_spec(FABRICATED, {}, "failure", membership=TIER) == UNMEASURED
        assert classify_for_spec(INCUMBENT, {}, "failure", membership=TIER) == UNMEASURED

    def test_per_spec_lines_still_decide_when_they_exist(self) -> None:
        """Membership never overrides a run that graded the spec directly."""
        graded = {INCUMBENT: "failure"}
        assert classify_for_spec(INCUMBENT, graded, "success", membership=TIER) == FAIL
        assert classify_for_spec(FOREIGN, graded, "success", membership=TIER) == UNMEASURED


# ── the subject's population ─────────────────────────────────────────────────────────────────


class TestSpecCoverage:
    def test_no_run_graded_specs_so_membership_is_unknown(self) -> None:
        c = SpecCoverage(
            INCUMBENT,
            runs_read=9,
            runs_with_spec_lines=0,
            runs_mentioning_spec=0,
            membership=frozenset(),
        )
        assert c.state == MEMBERSHIP_UNKNOWN
        assert "0 run(s) graded specs at all" in "\n".join(c.render())

    def test_runs_graded_specs_and_this_one_was_not_among_them(self) -> None:
        c = SpecCoverage(
            FABRICATED, runs_read=9, runs_with_spec_lines=9, runs_mentioning_spec=0, membership=TIER
        )
        assert c.state == SPEC_NOT_PRESENT
        rendered = "\n".join(c.render())
        assert INCUMBENT in rendered, "it must name what it DID see, or the reader cannot act"

    def test_a_placed_subject_has_no_state_and_still_reports_its_population(self) -> None:
        c = SpecCoverage(
            INCUMBENT, runs_read=9, runs_with_spec_lines=9, runs_mentioning_spec=7, membership=TIER
        )
        assert c.state is None
        assert "named in 7 of 9 runs read" in "\n".join(c.render())

    def test_no_spec_asked_means_no_subject_line_at_all(self) -> None:
        assert SpecCoverage.unasked().state is None
        assert SpecCoverage.unasked().render() == []


# ── end to end, through the reader's own GitHub seam ─────────────────────────────────────────


def _log(info: str, gating: str, specs: dict[str, str] | None) -> str:
    lines = [
        f"2026-09-17T08:23:59.1030364Z INFORMATIONAL_RESULT={info}",
        f"2026-09-17T08:24:03.0218611Z gating step outcome: success / job status: success / "
        f"verdict: {gating}",
    ]
    for name, verdict in (specs or {}).items():
        lines.append(f"2026-09-17T08:24:05.0000000Z INFORMATIONAL_SPEC_RESULT={name}:{verdict}")
    return "\n".join(lines) + "\n"


def _sso_log(sso: str, info: str, specs: dict[str, str] | None) -> str:
    """An `e2e-sso` log — and one that ALSO carries an informational line, on purpose.

    Real SSO logs carry no `INFORMATIONAL_RESULT=`. This fake puts one in with a **different**
    value so the two tiers can be told apart: a reader that follows the job's own tier grades
    from `sso`, and one that hardcodes the informational tier grades from `info` (core#1468).
    """
    lines = [
        f"2026-09-17T08:23:59.1030364Z SSO specs outcome: success / job status: success / "
        f"verdict: {sso}",
        f"2026-09-17T08:24:01.0000000Z INFORMATIONAL_RESULT={info}",
    ]
    for name, verdict in (specs or {}).items():
        lines.append(f"2026-09-17T08:24:05.0000000Z INFORMATIONAL_SPEC_RESULT={name}:{verdict}")
    return "\n".join(lines) + "\n"


class FakeActions:
    """The reader's `_gh` / `_gh_log_or_none` seam, serving one CI run per commit."""

    #: The job name the fake serves. `collect` matches on a substring and derives the tier from
    #: it, so this is what decides which tier the reader should be reading.
    job_name = "staging / e2e-staging"

    def __init__(self, blocks: list[tuple[str, str, dict[str, str] | None]]) -> None:
        self.runs: list[dict] = []
        self.logs: dict[int, str] = {}
        t = T0
        for i, (info, gating, specs) in enumerate(blocks):
            rid = 1000 + i
            self.runs.append(
                {
                    "id": rid,
                    "path": ".github/workflows/ci.yml",
                    "status": "completed",
                    "created_at": t.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "head_sha": f"{i:08x}".ljust(40, "0"),
                }
            )
            self.logs[rid * 10] = _log(info, gating, specs)
            t += timedelta(minutes=20)
        self.runs.sort(key=lambda r: (r["created_at"], r["id"]), reverse=True)

    def gh(self, path: str, *, raw: bool = False) -> object:
        url = urlsplit(path)
        if re.fullmatch(r"repos/[^/]+/[^/]+/actions/runs/(\d+)/jobs", url.path):
            rid = int(url.path.split("/")[-2])
            return {
                "jobs": [
                    {
                        "id": rid * 10,
                        "name": self.job_name,
                        "status": "completed",
                        "conclusion": "success",
                    }
                ]
            }
        if url.path.endswith("/actions/runs"):
            q = {k: v[0] for k, v in parse_qs(url.query).items()}
            per_page, page = int(q.get("per_page", 30)), int(q.get("page", 1))
            return {"workflow_runs": self.runs[(page - 1) * per_page : page * per_page]}
        raise AssertionError(f"the reader made a call this fake does not serve: {path}")

    def log(self, repo: str, job_id: int) -> tuple[str | None, str]:
        return (self.logs[job_id], "") if job_id in self.logs else (None, "HTTP 404")


def _run(monkeypatch, capsys, fake: FakeActions, spec: str) -> tuple[int, str]:
    monkeypatch.setattr(streak, "_gh", fake.gh)
    monkeypatch.setattr(streak, "_gh_log_or_none", fake.log)
    code = streak.main(["--job", "e2e-staging", "--branch", "dev", "--spec", spec])
    return code, capsys.readouterr().out


#: Three runs that graded specs, all green for the incumbent.
GRADED = [("success", "clean", {INCUMBENT: "success"})] * 3
#: Three runs from before per-spec lines existed: a green tier and nothing else.
TIER_ONLY = [("success", "clean", None)] * 3


class TestTheCliCanTellItsSubjectsApart:
    def test_a_real_spec_of_this_job_grades_normally(self, monkeypatch, capsys) -> None:
        code, out = _run(monkeypatch, capsys, FakeActions(GRADED), INCUMBENT)
        assert f"subject        : {INCUMBENT} named in 3 of 3 runs read" in out
        assert "verdict        : graduate" in out
        assert code == 0

    def test_a_fabricated_spec_is_refused_rather_than_reported_not_yet(
        self, monkeypatch, capsys
    ) -> None:
        code, out = _run(monkeypatch, capsys, FakeActions(GRADED), FABRICATED)
        assert f"verdict        : {SPEC_NOT_PRESENT}" in out
        assert "not-yet" not in out, "'not-yet' reads as keep waiting; nobody should wait for this"
        assert code == 2, "2 is 'nothing could be measured' — slo_report.py's convention"

    def test_a_window_that_graded_no_specs_says_so(self, monkeypatch, capsys) -> None:
        code, out = _run(monkeypatch, capsys, FakeActions(TIER_ONLY), INCUMBENT)
        assert f"verdict        : {MEMBERSHIP_UNKNOWN}" in out
        assert code == 2

    def test_the_two_invocations_are_not_byte_identical(self, monkeypatch, capsys) -> None:
        """The defect's own signature. This is what was true of the real tool before the fix."""
        _, real = _run(monkeypatch, capsys, FakeActions(GRADED), INCUMBENT)
        _, fabricated = _run(monkeypatch, capsys, FakeActions(GRADED), FABRICATED)
        assert real != fabricated
        assert FABRICATED in fabricated

    def test_control_a_tier_only_window_does_not_graduate_a_stranger(
        self, monkeypatch, capsys
    ) -> None:
        """Before the fix this printed three PASSes and `graduate` for a name nobody invented."""
        code, out = _run(monkeypatch, capsys, FakeActions(TIER_ONLY), FABRICATED)
        assert "verdict        : graduate" not in out
        assert code == 2


# ── the production path must not get the permissive default (AC4) ────────────────────────────


def _collect_source() -> ast.FunctionDef:
    tree = ast.parse(Path(streak.__file__).read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == "collect":
            return node
    raise AssertionError("scripts/e2e_tier_streak.py no longer defines collect()")


def test_the_production_path_passes_membership() -> None:
    """`membership=None` keeps the old, permissive behaviour — so the one caller must pass it.

    Asserted on the AST rather than on the text: a call spelled in a comment satisfies a grep
    (`QA_RULES` §24a), and this is exactly the kind of keyword a refactor drops silently.
    """
    calls = [
        node
        for node in ast.walk(_collect_source())
        if isinstance(node, ast.Call) and getattr(node.func, "id", None) == "classify_for_spec"
    ]
    assert calls, "collect() no longer classifies per spec — repoint this guard"
    for call in calls:
        assert "membership" in [kw.arg for kw in call.keywords], (
            "collect() called classify_for_spec without membership=, so a tier verdict can be "
            "attributed to a spec this window never saw (core#1480)"
        )


# ── the census (AC5) ─────────────────────────────────────────────────────────────────────────

#: Scripts that print a verdict token, and whether they have been audited against this property.
#:
#: 🚨 **This is a CENSUS, not a contract check.** It enumerates and forces triage; it does not
#: verify that the `audited` ones are correct — naming it a contract check would be a guard
#: claiming more than it does (`QA_RULES` §20). Five of these implement the property already, each
#: in its own vocabulary and none aware of the others, which is why the knowledge never
#: transferred and why the enumeration is the useful artifact.
READERS_AUDITED = frozenset(
    {
        # reports `scanned=0` as its own outcome rather than as clean
        "scripts/check_attribution_trailers.py",
        # counts PER REPO, never in total, so one repo cannot cover another
        "scripts/check_scheduled_workflows.py",
        # core#1480 — this file's subject
        "scripts/e2e_tier_streak.py",
        # PASS / FAIL / NO_VERDICT behind a min_samples floor; exit 2 when blind
        "scripts/slo_report.py",
        # "a WINDOW result, not an attribution failure" — core#1447 / core#1448's own fix
        "scripts/verify_e2e_attribution.py",
        # executed / skipped / collected, and refuses on collected == 0
        "e2e/scripts/assert_overage_coverage.py",
    }
)

#: Verdict-printing readers **not** yet audited, each with the reason it is not urgent.
READERS_PENDING = frozenset(
    {
        # emits GATING_FLAKY_STATUS with a `no-evidence` state; its population is one run's own
        # report rather than a window, so the failure mode is narrower.
        "e2e/scripts/detect_flaky_gating.py",
        # the EMITTER of the per-spec lines this file's subject consumes. A spec missing here is
        # what `membership` now makes visible downstream, so it is covered by its consumer.
        "e2e/scripts/informational_spec_results.py",
    }
)

_VERDICT_WORDS = re.compile(r"verdict|VERDICT|RESULT=|_STATUS=")


def _verdict_printing_readers() -> set[str]:
    found = set()
    for folder in ("scripts", "e2e/scripts"):
        for path in sorted((ROOT / folder).glob("*.py")):
            if _VERDICT_WORDS.search(path.read_text(encoding="utf-8")):
                found.add(f"{folder}/{path.name}")
    return found


def test_the_census_scan_is_not_vacuous() -> None:
    found = _verdict_printing_readers()
    assert len(found) >= 6, f"the scan found only {len(found)} readers: {sorted(found)}"
    assert "scripts/e2e_tier_streak.py" in found
    assert "scripts/slo_report.py" in found


def test_every_verdict_printing_reader_is_triaged_against_this_property() -> None:
    found = _verdict_printing_readers()
    known = READERS_AUDITED | READERS_PENDING
    assert found == known, (
        "the set of scripts that print a verdict has changed: "
        f"new={sorted(found - known)} gone={sorted(known - found)}. A reader that prints a "
        "verdict must be triaged against core#1480 — does it establish that its subject is in "
        "the population it measured, and does it report that population? Add it to "
        "READERS_AUDITED once it does, or to READERS_PENDING with the reason it can wait."
    )


def test_the_two_lists_do_not_overlap() -> None:
    assert not (READERS_AUDITED & READERS_PENDING)


@pytest.mark.parametrize("name", sorted(READERS_AUDITED | READERS_PENDING))
def test_every_listed_reader_still_exists(name: str) -> None:
    """A census naming a deleted file is a census of nothing."""
    assert (ROOT / name).is_file(), name


# ── core#1468's two residuals, both of them "report what you could see" ───────────────────────


class SsoFakeActions(FakeActions):
    """The same seam, serving an `e2e-sso` job whose log carries BOTH tiers' verdict lines."""

    job_name = "e2e-sso"

    def __init__(self, blocks: list[tuple[str, str, dict[str, str] | None]]) -> None:
        super().__init__(blocks)
        for i, (sso, info, specs) in enumerate(blocks):
            self.logs[(1000 + i) * 10] = _sso_log(sso, info, specs)


def _run_job(monkeypatch, capsys, fake: FakeActions, job: str, *argv: str) -> tuple[int, str]:
    monkeypatch.setattr(streak, "_gh", fake.gh)
    monkeypatch.setattr(streak, "_gh_log_or_none", fake.log)
    code = streak.main(["--job", job, "--branch", "dev", *argv])
    return code, capsys.readouterr().out


#: 🚨 **The shape is the whole test, and my first draft got it wrong in both directions.**
#: With per-spec lines on EVERY run, `classify_for_spec` takes its `if per_spec:` branch and the
#: tier is never read. With them on NO run, membership is empty and the core#1480 gate refuses
#: before the tier is read. **Either way the tier choice is invisible and the guard is vacuous** —
#: which is what the mutation table said, and nothing else would have.
#:
#: So: one oldest run that grades the spec directly, establishing membership for the window,
#: then tier-only runs that only the tier branch can grade. The two tiers disagree in the loud
#: direction — SSO red, informational green — so reading the wrong one turns a red tier into a
#: three-green streak.
SSO_RED_INFO_GREEN = [
    ("clean", "success", {INCUMBENT: "success"}),
    ("specs_failed", "success", None),
    ("specs_failed", "success", None),
    ("specs_failed", "success", None),
]
#: The staging mirror: informational green, gating red, same shape.
INFO_GREEN_GATING_RED = [
    ("success", "clean", {INCUMBENT: "success"}),
    ("success", "gating_failed", None),
    ("success", "gating_failed", None),
    ("success", "gating_failed", None),
]


class TestTheSpecBranchReadsItsOwnJobsTier:
    """core#1468 residual 1. `--spec` hardcoded the informational tier whatever `--job` said.

    Observed: against `e2e-sso` — which is the **default** `--job` — no informational line
    exists, so every run graded `UNREADABLE`, including specs that genuinely run there.
    **The latent half is worse and is what these arms pin:** on a log that carries both, the old
    code attributed the *other tier's* verdict to the spec — a confident wrong answer rather than
    a visible absence.
    """

    def test_an_sso_job_takes_the_sso_tiers_red(self, monkeypatch, capsys) -> None:
        fake = SsoFakeActions(SSO_RED_INFO_GREEN)
        code, out = _run_job(monkeypatch, capsys, fake, "e2e-sso", "--spec", INCUMBENT)
        assert "  FAIL" in out, (
            "the SSO tier is `specs_failed` on the three newest runs; no FAIL means the spec was "
            f"graded from some other tier's line (core#1468)\n{out}"
        )
        assert "verdict        : graduate" not in out, (
            "reading the informational tier here turns a red SSO tier into a three-green streak "
            "— the direction that matters"
        )
        assert code != 0

    def test_control_a_staging_job_still_takes_the_informational_tiers_green(
        self, monkeypatch, capsys
    ) -> None:
        """The false-positive control, and it must be green before and after.

        Same shape, opposite job. A fix that simply stopped reading the informational tier — or
        that read the gating tier for everything — scores identically on the arm above and reds
        here.
        """
        fake = FakeActions(INFO_GREEN_GATING_RED)
        code, out = _run_job(monkeypatch, capsys, fake, "e2e-staging", "--spec", INCUMBENT)
        assert "  FAIL" not in out, f"the gating tier's red leaked into a spec question\n{out}"
        assert "verdict        : graduate" in out and code == 0

    def test_the_observed_half_an_sso_log_carries_no_per_spec_lines_at_all(
        self, monkeypatch, capsys
    ) -> None:
        """What a real `e2e-sso` window does: no run grades specs, so the subject is refused.

        This is the half core#1468 was filed about. It is not the tier discriminator — it passes
        whichever tier is read — and it is kept separate for exactly that reason.
        """
        fake = SsoFakeActions([("clean", "success", None)] * 3)
        code, out = _run_job(monkeypatch, capsys, fake, "e2e-sso", "--spec", INCUMBENT)
        assert f"verdict        : {MEMBERSHIP_UNKNOWN}" in out
        assert code == 2


class TestTheSummaryDoesNotCallAnUnreadableRunMeasured:
    """core#1468 residual 2.

    `measured` groups PASS, FAIL **and UNREADABLE** — right for the streak, which all three
    block or advance, and wrong as a word printed at a reader: an `UNREADABLE` run is precisely
    one that could not be read. The old line read `runs read: N (measured: N)` beside
    `unmeasured: 0 of N (0%)` on a window where **nothing** had been read.
    """

    def test_the_breakdown_names_each_class(self, monkeypatch, capsys) -> None:
        fake = FakeActions([("success", "clean", {INCUMBENT: "success"})] * 3)
        _, out = _run_job(monkeypatch, capsys, fake, "e2e-staging", "--spec", INCUMBENT)
        assert "a reading on 3 (3 pass / 0 fail)" in out, out
        assert "no reading on 0 (0 unmeasured, 0 unreadable)" in out, out

    def test_an_unreadable_window_is_not_reported_as_read(self, monkeypatch, capsys) -> None:
        """The exact shape core#1468 was filed about, with the numbers that made it misleading."""
        fake = FakeActions([("unparseable-token", "clean", None)] * 4)
        _, out = _run_job(monkeypatch, capsys, fake, "e2e-staging")
        assert "a reading on 0 (0 pass / 0 fail)" in out, out
        assert "no reading on 4 (0 unmeasured, 4 unreadable)" in out, out
        # And the older blindness line still says 0%, which is correct for what IT names —
        # the two lines together are what stop that 0% reading as "the window was fine".
        assert "unmeasured     : 0 of 4 runs (0%)" in out, out

    def test_control_a_mixed_window_splits_correctly(self, monkeypatch, capsys) -> None:
        """A guard that printed zeroes everywhere would pass both arms above."""
        fake = FakeActions(
            [
                ("success", "clean", None),
                ("failure", "clean", None),
                ("unknown", "clean", None),
                ("unparseable-token", "clean", None),
            ]
        )
        _, out = _run_job(monkeypatch, capsys, fake, "e2e-staging")
        assert "a reading on 2 (1 pass / 1 fail)" in out, out
        assert "no reading on 2 (1 unmeasured, 1 unreadable)" in out, out
