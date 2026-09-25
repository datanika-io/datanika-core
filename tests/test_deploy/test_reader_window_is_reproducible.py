"""A reader's WINDOW is part of its verdict's identity (core#1567).

``scripts/e2e_tier_streak.py`` decides whether an informational spec enters the **gating** tier —
the tier that holds production promotions (`QA_RULES` §10, [core#1130]). Run twice with identical
arguments, minutes apart, with nothing pushed to `dev` in between, it described **two populations
six days apart** and reported both with full confidence::

    oldest run read: 2026-09-23T20:29:33Z   runs read: 13   a reading on 5   8 superseded
    oldest run read: 2026-09-17T13:49:45Z   runs read: 12   a reading on 9   3 superseded

🔑 **The verdict was stable while the evidence under it was not**, which is exactly why nobody
found it: both branches printed ``not-yet``, the answer nobody questions. And the failure is not
symmetric — the 09-17/09-18 window **predates every fix the graduation was waiting on**, so in
that branch the instrument was *structurally unable to grant graduation* while looking identical
to the branch that could.

🚨 **Why the existing suite could never have caught it, and this is the point of the file.**
[core#1480]'s control set varies the **subject**: a real spec of the job, a real spec of a
*different* job, and a fabricated name. All three still discriminate correctly. ``FakeActions`` in
``test_reader_subject_coverage.py`` even sorts its runs by ``(created_at, id)`` — it manufactures a
**total order and a single fixed window**, so the axis this defect lives on is held constant by the
harness itself. *A control set is a population claim like any other; ask which dimension it holds
fixed.*

So every fake here serves a **different payload to the second look** and varies nothing else.

### Two mechanisms, and only one of them is attributed

1. ✅ **Attributed, measured 2026-09-25.** Every push to `dev` starts **two** push workflows
   (``ci.yml`` and ``build-push-image.yml``) whose runs share a ``created_at`` to the second. On a
   real 25-run response ``created_at`` descends while ``id`` does **not**, and the pair order flips
   between tie groups inside one response. So ``per_page=N`` cuts a tie group, and **which member
   lands inside the page is not a function of the reader's arguments** — worth exactly the ±1
   ``ci.yml`` run that is ``13`` vs ``12`` in the reproduction. :meth:`Look.trimmed` drops the
   boundary tie group, because that is the group the page cannot prove it saw whole.
2. ❌ **Not attributed: the six-day branch.** The endpoint was measured stable over 8 back-to-back
   and 12 spaced samples in [core#1567], over 14 more separate processes while fixing it, and the
   reader's own helpers over 13 in-process calls. The reader therefore does **not** claim to have
   fixed it. It looks twice, refuses to grade a window the two looks disagree about, and prints
   both fingerprints so the next occurrence attributes itself. *An unattributed fix is a guess; an
   unattributed refusal is the honest half of one.*
"""

from __future__ import annotations

import re
import sys
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import pytest

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:  # pragma: no cover - import plumbing
    sys.path.insert(0, str(ROOT))

from scripts.e2e_tier_streak import WINDOW_UNSTABLE, Look, WindowAgreement  # noqa: E402

from scripts import e2e_tier_streak as streak  # noqa: E402  isort:skip

SPEC = "a11y-sweep.spec.ts"
CI = ".github/workflows/ci.yml"
IMG = ".github/workflows/build-push-image.yml"


def _log(spec_verdict: str) -> str:
    """A staging log green for ``SPEC``, so an accepted window graduates and a refused one cannot.

    Deliberately silent about the environment: `attested_environment` returns `None` for every
    log written before core#1232 and silence is not graded LOCAL, so the veto stays out of the
    way of the property under test.
    """
    return "\n".join(
        (
            "2026-09-24T10:00:00.0000000Z INFORMATIONAL_RESULT=success",
            f"2026-09-24T10:00:01.0000000Z INFORMATIONAL_SPEC_RESULT={SPEC}:{spec_verdict}",
        )
    )


def _pair(rid: int, created: str, *, ci_first: bool) -> list[dict]:
    """The two runs one push starts, sharing a ``created_at`` — the real tie group.

    ``ci_first`` is the tie-break, and it is the fact the listing does not pin: on the measured
    response the pair order flips from one group to the next inside a single page.
    """
    ci = {
        "id": rid,
        "path": CI,
        "status": "completed",
        "created_at": created,
        "head_sha": f"{rid:08x}".ljust(40, "0"),
    }
    img = {
        "id": rid + 1,
        "path": IMG,
        "status": "completed",
        "created_at": created,
        "head_sha": f"{rid:08x}".ljust(40, "0"),
    }
    return [ci, img] if ci_first else [img, ci]


def _window(day: int, n: int, *, ci_first: bool = True) -> list[dict]:
    """``n`` pushes on 2026-09-``day``, newest first — one page's worth of listing."""
    out: list[dict] = []
    for k in range(n):
        rid = 1000 + day * 100 + (n - k) * 2
        created = f"2026-09-{day:02d}T{20 - k:02d}:00:00Z"
        out.extend(_pair(rid, created, ci_first=ci_first))
    return out


class FakeActions:
    """The reader's `_gh` / `_gh_log_or_none` seam, serving a DIFFERENT window per look.

    ⚠️ Deliberately not the fake in ``test_reader_subject_coverage.py``: that one builds one
    sorted run list and slices it, which is a harness that cannot express this defect.
    """

    job_name = "staging / e2e-staging"

    def __init__(self, looks: list[list[dict]], *, spec_verdict: str = "success") -> None:
        #: One entry per look at the runs listing, consumed in order; the last repeats.
        self.looks = looks
        self.spec_verdict = spec_verdict
        self.listing_calls = 0

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
            runs = self.looks[min(self.listing_calls, len(self.looks) - 1)]
            self.listing_calls += 1
            per_page = int(q.get("per_page", 30))
            return {"workflow_runs": runs[:per_page], "total_count": len(runs)}
        raise AssertionError(f"the reader made a call this fake does not serve: {path}")

    def log(self, repo: str, job_id: int) -> tuple[str | None, str]:
        return _log(self.spec_verdict), ""


def _run(monkeypatch, capsys, fake: FakeActions, *argv: str) -> tuple[int, str]:
    monkeypatch.setattr(streak, "_gh", fake.gh)
    monkeypatch.setattr(streak, "_gh_log_or_none", fake.log)
    code = streak.main(["--job", "e2e-staging", "--branch", "dev", "--spec", SPEC, *argv])
    return code, capsys.readouterr().out


# ── the defect: two looks, two windows, one confident verdict ─────────────────────────────────


class TestTheWindowMustBeReproducible:
    def test_two_looks_six_days_apart_are_refused_not_graded(self, monkeypatch, capsys) -> None:
        """core#1567's own shape: look one is 09-23/09-24, look two is 09-17/09-18.

        Before the fix this graded whichever look it happened to get and printed a confident
        verdict. **Both real branches said `not-yet`**, so the test cannot assert on the verdict
        word alone — it asserts the reader SAYS the window moved.
        """
        fake = FakeActions([_window(24, 6), _window(17, 6)])
        code, out = _run(monkeypatch, capsys, fake)
        assert f"verdict        : {WINDOW_UNSTABLE}" in out
        assert "SHARE NO TIME AT ALL" in out
        assert code == 2, "2 is 'nothing could be measured' — slo_report.py's convention (§18a)"

    def test_the_refusal_names_both_windows_so_it_attributes_itself(
        self, monkeypatch, capsys
    ) -> None:
        """The cause is NOT isolated, so the output has to carry the evidence for next time.

        🔑 **The first line of this test was added because MUTATION found it missing.** Without
        it the assertions below pass against a comparator that always agrees: both looks' lines
        and both fingerprints are printed either way, so the test named for *attributing a
        refusal* was green when there was no refusal. A reader would not have found that; only
        the always-agree mutant did. (`QA_RULES` §2, WORKFLOW_RULES §4's third door.)
        """
        fake = FakeActions([_window(24, 6), _window(17, 6)])
        _, out = _run(monkeypatch, capsys, fake)
        assert f"verdict        : {WINDOW_UNSTABLE}" in out, "no refusal to attribute"
        assert "2026-09-24" in out and "2026-09-17" in out
        fps = re.findall(r"fp=([0-9a-f]{12})", out)
        assert len(fps) == 2, "both looks must print a fingerprint, or they cannot be compared"
        assert fps[0] != fps[1]

    def test_a_disagreement_inside_a_shared_stretch_is_refused_too(
        self, monkeypatch, capsys
    ) -> None:
        """The malignant case need not be disjoint: same days, one run swapped for another.

        A stale replica is the disjoint shape; a page whose contents are not a function of its
        arguments is this one. Both make the verdict unreproducible.
        """
        first = _window(24, 6)
        second = [dict(r) for r in first]
        second[4]["id"] = 999_001  # a different run, same instant, inside both looks' overlap
        second[5]["id"] = 999_002
        code, out = _run(monkeypatch, capsys, FakeActions([first, second]))
        assert f"verdict        : {WINDOW_UNSTABLE}" in out
        assert "DISAGREE about which runs lie between" in out
        assert code == 2

    def test_it_graduates_when_the_two_looks_agree(self, monkeypatch, capsys) -> None:
        """FALSE-POSITIVE CONTROL. Without it, a reader that refused everything scores the same.

        (Coordinator rule 10's inverse: *a guard that refuses everything is not discriminating,
        and the obvious repair is to loosen it.*)
        """
        fake = FakeActions([_window(24, 6)])
        code, out = _run(monkeypatch, capsys, fake)
        assert WINDOW_UNSTABLE not in out
        assert "verdict        : graduate" in out
        assert code == 0
        assert fake.listing_calls == 2, "count mode must look twice, or there is nothing to compare"

    def test_a_new_push_arriving_between_the_looks_is_not_a_disagreement(
        self, monkeypatch, capsys
    ) -> None:
        """FALSE-POSITIVE CONTROL, and the one that decides whether this is usable.

        `dev` takes several pushes an hour. A reader that refused whenever the head moved would
        refuse most of the time on a busy day, and a check that reds on a healthy system gets
        deleted. The rule is agreement about the stretch **both** looks cover, never identity.
        """
        first = _window(24, 6)
        second = _pair(9_999, "2026-09-24T21:00:00Z", ci_first=True) + first
        code, out = _run(monkeypatch, capsys, FakeActions([first, second]))
        assert WINDOW_UNSTABLE not in out, "a newer push is not a disagreement about the past"
        assert "so the window is reproducible" in out
        assert code == 0

    def test_both_ends_of_the_window_are_printed(self, monkeypatch, capsys) -> None:
        """AC1. One end cannot be compared with anything, which is why this went unnoticed."""
        _, out = _run(monkeypatch, capsys, FakeActions([_window(24, 6)]))
        assert re.search(r"^oldest run read: 2026-09-\d\dT", out, re.M)
        assert re.search(r"^newest run read: 2026-09-\d\dT", out, re.M)
        assert re.search(r"^window         : \d+ runs listed  fp=[0-9a-f]{12}", out, re.M)

    def test_since_mode_is_pinned_and_looks_once(self, monkeypatch, capsys) -> None:
        """AC3. ``--since`` was already deterministic and must not start paying for a second look.

        Its older end is an argument rather than a page boundary, so only the head can differ
        between two looks — and a run that arrived after the first look is not a disagreement
        about the past. The pinned mode pages up to ten times; doubling that would be real cost
        for a property it already has.
        """
        fake = FakeActions([_window(24, 6)])
        code, out = _run(monkeypatch, capsys, fake, "--since", "2026-09-22T00:00:00Z")
        assert "window         : pinned by --since" in out
        assert WINDOW_UNSTABLE not in out
        assert fake.listing_calls == 1
        assert code == 0


# ── the attributed half: the page boundary cuts a tie group ───────────────────────────────────


class TestTheBoundaryTieGroupIsNotAnArgument:
    """Measured on the real listing 2026-09-25, and the only half of core#1567 with a cause.

    ``created_at`` descends; ``id`` does not; the ``ci.yml``/``build-push-image.yml`` pair order
    flips between tie groups within one response. A ``per_page`` cut therefore keeps *either*
    member of the boundary group, which moves the count of graded runs by one for reasons the
    caller cannot see.
    """

    @pytest.mark.parametrize("ci_first", [True, False])
    def test_the_graded_window_is_the_same_whichever_member_the_page_kept(
        self, monkeypatch, capsys, ci_first: bool
    ) -> None:
        """The two tie-breaks must produce the SAME reading, and that is the whole property."""
        runs = _window(24, 6, ci_first=ci_first)
        code, out = _run(monkeypatch, capsys, FakeActions([runs]), "--runs", "11")
        line = next(ln for ln in out.splitlines() if ln.startswith("runs read"))
        assert code == 0
        assert "verdict        : graduate" in out
        # 11 of 12 listed: the cut lands inside the OLDEST pair, so that push is dropped whole.
        assert "runs read      : 5 " in line, line

    def test_trimmed_drops_only_the_boundary_group(self) -> None:
        look = Look((5, 4, 3, 2, 1), ("T3", "T3", "T2", "T1", "T1"))
        assert look.trimmed().ids == (5, 4, 3)

    def test_trimmed_refuses_to_empty_itself(self) -> None:
        """A coverage defect is not an acceptable price for a reproducibility one."""
        look = Look((3, 2, 1), ("T1", "T1", "T1"))
        assert look.trimmed().ids == (3, 2, 1)

    def test_an_empty_look_is_not_a_crash(self) -> None:
        empty = Look((), ())
        assert empty.trimmed().ids == ()
        assert empty.newest is None and empty.oldest is None

    def test_two_empty_looks_agree_rather_than_reporting_instability(self) -> None:
        """This assertion FAILED against the first cut of the fix, and that is why it is here.

        An empty listing is `no-data` — *"there is nothing here to read"* — which is the call
        `JobCoverage` already makes at zero runs considered. Grading it as instability refuses a
        new branch and every quiet weekend, and the natural repair for *"it refuses everything"*
        is to loosen the comparator until it permits the real defect too.
        """
        empty = Look((), ())
        assert WindowAgreement("count", empty, empty).state is None

    def test_one_empty_look_and_one_populated_one_is_instability(self) -> None:
        """The asymmetric case is a real signal and must not be folded into the one above."""
        empty = Look((), ())
        seen = Look((2, 1), ("T2", "T1"))
        assert WindowAgreement("count", seen, empty).state == WINDOW_UNSTABLE
        assert WindowAgreement("count", empty, seen).state == WINDOW_UNSTABLE


# ── controls on the comparator itself (QA_RULES §23: build the control into the guard) ────────


class TestTheComparatorCanSeeAndCanRefuse:
    """A comparator that always agrees and one that always refuses both pass a one-sided suite."""

    A = Look((4, 3, 2, 1), ("T4", "T3", "T2", "T1"))

    def test_identical_looks_agree(self) -> None:
        assert WindowAgreement("count", self.A, self.A).state is None

    def test_disjoint_looks_refuse(self) -> None:
        far = Look((8, 7, 6, 5), ("U4", "U3", "U2", "U1"))
        agreement = WindowAgreement("count", self.A, far)
        assert agreement.overlap is None
        assert agreement.state == WINDOW_UNSTABLE

    def test_a_swapped_run_inside_the_overlap_refuses(self) -> None:
        swapped = Look((4, 99, 2, 1), ("T4", "T3", "T2", "T1"))
        assert WindowAgreement("count", self.A, swapped).state == WINDOW_UNSTABLE

    def test_a_missing_second_look_is_not_a_refusal(self) -> None:
        """A single look cannot disagree with anything, and must not be reported as if it had."""
        assert WindowAgreement("count", self.A, None).state is None

    def test_the_pinned_mode_never_refuses(self) -> None:
        assert WindowAgreement.pinned().state is None
        assert "pinned by --since" in " ".join(WindowAgreement.pinned().render())
