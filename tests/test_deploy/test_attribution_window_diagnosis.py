"""The window refusal must say WHICH window it looked at (core#1462).

`verify_e2e_attribution.py` refuses when no staging job carries the head, and
`promotion_gate.sh` turns that into exit 3. **The refusal is correct and is not what this
file changes.** What it could not do is tell a promoter which of its own two explanations
had happened:

    Either widen --pages, or CI genuinely has not run on it

On 2026-09-17 **neither had.** Measured three times on head `c321f2eb`:

* 15:51:45Z — `promotion_gate.sh` -> the window message, exit 3.
* 15:52:37Z — the same script, same worktree, same interpreter, same default `--pages`,
  52 seconds later -> exit 0, with all three verdicts attributed to that commit's own
  deploy at 15:31:22Z.
* 2026-09-18 00:20Z — exit 0 again, and the window read `newest=2026-09-17T15:08:51Z`,
  i.e. the head's own CI run was *inside* the window. `actions/runs?created>=15:52`
  returned `total_count=0`, so no run was created between the two readings: both windows
  contain the same objects. The list response the first call received did not contain runs
  that already existed.

So the refusal named a cause that had not happened, twice over. The cost is not cosmetic —
a promoter who reads *"CI genuinely has not run"* goes and waits for CI that already ran.

Two changes are pinned here:

1. **The refusal prints the window's own boundaries** — how many runs came back, how many
   were `ci.yml`, the newest and oldest `created_at`, and how many carried the head. With
   those, *"the window ends before my commit"* (scrolled) and *"the window spans my commit
   and still lacks it"* (a stale read) stop looking identical.
2. **One re-read before concluding.** Cheap, and the only thing that separates a transient
   list response from a real absence. The refusal stands on the *second* reading.

⚠️ The exit code is deliberately untouched. A promoter who sees exit 3 and re-runs is the
correct outcome; a promoter who cannot tell why is the cost. `test_the_refusal_is_not_relaxed`
and `test_no_verdict_file_is_written_on_the_window_path` are the guards on that, and they
are the ones to read first if this file ever goes red.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import scripts.verify_e2e_attribution as vea  # noqa: E402

FULL = "c321f2ebcce037c444922c30d3b72ee16708b610"
OTHER = "99aa88bb77cc66dd55ee44ff33001122334455aa"
SHORT = FULL[:8]
MUT = "deploy-staging"


def job(sha: str, name: str, started: str, completed: str, conclusion: str | None) -> vea.Job:
    return vea.Job(
        run_id=1,
        head_sha=sha,
        name=name,
        started_at=started,
        completed_at=completed,
        conclusion=conclusion,
    )


def clean_jobs(sha: str) -> list[vea.Job]:
    """A commit whose staging verdicts all describe its own build."""
    return [
        job(sha, MUT, "2026-09-17T15:25:00Z", "2026-09-17T15:31:22Z", "success"),
        job(sha, "smoke-staging", "2026-09-17T15:32:00Z", "2026-09-17T15:33:00Z", "success"),
        job(sha, "e2e-staging", "2026-09-17T15:32:00Z", "2026-09-17T15:40:00Z", "success"),
        job(sha, "e2e-sso", "2026-09-17T15:32:00Z", "2026-09-17T15:36:00Z", "success"),
    ]


def window(**kw) -> vea.Window:
    base = {
        "returned": 100,
        "ci_runs": 48,
        "newest": "2026-09-17T15:08:51Z",
        "oldest": "2026-09-15T23:36:01Z",
        "for_head": 0,
    }
    base.update(kw)
    return vea.Window(**base)


@pytest.fixture
def no_network(monkeypatch):
    def _refuse(path):  # pragma: no cover - reaching here means the test hit the network
        raise AssertionError(f"unexpected API call: {path}")

    monkeypatch.setattr(vea, "_gh", _refuse)


@pytest.fixture
def stub_window(monkeypatch):
    """The refusal path's two extra reads, stubbed. Overridden per-test where it matters."""
    monkeypatch.setattr(vea, "read_window", lambda *a, **k: window())
    monkeypatch.setattr(vea, "commit_date", lambda *a, **k: None)


# ── one re-read before concluding ───────────────────────────────────────────────────────


def test_a_transient_empty_first_read_is_recovered_by_the_re_read(no_network, monkeypatch, capsys):
    """The measured 2026-09-17 case: the commit was there, the first list response was not.

    Without the re-read this run exits 1 and `promotion_gate.sh` turns it into exit 3 — a
    promotion blocked on a list response.
    """
    reads = iter([[], clean_jobs(FULL)])
    monkeypatch.setattr(vea, "collect", lambda *a, **k: next(reads))
    rc = vea.main(["--sha", SHORT, "--branch", "dev"])
    out = capsys.readouterr().out
    assert rc == 0, out
    assert "attributed" in out
    # It must SAY it needed a second reading — a silent recovery hides a real API symptom.
    assert "second reading" in out


def test_the_re_read_happens_exactly_once(no_network, monkeypatch, stub_window):
    """Not a retry loop. One read, one re-read, then the refusal stands."""
    calls = []

    def _collect(*a, **k):
        calls.append(1)
        return []

    monkeypatch.setattr(vea, "collect", _collect)
    assert vea.main(["--sha", SHORT, "--branch", "dev"]) == 1
    assert len(calls) == 2, f"expected one read plus one re-read, got {len(calls)}"


def test_an_ambiguous_prefix_found_only_on_the_re_read_is_still_reported_as_ambiguous(
    no_network, monkeypatch, stub_window, capsys
):
    """The re-read must not swallow an ambiguity into the window refusal.

    Drafting this change, the first version fell through to "no staging jobs found ... in
    TWO consecutive readings" when the re-read returned two matching commits — a refusal
    naming an absence, for a result that was the opposite of absent.
    """
    a = "abc12300" + "0" * 32
    b = "abc12311" + "1" * 32
    reads = iter([[], clean_jobs(a) + clean_jobs(b)])
    monkeypatch.setattr(vea, "collect", lambda *a_, **k: next(reads))
    rc = vea.main(["--sha", "abc123", "--branch", "dev"])
    out = capsys.readouterr().out
    assert rc == 1
    assert "ambiguous" in out
    assert "Pass more characters" in out
    assert "TWO consecutive readings" not in out


# ── the refusal names its own window ────────────────────────────────────────────────────


def test_the_refusal_prints_the_window_boundaries(no_network, monkeypatch, stub_window, capsys):
    monkeypatch.setattr(vea, "collect", lambda *a, **k: clean_jobs(OTHER))
    rc = vea.main(["--sha", SHORT, "--branch", "dev"])
    out = capsys.readouterr().out
    assert rc == 1
    assert "returned=100" in out
    assert "ci.yml-runs=48" in out
    assert "runs-for-the-head=0" in out
    assert "newest=2026-09-17T15:08:51Z" in out
    assert "oldest=2026-09-15T23:36:01Z" in out


def test_a_window_that_spans_the_commit_is_named_a_stale_read(no_network, monkeypatch, capsys):
    """The 2026-09-17 shape: the commit's date is INSIDE the window and still absent.

    This is the discrimination the issue asks for. *"Widen --pages"* is the wrong advice
    here and sends the promoter to do nothing useful.
    """
    monkeypatch.setattr(vea, "collect", lambda *a, **k: clean_jobs(OTHER))
    monkeypatch.setattr(vea, "read_window", lambda *a, **k: window())
    monkeypatch.setattr(vea, "commit_date", lambda *a, **k: "2026-09-16T12:00:00Z")
    rc = vea.main(["--sha", SHORT, "--branch", "dev"])
    out = capsys.readouterr().out
    assert rc == 1
    assert "STALE READ" in out
    assert "the window already spans" in out
    # And it must not send the promoter to widen a window that already spans the commit.
    assert "Raising --pages will not help" in out


def test_a_commit_older_than_the_window_is_named_a_scrolled_window(no_network, monkeypatch, capsys):
    monkeypatch.setattr(vea, "collect", lambda *a, **k: clean_jobs(OTHER))
    monkeypatch.setattr(vea, "read_window", lambda *a, **k: window())
    monkeypatch.setattr(vea, "commit_date", lambda *a, **k: "2026-09-10T08:00:00Z")
    rc = vea.main(["--sha", SHORT, "--branch", "dev"])
    out = capsys.readouterr().out
    assert rc == 1
    assert "SCROLLED WINDOW" in out
    assert "Raise --pages" in out
    assert "STALE READ" not in out


def test_an_unavailable_window_says_so_rather_than_inventing_one(no_network, monkeypatch, capsys):
    """`read_window` returning None must not become a fabricated boundary.

    An absence reported as a number is the defect this whole script exists to catch.
    """
    monkeypatch.setattr(vea, "collect", lambda *a, **k: clean_jobs(OTHER))
    monkeypatch.setattr(vea, "read_window", lambda *a, **k: None)
    monkeypatch.setattr(vea, "commit_date", lambda *a, **k: None)
    rc = vea.main(["--sha", SHORT, "--branch", "dev"])
    out = capsys.readouterr().out
    assert rc == 1
    assert "window boundaries unavailable" in out
    assert "returned=" not in out


def test_an_unknown_commit_date_refuses_to_pick_an_explanation(
    no_network, monkeypatch, stub_window, capsys
):
    """No date, no verdict on which explanation applies — the boundaries still print."""
    monkeypatch.setattr(vea, "collect", lambda *a, **k: clean_jobs(OTHER))
    rc = vea.main(["--sha", SHORT, "--branch", "dev"])
    out = capsys.readouterr().out
    assert rc == 1
    assert "returned=100" in out
    assert "cannot be settled from here" in out
    assert "STALE READ" not in out
    assert "SCROLLED WINDOW" not in out


# ── the refusal itself is untouched ─────────────────────────────────────────────────────


def test_the_refusal_is_not_relaxed(no_network, monkeypatch, stub_window, capsys):
    """A genuinely absent commit still exits 1, and still reads as a WINDOW result.

    🔑 Read this first if this file goes red. The issue explicitly asks NOT to weaken the
    exit code: the diagnosis was missing, not the refusal.
    """
    monkeypatch.setattr(vea, "collect", lambda *a, **k: clean_jobs(OTHER))
    rc = vea.main(["--sha", SHORT, "--branch", "dev"])
    out = capsys.readouterr().out
    assert rc == 1
    assert "::warning::" in out
    assert "::error::" not in out
    assert "WINDOW result" in out
    assert "neither" in out and "pass" in out


def test_no_verdict_file_is_written_on_the_window_path(
    no_network, monkeypatch, stub_window, tmp_path
):
    """The window path must leave NO artifact — `promotion_gate.sh` exit 3 depends on it.

    An artifact here would be graded as a verdict, and the gate would report exit 4 ("a
    reading that says no") for something that produced no reading at all.
    """
    monkeypatch.setattr(vea, "collect", lambda *a, **k: clean_jobs(OTHER))
    vf = tmp_path / "attribution.verdict"
    assert vea.main(["--sha", SHORT, "--branch", "dev", "--verdict-file", str(vf)]) == 1
    assert not vf.exists(), "the window path must produce no verdict artifact"


# ── negative controls: each guard seen able to fail ─────────────────────────────────────


def test_the_guards_can_fail():
    """Pre-fix semantics, expressed as the text and data the old code would have produced.

    Without this, every assertion above is a green that would look identical had the
    feature never been written.
    """
    # (a) The old refusal carried no window numbers and no verdict on which cause applied.
    old_refusal = (
        "::warning::no staging jobs found for c321f2eb in the last 100 runs on `dev`.\n"
        "This is a WINDOW result, not an attribution failure: the scan did not reach this\n"
        "commit. Either widen --pages, or CI genuinely has not run on it — and neither\n"
        "of those is a pass."
    )
    assert "returned=" not in old_refusal
    assert "newest=" not in old_refusal
    assert "STALE READ" not in old_refusal
    assert "SCROLLED WINDOW" not in old_refusal
    # ...and it still satisfies the untouched-refusal guard, which is the point: that guard
    # must NOT be what proves the fix landed.
    assert "WINDOW result" in old_refusal
    assert "neither" in old_refusal and "pass" in old_refusal

    # (b) Without a re-read, the measured transient loses the promotion.
    reads = iter([[], clean_jobs(FULL)])
    assert not [j for j in next(reads) if j.head_sha.startswith(SHORT)]
    assert [j for j in next(reads) if j.head_sha.startswith(SHORT)]
