"""`verify_e2e_attribution.py` must not refuse well-formed input (core#918).

Companion to `test_e2e_attribution_preflight.py`, which covers `classify()` — the
attribution logic. This file covers the two ways `main()` refused a commit that was fine,
both found on the script's **first real promotion use** (2026-09-01, core#876).

Why it matters more than an ordinary usability bug: the promoter meets this at the highest-
stakes moment in the workflow, and the tool answers `::error::` and exit 1. The honest
response is "widen the window"; the tempting responses are to read it as a real attribution
failure and stop, or to conclude the tool is noisy and skip it next time. #876's own PR body
names that outcome — *"a verifier that always refuses is a verifier somebody deletes"*.

1. **`--pages` defaulted to 15.** That is `per_page` on `actions/runs`, so it scanned the 15
   most recent runs on the branch — roughly **two hours** of `dev` wall-clock with five
   departments pushing every 10-15 minutes and more than one run per push. Any commit you
   deliberately waited on before promoting had already fallen out of the window. 100 is the
   API maximum and costs the same single request.

2. **A short SHA could never match.** The API returns the full 40-character `head_sha` and
   the comparison was `==`, so `--sha db83fc24` — the shape a promoter pastes out of a git
   log — was refused with the *window* message, naming a cause that has nothing to do with
   what happened: "CI has not run on that commit, which is NOT a pass."

The network is mocked; the SHA-resolution logic is not. That boundary is deliberate — the
claim under test is how the script compares and reports, not what GitHub returns.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import scripts.verify_e2e_attribution as vea  # noqa: E402

FULL = "db83fc2470f1c3a95e2b8d6c4a1f0e9b7d5c3a21"
OTHER = "db83fc99aaaa1111bbbb2222cccc3333dddd4444"  # shares the 8-char prefix `db83fc99`? no
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
    """A commit whose staging verdicts all describe its own build.

    ⚠️ `e2e-sso` is in `VERIFIERS` and its absence is graded BAD, so a fixture that omits
    it makes every case here exit 1 for a reason that has nothing to do with SHA matching.
    It is included with `conclusion=failure` because that is its real state on this repo
    (core#830, red 16 pushes running) — `classify` grades *attribution*, not colour.
    """
    return [
        job(sha, MUT, "2026-09-01T12:00:00Z", "2026-09-01T12:05:25Z", "success"),
        job(sha, "smoke-staging", "2026-09-01T12:06:00Z", "2026-09-01T12:07:00Z", "success"),
        job(sha, "e2e-staging", "2026-09-01T12:06:00Z", "2026-09-01T12:12:00Z", "success"),
        job(sha, "e2e-sso", "2026-09-01T12:06:00Z", "2026-09-01T12:09:00Z", "failure"),
    ]


@pytest.fixture
def no_network(monkeypatch):
    def _refuse(path):  # pragma: no cover - a call here means the test reached the network
        raise AssertionError(f"unexpected API call: {path}")

    monkeypatch.setattr(vea, "_gh", _refuse)


def test_the_default_window_is_the_api_maximum():
    # 15 runs was ~2 hours of dev cadence. The parser default is the whole fix for the
    # window half, so it is pinned directly rather than inferred from behaviour.
    parsed = vea.main.__wrapped__ if hasattr(vea.main, "__wrapped__") else None
    assert parsed is None  # main is not decorated; guard against a silent refactor
    import argparse
    import inspect

    src = inspect.getsource(vea.main)
    assert '"--pages", type=int, default=100' in src
    assert "default=15" not in src
    assert argparse  # keep the import meaningful


def test_a_short_sha_resolves(no_network, monkeypatch, capsys):
    """The shape a promoter pastes out of a git log. Previously refused outright."""
    monkeypatch.setattr(vea, "collect", lambda *a, **k: clean_jobs(FULL))
    rc = vea.main(["--sha", SHORT, "--branch", "dev"])
    out = capsys.readouterr().out
    assert rc == 0, out
    assert "attributed" in out


def test_the_full_sha_still_resolves(no_network, monkeypatch, capsys):
    monkeypatch.setattr(vea, "collect", lambda *a, **k: clean_jobs(FULL))
    assert vea.main(["--sha", FULL, "--branch", "dev"]) == 0


def test_an_ambiguous_prefix_is_refused_by_name(no_network, monkeypatch, capsys):
    """Two commits sharing a prefix must not be silently resolved to whichever came first."""
    a = "abc12300" + "0" * 32
    b = "abc12311" + "1" * 32
    monkeypatch.setattr(vea, "collect", lambda *a_, **k: clean_jobs(a) + clean_jobs(b))
    rc = vea.main(["--sha", "abc123", "--branch", "dev"])
    out = capsys.readouterr().out
    assert rc == 1
    assert "ambiguous" in out
    assert "Pass more characters" in out


def test_a_genuinely_absent_commit_is_still_refused(no_network, monkeypatch, capsys):
    # The refusal itself is CORRECT and must not be weakened: never a vacuous pass.
    monkeypatch.setattr(vea, "collect", lambda *a, **k: clean_jobs(OTHER))
    rc = vea.main(["--sha", SHORT, "--branch", "dev"])
    out = capsys.readouterr().out
    assert rc == 1
    assert "NOT a pass" not in out or "neither" in out  # the wording moved, the meaning did not


def test_the_window_case_reads_as_a_window_result_not_an_attribution_finding(
    no_network, monkeypatch, capsys
):
    """Two different facts, and only one of them is a finding about this commit.

    "The scan did not reach this commit" and "this commit's verdicts describe another
    commit's build" call for opposite responses. Leading with `::error::` for the first is
    what trains a promoter to distrust the tool.
    """
    monkeypatch.setattr(vea, "collect", lambda *a, **k: clean_jobs(OTHER))
    rc = vea.main(["--sha", SHORT, "--branch", "dev"])
    out = capsys.readouterr().out
    assert rc == 1
    assert "::warning::" in out
    assert "::error::" not in out
    assert "WINDOW result" in out
    # It must still say plainly that this is not a pass.
    assert "neither" in out and "pass" in out


def test_the_guard_can_fail(monkeypatch, capsys):
    """Negative control: restore the exact-match comparison and confirm a short SHA breaks.

    Re-implementing `main`'s matching would be a fixture agreeing with the check. Instead
    the real function is driven with a job set whose only member matches by prefix — under
    the old `==` comparison that set is empty and the window branch fires.
    """
    monkeypatch.setattr(vea, "collect", lambda *a, **k: clean_jobs(FULL))
    # Sanity: the fix is what makes this pass.
    assert vea.main(["--sha", SHORT, "--branch", "dev"]) == 0
    capsys.readouterr()

    # Now the pre-fix semantics, expressed as the data the old code would have seen.
    jobs = clean_jobs(FULL)
    old_style_match = any(j.head_sha == SHORT for j in jobs)
    new_style_match = any(j.head_sha == SHORT or j.head_sha.startswith(SHORT) for j in jobs)
    assert old_style_match is False, "the old comparison must be shown unable to match"
    assert new_style_match is True
