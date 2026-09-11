"""The promotion pre-flight must not crash when `e2e_tier_streak`'s contract moves.

core#1285. `scripts/verify_e2e_attribution.py` imports `_gh_log_or_none`,
`parse_verdict_line`, `classify_verdict` and `parse_specs_outcome` out of
`scripts/e2e_tier_streak.py` -- deliberately, because two independent definitions
of the same verdict drift apart invisibly. The cost of sharing them is that a
change on one side can break the other, and that has now happened **twice**:

* core#1205: `parse_verdict_line` began refusing a log carrying two tiers unless
  the caller named one. This caller named none, so it raised on every
  `e2e-staging` log.
* core#1273/#1285: `_gh_log_or_none` began returning `(log, reason)` so the reason
  would stop being discarded. This caller still unpacked a bare string and died
  with `'tuple' object has no attribute 'splitlines'`.

Both times the gate **crashed at the moment it was being relied on**, and a crash
is neither a pass nor a refusal -- it is no reading at all, from the tool whose
entire purpose is to produce one. Both times it was found by a human running a
promotion, not by CI.

These tests exercise the seam with the network stubbed, so a signature change
fails here instead of on the next promotion.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

_SCRIPTS = Path(__file__).resolve().parents[2] / "scripts"


def _load(name: str):
    path = _SCRIPTS / f"{name}.py"
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader, f"cannot load {path}"
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod  # before exec: dataclasses resolve hints through it
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture(scope="module")
def streak():
    if str(_SCRIPTS) not in sys.path:
        sys.path.insert(0, str(_SCRIPTS))
    return _load("e2e_tier_streak")


@pytest.fixture(scope="module")
def preflight(streak):
    return _load("verify_e2e_attribution")


_LOG = "\n".join(
    [
        "some preamble",
        "E2E_RESULT=PASS",
        "GATING_RESULT=PASS",
        "collected 42 items",
    ]
)


def _job(preflight, name: str = "e2e-staging", sha: str = "abc123"):
    return preflight.Job(
        run_id=1,
        head_sha=sha,
        name=name,
        started_at="2026-09-11T10:00:00Z",
        completed_at="2026-09-11T10:05:00Z",
        conclusion="success",
        job_id=99,
    )


class TestTheSharedHelperStillReturnsWhatTheCallerUnpacks:
    def test_gh_log_or_none_returns_a_two_tuple(self, streak) -> None:
        """The contract itself, asserted where a change to it is visible.

        Checked via the annotation rather than a live call: the live call needs
        the network and a real job id, and a test that cannot run offline is a
        test that gets skipped exactly when the suite is being trusted.
        """
        ann = streak._gh_log_or_none.__annotations__.get("return")
        assert ann is not None, "the helper lost its return annotation; contract unpinnable"
        text = str(ann)
        assert "tuple" in text.lower(), (
            f"`_gh_log_or_none` no longer returns a tuple (now {text!r}). "
            "scripts/verify_e2e_attribution.py unpacks two values and will crash."
        )

    def test_the_preflight_consumes_the_tuple_without_crashing(
        self, streak, preflight, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(streak, "_gh_log_or_none", lambda repo, job_id: (_LOG, "ok"))
        out = preflight.verdict_classes_for("owner/repo", [_job(preflight)], "abc123")
        assert isinstance(out, dict)

    def test_an_unreadable_log_is_reported_not_swallowed(
        self, streak, preflight, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
    ) -> None:
        """core#1273's whole point: the REASON must reach the operator.

        Returning `None` for every failure alike once produced "18 attempts, all
        failed" with not one word about why.
        """
        monkeypatch.setattr(streak, "_gh_log_or_none", lambda repo, job_id: (None, "404 expired"))
        preflight.verdict_classes_for("owner/repo", [_job(preflight)], "abc123")
        err = capsys.readouterr().err
        assert "404 expired" in err, "the reason was discarded again"
        assert "e2e-staging" in err, "the note does not say which job"


class TestTheNegativeControlStillControls:
    def test_a_bare_string_return_breaks_the_preflight(
        self, streak, preflight, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Anti-vacuity: prove the caller genuinely unpacks two values.

        If this ever stops raising, the test above is passing because the caller
        tolerates both shapes -- at which point it is asserting nothing, and the
        contract it exists to pin has quietly stopped being pinned.
        """
        monkeypatch.setattr(streak, "_gh_log_or_none", lambda repo, job_id: _LOG)
        with pytest.raises((ValueError, AttributeError, TypeError)):
            preflight.verdict_classes_for("owner/repo", [_job(preflight)], "abc123")

    def test_every_name_the_preflight_imports_still_exists(self, streak) -> None:
        """The other half of the seam. core#1205 moved one of these under us."""
        for name in (
            "_gh_log_or_none",
            "classify_verdict",
            "parse_specs_outcome",
            "parse_verdict_line",
        ):
            assert hasattr(streak, name), (
                f"verify_e2e_attribution imports {name!r} from e2e_tier_streak, "
                "which no longer defines it -- the pre-flight will fail at import."
            )
