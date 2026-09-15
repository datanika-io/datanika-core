"""The tracked load profile must assert the SLOs the document actually publishes.

core#778. The only load baseline this project has
(`plans/infra/LOAD_TEST_BASELINE_2026-04-21.md`) records k6 results whose script was
never committed — so it is not reproducible even in principle, independently of the
machine having been terminated on 2026-07-14. `tests/load/api_read.js` is the tracked
replacement.

A tracked profile is only worth having if it measures what the document commits to.
These tests pin the two together, so a threshold cannot be relaxed in the profile to
make a run pass while `docs/slo_targets.md` keeps publishing the stricter number —
which is the same defect as relaxing an alert rule to match production, and the
reason that document already forbids the registry from restating its thresholds.

They also pin the two things a load profile most easily lies about:

* that it makes any requests at all — zero requests posts perfect latency;
* that its numbers are recorded as a FLOOR UNDER CONTENTION, because staging and
  production share one machine and load applied is load taken.
"""

from __future__ import annotations

import re
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
_PROFILE = _ROOT / "tests" / "load" / "api_read.js"
_TARGETS = _ROOT / "docs" / "slo_targets.md"


def _profile() -> str:
    assert _PROFILE.is_file(), f"tracked load profile missing at {_PROFILE}"
    text = _PROFILE.read_text(encoding="utf-8")
    assert len(text) > 1000, "profile is implausibly short; did something truncate it?"
    return text


def _targets() -> str:
    assert _TARGETS.is_file(), f"{_TARGETS} missing"
    return _TARGETS.read_text(encoding="utf-8")


class TestTheProfileAssertsThePublishedNumbers:
    def test_read_p95_matches_the_document(self) -> None:
        assert "p(95)<200" in _profile(), "read p95 threshold is not the published 200 ms"
        assert "**200 ms**" in _targets(), (
            "docs/slo_targets.md no longer publishes 200 ms for REST reads -- the "
            "profile and the document have drifted; change both or neither."
        )

    def test_agent_p95_matches_the_document(self) -> None:
        assert "p(95)<150" in _profile()
        assert "**150 ms**" in _targets()

    def test_health_p95_matches_the_document(self) -> None:
        assert "p(95)<50" in _profile()
        assert "**50 ms**" in _targets()

    def test_every_endpoint_it_drives_is_read_only(self) -> None:
        """Staging shares a database host with production. A mutating profile could
        leave residue there, so the safety property is asserted, not intended."""
        text = _profile()
        for forbidden in ("http.post(", "http.put(", "http.del(", "http.patch("):
            assert forbidden not in text, (
                f"{forbidden} in a profile that runs against a box shared with production"
            )


class TestItCannotPassWithoutMeasuringAnything:
    def test_there_is_a_floor_on_request_count(self) -> None:
        """Zero requests yields perfect latency and would otherwise pass every
        threshold -- the exact shape this repository keeps finding elsewhere."""
        assert "http_reqs" in _profile()
        assert re.search(r"http_reqs:\s*\[`count>", _profile()), (
            "no floor on requests made: a run that issued nothing would report clean"
        )

    def test_the_summary_says_so_when_nothing_ran(self) -> None:
        assert "NO REQUESTS WERE MADE" in _profile()
        assert "not a pass" in _profile(), (
            "the empty-run message must say it is not a pass, not merely state the count"
        )


class TestTheContentionCaveatTravelsWithTheNumber:
    def test_the_profile_prints_the_caveat_in_its_summary(self) -> None:
        """A caveat that lives only in a document is a caveat that gets separated from
        the number the first time someone quotes it in a message."""
        text = _profile()
        assert "FLOOR UNDER CONTENTION" in text
        assert "handleSummary" in text, "the caveat must be emitted with the result"

    def test_the_targets_document_carries_the_inherited_marking(self) -> None:
        """core#778's other half: the published rps figures came from a terminated box."""
        t = _targets()
        assert "INHERITED" in t, "docs/slo_targets.md is not marked as inherited"
        assert "terminated" in t
        assert "2026-07-14" in t, "the marking must name WHEN the hardware went away"
        assert "floor under contention" in t.lower(), (
            "the document must state what a live-box figure would and would not mean"
        )
