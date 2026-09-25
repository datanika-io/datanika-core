"""A throughput figure in application code must name the machine it was measured on.

core#1592. `api_middleware.py` carried *"k6 Run 4 (2026-04-18) measured 60 req/s at 100
VUs with 4 workers"* with no provenance marker, and it was the **only** req/s figure in
shipping application code — every other live citation was in `scripts/loadtest/`, `docs/`
or tests, all of which core#778's sweep covered. A reader in the middleware therefore had
no reason to suspect the number described a Hetzner CPX31 that was terminated on
2026-07-14.

🔑 **The invariant, not today's instance** (`WORKFLOW_RULES` §5a): *an unmarked inherited
claim is indistinguishable from a measurement.* That is `docs/slo_targets.md`'s own
sentence, and `tests/test_deploy/test_slo_load_rows_are_marked.py` enforces it there, per
row. Application code had no equivalent, which is why this figure survived a sweep that
found 107 citations.

⚠️ **This asserts the PRESENCE of a provenance marker, never the ABSENCE of "60"**
(`WORKFLOW_RULES` §4). A ban on the number would be satisfied by deleting the reasoning —
and the reasoning is the part that earns its place, since the 60-against-97 gap is the
evidence that requests were serializing before E12. It would also be satisfied by the
corrected text *explaining why 60 is wrong*, which is the denial-satisfies-the-ban trap.

The population is **1** today. That is not a reason to skip the guard: the cost of the
second one arriving unmarked is a reader treating a retired machine's number as current
capacity, and `test_the_scan_finds_the_known_figure` keeps the scan honest if it ever
drops to 0 for the wrong reason.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

#: `tests/test_deploy/…` -> repo root.
REPO = Path(__file__).resolve().parents[2]
APP = REPO / "datanika"

#: A throughput claim, in the forms this repo actually writes them. Deliberately not
#: matching bare integers: `limit = 60` in rate-limit code is a configured cap, not a
#: measurement, and flagging it would train readers to add noise markers.
THROUGHPUT = re.compile(r"\d+\s*(?:req/s|requests/s|req/sec|runs/min|rps\b)", re.IGNORECASE)

#: Any ONE of these, within `_WINDOW` lines, discharges the requirement. They are the
#: vocabulary `docs/slo_targets.md` and `LOAD_TEST_BASELINE_2026-04-21.md` already use, so
#: a marker that satisfies this guard is one a reader recognises.
PROVENANCE = (
    "cpx31",
    "pointer.gr",
    "slo_targets",
    "inherited",
    "terminated",
    "current-host",
    "current host",
)

#: A figure and its marker have to be close enough that one read catches both. 25 lines is
#: the length of a long module docstring; further away and the reader has scrolled.
_WINDOW = 25


def _python_sources() -> list[Path]:
    return sorted(p for p in APP.rglob("*.py") if "migrations" not in p.parts)


def _unmarked(text: str) -> list[tuple[int, str]]:
    """Throughput figures with no provenance marker within `_WINDOW` lines either way."""
    lines = text.splitlines()
    out = []
    for i, line in enumerate(lines):
        if not THROUGHPUT.search(line):
            continue
        window = "\n".join(lines[max(0, i - _WINDOW) : i + _WINDOW + 1]).lower()
        if not any(m in window for m in PROVENANCE):
            out.append((i + 1, line.strip()))
    return out


class TestEveryThroughputFigureNamesItsHardware:
    def test_no_unmarked_throughput_figure_in_application_code(self):
        offenders = {
            str(p.relative_to(REPO)): hits
            for p in _python_sources()
            if (hits := _unmarked(p.read_text(encoding="utf-8")))
        }
        assert not offenders, (
            f"Throughput figures with no provenance marker: {offenders}\n"
            "An unmarked inherited claim is indistinguishable from a measurement. Name the "
            "machine (e.g. 'the terminated Hetzner CPX31', 'pointer.gr, run 10') or point at "
            "docs/slo_targets.md, within 25 lines of the number."
        )

    def test_the_scan_finds_the_known_figure(self):
        """Anti-vacuity, and it is load-bearing here because the population is 1.

        If the regex stopped matching, the check above would pass over an empty set and
        read exactly like a clean tree. Assert the one figure we know exists is seen.
        """
        text = (APP / "services" / "api_middleware.py").read_text(encoding="utf-8")
        assert THROUGHPUT.search(text), (
            "the throughput regex no longer matches api_middleware.py's k6 figure, so the "
            "check above is scanning for a shape that is not there"
        )


class TestTheGuardDiscriminates:
    """A scan that cannot fail is decoration — drive it with both populations."""

    @pytest.mark.parametrize(
        "sample",
        [
            "# k6 Run 4 measured 60 req/s at 100 VUs\n",
            '"""Sustained 48 requests/s in the ladder."""\n',
            "# the worker cleared 60 runs/min\n",
        ],
    )
    def test_an_unmarked_figure_is_flagged(self, sample):
        assert _unmarked(sample), sample

    @pytest.mark.parametrize(
        "sample",
        [
            "# k6 Run 4 measured 60 req/s — on the terminated Hetzner CPX31\n",
            "# ~50 req/s, run 10, measured on pointer.gr\n",
            "# 48 req/s inherited; see docs/slo_targets.md\n",
        ],
    )
    def test_a_marked_figure_is_not_flagged(self, sample):
        assert not _unmarked(sample), sample

    @pytest.mark.parametrize("sample", ["limit = 60\n", "retry_after = 60  # seconds\n"])
    def test_a_configured_number_is_not_a_measurement(self, sample):
        """Negative control: the guard must not demand provenance for a config value.

        Without this, the cheapest way to go green is to sprinkle markers onto rate-limit
        constants, and the marker stops meaning anything.
        """
        assert not _unmarked(sample), sample

    def test_a_marker_far_from_the_figure_does_not_count(self):
        """Proximity is the point — a marker 40 lines away is not read with the number."""
        far = "# measured on the terminated CPX31\n" + ("#\n" * 40) + "# 60 req/s sustained\n"
        assert _unmarked(far), "a distant marker was accepted"
