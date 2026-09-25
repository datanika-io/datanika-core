"""Every load figure in ``docs/slo_targets.md`` must carry its provenance, per ROW (core#778).

🔴 **The defect this closes is a universality claim that nothing enforced.** The document's
callout has said *"Every load figure in this document traces to ... the terminated CPX31"* since
2026-09-11. Measured 2026-09-25: **4 of 7 latency rows and 1 of 5 throughput rows carried no
per-row marker at all.** The claim was in prose, in the right file, by the right department — and
the rows it claimed to cover did not implement it.

🔑 **Why per-row and not per-document.** A reader arrives at ``## Throughput SLOs`` through an
anchor, a search hit, or a grep. They never see a callout eighty lines above. So the marker has to
travel with the number, which is the same reason coordinator rule 32 exists: *a rule recorded as a
comment beside the data it governs is not a guard, and it is worse than nothing, because the
comment is what makes the next author believe the constraint is being looked after.*

⚠️ **This asserts the INVARIANT, not today's rows** (WORKFLOW_RULES §5a). It does not pin which
rows exist, how many there are, or what the figures say — only that a row stating a load rate also
states where that rate came from. Adding a row is free; adding an *unattributed* row is not.

⚠️ **And it accepts `measured` as readily as `inherited`.** A guard that only accepted "inherited"
would go red on the correct future state — the day a figure is re-measured on the current host —
which is §5a's *"red on the correct change"* failure. What is banned is a bare number.
"""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SLO = ROOT / "docs" / "slo_targets.md"

# A load RATE, not a latency and not a duration. `rps`/`req/s` are rates; `runs/min` is the
# Celery throughput unit this document uses. Deliberately excludes `ms`, `%` and bare seconds.
RATE = re.compile(r"\b\d+(?:\.\d+)?\s*(?:rps|req/s|runs/min)\b", re.IGNORECASE)

# Provenance: either the figure is inherited from hardware we no longer own, or it was measured
# and says when. Both are acceptable; a bare number is not.
PROVENANCE = re.compile(r"inherited|measured\s+20\d\d-\d\d-\d\d", re.IGNORECASE)


def _table_rows(text: str) -> list[tuple[int, str]]:
    """Markdown table body rows only — not the header, not the alignment row, not prose.

    The callout is a block quote (`> ...`) and must be excluded: it is the very text whose
    universality claim went unenforced, so counting it as a marker would let the document
    satisfy this guard exactly the way it already failed to.
    """
    rows = []
    for i, line in enumerate(text.splitlines(), start=1):
        s = line.strip()
        if not s.startswith("|") or s.startswith(">"):
            continue
        if set(s) <= set("|-: "):  # the alignment row
            continue
        rows.append((i, s))
    return rows


def test_every_slo_row_stating_a_load_rate_also_states_where_it_came_from():
    text = SLO.read_text(encoding="utf-8")
    rate_rows = [(n, r) for n, r in _table_rows(text) if RATE.search(r)]

    # Population control. A filter returning 0 of N has measured nothing, and this assertion is
    # the one that would silently pass if the regex, the row parser or the filename ever drifted.
    assert len(rate_rows) >= 5, (
        f"only {len(rate_rows)} table rows in {SLO.name} were recognised as stating a load rate. "
        f"This guard's whole value is its population; before believing the rows are clean, "
        f"establish that they were seen at all."
    )

    unmarked = [(n, r) for n, r in rate_rows if not PROVENANCE.search(r)]
    assert not unmarked, (
        "these rows state a load rate with no provenance, so they read as bars this deployment "
        "has cleared rather than bars a terminated machine once cleared:\n"
        + "\n".join(f"  line {n}: {r}" for n, r in unmarked)
    )


def test_the_guard_can_fail_and_does_not_fire_on_a_marked_row():
    """Both halves in one test: the corrected shape passes AND the pre-fix shape still fails.

    One without the other is how a control comes to pass by gutting its own guard. The two rows
    below are the real before/after of line 88 of the document.
    """
    before = "| **REST API — write** | POST | **500 ms** | 1500 ms | k6 at 10 rps sustained |"
    after = before[:-1] + "⚠️ inherited, see callout |"
    measured = before[:-1] + "measured 2026-09-24 on pointer.gr |"

    for label, row in (("pre-fix row", before),):
        assert RATE.search(row), f"{label}: the rate regex cannot see its own subject"
        assert not PROVENANCE.search(row), (
            f"{label}: the guard does not refuse an unattributed rate, so it would have passed "
            f"on the state it was written to end"
        )
    for label, row in (("inherited marker", after), ("measured marker", measured)):
        assert PROVENANCE.search(row), f"{label}: a correctly attributed row is refused"

    # A latency-only row must not be dragged into the population — that is how a guard becomes a
    # wall, and the obvious repair for a wall is to loosen it until it matches nothing.
    latency_only = "| **Health probes** | /healthz | **50 ms** | 150 ms | blackbox every 15 s |"
    assert not RATE.search(latency_only), (
        "the rate regex matched a row that states no rate; a guard that selects everything "
        "discriminates nothing"
    )


def test_the_callout_itself_does_not_satisfy_the_per_row_requirement():
    """The block quote is excluded by construction, and that exclusion is the point.

    If the callout counted, the document would satisfy this guard in exactly the configuration
    that failed: a universality claim in a box nobody scrolls to, over rows carrying nothing.
    """
    text = SLO.read_text(encoding="utf-8")
    assert "inherited" in text, "anchor gone — the callout no longer mentions provenance at all"
    quoted = [ln for ln in text.splitlines() if ln.strip().startswith(">") and RATE.search(ln)]
    assert quoted, (
        "no block-quoted line states a rate, so this test is no longer exercising the exclusion "
        "it exists to pin"
    )
    for ln in quoted:
        assert (n := ln.strip()) and n.startswith(">"), n
        assert not any(n == r for _, r in _table_rows(text)), (
            f"a block-quoted line was collected as a table row, so the callout can satisfy the "
            f"per-row requirement: {n}"
        )
