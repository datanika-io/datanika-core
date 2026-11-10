"""Runbook `/metrics` checks must assert a SAMPLE LINE, never a metric name.

core#907. ``prometheus_client`` emits ``# HELP`` and ``# TYPE`` for a labelled metric
with **zero children** and no sample line, so::

    curl -sf .../metrics | grep -E "bytes_processed|bytes_quota"

returns non-empty for a counter that has never recorded anything. Measured on production
2026-09-01: all three V2 billing metrics were present as headers with 0 sample lines.

Two failure modes, and the second is the dangerous one:

* Today the P4 step is **unpassable** -- ``/metrics`` is not routed through Apache, so the
  public URL serves the Reflex SPA and the grep matches nothing.
* Adding the vhost entry alone would make it **unfailable**, which reads as the repair
  having worked, on the go/no-go path for charging customers.

Scope note: only fenced code blocks that actually read ``/metrics`` are examined. Prose
*describing* the anti-pattern must stay legal -- the warning is the most useful part of the
runbook -- and unrelated greps (``docker ps | grep datanika-app``) are not metric asserts.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

RUNBOOKS = sorted((Path(__file__).resolve().parents[2] / "docs" / "runbooks").glob("*.md"))

_ANCHORED = re.compile(r"\^[A-Za-z_][A-Za-z0-9_]*")
_VALUE = re.compile(r"(\\}|\}|\s)\s*\[0-9\]")
_METRICISH = re.compile(r"(datanika|bytes_|_total\b|_bucket\b|_count\b)")
_PUBLIC = re.compile(r"https?://[a-z.-]*datanika\.io")


def _blocks(text: str) -> list[str]:
    """Fenced code blocks only.

    Deliberately fenced-only, in both directions. Prose *describing* the anti-pattern is
    the most useful line in the runbook and must stay legal, so it is never scanned. The
    accepted cost is that a bad command written outside a fence is not caught -- these
    runbooks fence every command, and widening this to raw prose made the warning itself
    unwritable, which is a worse outcome than the gap.
    """
    return text.split("```")[1::2]


def _metrics_blocks(text: str) -> list[str]:
    return [b for b in _blocks(text) if "/metrics" in b]


def _grep_patterns(block: str) -> list[str]:
    out = []
    for line in block.splitlines():
        if "grep" not in line:
            continue
        for m in re.finditer(r"""grep[^'"\n]*(['"])(.+?)\1""", line):
            out.append(m.group(2))
    return out


_QUOTA_RUNBOOKS = (
    "RUNBOOK_V2_P3_STAGING_SMOKE_TEST.md",
    "RUNBOOK_V2_P4_FLAG_FLIP.md",
)
# The line cloud's route test serves for one seeded QuotaDecision (org 23).
_SERVED_QUOTA_SAMPLE = (
    'datanika_cloud_bytes_quota_rejected_total{org_id="23",reason="predicted_over_limit"} 1.0'
)
_QUOTA_TYPE_LINE = "# TYPE datanika_cloud_bytes_quota_rejected_total counter"
_QUOTA_CHECKBOX = re.compile(
    r"^- \[ \] `datanika_cloud_bytes_quota_rejected_total"
    r"\{org_id=\"…\",reason=\"…\"\}` has at least one sample line, value > 0",
    re.MULTILINE,
)
_KEPT_GREP_LINES = (
    "grep -E '^datanika_cloud_bytes_ledger_scrape_ok [0-9]' m.txt",
    "grep -E '^datanika_cloud_bytes_processed_total"
    '\\{([^}]*,)?org_id="[0-9]+"(,[^}]*)?\\} [0-9]\' m.txt',
)
_QUOTA_GOOD = (
    "```bash\n"
    "curl -sf http://127.0.0.1:8000/metrics > m.txt\n"
    "grep -E '^datanika_cloud_bytes_quota_rejected_total"
    '\\{org_id="[0-9]+",reason="[a-z_]+"\\} [0-9]\' m.txt\n'
    "```\n"
    '- [ ] `datanika_cloud_bytes_quota_rejected_total{org_id="…",reason="…"}` '
    "has at least one sample line, value > 0\n"
)


def _quota_sample_patterns(text: str) -> list[str]:
    """Fenced /metrics greps that anchor the quota counter and a value class."""
    found = []
    for block in _metrics_blocks(text):
        for pat in _grep_patterns(block):
            if not pat.startswith("^datanika_cloud_bytes_quota_rejected_total"):
                continue
            if _ANCHORED.search(pat) and _VALUE.search(pat):
                found.append(pat)
    return found


def unanchored_metric_greps(text: str) -> list[str]:
    """Greps on /metrics output that name a metric without anchoring a sample line."""
    bad = []
    for block in _metrics_blocks(text):
        for pat in _grep_patterns(block):
            if not _METRICISH.search(pat):
                continue
            if _ANCHORED.search(pat) and _VALUE.search(pat):
                continue
            bad.append(pat)
    return bad


def public_metrics_reads(text: str) -> list[str]:
    """Commands fetching /metrics over a public hostname (it serves the SPA)."""
    return [
        ln.strip()
        for block in _blocks(text)
        for ln in block.splitlines()
        if "/metrics" in ln and _PUBLIC.search(ln)
    ]


# --------------------------------------------------------------------------
# Negative controls -- the EXACT pre-fix shapes. If these stop being rejected,
# the guard is gutted and every assertion below is vacuous.
# --------------------------------------------------------------------------

PRE_FIX = (
    '```bash\ncurl -sf https://app.datanika.io/metrics | grep -E "bytes_processed|bytes_quota"\n```'
)
NAME_ONLY = (
    "```bash\n"
    "curl -sf http://127.0.0.1:8000/metrics"
    " | grep -E 'datanika_cloud_bytes_processed_total'\n"
    "```"
)
# The runbooks' real per-tenant grep. It used to be `\{org_id="[0-9]+"\} [0-9]`, which this guard
# accepts and which matches no line the collector renders: labels render sorted, as
# `{mode="…",org_id="…"}` (core#895). This file checks shape only; cloud's
# `test_bytes_metrics_reach_the_metrics_route.py` runs each grep against a served line.
GOOD = (
    "```bash\ncurl -sf http://127.0.0.1:8000/metrics > m.txt\n"
    r"""grep -E '^datanika_cloud_bytes_processed_total\{([^}]*,)?"""
    r"""org_id="[0-9]+"(,[^}]*)?\} [0-9]' m.txt"""
    "\n```"
)
GOOD_UNLABELLED = (
    "```bash\ncurl -sf http://127.0.0.1:8000/metrics > m.txt\n"
    "grep -E '^datanika_cloud_bytes_ledger_scrape_ok [0-9]' m.txt\n```"
)
PROSE_WARNING = (
    "A `curl https://app.datanika.io/metrics` returns the SPA, so never write the check that way."
)
UNRELATED_GREP = "```bash\ndocker ps --format '{{.Names}}' | grep datanika-app\n```"


def test_control_rejects_the_original_p4_command():
    assert unanchored_metric_greps(PRE_FIX) == ["bytes_processed|bytes_quota"]
    assert public_metrics_reads(PRE_FIX), "must reject the public-hostname read"


def test_control_rejects_a_name_only_grep():
    assert unanchored_metric_greps(NAME_ONLY) == ["datanika_cloud_bytes_processed_total"]


def test_control_accepts_a_labelled_sample_line_grep():
    assert unanchored_metric_greps(GOOD) == []
    assert public_metrics_reads(GOOD) == []


def test_control_accepts_the_unlabelled_scrape_ok_gauge():
    """No braces: an unlabelled gauge must not be forced to carry a label selector."""
    assert unanchored_metric_greps(GOOD_UNLABELLED) == []


def test_control_allows_prose_describing_the_antipattern():
    """The warning is the most useful line in the runbook; it must stay legal."""
    assert public_metrics_reads(PROSE_WARNING) == []


def test_control_ignores_greps_unrelated_to_metrics():
    """`docker ps | grep datanika-app` is not a metric assertion."""
    assert unanchored_metric_greps(UNRELATED_GREP) == []


def test_control_accepts_a_quota_rejection_sample_line():
    """A labelled rejection line is the shape the collector serves, not a header."""
    patterns = _quota_sample_patterns(_QUOTA_GOOD)
    assert patterns == [
        '^datanika_cloud_bytes_quota_rejected_total\\{org_id="[0-9]+",reason="[a-z_]+"\\} [0-9]'
    ]
    assert re.search(patterns[0], _SERVED_QUOTA_SAMPLE)
    assert re.search(patterns[0], _QUOTA_TYPE_LINE) is None
    assert _QUOTA_CHECKBOX.search(_QUOTA_GOOD)


def test_control_a_bare_quota_name_is_not_a_sample_line():
    """Naming the counter, outside a /metrics fence, is the pre-fix checkbox."""
    named = (
        "- [ ] `datanika_cloud_bytes_quota_rejected_total` — cannot be asserted from `/metrics`\n"
    )
    assert _quota_sample_patterns(named) == []
    assert _QUOTA_CHECKBOX.search(named) is None


# --------------------------------------------------------------------------
# The real assertions.
# --------------------------------------------------------------------------


def test_runbooks_exist():
    assert RUNBOOKS, "no runbooks found - path wrong, so every test below is vacuous"


@pytest.mark.parametrize("rb", RUNBOOKS, ids=lambda p: p.name)
def test_no_runbook_reads_metrics_over_a_public_hostname(rb: Path):
    offenders = public_metrics_reads(rb.read_text(encoding="utf-8"))
    assert not offenders, (
        f"{rb.name}: /metrics is not routed through Apache and serves the Reflex SPA "
        f"(200 text/html). Read it on the box against 127.0.0.1. Offending: {offenders}"
    )


@pytest.mark.parametrize("rb", RUNBOOKS, ids=lambda p: p.name)
def test_every_metric_grep_asserts_a_sample_line(rb: Path):
    offenders = unanchored_metric_greps(rb.read_text(encoding="utf-8"))
    assert not offenders, (
        f"{rb.name}: assert a sample line, never a metric name -- prometheus_client emits "
        f"HELP/TYPE for a labelled metric with zero children, so these cannot fail. "
        f"Offending: {offenders}"
    )


def _runbook(name: str) -> str:
    return next(p for p in RUNBOOKS if p.name == name).read_text(encoding="utf-8")


@pytest.mark.parametrize("name", _QUOTA_RUNBOOKS)
def test_quota_rejection_checkbox_asserts_a_served_sample_line(name: str):
    """cloud#146 acceptance 4. The grep has to match the line the collector serves."""
    text = _runbook(name)
    patterns = _quota_sample_patterns(text)
    assert len(patterns) == 1, patterns
    assert re.search(patterns[0], _SERVED_QUOTA_SAMPLE)
    assert re.search(patterns[0], _QUOTA_TYPE_LINE) is None
    assert _QUOTA_CHECKBOX.search(text)


@pytest.mark.parametrize("name", _QUOTA_RUNBOOKS)
def test_existing_bytes_greps_stay(name: str):
    text = _runbook(name)
    for line in _KEPT_GREP_LINES:
        assert line in text, line


@pytest.mark.parametrize("name", _QUOTA_RUNBOOKS)
def test_quota_runbooks_do_not_grep_a_paddle_series(name: str):
    text = _runbook(name)
    for block in _metrics_blocks(text):
        for pat in _grep_patterns(block):
            assert "paddle" not in pat, pat


def test_engineering_rules_keeps_a_single_bytes_grep():
    rules = Path(__file__).resolve().parents[2] / "docs" / "ENGINEERING_RULES.md"
    text = rules.read_text(encoding="utf-8")
    found = re.findall(r"grep -E '(\^datanika_cloud_bytes_[^']*)'", text)
    assert found == [
        '^datanika_cloud_bytes_processed_total\\{([^}]*,)?org_id="[0-9]+"(,[^}]*)?\\} [0-9]'
    ]
