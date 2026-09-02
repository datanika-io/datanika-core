"""Every SLO in ``docs/slo_targets.md`` must have a named instrument, or fail.

The defect this guards against
------------------------------
``docs/slo_targets.md`` shipped 2026-04-14 with 26 numeric commitments and no
instrument read any of them until 2026-09-01 ([core#721]). A target nothing
measures is indistinguishable from a target being met — it can be neither
violated nor achieved — so the document read as a commitment while committing to
nothing.

Adding a row to that document is therefore not a free act. This module makes it
cost something: a new SLO with no decision about how it is measured **fails the
build**.

What this guard can and cannot see
----------------------------------
It runs offline, in CI, with no Prometheus. So it verifies **structure**:

* every documented SLO has a registry decision, and every decision corresponds
  to a documented SLO (both directions — a stale registry entry is as much a
  defect as a missing one);
* no registry entry restates a threshold, because a registry that carried its
  own copy of a number could be quietly relaxed to match production;
* the currently-unmeasured set matches a checked-in baseline **exactly**, so the
  count can go down and never up, and a swap cannot hide inside a stable total;
* for ``source: app`` entries, the metric named in the query actually exists in
  ``datanika/services/metrics.py``.

It does **not** verify that a query measures the right thing. Nothing static
can. ``scripts/slo_report.py`` run against production is what does that, and
``docs/slo_baseline.md`` records the answer.

[core#721]: https://github.com/datanika-io/datanika-core/issues/721
"""

from __future__ import annotations

import re
import shutil
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "scripts"))

from slo_report import (  # noqa: E402
    EXPECTED_SECTION_COUNTS,
    MAX,
    MIN,
    NEEDS_INSTRUMENT,
    NO_VERDICT,
    NO_VERDICT_CLASSES,
    Prometheus,
    evaluate,
    load_registry,
    no_verdict_breakdown,
    parse_slo_doc,
    parse_target,
    registry_query,
)

SLO_DOC = REPO_ROOT / "docs" / "slo_targets.md"
SLO_REGISTRY = REPO_ROOT / "docs" / "slo_instruments.yml"
METRICS_MODULE = REPO_ROOT / "datanika" / "services" / "metrics.py"

# ---------------------------------------------------------------------------
# The baseline. This is the number the work is against.
#
# 18 of 26 SLOs have no instrument that can produce a verdict. Every entry below
# is one commitment production cannot currently violate or meet. Shrink this
# list by building the instrument; the test fails if it grows, and it also fails
# if an entry is removed without the registry agreeing — a stale baseline hides
# exactly as well as a missing one.
#
# Measured against production 2026-09-01: docs/slo_baseline.md
# ---------------------------------------------------------------------------
UNMEASURED_BASELINE = {
    # No instrument exists at all.
    "service-level-indicators-websocket-event-reflex-event-round-trip-for-a-state-update",
    "throughput-slos-rest-api-sustained-throughput-before-p95-regression",
    "throughput-slos-celery-worker-simple-pipeline-runs-duckdb-duckdb-1k-rows",
    "throughput-slos-celery-worker-upload-staging-runs-10k-rows-local-csv",
    "throughput-slos-scheduler-schedule-dispatch-latency-fire-time-task-enqueue",
    "throughput-slos-signup-first-event-user-hits-submit-first-reflex-event-round-trip",
    "pipeline-level-slos-pipeline-trigger-task-enqueued-in-celery",
    "pipeline-level-slos-task-enqueued-dlt-extract-started",
    "pipeline-level-slos-dlt-extract-complete-dbt-transformation-started",
    "pipeline-level-slos-end-to-end-small-pipeline-no-scheduling-delay",
    "saturation-slos-postgres-connections-in-use",  # wrong denominator available
    "saturation-slos-redis-memory",  # no exporter, and 60% of unlimited is not a number
}

# Wired to an instrument we have measured to be defective ([core#895]). These
# produce a number; the number is deliberately not scored. Removing a name here
# without removing `blocked_by` from the registry fails the test.
BLOCKED_BASELINE = {
    "service-level-indicators-rest-api-read-api-v1-meta-api-v1-connections-api-v1-pipelines-get",
    "service-level-indicators-rest-api-write-post-api-v1-connections-post-api-v1-pipelines",
    "service-level-indicators-auth-api-v1-auth-signup-api-v1-auth-login",
    "service-level-indicators-agent-api-llms-txt-api-v1-agent-guide-md-api-v1-meta-agent-tiers",
    "error-rate-slos-any-5xx-on-rest-api",
    # core#908. The sixth `source: app` entry, added when the guard became an
    # invariant instead of an entry-by-entry habit. It read NO_VERDICT only
    # because its sufficiency query counts SUCCESSFUL webhook deliveries and
    # that count is 0 — shielded by traffic, not by a guard, and the first real
    # Paddle webhook would have made it PASS from a defective instrument.
    "error-rate-slos-webhook-handler-paddle-http-5xx",
}

VALID_SOURCES = {"app", "blackbox", "cadvisor", "node", "postgres", "celery"}

# Any digit that is not part of a `min_samples:` line, a quantile, a duration
# window, or a metric/issue name would be a restated threshold.
_ALLOWED_NUMERIC_KEYS = {"min_samples"}


@pytest.fixture(scope="module")
def rows():
    return parse_slo_doc(SLO_DOC)


@pytest.fixture(scope="module")
def registry():
    return load_registry(SLO_REGISTRY)


# ---------------------------------------------------------------------------
# Positive controls on the parser itself.
#
# Everything below rests on the parse being right. An instrument that reads an
# empty document and reports nothing wrong is the exact failure this whole file
# exists to prevent, so the parser is checked before it is trusted.
# ---------------------------------------------------------------------------


def test_parser_finds_every_section(rows):
    """A whole section can vanish inside a plausible total.

    The first version of the column lookup dropped all six Saturation rows and
    still returned exactly 20 against a floor of 20. Per-section counts are the
    assertion that discriminates; the total is not.
    """
    actual = {section: 0 for section in EXPECTED_SECTION_COUNTS}
    for row in rows:
        if row.section in actual:
            actual[row.section] += 1
    assert actual == EXPECTED_SECTION_COUNTS


def test_parser_reads_known_targets_correctly(rows):
    """Anchor a few known numbers, in their real units.

    Without this, a parser that returned 26 rows of garbage would satisfy every
    other test in the file.
    """
    by_id = {r.slo_id: r for r in rows}

    health = by_id["service-level-indicators-health-probes-healthz-readyz"]
    assert health.targets["p95"].value == pytest.approx(0.050)
    assert health.targets["p99"].value == pytest.approx(0.150)
    assert health.targets["p95"].bound == MAX

    disk = by_id["saturation-slos-disk-free-on-opt-datanika"]
    assert disk.targets["target"].value == pytest.approx(0.20)
    assert disk.targets["target"].bound == MIN, "disk FREE is a floor, not a ceiling"

    celery = by_id["error-rate-slos-celery-task-failure-rate-excluding-user-error-exceptions"]
    assert celery.targets["target"].value == pytest.approx(0.01)
    assert celery.targets["target"].bound == MAX


def test_quantile_prefix_is_not_mistaken_for_the_number():
    """`p95 < 500 ms` means 500 ms, not 95.

    Six of the document's 26 rows are written this way. The obvious regex takes
    the 95 out of `p95` and yields a dimensionless 95 — a target every latency
    on earth satisfies, reported as a pass.
    """
    target = parse_target("**p95 < 500 ms**")
    assert target is not None
    assert target.value == pytest.approx(0.5)
    assert target.unit == "seconds"
    assert target.quantile == "p95"
    assert target.bound == MAX


def test_zero_target_is_satisfiable_by_zero():
    """The Paddle webhook SLO is `0` 5xx.

    Under a strict `<` that commitment is unsatisfiable by any real number, so a
    perfectly healthy system would report FAIL forever — which trains people to
    ignore the report.
    """
    target = parse_target("**0**")
    assert target is not None
    assert target.satisfied_by(0.0) is True
    assert target.satisfied_by(1.0) is False


# ---------------------------------------------------------------------------
# The guard proper.
# ---------------------------------------------------------------------------


def test_every_documented_slo_has_an_instrument_decision(rows, registry):
    """The core assertion: no SLO may exist without a decision about measuring it."""
    documented = {r.slo_id for r in rows}
    declared = set(registry)

    undecided = documented - declared
    assert not undecided, (
        "These SLOs are published in docs/slo_targets.md and no instrument has "
        "been assigned to them. Add an entry to docs/slo_instruments.yml — "
        "`status: measured` with a query, or `status: unmeasured` with a reason. "
        "An SLO nothing reads cannot be violated or met.\n  " + "\n  ".join(sorted(undecided))
    )

    orphaned = declared - documented
    assert not orphaned, (
        "These registry entries name no SLO in docs/slo_targets.md. Either the "
        "document dropped a commitment silently, or an id changed. A stale "
        "registry hides a gap exactly as well as a missing one.\n  " + "\n  ".join(sorted(orphaned))
    )


def test_registry_never_restates_a_threshold():
    """The document is the only place a target number may live.

    A registry carrying its own copy could be relaxed until it matched whatever
    production happens to do, and the report would go green without anyone
    editing a commitment. That is the same defect as a guard that passes because
    it looks at nothing.
    """
    import yaml

    raw = yaml.safe_load(SLO_REGISTRY.read_text(encoding="utf-8"))
    offenders = []
    for slo_id, entry in raw["slos"].items():
        for key in entry:
            if key in _ALLOWED_NUMERIC_KEYS:
                continue
            if key in {"target", "targets", "threshold", "value", "limit"}:
                offenders.append(f"{slo_id}: forbidden key '{key}'")
    assert not offenders, (
        "docs/slo_instruments.yml must not carry threshold numbers; they are "
        "parsed from docs/slo_targets.md.\n  " + "\n  ".join(offenders)
    )


def test_measured_entries_are_complete(registry):
    """A `measured` entry with no sample floor is a p95-over-three-requests waiting to happen."""
    problems = []
    for slo_id, entry in registry.items():
        if entry.get("status") != "measured":
            continue
        if not entry.get("query") and not entry.get("queries"):
            problems.append(f"{slo_id}: no query")
        if not entry.get("samples_query"):
            problems.append(f"{slo_id}: no samples_query — sufficiency cannot be checked")
        if int(entry.get("min_samples", 0)) < 1:
            problems.append(f"{slo_id}: min_samples must be >= 1")
        if entry.get("source") not in VALID_SOURCES:
            problems.append(f"{slo_id}: source {entry.get('source')!r} not in {VALID_SOURCES}")
    assert not problems, "\n  ".join([""] + problems)


def test_every_target_column_gets_its_own_query(rows, registry):
    """Seven SLIs commit to BOTH a p95 and a p99. Both must be scored.

    Scoring only the p95 would leave seven p99 commitments exactly as unread as
    the whole document was before this work — the same defect, one level down,
    and invisible because the row would still appear in the report.
    """
    missing = []
    for row in rows:
        entry = registry.get(row.slo_id)
        if not entry or entry.get("status") != "measured":
            continue
        for target_key in row.targets:
            if not registry_query(entry, target_key):
                missing.append(
                    f"{row.slo_id}: document commits to {target_key}, registry has no query"
                )
    assert not missing, (
        "These published commitments have no instrument even though their SLO "
        "row does:\n  " + "\n  ".join(missing)
    )


def test_unmeasured_entries_say_why(registry):
    problems = [
        slo_id
        for slo_id, entry in registry.items()
        if entry.get("status") == "unmeasured" and len((entry.get("reason") or "").strip()) < 40
    ]
    assert not problems, (
        "An `unmeasured` entry without a substantive reason is an unexplained "
        "gap, and the next reader will re-derive it.\n  " + "\n  ".join(problems)
    )


def test_unmeasured_entries_say_what_they_would_need(registry):
    """`reason` says why there is no verdict. `needs` says what would produce one.

    They are different facts and only one of them is actionable. Without `needs`
    the honest answer to *"which of these can we close cheaply?"* is a fresh
    investigation every time — which is how the same twelve gaps were re-derived
    from scratch on two consecutive days.

    Writing it down also forces the distinction that matters most here: three of
    the four pipeline-level SLOs need a **schema change** before any exporter
    could help, and two of the throughput SLOs need **load** rather than an
    instrument at all. "No metric records this" is true of all of them and
    directs the work nowhere.
    """
    problems = [
        slo_id
        for slo_id, entry in registry.items()
        if entry.get("status") == "unmeasured" and len((entry.get("needs") or "").strip()) < 40
    ]
    assert not problems, (
        "These SLOs record why they are unmeasured but not what would measure "
        "them. Add a `needs:` naming the specific missing thing — a metric, a "
        "column, a load harness, or a decision.\n  " + "\n  ".join(problems)
    )


def test_app_sourced_queries_reference_metrics_we_actually_emit(registry):
    """A query against a metric core never defines returns no series, forever.

    Prometheus answers such a query with an empty result and HTTP 200. Under
    `noDataState: OK` semantics that is the colour of health; here it becomes
    NO_VERDICT, which is better, but it is still a typo that can hide for months.
    Only the app-sourced half is checkable statically — exporter metric names
    live in third-party images and this test deliberately does not pretend
    otherwise.
    """
    metrics_src = METRICS_MODULE.read_text(encoding="utf-8")
    defined = set(re.findall(r'^\s*"([a-z_][a-z0-9_]*)",\s*$', metrics_src, re.MULTILINE))
    assert "http_requests_total" in defined, (
        "the metric-name scrape of datanika/services/metrics.py found nothing "
        "recognisable — this control is broken, not passing"
    )

    problems = []
    for slo_id, entry in registry.items():
        if entry.get("source") != "app":
            continue
        exprs = [entry.get("query") or "", entry.get("samples_query") or ""]
        exprs += list((entry.get("queries") or {}).values())
        for expr in exprs:
            for name in re.findall(r"\b([a-z_][a-z0-9_]*)_(?:bucket|count|sum|total)\b", expr):
                base = f"{name}_total" if f"{name}_total" in defined else name
                if base not in defined and name not in defined:
                    problems.append(f"{slo_id}: query references unknown app metric {name!r}")
    assert not problems, "\n  ".join([""] + sorted(set(problems)))


def test_unmeasured_set_matches_the_baseline_exactly(registry):
    """The ratchet. Fails in both directions, on purpose.

    Growing is a new blind spot. Shrinking without updating this list means the
    baseline is stale, and a stale baseline is how a swap — one SLO gaining an
    instrument while another silently loses one — hides inside a stable count.
    """
    actual = {k for k, v in registry.items() if v.get("status") == "unmeasured"}

    added = actual - UNMEASURED_BASELINE
    assert not added, (
        f"{len(added)} SLO(s) became unmeasured. Production can no longer "
        f"violate or meet them:\n  " + "\n  ".join(sorted(added))
    )
    removed = UNMEASURED_BASELINE - actual
    assert not removed, (
        f"{len(removed)} SLO(s) gained an instrument — good. Remove them from "
        f"UNMEASURED_BASELINE in this file so the count is honest:\n  "
        + "\n  ".join(sorted(removed))
    )


def test_blocked_set_matches_the_baseline_exactly(registry):
    """Same ratchet for SLOs wired to instruments we know are broken ([core#895])."""
    actual = {k for k, v in registry.items() if v.get("blocked_by")}
    assert actual == BLOCKED_BASELINE, (
        "The set of SLOs blocked on a defective instrument changed.\n"
        f"  unexpectedly blocked: {sorted(actual - BLOCKED_BASELINE)}\n"
        f"  no longer blocked:    {sorted(BLOCKED_BASELINE - actual)}\n"
        "If an instrument was fixed, drop `blocked_by` from the registry AND "
        "the name from BLOCKED_BASELINE in the same PR."
    )


def test_every_app_sourced_slo_is_blocked_while_core_895_is_open(registry):
    """The invariant, not the instance ([core#908]).

    Every ``source: app`` SLO is computed from ``http_requests_total`` /
    ``http_request_duration_seconds``, which [core#895] measured to be recorded
    as roughly one Granian worker's share. `blocked_by` is what stops
    ``scripts/slo_report.py`` scoring them.

    Five of the six carried it and the sixth did not, because the guard was
    applied entry-by-entry from memory rather than as a rule. That sixth entry
    (`error-rate-slos-webhook-handler-paddle-http-5xx`) was not producing a wrong
    verdict only because its sufficiency query counts **successful** webhook
    deliveries and that count is 0 at 0 paying users — it was shielded by an
    accident of traffic, not by a guard. The first successful Paddle webhook
    would have flipped it to a **PASS computed from a defective instrument**, on
    the exact day someone first reads this report for real. `docs/QA_RULES.md`
    §18c: an instrument you have MEASURED to be broken must never report PASS.

    ⚠️ **Delete this test in the same PR that fixes [core#895]**, together with
    all six `blocked_by` keys and `BLOCKED_BASELINE`. [core#895]'s AC3 says five
    entries; it must say six, or the fix unguards five and leaves this one
    blocked forever — and a stuck NO_VERDICT reads as "no instrument", which is
    the failure [core#721] existed to end.
    """
    app_sourced = {k for k, v in registry.items() if v.get("source") == "app"}
    assert app_sourced, (
        "no SLO entry has `source: app` at all. Either every one was rewired to "
        "an exporter — in which case delete this test and the blocked_by keys — "
        "or the registry stopped parsing, and this test is passing vacuously."
    )
    unguarded = sorted(k for k in app_sourced if not registry[k].get("blocked_by"))
    assert not unguarded, (
        f"{len(unguarded)} of {len(app_sourced)} `source: app` SLOs have no "
        f"`blocked_by`, so slo_report.py will score them from the per-process "
        f"HTTP counters core#895 measured as defective: {unguarded}\n"
        'Add `blocked_by: "core#895 (per-process HTTP metrics)"` and the name to '
        "BLOCKED_BASELINE in the same commit."
    )


def test_the_coverage_gap_is_visible_as_a_number(rows, registry):
    """Print the split. '26 SLOs' reads like coverage until you look.

    🚨 **Counted in COMMITMENTS, not registry rows, and the difference is not
    pedantic.** The previous version of this test counted registry entries —
    12 `unmeasured`, 6 `blocked_by` — and subtracted them from `len(rows)`.
    Those figures were then published in `docs/slo_baseline.md` as a breakdown
    of the 33 *commitments*, giving *"14 no instrument · 6 defective · 3 not
    enough samples"*. The measured answer is **13 · 4 · 6**, and all three cells
    were wrong:

    * the WebSocket entry is one row carrying **two** commitments (p95 and p99),
      so 12 unmeasured rows are 13 unmeasured commitments;
    * four of the six `blocked_by` entries never reach the blocked branch,
      because `scripts/slo_report.py` checks sample sufficiency **first** and
      they have zero samples. They are waiting on traffic, not on core#895.

    The buckets are not interchangeable. "No instrument" is engineering work,
    "defective instrument" is unblocked by fixing one named defect, and "not
    enough samples" is a traffic problem no engineering resolves. Being told six
    were blocked on core#895 when four are, and three were waiting on traffic
    when six are, sends the work to the wrong place.
    """
    verdicts = evaluate(rows, registry, None)
    commitments = len(verdicts)
    no_instrument = sum(1 for v in verdicts if v.reason_class == NEEDS_INSTRUMENT)

    unmeasured_rows = sum(1 for v in registry.values() if v.get("status") == "unmeasured")
    assert no_instrument != unmeasured_rows, (
        f"unmeasured registry rows ({unmeasured_rows}) and unmeasured commitments "
        f"({no_instrument}) are equal, so this test can no longer detect the "
        "confusion it exists for. If a multi-target SLO stopped being unmeasured, "
        "say so here deliberately."
    )
    assert commitments > no_instrument, "not one SLO in the document has an instrument"
    print(
        f"\nSLO instrument coverage: {commitments} commitments across {len(rows)} "
        f"documented SLOs — {no_instrument} have no instrument at all.\n"
        "The defective-instrument and insufficient-samples cells CANNOT be counted "
        "here: both depend on live sample counts, so only a production run of "
        "scripts/slo_report.py can fill them in. Copy that run's breakdown into "
        "docs/slo_baseline.md rather than tallying anything by hand."
    )


def test_every_no_verdict_carries_a_reason_class(rows, registry):
    """No NO_VERDICT may reach the report unclassified.

    The breakdown is only trustworthy if it is exhaustive. An unclassified
    verdict would silently drop out of every bucket and make the cells sum to
    less than the total — which is precisely the arithmetic that would go
    unnoticed, because a reader checks the cells against each other and not
    against the total.
    """
    verdicts = evaluate(rows, registry, None)
    unclassified = [
        (v.slo_id, v.detail[:60])
        for v in verdicts
        if v.state == NO_VERDICT and v.reason_class not in NO_VERDICT_CLASSES
    ]
    assert not unclassified, (
        "these NO_VERDICTs have no reason class, so the breakdown does not "
        f"account for them:\n  {unclassified}"
    )

    counted = sum(
        sum(1 for v in verdicts if v.state == NO_VERDICT and v.reason_class == cls)
        for cls in NO_VERDICT_CLASSES
    )
    assert counted == sum(1 for v in verdicts if v.state == NO_VERDICT), (
        "the per-class counts do not sum to the NO_VERDICT total; a verdict is "
        "being counted twice or not at all"
    )


def test_control_the_breakdown_discriminates(rows, registry):
    """A classifier that returns one label for everything is not a classifier.

    Offline, every commitment is NO_VERDICT — so a breakdown that put them all in
    one bucket would still render, still sum correctly, and still say nothing.
    Two classes must be present, and the one that is *not* an artefact of being
    offline has to be the real one.
    """
    verdicts = evaluate(rows, registry, None)
    present = {v.reason_class for v in verdicts if v.state == NO_VERDICT}
    assert len(present) >= 2, (
        f"every offline NO_VERDICT landed in the same bucket ({present}); the "
        "breakdown cannot distinguish anything"
    )
    assert NEEDS_INSTRUMENT in present, (
        "no commitment was classified as lacking an instrument even though the "
        "registry declares entries `unmeasured` — the classifier is not reading "
        "the registry"
    )

    text = no_verdict_breakdown(verdicts)
    assert "UNCLASSIFIED" not in text, text
    assert NEEDS_INSTRUMENT in text, (
        "the rendered breakdown omits the class that matters most:\n" + text
    )


# ---------------------------------------------------------------------------
# Forced-red controls.
#
# A guard that has never been seen to fail is not a guard. These mutate copies
# of the REAL document and the REAL registry — a synthetic fixture would only
# prove the assertion works on a fixture.
# ---------------------------------------------------------------------------


@pytest.fixture()
def real_copies(tmp_path):
    doc = tmp_path / "slo_targets.md"
    reg = tmp_path / "slo_instruments.yml"
    shutil.copyfile(SLO_DOC, doc)
    shutil.copyfile(SLO_REGISTRY, reg)
    return doc, reg


def test_control_a_new_undecided_slo_is_caught(real_copies):
    """Add a real-looking row to the real document; the guard must notice."""
    doc, reg = real_copies
    text = doc.read_text(encoding="utf-8")
    marker = "| Disk free on `/opt/datanika` | **> 20 %** |"
    assert marker in text, "the anchor row moved; this control is testing nothing"
    doc.write_text(
        text.replace(marker, marker + "\n| **Kafka consumer lag** | **< 5 s** |"),
        encoding="utf-8",
    )

    documented = {r.slo_id for r in parse_slo_doc(doc)}
    declared = set(load_registry(reg))
    assert documented - declared, (
        "a new SLO was added to the document with no registry decision and the "
        "guard did not notice — the guard is broken"
    )


def test_control_a_deleted_slo_is_caught(real_copies):
    """Silently dropping a commitment must fail, not quietly reduce the count.

    Note *which* assertion catches it: the per-section count floor fires before
    the orphan check gets a chance, because one deleted row takes the total
    below MIN_EXPECTED_SLOS. That ordering is deliberate — a shrinking document
    is a fact about the document, and it should be reported as one rather than
    as a registry inconsistency. The orphan check is exercised in isolation by
    the next test.
    """
    doc, _ = real_copies
    text = doc.read_text(encoding="utf-8")
    marker = "| Redis memory | **< 60 %** of maxmemory |"
    assert marker in text, "the anchor row moved; this control is testing nothing"
    doc.write_text(text.replace(marker + "\n", ""), encoding="utf-8")

    with pytest.raises(RuntimeError, match="parsed only 25 SLOs"):
        parse_slo_doc(doc)


def test_cli_runs_and_an_unmeasured_slo_does_not_exit_zero():
    """Pin the exit-code contract, which is the whole point of the tool.

    Everything above tests the pieces. This tests that the thing runs, and that
    ``--offline`` — where nothing at all was measured — exits **2**, not 0. If
    this ever returns 0 a scheduled job goes green while measuring nothing,
    which is the exact failure `docs/slo_targets.md` spent four months in.

    ``--report-only`` is the deliberate escape hatch and must exit 0, so that the
    two are never confused with each other.
    """
    from slo_report import main

    assert main(["--offline"]) == 2, "an unmeasured SLO must not exit 0"
    assert main(["--offline", "--report-only"]) == 0, "--report-only is for humans reading a table"


def test_render_names_every_state(rows, registry, capsys):
    """The split has to be printed. '26 SLOs' reads like coverage until you look."""
    from slo_report import render

    text = render(evaluate(rows, registry, None))
    assert "NO_VERDICT" in text
    assert "is not a pass" in text, "the report must say plainly that NO_VERDICT is not a pass"
    assert "Commitments in document: 33" in text, (
        "the report must print the total; got:\n" + text[-400:]
    )


def test_control_an_orphaned_registry_entry_is_caught(real_copies):
    """A registry key naming no documented SLO must be caught in its own right.

    This is the direction that hides best: the report still prints 26 lines and
    every one of them looks decided.
    """
    doc, reg = real_copies
    reg.write_text(
        reg.read_text(encoding="utf-8")
        + "\n  a-commitment-no-longer-in-the-document:\n"
        + "    status: unmeasured\n"
        + "    reason: >-\n"
        + "      Placeholder used by the forced-red control for the orphan check.\n",
        encoding="utf-8",
    )

    documented = {r.slo_id for r in parse_slo_doc(doc)}
    declared = set(load_registry(reg))
    assert declared - documented == {"a-commitment-no-longer-in-the-document"}, (
        "an orphaned registry entry went undetected — the guard is broken"
    )


def test_control_a_gutted_document_raises_rather_than_reporting_zero(real_copies):
    """An empty parse must never read as full coverage.

    This is `docs/QA_RULES.md` §1 applied to this instrument: nothing found,
    nothing wrong, and nothing measured are three different facts wearing one
    face.
    """
    doc, _ = real_copies
    doc.write_text("# SLO Targets\n\nAll tables removed.\n", encoding="utf-8")
    with pytest.raises(RuntimeError, match="parsed only 0 SLOs"):
        parse_slo_doc(doc)


def test_control_unreachable_prometheus_is_no_verdict_not_pass(rows, registry):
    """The failure mode that matters most in a scheduled job.

    If the monitoring stack is down, every query fails. A reporter that treated
    a failed query as 'nothing wrong' would go green at precisely the moment it
    is blind — which is how `noDataState: OK` rules have burned this project
    repeatedly. Port 1 is closed on every host we run on.
    """
    verdicts = evaluate(rows, registry, Prometheus("http://127.0.0.1:1", timeout=1.0))
    assert verdicts, "evaluate() returned nothing"
    assert all(v.state == NO_VERDICT for v in verdicts), (
        "with Prometheus unreachable every SLO must be NO_VERDICT; got "
        + repr([(v.slo_id, v.state) for v in verdicts if v.state != NO_VERDICT])
    )
