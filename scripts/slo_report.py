#!/usr/bin/env python3
"""Read ``docs/slo_targets.md`` and produce a per-SLO verdict against Prometheus.

Why this exists
---------------
``docs/slo_targets.md`` shipped 2026-04-14 and until today **no instrument read
it**. A target nothing measures is indistinguishable from a target being met:
it cannot be violated and it cannot be achieved. This script is the instrument.

The three states are the whole point
------------------------------------
``PASS`` and ``FAIL`` are ordinary. ``NO_VERDICT`` is the state this file exists
to make visible, and it is **not** a pass. An SLO is ``NO_VERDICT`` when:

* the registry declares it ``unmeasured`` (nothing in production reads it), or
* its query returns no series, or
* it returned a number computed from **fewer samples than the SLO needs**.

That last one matters more than it looks. A p95 over three requests is a
beautiful number that means nothing; reported as a pass it is exactly the
"green that proves nothing" this codebase keeps producing. ``min_samples``
turns it into ``NO_VERDICT`` instead.

Exit codes are distinct so "we don't know" can never be read as "we're fine":

===== ==========================================================
 0     every SLO measured and within target
 1     at least one SLO measured and OUT of target
 2     no failures, but at least one SLO has NO VERDICT
===== ==========================================================

Strict is the default. ``--report-only`` forces exit 0 and is for humans
reading the table, never for a scheduled job.

The doc is authoritative for every number
-----------------------------------------
Thresholds are parsed out of ``docs/slo_targets.md`` and are **never** written
in the registry. A registry that carried its own copy of a threshold could be
quietly relaxed to match whatever production happens to do, which is the same
defect as a guard that passes because it looks at nothing.
``tests/test_slo/test_slo_coverage.py`` enforces that mechanically.

Usage
-----
Prometheus binds to 127.0.0.1 on the app box, so run it there or over a tunnel::

    ssh -i ~/.ssh/id_ed25519 root@185.25.22.188 \\
        'python3 -' < scripts/slo_report.py            # needs the .md + .yml too

    python scripts/slo_report.py --prometheus http://127.0.0.1:9090
    python scripts/slo_report.py --offline              # registry audit, no network
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parent.parent
SLO_DOC = REPO_ROOT / "docs" / "slo_targets.md"
SLO_REGISTRY = REPO_ROOT / "docs" / "slo_instruments.yml"

# A parse that finds nothing must never read as "nothing is wrong". The doc has
# carried 26 SLOs since April; if the parser returns fewer, either the document
# was gutted or the parser broke, and both are failures.
#
# ⚠️ The floor alone is not enough, and this was caught the hard way while
# writing it: the first version of the column lookup silently dropped all six
# Saturation rows, and the total came to exactly 20 against a floor of 20. A
# whole section can vanish inside a total. `tests/test_slo/test_slo_coverage.py`
# therefore asserts the count **per section**, which is the assertion that
# actually discriminates.
MIN_EXPECTED_SLOS = 26

# section heading -> how many SLO rows it must yield.
EXPECTED_SECTION_COUNTS = {
    "Service-level indicators": 7,
    "Throughput SLOs": 5,
    "Pipeline-level SLOs (out of k6 scope but documented here)": 4,
    "Error-rate SLOs": 4,
    "Saturation SLOs": 6,
}

PASS, FAIL, NO_VERDICT = "PASS", "FAIL", "NO_VERDICT"

# ---------------------------------------------------------------------------
# Target parsing: turn the doc's prose cells into (comparator, value, unit).
# ---------------------------------------------------------------------------

_UNIT_TO_SECONDS = {"ms": 1e-3, "s": 1.0}

# ⚠️ A quantile prefix is not the number. "p95 < 500 ms" must yield 500 ms, and
# the obvious regex yields **95**, silently, as a dimensionless count — a target
# every latency on earth satisfies. Six of the doc's 26 rows are written that
# way. Strip the prefix before matching anything.
_QUANTILE_PREFIX_RE = re.compile(r"\bp(?:50|75|90|95|99(?:\.\d+)?)\b\s*")

# "**200 ms**", "< 500 ms", "≥ 100 rps", "< 0.1 %", "**0**", "> 20 %"
_TARGET_RE = re.compile(
    r"""(?P<cmp>[<>]=?|≥|≤)?\s*
        (?P<num>\d+(?:\.\d+)?)\s*
        (?P<unit>ms|s\b|%|rps|runs/min)?""",
    re.VERBOSE,
)

MAX, MIN = "max", "min"  # observed must not exceed / must be at least


@dataclass(frozen=True)
class Target:
    """One numeric commitment, normalised to a base unit.

    ``seconds`` for latency, ``fraction`` for percentages, bare number for rates.
    """

    raw: str
    value: float
    unit: str
    bound: str  # MAX or MIN
    quantile: str = ""  # "p95" when the cell said so, else ""

    def satisfied_by(self, observed: float) -> bool:
        # Inclusive on purpose. The doc's one exact target is the Paddle webhook
        # "0 5xx"; under a strict `<` that commitment is unsatisfiable by any
        # real number, so a correct system would report FAIL forever.
        return observed <= self.value if self.bound == MAX else observed >= self.value


def _strip_md(cell: str) -> str:
    return cell.replace("**", "").replace("`", "").strip()


def parse_target(cell: str) -> Target | None:
    """Parse one target cell. Returns None for prose with no commitment in it."""
    text = _strip_md(cell)
    if not text or text in {"-", "—", "n/a"}:
        return None

    quantile_match = _QUANTILE_PREFIX_RE.search(text)
    quantile = quantile_match.group(0).strip() if quantile_match else ""
    searchable = _QUANTILE_PREFIX_RE.sub("", text, count=1)

    m = _TARGET_RE.search(searchable)
    if not m:
        return None

    value = float(m.group("num"))
    unit = m.group("unit") or ""
    raw_cmp = m.group("cmp") or ""

    if unit in _UNIT_TO_SECONDS:
        value *= _UNIT_TO_SECONDS[unit]
        unit = "seconds"
    elif unit == "%":
        value /= 100.0
        unit = "fraction"
    elif unit not in {"rps", "runs/min"}:
        unit = "count"

    bound = MIN if raw_cmp in {">", ">=", "≥"} else MAX
    return Target(raw=text, value=value, unit=unit, bound=bound, quantile=quantile)


# ---------------------------------------------------------------------------
# Document parsing
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SloRow:
    slo_id: str
    section: str
    subject: str
    targets: dict[str, Target]
    source_line: int


def _slugify(*parts: str) -> str:
    text = " ".join(parts).lower()
    text = re.sub(r"`[^`]*`", " ", text)  # code spans are examples, not names
    text = re.sub(r"[^a-z0-9]+", "-", text)
    return re.sub(r"-+", "-", text).strip("-")


# Which table columns carry commitments, per section heading. Anything not named
# here is descriptive (the doc's "Measurement" column, for instance).
_SECTION_TARGET_COLUMNS: dict[str, tuple[str, ...]] = {
    "Service-level indicators": ("p95", "p99"),
    "Throughput SLOs": ("target",),
    "Pipeline-level SLOs (out of k6 scope but documented here)": ("target",),
    "Error-rate SLOs": ("target",),
    "Saturation SLOs": ("target",),
}


def parse_slo_doc(path: Path = SLO_DOC) -> list[SloRow]:
    """Extract every SLO row from the markdown document.

    Raises if the result is implausibly small — see MIN_EXPECTED_SLOS.
    """
    rows: list[SloRow] = []
    section = ""
    header: list[str] = []

    for lineno, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        stripped = line.strip()

        if stripped.startswith("## "):
            section = stripped[3:].strip()
            header = []
            continue

        if not stripped.startswith("|"):
            header = []
            continue

        cells = [c.strip() for c in stripped.strip("|").split("|")]
        if all(set(c) <= {"-", ":", " "} and c for c in cells):
            continue  # the |---|---| separator
        if not header:
            header = [_strip_md(c).lower() for c in cells]
            continue
        if section not in _SECTION_TARGET_COLUMNS:
            continue

        wanted = _SECTION_TARGET_COLUMNS[section]
        targets: dict[str, Target] = {}

        for key in wanted:
            if key == "target":
                # "Target", but also "Target (sustained 5 min)". Matching only the
                # bare word dropped every Saturation row while the total still
                # cleared the floor.
                idx = next((i for i, h in enumerate(header) if h.startswith("target")), None)
            else:
                idx = next(
                    (i for i, h in enumerate(header) if h.startswith(f"target ({key}")),
                    None,
                )
            if idx is None or idx >= len(cells):
                continue
            parsed = parse_target(cells[idx])
            if parsed is not None:
                targets[key] = parsed

        if not targets:
            continue

        # Subject = the first two descriptive columns, which name the thing.
        subject_cells = [_strip_md(c) for c in cells[: max(1, len(cells) - len(wanted))]]
        subject = " — ".join(c for c in subject_cells if c)[:160]
        rows.append(
            SloRow(
                slo_id=_slugify(section.split("(")[0], *subject_cells[:2]),
                section=section,
                subject=subject,
                targets=targets,
                source_line=lineno,
            )
        )

    if len(rows) < MIN_EXPECTED_SLOS:
        per_section = {s: sum(1 for r in rows if r.section == s) for s in EXPECTED_SECTION_COUNTS}
        raise RuntimeError(
            f"parsed only {len(rows)} SLOs from {path} (expected >= {MIN_EXPECTED_SLOS}). "
            f"Per section: {per_section}. Either the document was gutted or this "
            "parser no longer matches its table shape. An empty parse must never "
            "read as full coverage."
        )
    return rows


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------


@dataclass
class Verdict:
    slo_id: str
    subject: str
    state: str
    detail: str
    observed: str = ""
    target: str = ""
    samples: str = ""
    notes: list[str] = field(default_factory=list)


def load_registry(path: Path = SLO_REGISTRY) -> dict[str, dict[str, Any]]:
    import yaml  # imported late so --help works without the dependency

    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    entries = data.get("slos") or {}
    if not entries:
        raise RuntimeError(
            f"{path} declares no SLO instruments. An empty registry would make "
            "every SLO trivially unmeasured and the report trivially quiet."
        )
    return entries


# ---------------------------------------------------------------------------
# Prometheus
# ---------------------------------------------------------------------------


class Prometheus:
    def __init__(self, base_url: str, timeout: float = 30.0) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def query(self, expr: str) -> tuple[list[float], str | None]:
        """Return (values, error). An empty list with no error means 'no series'."""
        url = f"{self.base_url}/api/v1/query?" + urllib.parse.urlencode({"query": expr})
        try:
            with urllib.request.urlopen(url, timeout=self.timeout) as resp:  # noqa: S310
                payload = json.loads(resp.read().decode("utf-8"))
        except Exception as exc:  # noqa: BLE001 - any failure is a non-verdict
            return [], f"{type(exc).__name__}: {exc}"
        if payload.get("status") != "success":
            return [], str(payload.get("error", "prometheus returned an error"))
        result = payload.get("data", {}).get("result", [])
        values: list[float] = []
        for series in result:
            try:
                values.append(float(series["value"][1]))
            except (KeyError, IndexError, TypeError, ValueError):
                continue
        return values, None


# ---------------------------------------------------------------------------
# Evaluation
# ---------------------------------------------------------------------------


def _fmt(value: float, unit: str) -> str:
    if unit == "seconds":
        return f"{value * 1000:.1f} ms"
    if unit == "fraction":
        return f"{value * 100:.3f} %"
    return f"{value:g}"


def registry_query(entry: dict[str, Any], target_key: str) -> str | None:
    """The query for one target key.

    Seven of the document's rows carry **two** commitments (p95 and p99). Scoring
    only one of them would leave the other exactly as unread as the whole file
    was before this script existed, so every target key gets its own verdict and
    its own query.
    """
    queries = entry.get("queries")
    if isinstance(queries, dict):
        return queries.get(target_key)
    return entry.get("query")


def evaluate(
    rows: list[SloRow],
    registry: dict[str, dict[str, Any]],
    prom: Prometheus | None,
) -> list[Verdict]:
    verdicts: list[Verdict] = []

    for row in rows:
        entry = registry.get(row.slo_id)

        for target_key, target in row.targets.items():
            # One verdict per commitment, not per table row.
            vid = row.slo_id if len(row.targets) == 1 else f"{row.slo_id}#{target_key}"

            if entry is None:
                verdicts.append(
                    Verdict(
                        vid,
                        row.subject,
                        NO_VERDICT,
                        "no registry entry — this SLO has never been assigned an instrument",
                    )
                )
                continue

            if entry.get("status") == "unmeasured":
                verdicts.append(
                    Verdict(
                        vid,
                        row.subject,
                        NO_VERDICT,
                        f"declared unmeasured: {entry.get('reason', '(no reason given)')}",
                        target=target.raw,
                    )
                )
                continue

            expr = registry_query(entry, target_key)
            if not expr:
                verdicts.append(
                    Verdict(
                        vid,
                        row.subject,
                        NO_VERDICT,
                        f"the document commits to a {target_key} target and the "
                        "registry supplies no query for it",
                        target=_fmt(target.value, target.unit),
                    )
                )
                continue

            if prom is None:
                verdicts.append(
                    Verdict(
                        vid,
                        row.subject,
                        NO_VERDICT,
                        "offline mode — no Prometheus queried",
                        target=_fmt(target.value, target.unit),
                    )
                )
                continue

            # Sample sufficiency first: a number from too few samples is not a
            # number. A p95 over three requests is a beautiful, meaningless
            # figure, and scored as a pass it is worse than no figure at all.
            min_samples = int(entry.get("min_samples", 1))
            sample_values, sample_err = prom.query(entry["samples_query"])
            if sample_err:
                verdicts.append(
                    Verdict(vid, row.subject, NO_VERDICT, f"sample query failed: {sample_err}")
                )
                continue
            observed_samples = sum(sample_values) if sample_values else 0.0
            if observed_samples < min_samples:
                verdicts.append(
                    Verdict(
                        vid,
                        row.subject,
                        NO_VERDICT,
                        f"only {observed_samples:.0f} samples, need >= {min_samples} "
                        "before a quantile or ratio means anything",
                        target=_fmt(target.value, target.unit),
                        samples=f"{observed_samples:.0f}",
                    )
                )
                continue

            values, err = prom.query(expr)
            if err:
                verdicts.append(Verdict(vid, row.subject, NO_VERDICT, f"query failed: {err}"))
                continue
            if not values:
                verdicts.append(
                    Verdict(
                        vid,
                        row.subject,
                        NO_VERDICT,
                        "query returned no series — the metric this SLO needs is "
                        "not being collected (or the selector matches nothing)",
                        target=_fmt(target.value, target.unit),
                        samples=f"{observed_samples:.0f}",
                    )
                )
                continue

            # Worst case across returned series: with blue/green and multiple
            # containers a mean would hide the one that is out of target.
            observed = max(values) if target.bound == MAX else min(values)
            ok = target.satisfied_by(observed)

            # An SLO wired to a KNOWN-DEFECTIVE instrument must never report
            # PASS. The moment traffic arrives a broken meter starts emitting
            # confident greens — strictly worse than measuring nothing, because
            # a green is acted on. The number is shown; it is just not scored.
            blocked_by = entry.get("blocked_by")
            if blocked_by:
                verdicts.append(
                    Verdict(
                        vid,
                        row.subject,
                        NO_VERDICT,
                        f"instrument known defective ({blocked_by}) — reads "
                        f"{_fmt(observed, target.unit)} against {target.bound} "
                        f"{_fmt(target.value, target.unit)}, but that reading is "
                        "not trustworthy and is deliberately not scored",
                        observed=_fmt(observed, target.unit),
                        target=_fmt(target.value, target.unit),
                        samples=f"{observed_samples:.0f}",
                        notes=list(entry.get("notes", [])),
                    )
                )
                continue

            verdicts.append(
                Verdict(
                    vid,
                    row.subject,
                    PASS if ok else FAIL,
                    f"{'within' if ok else 'OUT OF'} target "
                    f"({_fmt(observed, target.unit)} vs {target.bound} "
                    f"{_fmt(target.value, target.unit)}), n={observed_samples:.0f}",
                    observed=_fmt(observed, target.unit),
                    target=_fmt(target.value, target.unit),
                    samples=f"{observed_samples:.0f}",
                    notes=list(entry.get("notes", [])),
                )
            )

    return verdicts


def render(verdicts: list[Verdict]) -> str:
    out: list[str] = []
    width = max((len(v.slo_id) for v in verdicts), default=10)
    for state in (FAIL, NO_VERDICT, PASS):
        group = [v for v in verdicts if v.state == state]
        if not group:
            continue
        out.append(f"\n=== {state}  ({len(group)}) ===")
        for v in group:
            out.append(f"  {v.slo_id.ljust(width)}  {v.detail}")
            for note in v.notes:
                out.append(f"  {' ' * width}  ^ {note}")
    counts = {s: sum(1 for v in verdicts if v.state == s) for s in (PASS, FAIL, NO_VERDICT)}
    out.append(
        f"\nCommitments in document: {len(verdicts)}   "
        f"PASS {counts[PASS]}   FAIL {counts[FAIL]}   NO_VERDICT {counts[NO_VERDICT]}"
    )
    if counts[NO_VERDICT]:
        out.append(
            f"NO_VERDICT is not a pass. {counts[NO_VERDICT]} of {len(verdicts)} SLOs "
            "cannot currently be violated or met by anything production measures."
        )
    return "\n".join(out)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--prometheus", default="http://127.0.0.1:9090")
    ap.add_argument("--offline", action="store_true", help="audit the registry, query nothing")
    ap.add_argument("--report-only", action="store_true", help="always exit 0 (humans only)")
    ap.add_argument("--doc", type=Path, default=SLO_DOC)
    ap.add_argument("--registry", type=Path, default=SLO_REGISTRY)
    args = ap.parse_args(argv)

    rows = parse_slo_doc(args.doc)
    registry = load_registry(args.registry)
    prom = None if args.offline else Prometheus(args.prometheus)

    verdicts = evaluate(rows, registry, prom)
    print(render(verdicts))

    if args.report_only:
        return 0
    if any(v.state == FAIL for v in verdicts):
        return 1
    if any(v.state == NO_VERDICT for v in verdicts):
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
