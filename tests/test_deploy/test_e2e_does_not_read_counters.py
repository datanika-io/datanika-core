"""No E2E assertion may read a Prometheus counter off staging (core#896 / core#895).

Asked and answered on 2026-09-11: **no E2E spec reads `/metrics` or any counter today.** This
file is what keeps that true, because the next person to want one will not know why they should
not — and the reasons are two independent traps that both read clean.

🚨 **Trap 1 — the labels look perfect for the wrong reason** (core#896). Staging's live label set
is templated throughout — `/api/v1/pipelines/:id`, `/api/v1/connections/:id` — which is exactly
what a working fix produces. It looks that way because **the E2E suite only ever requests routes
that exist**, so the unmatched-path bucket the fix exists to create is never exercised there.

🚨 **Trap 2 — the counter samples one worker of several** (core#895). Staging's compose carries
**no `PROMETHEUS_MULTIPROC_DIR`**, so each Granian worker keeps its own registry and a scrape is
answered by whichever accepted the connection. Measured on staging 2026-09-11, six scrapes of one
counter: **`416, 502, 502, 416, 502, 416`**.

⚠️ **And production is being fixed while staging is not**, so after that promotion the two stop
agreeing — a staging counter will be wrong in two independent ways while reading clean, and the
divergence will look like a regression in the fix rather than a property of the environment.

**What is allowed instead**: assert on the API's own responses (status, body, ids), which is what
every isolation and boundary spec already does. A counter is a statement about the process that
served the request; the E2E suite does not know which process that was.
"""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
E2E = REPO_ROOT / "e2e"

#: Reading a counter, in any of the shapes an author would reach for.
COUNTER_READS = re.compile(
    r"""/metrics\b|http_requests_total|http_request_duration|\bprometheus\b""",
    re.I,
)

#: Files that may mention the above without reading one — this file's own subject matter.
ALLOWLIST = frozenset(
    {
        # (empty today, and that is the point: the first entry is a decision)
    }
)

#: Suffixes an E2E author actually writes.
SUFFIXES = (".ts", ".mts", ".mjs", ".js")


def _e2e_files() -> list[Path]:
    out = subprocess.run(
        ["git", "-C", str(REPO_ROOT), "ls-files", "e2e"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout.splitlines()
    return [REPO_ROOT / p for p in out if p.endswith(SUFFIXES)]


def _strip_comments(text: str) -> str:
    """Drop `//` lines and `/* */` blocks.

    core#1260, three departments deep: a substring check over a whole file is satisfied by a
    comment, and this file's own docstring would otherwise be copied into a spec as a note and
    trip the guard on the very explanation of the guard.
    """
    text = re.sub(r"/\*.*?\*/", "", text, flags=re.S)
    return "\n".join(ln for ln in text.splitlines() if not ln.lstrip().startswith(("//", "*")))


# ── controls first ───────────────────────────────────────────────────────────────────────


def test_control_the_scan_sees_the_e2e_tree_at_all() -> None:
    """A scan over an empty file list passes forever. 44 E2E-shaped matches were measured
    across 6 files when this was written; the floor is deliberately far below that."""
    files = _e2e_files()
    assert len(files) >= 10, f"only {len(files)} e2e files found — the scan is looking nowhere"
    bodies = "\n".join(f.read_text(encoding="utf-8", errors="replace") for f in files)
    assert "apiBudget" in bodies or "request.get" in bodies, (
        "the e2e tree does not look like the e2e tree; this scan is measuring something else"
    )


def test_control_the_pattern_can_fire_and_ignores_a_comment() -> None:
    """Both directions. A pattern that never fires passes vacuously; one that fires on a
    comment makes the guard cry wolf on a spec that merely explains why it does not do this."""
    live = "const r = await request.get(`${BASE}/metrics`);"
    assert COUNTER_READS.search(_strip_comments(live)), "the pattern cannot fire at all"

    commented = "// never read /metrics here — see core#895\nconst r = await request.get(url);"
    assert not COUNTER_READS.search(_strip_comments(commented)), (
        "a comment explaining the rule tripped the rule"
    )


# ── the assertion ────────────────────────────────────────────────────────────────────────


def test_no_e2e_file_reads_a_prometheus_counter() -> None:
    offenders: list[str] = []
    for path in _e2e_files():
        rel = str(path.relative_to(REPO_ROOT)).replace("\\", "/")
        if rel in ALLOWLIST:
            continue
        source = _strip_comments(path.read_text(encoding="utf-8", errors="replace"))
        for n, line in enumerate(source.splitlines(), 1):
            if COUNTER_READS.search(line):
                offenders.append(f"{rel}:{n}: {line.strip()[:100]}")

    assert not offenders, (
        "an E2E file reads a Prometheus counter:\n  "
        + "\n  ".join(offenders)
        + "\n\nA counter read off staging is wrong in TWO independent ways while reading clean:\n"
        "  core#896 — staging's labels look perfect because the suite only requests routes\n"
        "             that exist, so the bucket the fix creates is never exercised there;\n"
        "  core#895 — staging has no PROMETHEUS_MULTIPROC_DIR, so a scrape samples ONE worker\n"
        "             of several (measured: 416, 502, 502, 416, 502, 416 across six scrapes).\n"
        "Assert on the API's own response instead — status, body, ids — which is what every\n"
        "isolation spec already does. If you genuinely need a counter, add the file to\n"
        "ALLOWLIST in the same commit and say which of the two traps does not apply."
    )
