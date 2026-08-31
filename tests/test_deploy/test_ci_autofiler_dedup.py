"""Every CI auto-filer must dedup, and no two jobs may share a dedup key (core#818).

What went wrong
---------------
`ci.yml` grew three `gh issue create` steps and `overage-e2e-nightly.yml` a fourth. Two
of the three in `ci.yml` deduped; the `e2e-sso` copy was a bare `gh issue create`. When
`e2e-sso` went red for a real product bug (core#830) it filed **one issue per push** —
ten in an afternoon (#824, #826, #829, #834, #839, #840, #842, #844, #847, …) — and fired
an unconditional Telegram alongside each. The tracker went ~15% duplicate and the alarm
stopped carrying information.

Why the obvious fix was a trap, and why this test exists
--------------------------------------------------------
The remedy everyone reaches for is "lift the dedup block from `e2e-staging` verbatim".
That block keys on the **`e2e-failure`** label — and `e2e-staging` and `e2e-sso` *both*
write it. Copying it crosses the wires: a staging failure comments onto an SSO thread and
an SSO failure onto a staging one, so a two-cause outage reads as one and the second
cause is invisible. QA flagged it before it shipped; nothing mechanical would have.

So a test that only asked *"does every filer dedup?"* would go green on the broken fix.
It has to ask the second question too: **are the keys distinguishable?**

The model
---------
Each filer step declares a *channel*: the thing it searches for to decide whether this
failure is already being tracked. A channel is either a label
(`gh issue list --label X`) or a title phrase (`gh issue list --search "in:title P"` —
`scheduled-workflow-watchdog.yml` uses this form). Three invariants:

1. **Every filer has a channel.** A `gh issue create` with no lookup files one issue per
   occurrence, forever. This is the core#818 defect.
2. **A label channel is written by the issue it files.** A filer that searches `X` but
   files with label `Y` can never find its own thread, so it dedups on paper and spams in
   practice — indistinguishable from having no dedup at all until it is red twice.
3. **Channels are disjoint across jobs.** Sharing a channel *within* a job is legitimate
   and deliberate: `e2e-staging`'s red filer and its flaky-gating filer are the same event
   reported two ways and belong on one thread. Sharing one *across* jobs is the wire-cross.

Derived, not restated
---------------------
The channel set is parsed out of the workflows, in the manner of
`test_deploy_service_coverage.py`. Nothing here lists the expected labels: add a fourth
filer and this test has an opinion about it without being edited. A restated list drifts,
and drift is the bug being hunted.

Negative controls are part of the module
----------------------------------------
`_audit` is a pure function over `{source_name: yaml_text}` so the last two tests can run
it against the two shapes that must fail — the pre-fix `e2e-sso` step, and the
verbatim-copy fix. A checker that has never been shown to reject anything is this
project's signature defect wearing a test's clothes.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
WORKFLOWS = ROOT / ".github" / "workflows"

# A shell line continuation, so each `gh ...` invocation collapses onto one line.
_CONT = re.compile(r"\\\n\s*")
# `--label foo` / `--label "foo"` / `--label 'foo'`
_LABEL = re.compile(r"--label\s+[\"']?([A-Za-z0-9_.\-]+)[\"']?")
# `--search "in:title Some phrase"`
_TITLE_SEARCH = re.compile(r"--search\s+[\"']in:title\s+([^\"']+)[\"']")


def _invocations(run: str) -> list[str]:
    """One shell command per element, continuations joined, comments dropped.

    Comments are dropped because these steps explain themselves at length and the
    explanations quote the very commands being searched for — the failure
    `test_deploy_service_coverage.py` hit when it parsed a service called `won` out of
    prose. This module's own docstrings would otherwise register as filers.
    """
    joined = _CONT.sub(" ", run)
    return [line.strip() for line in joined.splitlines() if not line.lstrip().startswith("#")]


class Filer:
    """One `gh issue create` step, with whatever dedup channel it declares."""

    def __init__(self, source: str, job: str, step: str, run: str) -> None:
        self.source = source
        self.job = job
        self.step = step
        cmds = _invocations(run)
        creates = [c for c in cmds if "gh issue create" in c]
        lookups = [c for c in cmds if "gh issue list" in c]

        self.labels_written: set[str] = set()
        for c in creates:
            self.labels_written.update(_LABEL.findall(c))

        self.channel: str | None = None
        for c in lookups:
            if m := _TITLE_SEARCH.search(c):
                self.channel = f"title:{m.group(1).strip()}"
                break
            if m2 := _LABEL.search(c):
                self.channel = f"label:{m2.group(1)}"
                break

    @property
    def where(self) -> str:
        return f"{self.source}::{self.job}::{self.step}"

    @property
    def job_key(self) -> str:
        return f"{self.source}::{self.job}"


def _filers(sources: dict[str, str]) -> list[Filer]:
    found: list[Filer] = []
    for name, text in sources.items():
        doc = yaml.safe_load(text) or {}
        for job_id, job in (doc.get("jobs") or {}).items():
            for i, step in enumerate(job.get("steps") or []):
                run = step.get("run")
                if not isinstance(run, str) or "gh issue create" not in run:
                    continue
                found.append(Filer(name, job_id, step.get("name") or f"step[{i}]", run))
    return found


def _audit(sources: dict[str, str]) -> dict[str, list[str]]:
    """Return the three problem classes. Empty lists mean clean."""
    filers = _filers(sources)
    no_channel = [f.where for f in filers if f.channel is None]

    unwritable = [
        f"{f.where} searches {f.channel} but files {sorted(f.labels_written) or 'no labels'}"
        for f in filers
        if f.channel
        and f.channel.startswith("label:")
        and f.channel.split(":", 1)[1] not in f.labels_written
    ]

    by_channel: dict[str, set[str]] = {}
    for f in filers:
        if f.channel:
            by_channel.setdefault(f.channel, set()).add(f.job_key)
    crossed = [
        f"{channel} is the dedup key for {len(jobs)} different jobs: {sorted(jobs)}"
        for channel, jobs in by_channel.items()
        if len(jobs) > 1
    ]
    return {"no_channel": no_channel, "unwritable": unwritable, "crossed": crossed}


def _real_sources() -> dict[str, str]:
    sources = {p.name: p.read_text(encoding="utf-8") for p in sorted(WORKFLOWS.glob("*.yml"))}
    assert sources, f"no workflows found under {WORKFLOWS}"
    return sources


@pytest.fixture(scope="module")
def report() -> dict[str, list[str]]:
    return _audit(_real_sources())


def test_the_auditor_actually_finds_the_filers() -> None:
    """Guard the guard: a parser that finds nothing reports everything as clean.

    Without this, a regex or YAML-shape change silently turns the three tests below into
    assertions about an empty list — green, and measuring nothing.
    """
    filers = _filers(_real_sources())
    assert len(filers) >= 4, f"expected at least 4 auto-filers, parsed {len(filers)}"
    assert all(f.labels_written or f.channel for f in filers)


def test_every_autofiler_dedups(report: dict[str, list[str]]) -> None:
    """core#818: a filer with no lookup opens one issue per occurrence, forever."""
    assert report["no_channel"] == [], (
        "auto-filer with no dedup lookup — it will file one issue per run:\n  "
        + "\n  ".join(report["no_channel"])
    )


def test_a_filer_can_find_the_issues_it_files(report: dict[str, list[str]]) -> None:
    """Searching a label the filer does not write dedups on paper and spams in practice."""
    assert report["unwritable"] == [], "\n  ".join(
        ["dedup key is never written by the filer that searches it:", *report["unwritable"]]
    )


def test_dedup_keys_are_distinguishable_across_jobs(report: dict[str, list[str]]) -> None:
    """The trap in the obvious fix: `e2e-failure` is written by two jobs, so keying on it
    makes a staging failure comment onto an SSO thread and vice versa."""
    assert report["crossed"] == [], (
        "two jobs share one dedup channel — their failures will cross-comment:\n  "
        + "\n  ".join(report["crossed"])
    )


# ── negative controls ────────────────────────────────────────────────────────────────
# Both are the real shapes, not invented ones: the first is the `e2e-sso` step exactly as
# it stood before core#818, the second is the fix that would have been applied by copying
# the `e2e-staging` block verbatim. If either of these goes green, the tests above are
# incapable of failing and prove nothing.

_PRE_FIX = """
jobs:
  e2e-sso:
    steps:
      - name: File issue on failure
        run: |
          gh issue create \\
            --repo ${{ github.repository }} \\
            --title "[QA] e2e-sso failure on $SHORT_SHA" \\
            --body "$body" \\
            --label "e2e-failure"
"""

_VERBATIM_COPY = """
jobs:
  e2e-staging:
    steps:
      - name: File issue on failure
        run: |
          existing=$(gh issue list --label e2e-failure --state open --limit 1 \\
            --json number --jq '.[0].number // empty')
          if [ -n "$existing" ]; then
            gh issue comment "$existing" --body "again"
          else
            gh issue create --title "[QA] e2e-staging failure" --label "e2e-failure"
          fi
  e2e-sso:
    steps:
      - name: File issue on failure
        run: |
          existing=$(gh issue list --label e2e-failure --state open --limit 1 \\
            --json number --jq '.[0].number // empty')
          if [ -n "$existing" ]; then
            gh issue comment "$existing" --body "again"
          else
            gh issue create --title "[QA] e2e-sso failure" --label "e2e-failure"
          fi
"""


def test_auditor_rejects_a_filer_with_no_dedup() -> None:
    report = _audit({"pre-fix.yml": _PRE_FIX})
    assert report["no_channel"] == ["pre-fix.yml::e2e-sso::File issue on failure"]


def test_auditor_rejects_the_verbatim_copy_that_crosses_the_wires() -> None:
    report = _audit({"naive.yml": _VERBATIM_COPY})
    assert report["no_channel"] == [], "the naive fix DOES dedup — that is what makes it plausible"
    assert len(report["crossed"]) == 1, report
    assert "label:e2e-failure" in report["crossed"][0]
