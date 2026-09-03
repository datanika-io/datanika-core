#!/usr/bin/env python3
"""Fail loudly when a scheduled GitHub Actions workflow has stopped firing.

Why this exists (core#628). On 2026-08-30 ``daily-rebuild.yml`` in
``datanika-landing`` was found in state ``disabled_inactivity``, **70 days**
after its last run. GitHub auto-disables ``schedule:`` triggers in a public
repository after 60 days without repository activity, and sends no notification
when it does. So no scheduled blog post auto-published for ten weeks, and
nothing said so.

A cron whose failure mode is silence is the same defect class as the alert rules
in core#615 / core#616: **it looks exactly like a healthy system.** The only way
to tell the difference is for something else to assert that it fired.

This lives in ``datanika-core``, not in the repo it watches, on purpose. Core has
near-daily activity, so its own schedule does not fall to the 60-day rule. A
watchdog inside ``datanika-landing`` would have been disabled by the same rule,
on the same day, as the workflow it was watching.

🚨 **The "thin shell" is what broke, and it broke totally (core#691, 2026-09-03).**
This docstring used to end by saying the pure functions were the part worth
testing and that ``main()`` was a thin shell around ``gh api``. That sentence was
the defect. On 2026-08-31 Dependabot became active on ``datanika-core``, GitHub
began listing a synthesised workflow at path ``dynamic/dependabot/update-graph``,
``contents/dynamic/...`` 404'd, ``_gh`` raised, and the watchdog died on every
run for three consecutive nights having checked **nothing**. 26 unit tests were
green throughout, because every one of them exercises the comparator and not one
of them exercises collection.

Two consequences are designed for below and should not be undone:

* Collection is filtered by :func:`is_repository_workflow`, and the counts are
  asserted **per repo**. Core is collected first, so its four workflows made a
  total-count guard pass while ``datanika-landing`` -- the repo the watchdog was
  built for -- was never reached at all.
* A crash and a finding are no longer the same signal. Both still exit non-zero,
  because the workflow needs that, but the reporting step now files a
  *differently titled* issue when the watchdog itself fails, so "the monitor is
  broken" arrives in the same channel as "the thing being monitored is broken".
  Previously it arrived only as a red tick in the Actions tab, which is
  indistinguishable at a glance from the run that correctly filed core#691.

See ``tests/test_deploy/test_scheduled_workflow_watchdog.py``.
"""

from __future__ import annotations

import argparse
import base64
import json
import re
import subprocess
import sys
from dataclasses import dataclass, field
from datetime import UTC, datetime

HOUR = 3600
DAY = 24 * HOUR

# GitHub's scheduler skew is large and real, and a tight bound here would be a
# check that cannot pass -- the mirror of the alert family fixed in core#604.
#
# Measured 2026-08-30, both repos:
#   datanika-landing  `0 6 * * *`   fired 09:22Z-11:03Z   ->  3h22m - 5h03m late
#   datanika-core     `0 3 * * *`   fired 09:05Z          ->  6h05m late
#   datanika-core     `30 3 * * *`  fired 09:22Z          ->  5h52m late
#
# What matters is not the skew itself but its RANGE, because staleness is the
# gap between consecutive runs: a cron that fires 0h late one day and 12h late
# the next leaves a 36h gap while being perfectly healthy. So the grace has to
# cover the spread, not the mean. 14h is comfortably over twice the worst skew
# observed across both repos, and still catches a genuinely dead cron inside two
# days -- against a fault that ran undetected for seventy.
GRACE_SECONDS = 14 * HOUR

_NUMERIC_DOW = re.compile(r"^[0-7]$")
_NAMED_DOW = re.compile(r"^(MON|TUE|WED|THU|FRI|SAT|SUN)$", re.IGNORECASE)
_STEP = re.compile(r"^\*/(\d+)$")


def _step_of(value: str) -> int | None:
    match = _STEP.match(value)
    if not match:
        return None
    step = int(match.group(1))
    return step if step > 0 else None


def cron_period_seconds(expr: str) -> int | None:
    """Seconds between fires, or None when the shape is not one we model.

    Deliberately narrow. ``None`` means "cannot judge staleness", which is
    reported but never fails the run: guessing a period wrong would produce
    exactly the kind of unfalsifiable check this script exists to prevent.
    """
    fields = expr.split()
    if len(fields) != 5:
        return None
    minute, hour, dom, month, dow = fields
    if month != "*" or dom != "*":
        return None

    if dow == "*":
        if minute == "*":
            return None  # every minute; nothing here is scheduled that way
        step = _step_of(minute)
        if step is not None:
            return step * 60 if hour == "*" else None
        if not minute.isdigit():
            return None
        if hour == "*":
            return HOUR
        step = _step_of(hour)
        if step is not None:
            return step * HOUR
        return DAY if hour.isdigit() else None

    # A day-of-week restriction: weekly only when it names exactly one day.
    if minute.isdigit() and hour.isdigit() and (_NAMED_DOW.match(dow) or _NUMERIC_DOW.match(dow)):
        return 7 * DAY
    return None


@dataclass
class WorkflowState:
    """One workflow, as the watchdog needs to see it."""

    repo: str
    name: str
    path: str
    state: str
    created_at: datetime
    crons: list[str] = field(default_factory=list)
    last_schedule_run: datetime | None = None
    #: The workflow RECORD exists and is listed by the API, but its file is not on
    #: the default branch. See :func:`default_branch_workflow_paths` (core#982).
    file_missing: bool = False

    @property
    def ref(self) -> str:
        return f"{self.repo} :: {self.path}"


def find_problems(
    workflows: list[WorkflowState],
    now: datetime,
    grace: int = GRACE_SECONDS,
) -> tuple[list[str], list[str]]:
    """Return ``(problems, notes)``. Only ``problems`` should fail the run."""
    problems: list[str] = []
    notes: list[str] = []

    for wf in workflows:
        # core#982 — a third state, distinct from both "synthesised by GitHub" and
        # "healthy". Handled BEFORE the no-crons skip: a file we cannot read has no
        # crons, so the skip below would swallow it silently.
        if wf.file_missing:
            if wf.last_schedule_run is not None:
                problems.append(
                    f"{wf.ref} is registered as `{wf.state}` but its FILE is absent from "
                    f"the default branch -- and it HAS fired on a `schedule` event before, "
                    f"most recently {wf.last_schedule_run.isoformat()}. A workflow that "
                    f"used to run on a schedule and whose file is now gone is a schedule "
                    f"that has stopped, which is exactly what this watchdog exists to find. "
                    f"Either restore the file to the default branch, or -- if the removal "
                    f"was intended -- retire the leftover record by deleting that "
                    f"workflow's runs."
                )
            else:
                notes.append(
                    f"{wf.ref} is registered as `{wf.state}` but its FILE is absent from "
                    f"the default branch, and it has NEVER fired on a `schedule` event. "
                    f"Two readings, and the API cannot separate them -- do not assume the "
                    f"first: (a) a leftover record from a branch that carried a workflow "
                    f"file and was then deleted; GitHub keeps the record, and it is retired "
                    f"by deleting that workflow's runs. (b) a scheduled workflow removed "
                    f"from the default branch before it ever fired, in which case the "
                    f"schedule it was supposed to keep is simply not running."
                )
            continue

        if not wf.crons:
            continue

        if wf.state != "active":
            problems.append(
                f"{wf.ref} is in state `{wf.state}`, not `active`. It carries a "
                f"schedule ({', '.join(wf.crons)}) and is not running. GitHub "
                f"disables schedules in a public repo after 60 days without "
                f"repository activity, and notifies nobody when it does."
            )
            continue

        periods = [cron_period_seconds(c) for c in wf.crons]
        known = [p for p in periods if p is not None]
        if not known:
            notes.append(
                f"{wf.ref}: cannot model the period of {wf.crons!r}, so staleness "
                f"was NOT checked. Only its `active` state was verified."
            )
            continue

        budget = min(known) + grace

        if wf.last_schedule_run is None:
            age = (now - wf.created_at).total_seconds()
            if age > budget:
                problems.append(
                    f"{wf.ref} is `active` and has **never** run on a `schedule` "
                    f"event, {age / HOUR:.1f}h after it was created. Expected a "
                    f"fire within {budget / HOUR:.0f}h ({min(known) / HOUR:.0f}h "
                    f"period + {grace / HOUR:.0f}h grace). The schedule most "
                    f"likely never registered -- check the file is on the "
                    f"DEFAULT branch."
                )
            else:
                notes.append(
                    f"{wf.ref}: created {age / HOUR:.1f}h ago and has not fired "
                    f"yet. Too new to judge -- still inside the "
                    f"{budget / HOUR:.0f}h budget."
                )
            continue

        age = (now - wf.last_schedule_run).total_seconds()
        if age > budget:
            problems.append(
                f"{wf.ref} last ran on a `schedule` event at "
                f"{wf.last_schedule_run.isoformat()} -- {age / DAY:.1f} days ago. "
                f"Its cron ({', '.join(wf.crons)}) should have fired within "
                f"{budget / HOUR:.0f}h. It is `active`, so this is NOT the 60-day "
                f"disable; something else is stopping it."
            )

    return problems, notes


# ---------------------------------------------------------------------------
# I/O layer. Everything below shells out to `gh`; none of it is unit-tested.
# ---------------------------------------------------------------------------


def _gh(*args: str) -> str:
    # `encoding=` is not optional: bare `text=True` decodes with the platform
    # locale codec, which on the Windows dev box is cp1251. The runner is UTF-8,
    # so a mojibake bug here would be invisible in CI and would appear only in
    # the local rehearsal path -- the one place a human checks this by hand.
    return subprocess.run(
        ["gh", *args], check=True, capture_output=True, text=True, encoding="utf-8"
    ).stdout


# GitHub's workflows API lists more than the files in `.github/workflows/`. It
# also returns workflows it SYNTHESISES, whose `path` is not a file in the
# repository at all:
#
#     dynamic/dependabot/update-graph          (appears when Dependabot is on)
#     dynamic/pages/pages-build-deployment     (appears when Pages is on)
#
# `repos/<repo>/contents/dynamic/...` 404s for these, and `gh` exits 1. That
# killed this watchdog stone dead for three nights -- see the module docstring.
#
# This is a WHITELIST, not a `dynamic/` blacklist, and deliberately so: GitHub
# only ever executes workflows from `.github/workflows/`, so anything outside it
# cannot be a workflow we own, and a prefix GitHub invents next year is handled
# without a code change.
#
# 🚨 It must stay a path test and must NOT become "swallow the 404". A 404 on a
# real `.github/workflows/*.yml` means the file we are asked to check is
# unreadable -- a token scope, a rename, an API change -- and that has to stay
# fatal. Turning it into "no crons found" would make this watchdog report
# `active, all fine` for a workflow it never actually looked at, which is the
# precise defect it exists to detect, relocated into the detector.
WORKFLOW_DIR_PREFIX = ".github/workflows/"


def is_repository_workflow(path: str) -> bool:
    """True when `path` is a workflow file that lives in this git repository."""
    return path.startswith(WORKFLOW_DIR_PREFIX)


def default_branch_workflow_paths(repo: str, ref: str) -> set[str]:
    """Workflow file paths that actually exist on ``ref``, from ONE directory listing.

    🚨 core#982. GitHub keeps a workflow **record** after the branch that carried
    its file is deleted. On 2026-09-03 ``importtime-probe.yml`` was listed as
    ``active`` while ``git ls-tree`` reported it ABSENT on both ``master`` and
    ``dev``: it came from a throwaway branch, pushed and deleted inside one
    session. Its path passes :func:`is_repository_workflow` -- it *is* a
    ``.github/workflows/`` path, so core#952's whitelist correctly lets it through
    -- and ``contents/<path>`` then 404s and kills the whole run before any repo is
    checked. That would have been the **fifth** consecutive failed night, reading
    as *"core#952's fix does not work"* when the fix works and a new instance of
    the same class had arrived.

    ⚠️ The obvious repair is ``try: _parse_crons(...) except CalledProcessError:
    continue``, and this module's standing rule forbids it: a 404 on a file we
    believe exists must stay fatal, or the watchdog reports ``active, all fine``
    for a workflow it never read -- the precise defect it exists to detect,
    relocated into the detector.

    So instead of catching the failure, ask the default branch what it *has*. One
    call per repo, no error-text parsing, and ``_parse_crons``'s 404 keeps its
    original meaning: a file the listing says is present but which cannot be read
    is still an anomaly and still fatal.
    """
    listing = _gh(
        "api",
        f"repos/{repo}/contents/{WORKFLOW_DIR_PREFIX.rstrip('/')}?ref={ref}",
        "--jq",
        ".[].path",
    )
    paths = {line.strip() for line in listing.splitlines() if line.strip()}
    if not paths:
        # Anti-vacuity. An empty listing makes EVERY workflow look file-missing,
        # which would turn one unreadable directory into a page of plausible
        # findings about workflows that are perfectly healthy.
        raise RuntimeError(
            f"{repo}: the default branch ({ref}) lists no files under "
            f"{WORKFLOW_DIR_PREFIX}. Refusing to continue -- every workflow would "
            f"be reported as missing its file, which is a fault in this check "
            f"rather than in the repository."
        )
    return paths


def _last_schedule_run(repo: str, workflow_id: int | str) -> datetime | None:
    runs = json.loads(
        _gh("api", f"repos/{repo}/actions/workflows/{workflow_id}/runs?event=schedule&per_page=1")
    )
    latest = runs.get("workflow_runs") or []
    return _ts(latest[0]["created_at"]) if latest else None


def _parse_crons(repo: str, path: str, ref: str) -> list[str]:
    """Read the workflow file off the default branch and pull out its crons."""
    import yaml

    raw = _gh("api", f"repos/{repo}/contents/{path}?ref={ref}", "--jq", ".content")
    text = base64.b64decode(raw).decode("utf-8", "replace")
    doc = yaml.safe_load(text) or {}
    # YAML 1.1 parses a bare `on:` key as the boolean True. Handle both spellings.
    triggers = doc.get("on", doc.get(True)) or {}
    if not isinstance(triggers, dict):
        return []
    schedule = triggers.get("schedule") or []
    return [e["cron"] for e in schedule if isinstance(e, dict) and e.get("cron")]


def _ts(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def collect(repo: str) -> list[WorkflowState]:
    default_branch = _gh("api", f"repos/{repo}", "--jq", ".default_branch").strip()
    present = default_branch_workflow_paths(repo, default_branch)
    workflows = json.loads(_gh("api", f"repos/{repo}/actions/workflows", "--paginate"))
    out: list[WorkflowState] = []
    for wf in workflows.get("workflows", []):
        if not is_repository_workflow(wf["path"]):
            # Say so out loud. A silent skip is how a real workflow would one day
            # drop out of the watchlist without anyone noticing.
            print(f"  (skipping {repo} :: {wf['path']} -- synthesised by GitHub, not a repo file)")
            continue
        if wf["path"] not in present:
            # core#982. Reported, never swallowed and never guessed at: whether
            # this is harmless litter or a schedule that has stopped is decided
            # downstream, on whether it ever fired.
            out.append(
                WorkflowState(
                    repo=repo,
                    name=wf["name"],
                    path=wf["path"],
                    state=wf["state"],
                    created_at=_ts(wf["created_at"]),
                    crons=[],
                    last_schedule_run=_last_schedule_run(repo, wf["id"]),
                    file_missing=True,
                )
            )
            continue
        crons = _parse_crons(repo, wf["path"], default_branch)
        if not crons:
            continue
        out.append(
            WorkflowState(
                repo=repo,
                name=wf["name"],
                path=wf["path"],
                state=wf["state"],
                created_at=_ts(wf["created_at"]),
                crons=crons,
                last_schedule_run=_last_schedule_run(repo, wf["id"]),
            )
        )
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description="Watchdog for scheduled workflows.")
    ap.add_argument("repos", nargs="+", help="owner/name, one or more")
    args = ap.parse_args()

    workflows: list[WorkflowState] = []
    # Counted PER REPO, not in total. The repos are collected in order, so a
    # failure while collecting the first one means the later ones were never
    # looked at -- and a total-count guard passes happily on core's four while
    # landing, the repo this watchdog was built for, went unexamined. That is
    # not hypothetical: it is exactly what happened on 2026-08-31..09-02.
    #
    # ⚠️ core#982: counted on workflows that actually carry a cron, NOT on
    # ``len(found)``. A file-missing record is something to report, never
    # something that vouches for a repo being watched -- otherwise a repo whose
    # workflow files had all vanished would satisfy this guard with a page of
    # findings about workflows nothing is running.
    per_repo: dict[str, int] = {}
    for repo in args.repos:
        found = collect(repo)
        per_repo[repo] = len([w for w in found if w.crons])
        workflows.extend(found)

    empty = [repo for repo, n in per_repo.items() if n == 0]
    if empty:
        for repo in empty:
            print(f"::error::No scheduled workflows found in {repo}.")
        print("::error::This watchdog is supposed to have something to watch in")
        print("::error::every repo it is given. Either the schedules were removed,")
        print("::error::or the token cannot read that repo. Both are faults.")
        print(f"::error::Counts this run: {per_repo}")
        return 1

    problems, notes = find_problems(workflows, datetime.now(UTC))

    counts = ", ".join(f"{repo}={n}" for repo, n in per_repo.items())
    scheduled = [w for w in workflows if w.crons]
    orphans = [w for w in workflows if w.file_missing]
    print(
        f"Checked {len(scheduled)} scheduled workflow(s) across {len(args.repos)} repo(s): {counts}"
    )
    for wf in sorted(scheduled, key=lambda w: w.ref):
        last = wf.last_schedule_run.isoformat() if wf.last_schedule_run else "never"
        print(f"  [{wf.state:>20}] {wf.ref}")
        print(f"       cron={wf.crons}  last_schedule_run={last}")
    for wf in sorted(orphans, key=lambda w: w.ref):
        last = wf.last_schedule_run.isoformat() if wf.last_schedule_run else "never"
        print(f"  [{'FILE NOT ON DEFAULT':>20}] {wf.ref}")
        print(f"       state={wf.state}  last_schedule_run={last}")

    for note in notes:
        print(f"::notice::{note}")

    if not problems:
        # ⚠️ Deliberately does not say "all fine": an orphaned record is a note,
        # not a problem, and a closing line that hid it would be the summary
        # contradicting the body two screens above it.
        if orphans:
            print(
                f"\nAll {len(scheduled)} scheduled workflows are active and firing. "
                f"{len(orphans)} workflow record(s) have no file on the default "
                f"branch -- see the notices above and decide which reading applies."
            )
        else:
            print("\nAll scheduled workflows are active and firing.")
        return 0

    print()
    for problem in problems:
        print(f"::error::{problem}")
    with open("problems.md", "w", encoding="utf-8") as fh:
        fh.write("\n".join(f"- {p}" for p in problems) + "\n")
    return 1


if __name__ == "__main__":
    sys.exit(main())
