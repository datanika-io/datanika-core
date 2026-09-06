"""Compute the E2E tier graduation streak from run history (core#1130).

`docs/QA_RULES.md` §10 calls graduation *"mechanical: 3 consecutive greens on `dev`"*. Until
this script it was not mechanical — nothing computed it. A reviewer grepped the last N runs and
counted, and `ci.yml` says out loud that a collision-induced red *"RESETS the
three-consecutive-greens counter"*: a counter that existed only in somebody's head.

The sentence is also underspecified in a way that decides the answer. **It never says what
sequence "consecutive" ranges over**, and on a job whose history is mostly runs that measured
nothing, the candidate readings disagree.

Measured on `dev`, 2026-09-06, the nine completed `e2e-sso` runs after the SAML binding fix::

    17:23  e9e5b510  specs_failed  FAIL         1 failed, 3 skipped, 12 passed
    18:54  6a9a1d0d  wrong_build   UNMEASURED
    18:57  615cafe7  wrong_build   UNMEASURED
    19:01  88f707ac  wrong_build   UNMEASURED   3 skipped, 13 passed   <- SAML full flow PASSED
    19:13  be0bd9b9  no_verdict    UNMEASURED
    19:22  2e92cd6c  wrong_build   UNMEASURED   3 skipped, 13 passed
    19:26  680ef967  wrong_build   UNMEASURED   3 skipped, 13 passed
    19:32  51849052  cancelled     UNMEASURED
    19:38  df0c391c  clean         PASS         3 skipped, 13 passed

**Seven of nine runs carried no reading**, and four runs in which every SSO spec passed
contribute one green between them. Three readings of the same sentence:

* **calendar** — three *adjacent* runs, all green. Cannot be satisfied at this unmeasured rate,
  and an unsatisfiable bar gets lowered rather than met.
* **tally** — three greens *anywhere* in the window. Already satisfied on 2026-09-06 by greens
  that were never adjacent, on a day when exactly one run could report on its own commit.
* **measured** — three adjacent greens in the subsequence of runs that actually measured
  something. **This is what the script implements.**

Two asymmetries make the measured reading honest, and they are the part worth arguing about:

* an **UNMEASURED** run is transparent. `wrong_build` means "this run cannot report on this
  commit"; we know it carried no reading, so it neither advances nor resets.
* an **UNREADABLE** run breaks the streak. We do *not* know what it carried, and treating it as
  transparent is assuming it was not a red — the reassuring assumption, which is the one this
  project keeps paying for.

A streak assembled from a window that measured almost nothing reports **`sparse`** rather than
`graduate`. That is deliberately a third state and not a silent pass: the same three greens can
mean "stable across three runs" or "the only three readings in a fortnight", and only a human
should decide which. Same shape as the `empty` / `unknown` / `no-evidence` states this codebase
already uses wherever a verdict can be absent.

Usage
-----
::

    python scripts/e2e_tier_streak.py                      # e2e-sso on dev
    python scripts/e2e_tier_streak.py --job e2e-staging    # the informational tier
    python scripts/e2e_tier_streak.py --runs 40 --required 3

Exit 0 when the tier has graduated, 1 otherwise — including `no-data` and `sparse`, both of
which mean *"do not graduate on this evidence"* rather than *"the tier is red"*.

⚠️ **Actions logs expire.** GitHub retains them for 90 days by default, so a verdict older than
that reads as UNREADABLE and breaks a streak. That is the correct direction (an unreadable run is
not a green) but it means graduation evidence decays and cannot be reconstructed later. Record
the SHAs in the spec's tier header when a spec graduates, as `golden-path.spec.ts` already does.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from dataclasses import dataclass

REPO = "datanika-io/datanika-core"
WORKFLOW = ".github/workflows/ci.yml"

#: A run produced a green reading of the tier.
PASS = "PASS"  # noqa: S105  - a verdict class, not a credential
#: A run produced a red reading of the tier.
FAIL = "FAIL"
#: A run produced no reading, and we know why. Transparent to the streak.
UNMEASURED = "UNMEASURED"
#: A run produced no reading we can recover. Breaks the streak — see the module docstring.
UNREADABLE = "UNREADABLE"

#: `ci.yml`, `e2e-sso`, step "Classify what this job's result means".
SSO_VERDICTS: dict[str, str] = {
    "clean": PASS,
    # The specs ran and passed; the job is red for a non-test reason (artifact upload, or the
    # Authentik cleanup). core#873. That is a green reading of the SSO surface.
    "infra_only": PASS,
    "specs_failed": FAIL,
    # The specs may well have passed — four such runs on 2026-09-06 each carried `13 passed` —
    # but staging was not running this commit, so as the classifier's own message puts it,
    # "a green here would not have been this commit's green either". core#876.
    "wrong_build": UNMEASURED,
    "no_verdict": UNMEASURED,
    "cancelled": UNMEASURED,
}

#: `staging.yml`, `e2e-staging`, step "Report informational tier result".
INFORMATIONAL_VERDICTS: dict[str, str] = {
    "success": PASS,
    "failure": FAIL,
    # The informational tier is legitimately empty between graduations. Three greens over an
    # empty tier would "graduate" nothing at all.
    "empty": UNMEASURED,
    "unknown": UNMEASURED,
}

#: Verdict token -> class. **Both vocabularies**, because one policy is emitted by two jobs.
#:
#: `tests/test_deploy/test_e2e_tier_streak.py` pins this against the two workflow files in
#: BOTH directions, and the second direction is the one that matters:
#:
#: * forward — every token a classifier can emit must appear here, so a new verdict state is
#:   triaged deliberately instead of being silently absorbed as UNREADABLE;
#: * backward — every token here must still be found in the workflow. That is the anti-vacuity
#:   control, and it is derived rather than a floor. A floor of "at least 5 states" was the
#:   first attempt and it was measured tolerating the exact regression it existed to catch:
#:   the classifier emits six, so dropping one left five and the control stayed green.
VERDICT_CLASS: dict[str, str] = {**SSO_VERDICTS, **INFORMATIONAL_VERDICTS}

#: `STATE=<token>` assignments in a shell block. Anchored to an assignment so that a *read*
#: (`[ "$JOB_STATUS" = "success" ]`) cannot enter the vocabulary.
_STATE_ASSIGN = re.compile(r"(?<![\w$])STATE=([a-z_][a-z0-9_]*)")

#: The two verdict lines, as EXECUTED. Every runner log line carries an ISO stamp, and the
#: classifier's own source is echoed into the `##[group]Run` header with the variables
#: unexpanded — matching that would be measuring the script rather than the run, so both
#: patterns require a literal token where the shell source has `$STATE`.
_SSO_VERDICT = re.compile(
    r"SSO specs outcome: (?P<specs>[a-z_]+) / job status: [a-z_]+ / "
    r"verdict: (?P<verdict>[a-z_]+)\s*$"
)
_INFO_VERDICT = re.compile(r"(?:^|\s)INFORMATIONAL_RESULT=([a-z_]+)\s*$")

_ANSI = re.compile(r"\x1b\[[0-9;]*m")


def verdict_states_in_workflow(run_block: str) -> set[str]:
    """Every verdict token a classifier `run:` body can assign to `STATE`."""
    return set(_STATE_ASSIGN.findall(run_block))


def classify_verdict(verdict: str | None, specs_outcome: str | None = None) -> str:
    """Map a verdict token to its class. Anything unrecognised is UNREADABLE, never a PASS.

    `specs_outcome` disambiguates `wrong_build` (core#1151). A run whose specs FAILED is not a
    non-measurement just because the build could not be attributed: for a *graduation* question
    the property is stability, and specs failing anywhere in the window is exactly what should
    reset the streak -- even when that failure belongs to somebody else's build.

    ⚠️ Deliberately NOT "wrong_build always resets". Seven of ten runs were unmeasured on
    2026-09-06; resetting on all of them makes the bar unsatisfiable, which is the failure mode
    `docs/QA_RULES.md` §10 exists to avoid.
    """
    if verdict is None:
        return UNREADABLE
    if verdict in ("wrong_build", "cancelled") and specs_outcome == "failure":
        return FAIL
    return VERDICT_CLASS.get(verdict, UNREADABLE)


def parse_verdict_line(log_lines: list[str]) -> str | None:
    """The verdict token from a job log, or `None` if the log does not carry one.

    `None` is the honest answer for an empty log (a cancelled job's log is zero bytes) and for
    a log whose classifier never ran. It is deliberately not a class: the caller decides
    whether an absent line is UNMEASURED (because the job conclusion explains it) or
    UNREADABLE.

    The **last** match wins. The classifier line appears twice in a normal log — once as the
    echoed script source inside the `##[group]Run` header, and once as real output — and only
    the second describes this run.
    """
    found: str | None = None
    for raw in log_lines:
        line = _ANSI.sub("", raw).rstrip("\r\n")
        # The echoed source ends in a quote and carries `$STATE`; requiring the line to END at
        # the token rejects it without having to model the header's shape.
        if line.endswith('"') or line.endswith("'"):
            continue
        for pattern in (_SSO_VERDICT, _INFO_VERDICT):
            m = pattern.search(line)
            if m:
                found = m.group("verdict") if "verdict" in m.groupdict() else m.group(1)
    return found


def parse_specs_outcome(log_lines: list[str]) -> str | None:
    """`SPECS_OUTCOME` from the same classifier line the verdict comes from (core#1151).

    `wrong_build` is a verdict about **attribution**, and `ci.yml` decides it *before* it looks
    at the specs -- deliberately, since a run on the wrong build cannot report either way. So
    the token conflates `success`, `skipped` and `failure`, and reading it alone as "this run
    carried no reading" is a claim about the specs drawn from a field that records attribution
    (`ENGINEERING_RULES` §39).

    Returns `None` for lines that carry no such field (the `INFORMATIONAL_RESULT=` form), and
    for the echoed script source, whose `$SPECS_OUTCOME` is unexpanded.
    """
    found: str | None = None
    for raw in log_lines:
        line = _ANSI.sub("", raw).rstrip()
        if line.endswith('"') or line.endswith("'"):
            continue
        m = _SSO_VERDICT.search(line)
        if m:
            found = m.group("specs")
    return found


def streak(classes: list[str]) -> int:
    """Trailing consecutive PASSes over the MEASURED subsequence.

    `classes` is oldest-first. Trailing rather than "anywhere" because `ci.yml` already says a
    red *resets* the counter, and a counter that resets is by definition read from the end.
    """
    n = 0
    for cls in reversed(classes):
        if cls == UNMEASURED:
            continue  # transparent: we know this run carried no reading
        if cls != PASS:
            break  # FAIL resets; UNREADABLE blocks, because we cannot rule out a FAIL
        n += 1
    return n


@dataclass(frozen=True)
class Reading:
    """What the run history supports, and how much of it was actually a measurement."""

    streak: int
    required: int
    measured: int
    total: int
    span: int
    state: str

    @property
    def graduated(self) -> bool:
        return self.state == "graduate"

    @classmethod
    def from_classes(cls, classes: list[str], *, required: int = 3, max_span: int = 10) -> Reading:
        """Classify a history.

        `max_span` bounds how many CALENDAR runs the streak may be drawn from before it is
        reported `sparse`. It is a judgement, not a measurement, and it is exposed as a flag
        so that whoever changes it has to say so.
        """
        total = len(classes)
        measured = sum(1 for c in classes if c in (PASS, FAIL, UNREADABLE))
        n = streak(classes)

        # How many calendar runs the trailing streak reaches back through.
        span = 0
        seen = 0
        for c in reversed(classes):
            span += 1
            if c == UNMEASURED:
                continue
            if c != PASS:
                span -= 1  # the blocking run is not part of the streak's span
                break
            seen += 1
            if seen == n:
                break

        if total < required:
            state = "no-data"
        elif n < required:
            state = "not-yet"
        elif span > max_span:
            state = "sparse"
        else:
            state = "graduate"
        return cls(
            streak=n, required=required, measured=measured, total=total, span=span, state=state
        )


# --------------------------------------------------------------------------------------
# GitHub API layer
# --------------------------------------------------------------------------------------


def _gh(path: str, *, raw: bool = False) -> object:
    # S603/S607: fixed argv, no shell. `gh` is resolved from PATH deliberately — it lives at
    # /d/Tools/gh/bin on the dev machine and /usr/bin on a runner.
    #
    # `encoding="utf-8"` is not decoration: the Windows locale codec here is cp1251 and a bare
    # `text=True` mis-decodes every non-ASCII byte (WORKFLOW_RULES §7).
    out = subprocess.run(  # noqa: S603
        ["gh", "api", path],  # noqa: S607
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    if out.returncode != 0:
        raise SystemExit(f"gh api {path} failed:\n{out.stderr.strip()}")
    return out.stdout if raw else json.loads(out.stdout)


def _gh_log_or_none(repo: str, job_id: int) -> str | None:
    """A job's raw log, or `None` when GitHub will not serve it.

    Not every completed job has a retrievable log: a **cancelled** job's is zero bytes, and
    Actions logs are **expired after 90 days**, both answering `404`. Neither is a reason to
    crash, and — more importantly — neither is a reason to report a pass. The caller turns
    `None` into UNMEASURED when the job conclusion explains it and UNREADABLE otherwise, and
    UNREADABLE blocks a streak.
    """
    out = subprocess.run(  # noqa: S603
        ["gh", "api", f"repos/{repo}/actions/jobs/{job_id}/logs"],  # noqa: S607
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    return out.stdout if out.returncode == 0 else None


def collect(repo: str, branch: str, job_name: str, runs: int) -> list[tuple[str, str, str]]:
    """`(created_at, short_sha, class)` per completed run, oldest first.

    ``event=push`` is not optional. A `dev` head carries a `merge_group` run too, whose staging
    jobs are `skipped` **by design** — byte-identical to the condition that holds a promotion,
    produced by a run nobody asked for.
    """
    payload = _gh(f"repos/{repo}/actions/runs?branch={branch}&event=push&per_page={runs}")
    out: list[tuple[str, str, str]] = []
    fetched = failed = 0
    for run in payload.get("workflow_runs", []):  # type: ignore[union-attr]
        if run.get("path") != WORKFLOW or run.get("status") != "completed":
            continue
        jobs = _gh(f"repos/{repo}/actions/runs/{run['id']}/jobs?per_page=100")
        job = next(
            (j for j in jobs.get("jobs", []) if job_name in j["name"]),  # type: ignore[union-attr]
            None,
        )
        # An IN-PROGRESS job has no verdict YET, which is a different fact from a completed
        # job whose verdict cannot be read. Conflating them makes a healthy running job read
        # as an instrument failure, and a check that reds on a healthy system gets deleted.
        if job is None or job.get("status") != "completed":
            continue

        log = _gh_log_or_none(repo, job["id"])
        if log is None:
            failed += 1
        else:
            fetched += 1
        lines = log.splitlines() if log is not None else []
        verdict = parse_verdict_line(lines) if log is not None else None
        specs = parse_specs_outcome(lines) if log is not None else None

        if verdict is None and job.get("conclusion") == "cancelled":
            # A cancelled job's log is zero bytes by construction. That is a KNOWN
            # non-measurement, not a broken instrument — fall back to the job conclusion
            # rather than reporting the reader as broken.
            verdict = "cancelled"

        out.append((run["created_at"], run["head_sha"][:8], classify_verdict(verdict, specs)))

    # If NOTHING could be fetched, this is an instrument failure and must be loud. Silently
    # classifying every run UNREADABLE blocks a streak, which is the safe direction — and it
    # reads identically to a tier that is genuinely never measured.
    if failed and not fetched:
        raise SystemExit(
            f"could not fetch a single job log ({failed} attempts, all failed). "
            "This says nothing about the tier — fix the reader before reading the verdict."
        )
    return list(reversed(out))


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--repo", default=REPO)
    ap.add_argument("--branch", default="dev")
    ap.add_argument("--job", default="e2e-sso", help="job name substring (e2e-sso, e2e-staging)")
    ap.add_argument("--runs", type=int, default=25)
    ap.add_argument("--required", type=int, default=3)
    ap.add_argument(
        "--max-span",
        type=int,
        default=10,
        help="calendar runs the streak may span before it reports `sparse` (a judgement)",
    )
    args = ap.parse_args(argv)

    history = collect(args.repo, args.branch, args.job, args.runs)
    for created, sha, cls in history:
        print(f"{created}  {sha}  {cls}")

    r = Reading.from_classes(
        [c for _, _, c in history], required=args.required, max_span=args.max_span
    )
    print()
    print(f"job            : {args.job} on {args.branch}")
    print(f"runs read      : {r.total}  (measured: {r.measured})")
    print(f"trailing streak: {r.streak} / {r.required}   spanning {r.span} calendar run(s)")
    print(f"verdict        : {r.state}")
    if r.state == "sparse":
        print(
            "  -> the streak is real but drawn from a window that measured almost nothing.\n"
            "     Three greens across a fortnight are not three greens across three runs.\n"
            "     A human decides; this script will not graduate it."
        )
    if r.state == "no-data":
        print(
            f"  -> fewer than {r.required} completed runs were read. This is NOT 'the tier is\n"
            "     red' — it is 'there is nothing here to read'."
        )
    return 0 if r.graduated else 1


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
