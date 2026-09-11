"""Does this commit have a staging verdict that actually describes it? (core#876)

The promotion pre-flight asks "did `e2e-staging` pass on `dev`'s head?". That question has
a green answer for commits whose E2E never touched their own build. `staging-deploy`
serialises *access* to staging and never pins *identity*, so run A's verifier can start
seconds after run B's deploy finished and grade B's build under A's name.

Measured on `dev`, twice inside one promoted batch::

    1da0c21  deploy-staging  22:08:50 -> 22:12:44
    87da585  deploy-staging  22:12:46 -> 22:16:48
    1da0c21  e2e-staging     22:16:50 -> 22:24:05   green, describes 87da585

`scripts/assert-staging-sha.sh` stops this happening in *future* runs. This script answers
the promoter's question about runs that already happened — including the ones from before
that assertion shipped, which is every run currently in the tracker.

It also covers the half the in-run assertion cannot: GitHub keeps one pending job per
concurrency group, so a burst of pushes **cancels** a run's queued verification. A cancelled
job is neither green nor red; it reads as absent, and absent is what a promoter skims past.

Usage
-----
    python scripts/verify_e2e_attribution.py --sha <dev head>
    python scripts/verify_e2e_attribution.py            # resolves dev's head itself

Exit 0 only when every staging verdict for that commit genuinely describes it. Exit 1 is
"do not read these greens as this commit's" — not necessarily "do not promote", but the
promoter must then get an honest reading before deciding.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

REPO = "datanika-io/datanika-core"
WORKFLOW = ".github/workflows/ci.yml"
MUTATION = "deploy-staging"
VERIFIERS = ("smoke-staging", "e2e-staging", "e2e-sso")


def short_name(api_name: str) -> str:
    """`"staging / smoke-staging"` -> `"smoke-staging"`; anything else unchanged.

    core#975 moved the staging jobs into a reusable workflow held by one caller job, and a
    called workflow's check runs are named **`<caller job id> / <callee job id>`**. The
    rename is the cost of that change and was measured before it was made, not discovered
    after — but it lands squarely on this script, whose entire job is to look staging
    verdicts up by name.

    Stripping rather than renaming the constants, deliberately, for two reasons:

    * this script's OUTPUT is quoted in `RUNBOOK_DEV_TO_MASTER.md` and read by whoever is
      mid-promotion, and `smoke-staging` is the name they know;
    * a bare `smoke-staging` still resolves, so the script keeps working against runs that
      predate the move — which is exactly when someone is most likely to be reading old
      evidence.

    ⚠️ It splits on the LAST separator on purpose. A nested reusable workflow produces
    `a / b / c`, and the job that ran is `c`.
    """
    return api_name.rsplit(" / ", 1)[-1].strip()


#: A conclusion that is neither a pass nor a failure. The whole point of naming these is
#: that they are the ones a promoter's eye slides over.
NO_READING = {None, "", "cancelled", "skipped", "stale"}


@dataclass(frozen=True)
class Job:
    run_id: int
    head_sha: str
    name: str
    started_at: str
    completed_at: str
    conclusion: str | None
    #: Needed to fetch the job's own classifier verdict (core#1174). Defaulted so that
    #: existing constructions in tests keep working.
    job_id: int = 0

    def started(self) -> datetime:
        return _ts(self.started_at)

    def completed(self) -> datetime:
        return _ts(self.completed_at)


def _ts(value: str) -> datetime:
    return datetime.fromisoformat((value or "").replace("Z", "+00:00"))


# ── the decision, with no network in it ─────────────────────────────────────────────────


#: Verdict classes from `scripts/e2e_tier_streak.py` that mean **this job produced no reading**.
#: `PASS` and `FAIL` are readings and stay `attributed` — a red that genuinely belongs to this
#: commit is exactly what a promoter needs to see, and hiding it behind `no_verdict` would be
#: the same defect pointed the other way.
NO_READING_CLASSES = {"UNMEASURED", "UNREADABLE"}

#: A reading of something that is not the deployed system (core#1232). Reported separately from
#: `no_verdict` on purpose: the promoter's next move differs. `no_verdict` says *"go get a
#: reading"*; this says *"a reading exists and it is of the wrong machine"*, which is the more
#: dangerous of the two precisely because there is a build behind it.
LOCAL_CLASS = "LOCAL"


def classify(jobs: list[Job], sha: str, verdict_classes: dict[str, str] | None = None) -> dict:
    """Verdict per staging job for `sha`, plus what overtook it if anything did.

    `verdict_classes` maps job name -> the class its own classifier step reported, as produced
    by `scripts/e2e_tier_streak.classify_verdict`. Passed in rather than fetched so this
    function stays network-free, which is the property the section header promises.

    ── core#1174 ────────────────────────────────────────────────────────────────────────────
    Until 2026-09-07 this function asked exactly one question — *did a later deploy overtake
    this job's window?* — and answered `attributed` whenever nothing had. That is a true
    answer to a question the promoter is not asking.

    Measured on `dev` head `310137d0`: `e2e-sso` failed its own step 7 ("Assert staging is
    running THIS commit"), skipped steps 8-15, ran **zero** SSO specs, and self-classified
    `verdict: wrong_build`. Nothing had overtaken its window, so this function reported
    `OK e2e-sso attributed` and `main()` printed *"Every staging verdict for this commit
    describes this commit's own build"* and exited **0** — an all-clear over a tier that had
    measured nothing, in the last instrument a promoter reads before merging to `master`.

    🔑 The two readings were never contradicting, and that is why it was invisible: *"was this
    window overtaken?"* and *"did this job produce a reading?"* are different questions, and
    only the first was ever asked. Nobody was wrong; the question was.

    ⚠️ **A job whose log carries no classifier line is left `attributed`.** Not every job has a
    classifier step, and treating a missing line as a failure would red `smoke-staging` on
    every clean run — over-firing, which is how a guard gets switched off. The detail string
    says `verdict=<none>` so the absence is visible rather than assumed away. That residual is
    the remaining half of core#714's option 2 (tier the three jobs explicitly); this change is
    the narrow version, which is the whole property: **refuse to grade an absent reading.**
    """
    own = {j.name: j for j in jobs if j.head_sha == sha}
    deploy = own.get(MUTATION)
    verdict_classes = verdict_classes or {}
    findings: dict[str, dict] = {}

    for name in VERIFIERS:
        job = own.get(name)
        if job is None:
            findings[name] = {"verdict": "absent", "detail": "the job never ran"}
            continue
        if job.conclusion in NO_READING:
            findings[name] = {
                "verdict": "no_reading",
                "detail": f"conclusion={job.conclusion!r} — neither green nor red",
            }
            continue
        if deploy is None or deploy.conclusion != "success":
            findings[name] = {
                "verdict": "no_deploy",
                "detail": "this commit has no successful deploy-staging of its own",
            }
            continue
        if deploy.completed() > job.started():
            findings[name] = {
                "verdict": "impossible",
                "detail": "the verifier started before its own deploy finished",
            }
            continue

        overtaken = sorted(
            (
                j
                for j in jobs
                if j.name == MUTATION
                and j.head_sha != sha
                and j.conclusion == "success"
                and deploy.completed() <= j.completed() <= job.started()
            ),
            key=lambda j: j.completed(),
        )
        if overtaken:
            last = overtaken[-1]
            findings[name] = {
                "verdict": "misattributed",
                "detail": (
                    f"{last.head_sha[:8]}'s deploy finished at {last.completed_at} — "
                    f"inside this job's own window ({deploy.completed_at} -> "
                    f"{job.started_at}). This result describes {last.head_sha[:8]}'s build."
                ),
            }
        else:
            # core#1174. The window is this commit's — now ask whether the job actually
            # graded anything in it.
            klass = verdict_classes.get(name)
            if klass == LOCAL_CLASS:
                findings[name] = {
                    "verdict": "local_run",
                    "detail": (
                        "this job's log attests to an environment that is not the deployed "
                        "one, so its result describes a different machine — different image, "
                        "different network, no Apache, no blue/green. It may well be green; "
                        "it is not this commit's staging verdict."
                    ),
                }
            elif klass in NO_READING_CLASSES:
                findings[name] = {
                    "verdict": "no_verdict",
                    "detail": (
                        f"the job's own classifier reported {klass} — it produced NO reading "
                        f"of this commit (conclusion={job.conclusion}). The window was not "
                        f"overtaken; the job simply did not grade."
                    ),
                }
            else:
                shown = klass or "<none>"
                findings[name] = {
                    "verdict": "attributed",
                    "detail": (
                        f"conclusion={job.conclusion}, verdict={shown}, "
                        f"own deploy at {deploy.completed_at}"
                    ),
                }

    trustworthy = all(f["verdict"] == "attributed" for f in findings.values())
    return {"sha": sha, "jobs": findings, "trustworthy": trustworthy}


# ── the network half ────────────────────────────────────────────────────────────────────


def _gh(path: str) -> object:
    # S603/S607: the argv is a fixed literal plus a path built from our own constants and a
    # sha; no shell, so nothing is interpretable as a command. `gh` is resolved from PATH
    # deliberately — it lives at /d/Tools/gh/bin on the dev machine and /usr/bin on a runner.
    out = subprocess.run(  # noqa: S603
        ["gh", "api", path],  # noqa: S607
        capture_output=True,
        text=True,
        encoding="utf-8",
        check=False,
    )
    if out.returncode != 0:
        raise SystemExit(f"gh api {path} failed:\n{out.stderr.strip()}")
    return json.loads(out.stdout)


def collect(repo: str, branch: str, pages: int) -> list[Job]:
    """Every staging job on the recent `ci.yml` runs of `branch`."""
    runs = _gh(f"repos/{repo}/actions/runs?branch={branch}&per_page={pages}")
    jobs: list[Job] = []
    for run in runs.get("workflow_runs", []):  # type: ignore[union-attr]
        if run.get("path") != WORKFLOW:
            continue
        payload = _gh(f"repos/{repo}/actions/runs/{run['id']}/jobs")
        for job in payload.get("jobs", []):  # type: ignore[union-attr]
            name = short_name(job["name"])
            if name not in (MUTATION, *VERIFIERS):
                continue
            if not job.get("started_at"):
                continue
            jobs.append(
                Job(
                    run_id=run["id"],
                    head_sha=run["head_sha"],
                    name=name,
                    started_at=job["started_at"],
                    completed_at=job.get("completed_at") or job["started_at"],
                    conclusion=job.get("conclusion"),
                    job_id=job.get("id", 0),
                )
            )
    return jobs


def verdict_classes_for(repo: str, jobs: list[Job], sha: str) -> dict[str, str]:
    """Each verifier's own classifier verdict, as a class (core#1174).

    🔑 **The vocabulary is imported from `scripts/e2e_tier_streak.py`, never re-derived.**
    That module already defines `wrong_build` / `no_verdict` / `cancelled` and their classes,
    it is pinned against both workflow files in both directions by
    `tests/test_deploy/test_e2e_tier_streak.py`, and QA fixed a real polarity defect in it on
    2026-09-06 (`wrong_build` was transparent to the streak and could hide a spec failure).
    Two independent definitions of the same verdict is how they drift apart, and the drift is
    invisible until one of them is wrong at the moment it matters.

    A log GitHub will not serve (cancelled job: zero bytes; anything older than 90 days: 404)
    yields no entry, which leaves `classify()` on its existing behaviour rather than inventing
    a verdict from an absence.
    """
    sys.path.insert(0, str(Path(__file__).resolve().parent))
    from e2e_tier_streak import (  # noqa: PLC0415
        _gh_log_or_none,
        classify_verdict,
        parse_specs_outcome,
        parse_verdict_line,
    )

    out: dict[str, str] = {}
    for job in jobs:
        if job.head_sha != sha or job.name not in VERIFIERS or not job.job_id:
            continue
        # core#1285. `_gh_log_or_none` returns `(log, reason)` since core#1273 -- the
        # reason is returned rather than discarded, because answering `None` for every
        # failure alike once cost a diagnosis that one line of stderr would have ended.
        # This caller still unpacked it as a bare string and died with
        # "'tuple' object has no attribute 'splitlines'" -- the SECOND time this
        # pre-flight has crashed on a change to that module's contract (see the
        # core#1205 note just below). A crash is neither a pass nor a refusal: it is
        # no reading at all, from the gate whose entire job is to produce one.
        log, why = _gh_log_or_none(repo, job.job_id)
        if log is None:
            # Surfaced, not swallowed. `classify()` keeps its existing behaviour for a
            # job with no verdict class, which is correct -- but an operator reading
            # 'no verdict' deserves to know the log was unreadable and why.
            print(
                f"  note: no log for {job.name} ({why}); its verdict class is unavailable",
                file=sys.stderr,
            )
            continue
        lines = log.splitlines()
        # core#1205, second half. `parse_verdict_line` now REFUSES a log carrying two
        # tiers unless the caller names one — QA's fix, and the right shape. This
        # caller never named one, so after that landed it raised `AmbiguousVerdictError`
        # on every `e2e-staging` log and the promotion pre-flight crashed instead of
        # reporting. Fixed here rather than by loosening the parser: the refusal is
        # correct and the missing name was the defect.
        #
        # 🔑 The tier differs BY CALLER on the same log, which is the whole lesson.
        # `e2e_tier_streak.py` asks `informational` (it measures graduation of that
        # tier); a promotion asks **`gating`** — "did the specs that gate a release
        # pass?". Reading the other one is how a clean tier was reported as FAIL.
        #
        # `smoke-staging` emits neither pattern, so `gating` yields None, which
        # `classify()` already renders as `verdict=<none>` rather than a reading.
        tier = "sso" if "sso" in job.name else "gating"
        verdict = parse_verdict_line(lines, tier=tier)
        if verdict is None:
            continue
        out[job.name] = classify_verdict(verdict, parse_specs_outcome(lines))
    return out


def write_verdict(path: str | None, state: str, sha: str, detail: str = "") -> None:
    """Record a verdict that NAMES THE COMMIT it describes.

    The sha is not decoration. A verdict file left over from an earlier run would
    otherwise vouch for whatever head is promoted next — which is core#876's whole
    lesson ("a green that belongs to another commit is not evidence about this one")
    applied to the gate's own artifact instead of to the jobs it reads.

    Called only on a path where a verdict actually exists. An exception anywhere
    above leaves no file, and *that* is the signal: no artifact means no reading,
    which a caller must treat differently from a reading that says no.
    """
    if not path:
        return
    stamp = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    line = f"PROMOTION-GATE {state} sha={sha} at={stamp} {detail}".rstrip()
    Path(path).write_text(line + "\n", encoding="utf-8")


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--sha", help="commit to check (default: the branch head)")
    ap.add_argument("--repo", default=REPO)
    ap.add_argument("--branch", default="dev")
    # 100 is the API maximum for `per_page` and costs the same one request as 15
    # did (core#918). The old default of 15 covered roughly TWO HOURS of `dev`
    # wall-clock at this repo's cadence — five departments pushing every 10-15
    # minutes, more than one run per push — so any commit you deliberately waited
    # on before promoting had already fallen out of the window. It refused on
    # well-formed input at the highest-stakes moment in the workflow, which is how
    # `#876`'s own warning comes true: "a verifier that always refuses is a
    # verifier somebody deletes".
    ap.add_argument("--pages", type=int, default=100, help="how many recent runs to scan")
    # core#1287. A POSITIVE artifact, so a promotion consumes evidence rather than the
    # absence of a complaint. This script has crashed twice on a change to
    # `e2e_tier_streak`'s contract (core#1205, core#1285), and an unhandled exception
    # ALSO exits 1 — so "the gate refused" and "the gate broke" are the same signal to
    # anything reading only the exit code. The file is written only once a verdict
    # exists, so a crash leaves none and the caller can tell those two apart.
    ap.add_argument(
        "--verdict-file",
        help="write a machine-readable verdict here (an absent file means NO verdict)",
    )
    args = ap.parse_args(argv)

    sha = args.sha
    if not sha:
        ref = _gh(f"repos/{args.repo}/git/ref/heads/{args.branch}")
        sha = ref["object"]["sha"]  # type: ignore[index]

    jobs = collect(args.repo, args.branch, args.pages)

    # ⚠️ The API returns the FULL 40-character head_sha, so an exact comparison
    # refuses every short SHA — and refuses it with the window message above,
    # naming a cause that has nothing to do with what happened. A promoter who
    # pastes `db83fc24` off a git log is told CI has not run on that commit,
    # "which is NOT a pass". Resolving the prefix to the full SHA once, here, also
    # fixes `classify()`, which compares the same way at two more sites.
    matched = {j.head_sha for j in jobs if j.head_sha == sha or j.head_sha.startswith(sha)}
    if len(matched) > 1:
        print(
            f"::error::{sha} is ambiguous — it prefixes {len(matched)} commits: "
            f"{', '.join(sorted(s[:12] for s in matched))}. Pass more characters."
        )
        return 1
    if not matched:
        # Two different facts, deliberately worded apart (core#918). Only the
        # second is an attribution finding; the first is a search that did not
        # reach far enough, and leading with `::error::` for it trains people to
        # distrust the tool.
        print(
            f"::warning::no staging jobs found for {sha[:8]} in the last {args.pages} runs "
            f"on `{args.branch}`."
        )
        print("This is a WINDOW result, not an attribution failure: the scan did not reach this")
        print("commit. Either widen --pages, or CI genuinely has not run on it — and neither")
        print("of those is a pass.")
        return 1
    sha = matched.pop()

    result = classify(jobs, sha, verdict_classes_for(args.repo, jobs, sha))
    print(f"staging attribution for {sha[:8]} on {args.branch}\n")
    for name, finding in result["jobs"].items():
        mark = {"attributed": "OK  "}.get(finding["verdict"], "BAD ")
        print(f"  {mark}{name:<15} {finding['verdict']:<14} {finding['detail']}")

    print()
    if result["trustworthy"]:
        # ⚠️ core#1174: this sentence used to print over a tier that had measured NOTHING.
        # It now says what it actually checked, in both halves, because the old wording was
        # true and read as an all-clear — the exact shape this script exists to catch.
        print("Every staging verdict for this commit describes this commit's own build,")
        print("and every job produced an actual reading.")
        write_verdict(args.verdict_file, "OK", sha, "checks=all-attributed")
        return 0

    absent = [n for n, f in result["jobs"].items() if f["verdict"] == "no_verdict"]
    if absent:
        print(f"::error::{', '.join(absent)} produced NO READING of this commit (core#1174).")
        print("The window was not overtaken — the job simply did not grade. A green here")
        print("would be an all-clear over a tier that measured nothing.")
        print("Re-run this commit's deploy so staging is running it, then re-run the verifier.")
    if any(f["verdict"] not in ("attributed", "no_verdict") for f in result["jobs"].values()):
        print("::error::At least one staging verdict does NOT describe this commit (core#876).")
        print("Re-run the deploy for this SHA and let the verifiers run against it, then re-check.")
        print("A green that belongs to another commit is not evidence about this one.")
    # A REFUSAL is still a verdict, and is written as one. The caller must be able to
    # distinguish "I looked and the answer is no" from "I never produced an answer" —
    # collapsing those is how a crash gets read as tooling noise and waved through.
    write_verdict(args.verdict_file, "REFUSED", sha, "see stderr")
    return 1


if __name__ == "__main__":
    sys.exit(main())
