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
from datetime import datetime

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

    def started(self) -> datetime:
        return _ts(self.started_at)

    def completed(self) -> datetime:
        return _ts(self.completed_at)


def _ts(value: str) -> datetime:
    return datetime.fromisoformat((value or "").replace("Z", "+00:00"))


# ── the decision, with no network in it ─────────────────────────────────────────────────


def classify(jobs: list[Job], sha: str) -> dict:
    """Verdict per staging job for `sha`, plus what overtook it if anything did."""
    own = {j.name: j for j in jobs if j.head_sha == sha}
    deploy = own.get(MUTATION)
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
            findings[name] = {
                "verdict": "attributed",
                "detail": f"conclusion={job.conclusion}, own deploy at {deploy.completed_at}",
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
                )
            )
    return jobs


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

    result = classify(jobs, sha)
    print(f"staging attribution for {sha[:8]} on {args.branch}\n")
    for name, finding in result["jobs"].items():
        mark = {"attributed": "OK  "}.get(finding["verdict"], "BAD ")
        print(f"  {mark}{name:<15} {finding['verdict']:<14} {finding['detail']}")

    print()
    if result["trustworthy"]:
        print("Every staging verdict for this commit describes this commit's own build.")
        return 0
    print("::error::At least one staging verdict does NOT describe this commit (core#876).")
    print("Re-run the deploy for this SHA and let the verifiers run against it, then re-check.")
    print("A green that belongs to another commit is not evidence about this one.")
    return 1


if __name__ == "__main__":
    sys.exit(main())
