"""Turn a trivy JSON report into edge-triggered GitHub issues (core#1166).

WHY THIS EXISTS
---------------
`image-cve` in ci.yml answers *"did this PR introduce a CVE?"*. It has no
`schedule:` trigger, so it only evaluates when someone pushes — and an advisory
published on a quiet weekend is invisible until the next push. That is exactly
the two-day gap that prompted core#1166: **a correct red went unread**, and the
defect was never that the check was optional.

This script answers the other question — *"does what we are running right now
have a known CVE?"* — which is the only one whose answer can change while nobody
pushes.

DESIGN NOTES THAT ARE LOAD-BEARING
----------------------------------
**Edge-triggered, not state-triggered.** One issue per advisory id, filed only
when no open issue already names it. A notification on every red would have fired
on every `dev` push for those two days, and an alert that fires constantly is one
that gets muted — which reproduces "went unread" with more noise.

**Filed into the private tracker, not into this repository** (founder,
2026-09-17). This repository's tracker is public. Dedupe still reads the issues
already open here, because those stay open by the same decision, and a finding
that is already tracked must not be tracked twice. The filing token is therefore a
secret that reaches the private tracker; the job's own `GITHUB_TOKEN` cannot reach
another repository at all.

**Reach is checked before anything is built** (`--check-reach`). On a clean day
there is nothing to file, so a token that has lost its reach would otherwise read
green until the first advisory it cannot file. Write reach is proven by one no-op
write: the label is re-sent with its own values.

**The reader is the next agent session**, measured in core#1166: at 0 paying
users with no on-call, a 03:00 notification has no recipient, while *"your open
GitHub issues, filtered by the [Dept] tag"* is a documented mandatory
session-start read. Hence an issue, titled `[Infra]`, carrying the package and
the fixed version — because the only available action is a dependency bump and
the reader is whoever performs it.

**Volume was measured before this was built, not assumed.** The gate evaluates
CRITICAL+HIGH with `--ignore-unfixed`, and on `master fe0a9823` that set was
empty. The 130 CRITICAL+HIGH in the canary's `Total: 1432` are *unfixed* — no
patch exists — so they never reach here. Observed edge rate: one advisory across
20 consecutive green `dev` runs. No severity floor beyond the gate's own is
needed.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request

API = "https://api.github.com"
MARKER = "<!-- cve-watch -->"
CREDENTIAL_ENV = "CVE_FILING_TOKEN"
#: Where the issues this refers to live. Written out in full because the body is read in
#: ANOTHER repository, where a bare `#N` or `core#N` resolves to nothing.
CORE = "datanika-io/datanika-core"


def _request(
    url: str, token: str, method: str = "GET", body: dict | None = None
) -> tuple[int, dict, object]:
    """One API call, returning ``(status, headers, decoded body)`` without raising on 4xx/5xx."""
    # Runtime scheme guard. Every URL here is built from the API constant, so
    # this can only trip if someone later threads a caller-supplied URL through
    # — which is exactly when you want it to fail. The suppressions below are
    # for the static check only; this assertion is the actual protection.
    if not url.startswith("https://"):
        raise ValueError(f"refusing non-https request: {url!r}")
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)  # noqa: S310
    req.add_header("Authorization", f"Bearer {token}")
    req.add_header("Accept", "application/vnd.github+json")
    req.add_header("User-Agent", "datanika-cve-watch")
    if data is not None:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:  # noqa: S310
            raw = resp.read().decode()
            return resp.status, dict(resp.headers), json.loads(raw) if raw else {}
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode(errors="replace")
        try:
            decoded: object = json.loads(raw) if raw else {}
        except ValueError:
            decoded = {"message": raw[:200]}
        return exc.code, dict(exc.headers or {}), decoded


def _req(url: str, token: str, method: str = "GET", body: dict | None = None) -> object:
    status, _headers, decoded = _request(url, token, method, body)
    if status >= 400:
        raise urllib.error.HTTPError(url, status, f"HTTP {status}: {decoded}", None, None)
    return decoded


def findings(report: dict) -> list[dict]:
    """CRITICAL/HIGH vulnerabilities that have a fix.

    Trivy is already invoked with `--ignore-unfixed` and `--severity CRITICAL,HIGH`,
    so this is a belt-and-braces filter: if the workflow's flags are ever loosened,
    this script must not silently start filing hundreds of unfixable advisories.
    """
    out: list[dict] = []
    for result in report.get("Results") or []:
        for v in result.get("Vulnerabilities") or []:
            if v.get("Severity") not in ("CRITICAL", "HIGH"):
                continue
            if not v.get("FixedVersion"):
                continue
            out.append(
                {
                    "id": v.get("VulnerabilityID", "UNKNOWN"),
                    "pkg": v.get("PkgName", "?"),
                    "installed": v.get("InstalledVersion", "?"),
                    "fixed": v.get("FixedVersion", "?"),
                    "severity": v.get("Severity", "?"),
                    "title": (v.get("Title") or "").strip(),
                    "target": result.get("Target", "?"),
                }
            )
    # Deduplicate: the same advisory can appear against several targets.
    seen: dict[str, dict] = {}
    for f in out:
        seen.setdefault(f["id"], f)
    return sorted(seen.values(), key=lambda f: (f["severity"] != "CRITICAL", f["id"]))


def advisory_ids_in(title: str) -> set[str]:
    """Advisory ids mentioned in an issue title.

    Dedup depends on reading the id back out of a title this script wrote, so the
    title format is load-bearing and this parser is the contract. Kept as its own
    function so the tests exercise it rather than a copy of it.
    """
    return {
        tok.strip(":,")
        for tok in title.replace("(", " ").replace(")", " ").split()
        if tok.startswith(("CVE-", "GHSA-"))
    }


def open_advisories(repo: str, token: str) -> set[str]:
    """Advisory ids that already have an open ISSUE in ``repo`` — the edge, not the state.

    The issues endpoint also returns pull requests. A pull request is not a tracking
    issue, so a bump PR that names the id in its title does not count as tracked.
    """
    ids: set[str] = set()
    page = 1
    while page <= 10:
        url = f"{API}/repos/{repo}/issues?state=open&per_page=100&page={page}"
        try:
            batch = _req(url, token)
        except urllib.error.HTTPError as exc:  # pragma: no cover - network
            print(f"::error::cannot list issues in {repo}: {exc}", file=sys.stderr)
            raise
        if not isinstance(batch, list) or not batch:
            break
        for issue in batch:
            if "pull_request" in issue:
                continue
            ids |= advisory_ids_in(issue.get("title") or "")
        page += 1
    return ids


def known_advisories(repos: list[str], token: str) -> set[str]:
    """Advisory ids with an open issue in ANY of ``repos``."""
    ids: set[str] = set()
    for repo in repos:
        ids |= open_advisories(repo, token)
    return ids


def check_reach(repo: str, labels: list[str], token: str, request=_request) -> list[str]:
    """Problems that would stop the filer filing into ``repo``. Empty means it can file.

    Three facts, each of which fails silently on its own: the tracker is readable AND
    private, every label the filer applies exists, and the token can write there. Write
    reach is proven by re-sending one label's own colour and description, which changes
    nothing. On a refusal, GitHub's ``X-Accepted-GitHub-Permissions`` is quoted, because
    it names what the token lacks.
    """
    problems: list[str] = []
    status, headers, body = request(f"{API}/repos/{repo}", token)
    if status != 200:
        return [f"cannot read {repo}: HTTP {status} {_accepted(headers)}".rstrip()]
    if not isinstance(body, dict) or body.get("private") is not True:
        problems.append(
            f"{repo} is not private -- refusing to file scan findings into a public tracker"
        )
    current: dict[str, dict] = {}
    for label in labels:
        url = f"{API}/repos/{repo}/labels/{urllib.parse.quote(label, safe='')}"
        status, headers, body = request(url, token)
        if status != 200 or not isinstance(body, dict):
            problems.append(f"label `{label}` is missing from {repo}: HTTP {status}")
        else:
            current[label] = body
    if labels and labels[0] in current:
        label = labels[0]
        url = f"{API}/repos/{repo}/labels/{urllib.parse.quote(label, safe='')}"
        same = {
            "color": current[label].get("color"),
            "description": current[label].get("description"),
        }
        status, headers, _ = request(url, token, "PATCH", same)
        if status != 200:
            problems.append(
                f"the filing token cannot write issues in {repo}: HTTP {status} "
                f"{_accepted(headers)}".rstrip()
            )
    return problems


def _accepted(headers: dict) -> str:
    for key, value in (headers or {}).items():
        if key.lower() == "x-accepted-github-permissions":
            return f"(GitHub accepts: {value})"
    return ""


def body_for(f: dict) -> str:
    return f"""{MARKER}
**Found by the scheduled CVE scan against `master`** ({CORE}#1166), not by a PR. Nothing in any pull
request caused this and there is nothing in a diff to fix — an advisory was published or updated
against a package we already ship.

| | |
|---|---|
| advisory | `{f["id"]}` |
| severity | **{f["severity"]}** |
| package | `{f["pkg"]}` |
| installed | `{f["installed"]}` |
| **fixed in** | **`{f["fixed"]}`** |
| target | `{f["target"]}` |

{f["title"]}

### The action

Bump `{f["pkg"]}` to `{f["fixed"]}` or later. ⚠️ **Check the image, not the manifest** — {CORE}#602:
`uv pip install /cloud` and `uv pip install ./datanika-mcp` run *after* `uv sync --frozen` and never
consult the lock, so a graft install can move a package the lockfile pins.

### Why this is an issue here and not an alert

At 0 paying users with no on-call there is no recipient for a 03:00 notification, while open issues
filtered by `[Infra]` are a mandatory session-start read. Time-to-notice is therefore bounded by
session cadence, which is the accepted trade — this exists to make the finding *exist*, not urgent.
It is filed in this private tracker because the core repository's tracker is public.

### Closing this

Close it when the bump has shipped **and** a scan has come back clean. The scheduled scan will not
re-file while this issue is open, so a premature close is the one way to lose the finding.
"""


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--report", help="trivy JSON report (not needed with --check-reach)")
    ap.add_argument("--repo", required=True, help="owner/name of the tracker to file into")
    ap.add_argument(
        "--also-dedupe",
        action="append",
        default=[],
        metavar="OWNER/NAME",
        help="another tracker whose OPEN issues count as already tracked (read only)",
    )
    ap.add_argument("--label", action="append", default=[], help="label to apply; repeatable")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument(
        "--check-reach",
        action="store_true",
        help="verify the token can file into --repo (private, labels present, write reach)",
    )
    args = ap.parse_args()

    token = os.environ.get(CREDENTIAL_ENV, "")
    if not token and not args.dry_run:
        print(f"::error::{CREDENTIAL_ENV} is not set; cannot reach {args.repo}", file=sys.stderr)
        return 1

    if args.check_reach:
        problems = check_reach(args.repo, args.label, token)
        for problem in problems:
            print(f"::error::{problem}", file=sys.stderr)
        if problems:
            return 1
        print(
            f"reach: ok -- {args.repo} is readable and private, "
            f"{len(args.label)} label(s) present, write reach confirmed by a no-op label write"
        )
        return 0

    if not args.report:
        print("::error::--report is required unless --check-reach is given", file=sys.stderr)
        return 1

    with open(args.report, encoding="utf-8") as fh:
        report = json.load(fh)

    found = findings(report)
    print(f"fixable CRITICAL/HIGH advisories: {len(found)}")
    for f in found:
        print(f"  {f['severity']:8} {f['id']:20} {f['pkg']} {f['installed']} -> {f['fixed']}")

    if not found:
        print("nothing to file.")
        return 0

    trackers = [args.repo, *args.also_dedupe]
    # A dry run WITH a token reads the real trackers, so a pull request shows the true edge.
    known = known_advisories(trackers, token) if token else set()
    fresh = [f for f in found if f["id"] not in known]
    print(f"trackers read for dedupe: {', '.join(trackers) if token else 'none (no token)'}")
    print(f"already tracked by an open issue: {len(found) - len(fresh)}")
    print(f"NEW (edge) advisories to file: {len(fresh)}")
    labels_note = f" -- labels: {', '.join(args.label)}" if args.label else ""

    if args.dry_run:
        for f in fresh:
            print(f"  would file: [Infra] {f['severity']} {f['id']} in {f['pkg']}{labels_note}")
        return 0

    filed = []
    for f in fresh:
        created = _req(
            f"{API}/repos/{args.repo}/issues",
            token,
            method="POST",
            body={
                "title": f"[Infra] {f['severity']} {f['id']} in {f['pkg']} "
                f"({f['installed']} -> {f['fixed']})",
                "body": body_for(f),
                "labels": args.label,
            },
        )
        num = created.get("number") if isinstance(created, dict) else None
        # An issue-filer that cannot file is a silent failure. Assert the number
        # as a positive artifact rather than trusting the call's exit status.
        if not num:
            print(f"::error::filing {f['id']} returned no issue number", file=sys.stderr)
            return 1
        print(f"::notice::filed {args.repo}#{num} for {f['id']}")
        filed.append(num)

    print(f"filed {len(filed)} issue(s) into {args.repo}: {filed}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
