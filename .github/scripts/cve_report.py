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
import urllib.request

API = "https://api.github.com"
MARKER = "<!-- cve-watch -->"


def _req(url: str, token: str, method: str = "GET", body: dict | None = None) -> object:
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
    with urllib.request.urlopen(req, timeout=30) as resp:  # noqa: S310
        return json.loads(resp.read().decode())


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
    """Advisory ids that already have an open issue — the edge, not the state."""
    ids: set[str] = set()
    page = 1
    while page <= 10:
        url = f"{API}/repos/{repo}/issues?state=open&per_page=100&page={page}"
        try:
            batch = _req(url, token)
        except urllib.error.HTTPError as exc:  # pragma: no cover - network
            print(f"::error::cannot list issues: {exc}", file=sys.stderr)
            raise
        if not isinstance(batch, list) or not batch:
            break
        for issue in batch:
            ids |= advisory_ids_in(issue.get("title") or "")
        page += 1
    return ids


def body_for(f: dict) -> str:
    return f"""{MARKER}
**Found by the scheduled CVE scan against `master`** (core#1166), not by a PR. Nothing in any pull
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

Bump `{f["pkg"]}` to `{f["fixed"]}` or later. ⚠️ **Check the image, not the manifest** — core#602:
`uv pip install /cloud` and `uv pip install ./datanika-mcp` run *after* `uv sync --frozen` and never
consult the lock, so a graft install can move a package the lockfile pins.

### Why this is an issue and not an alert

At 0 paying users with no on-call there is no recipient for a 03:00 notification, while open issues
filtered by `[Infra]` are a mandatory session-start read. Time-to-notice is therefore bounded by
session cadence, which is the accepted trade — this exists to make the finding *exist*, not urgent.

### Closing this

Close it when the bump has shipped **and** a scan has come back clean. The scheduled scan will not
re-file while this issue is open, so a premature close is the one way to lose the finding.
"""


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--report", required=True, help="trivy JSON report")
    ap.add_argument("--repo", required=True, help="owner/name")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    token = os.environ.get("GITHUB_TOKEN", "")
    if not token and not args.dry_run:
        print("::error::GITHUB_TOKEN is not set; cannot file an issue", file=sys.stderr)
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

    known = set() if args.dry_run else open_advisories(args.repo, token)
    fresh = [f for f in found if f["id"] not in known]
    print(f"already tracked by an open issue: {len(found) - len(fresh)}")
    print(f"NEW (edge) advisories to file: {len(fresh)}")

    if args.dry_run:
        for f in fresh:
            print(f"  would file: [Infra] {f['severity']} {f['id']} in {f['pkg']}")
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
            },
        )
        num = created.get("number") if isinstance(created, dict) else None
        # An issue-filer that cannot file is a silent failure. Assert the number
        # as a positive artifact rather than trusting the call's exit status.
        if not num:
            print(f"::error::filing {f['id']} returned no issue number", file=sys.stderr)
            return 1
        print(f"::notice::filed #{num} for {f['id']}")
        filed.append(num)

    print(f"filed {len(filed)} issue(s): {filed}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
