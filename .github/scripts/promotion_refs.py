#!/usr/bin/env python3
"""Populate a promotion PR body with the `Closes #N` refs of the commits it promotes.

GitHub fires closing keywords only for PRs merged into the DEFAULT branch. Feature PRs
here merge into `dev`, so their own `Closes #N` never fires; the issue is closed only if
the promotion PR (dev -> master) carries the reference. WORKFLOW_RULES §8 makes that a
manual enumeration step, which is why issues leak.

References are gathered from two places, because neither alone is reliable:
  1. the commit messages being promoted -- our convention puts `(closes #N)` there, but
     not every commit follows it;
  2. the pull requests those commits came from -- the PR title/body almost always has it,
     and survives a rebase-merge that rewrote the commit message.

Idempotent: rewrites a single marked block, so repeated runs (synchronize events) never
duplicate. Never closes anything itself -- merging the promotion PR does that.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys

START = "<!-- promotion-refs:start -->"
END = "<!-- promotion-refs:end -->"

# `closes/fixes/resolves #12`, plus the participle forms GitHub also accepts.
KEYWORD = re.compile(
    r"\b(?:close[sd]?|fix(?:e[sd])?|resolve[sd]?)\s*:?\s+#(\d+)\b",
    re.IGNORECASE,
)


def run(*args: str) -> str:
    result = subprocess.run(args, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"  ! command failed: {' '.join(args)}\n    {result.stderr.strip()[:300]}")
        return ""
    return result.stdout


def gh_api(path: str) -> object:
    out = run("gh", "api", path)
    if not out.strip():
        return None
    try:
        return json.loads(out)
    except json.JSONDecodeError:
        return None


def main() -> int:
    repo = os.environ["REPO"]
    pr_number = os.environ["PR_NUMBER"]
    base_sha = os.environ["BASE_SHA"]
    head_sha = os.environ["HEAD_SHA"]

    # Commits being promoted. base..head is exactly "on dev, not yet on master".
    log = run("git", "log", "--format=%H%x1f%B%x1e", f"{base_sha}..{head_sha}")
    commits = [c for c in log.split("\x1e") if c.strip()]
    print(f"  commits being promoted: {len(commits)}")

    refs: dict[int, set[str]] = {}
    shas = []
    for entry in commits:
        sha, _, message = entry.strip().partition("\x1f")
        shas.append(sha)
        for num in KEYWORD.findall(message):
            refs.setdefault(int(num), set()).add(f"commit {sha[:7]}")

    # Also consult the source PRs: a rebase-merge can leave the keyword only on the PR.
    for sha in shas:
        pulls = gh_api(f"repos/{repo}/commits/{sha}/pulls")
        if not isinstance(pulls, list):
            continue
        for pull in pulls:
            if pull.get("base", {}).get("ref") == "master":
                continue  # a previous promotion PR, not a feature PR
            text = f"{pull.get('title', '')}\n{pull.get('body') or ''}"
            for num in KEYWORD.findall(text):
                refs.setdefault(int(num), set()).add(f"#{pull['number']}")

    if not refs:
        print("  no closing references found; leaving the body unchanged")
        return 0

    # Don't re-list issues that are already closed -- keeps the block honest about what
    # this promotion actually closes.
    lines = []
    for num in sorted(refs):
        issue = gh_api(f"repos/{repo}/issues/{num}")
        if not isinstance(issue, dict) or "number" not in issue:
            continue
        if issue.get("pull_request"):
            continue  # a PR, not an issue
        state = issue.get("state")
        title = (issue.get("title") or "").strip()
        via = ", ".join(sorted(refs[num]))
        if state == "closed":
            lines.append(f"- ~~Closes #{num}~~ — {title} _(already closed)_ · via {via}")
        else:
            lines.append(f"- Closes #{num} — {title} · via {via}")

    if not lines:
        print("  references found, but none resolve to issues; body unchanged")
        return 0

    block = "\n".join(
        [
            START,
            "",
            "### Issues closed by this promotion",
            "",
            "_Generated from the commits being promoted. Feature PRs merge into `dev`, "
            "which is not the default branch, so their own closing keywords never fire — "
            "these references are what actually closes the issues on merge._",
            "",
            *lines,
            "",
            END,
        ]
    )

    current = run("gh", "pr", "view", pr_number, "--repo", repo, "--json", "body", "-q", ".body") or ""
    if START in current and END in current:
        body = re.sub(
            re.escape(START) + r".*?" + re.escape(END), block, current, flags=re.DOTALL
        )
    else:
        body = (current.rstrip() + "\n\n" + block).lstrip()

    if body.strip() == current.strip():
        print("  body already up to date")
        return 0

    path = "/tmp/promotion-body.md"
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(body)
    run("gh", "pr", "edit", pr_number, "--repo", repo, "--body-file", path)
    print(f"  wrote {len(lines)} reference(s) into the PR body:")
    for line in lines:
        print(f"    {line}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
