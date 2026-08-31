#!/usr/bin/env python3
"""Label every OPEN issue whose work is already deployed to production (core#773).

Why this exists
---------------
`promotion_refs.py` turns closing keywords in the promoted commits into `Closes #N` on the
promotion PR, so merging it closes those issues. That covers only the commits that used a
**closing** keyword. Almost every commit in this project writes `refs #N`, which closes
nothing -- deliberately, because WORKFLOW_RULES section 4 records a real incident where
`closes #272` on a 4-of-36 partial fix retired the whole issue.

Measured on the 2026-08-31 promotion: 28 commits, roughly twelve issues shipped, exactly
**two** closing references generated. A repo-wide sweep the same night found **26** open
issues whose implementing commit was already an ancestor of the deployed `master` -- twelve
more than the promotion range alone revealed, including two that two departments had each
independently re-derived as open work.

So the gap is not the keyword discipline and must not be closed by linting commit messages.
The gap is that nothing reconciles the tracker against production **at the moment the
difference becomes true**.

What this does, and what it deliberately does not
-------------------------------------------------
It answers one mechanical question per open issue: *is a commit that references this issue
already an ancestor of the deployed production ref?* If yes, it applies the `shipped-to-prod`
label and names the issue in one summary comment on the promotion PR.

**It never closes anything.** Closing needs someone who knows whether the acceptance criteria
are fully met, and `refs` exists precisely because that is often "no". The label converts a
silent, invisible condition into a queryable one::

    gh issue list --repo <repo> --state open --label shipped-to-prod

An open issue *without* the label is genuinely not in production. An open issue *with* it is
shipped and unreconciled. That distinction is the whole product; before it, an issue's open
state carried no information at all.

The label is applied once and never removed, so each run's comment lists only what is *newly*
detected. The label is the memory; the comment is the delta.

Failing loudly
--------------
The landing promotion the same night generated an **empty** references block and the workflow
still reported `success`, which is indistinguishable from it never having run. This script
therefore treats "I found nothing to look at" as an error, not as a clean result: zero commits
scanned, zero references extracted, or zero open issues fetched all exit non-zero.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import tempfile

LABEL = "shipped-to-prod"
LABEL_COLOR = "0E8A16"
# ⚠️ GitHub rejects a label description over 100 characters with HTTP 422. The first live run
# hit exactly that: the label was never created, all 13 `gh issue edit` calls then failed with
# "'shipped-to-prod' not found", and the script still exited 0 and commented on the promotion
# PR claiming the issues were labelled. `test_label_description_fits_githubs_limit` pins it.
LABEL_DESC = (
    "Referencing commit is already deployed to production. "
    "Close it if its acceptance criteria are met."
)
LABEL_DESC_MAX = 100

START = "<!-- shipped-reconcile:start -->"
END = "<!-- shipped-reconcile:end -->"

# A reference qualified with another repository is not ours. `landing#406` and
# `datanika-io/datanika-cloud#119` both appear in core commit messages; treating either as a
# core issue number silently mislabels an unrelated issue.
FOREIGN = re.compile(
    r"(?:[\w.-]+/[\w.-]+#\d+)|(?:\b(?:datanika-)?(?:landing|cloud)\s*#\d+)",
    re.IGNORECASE,
)

# Our convention puts the reference in the subject: `[Dept] Title (refs #N)`, sometimes
# `(core#N)`. The subject is short and deliberate, so every `#N` in it counts.
SUBJECT_REF = re.compile(r"(?:core)?#(\d+)")

# Bodies are prose and mention issues in passing ("same family as #602"). Only a reference in
# a keyword position claims that this commit is work on that issue. `see` is excluded on
# purpose: it marks background reading, not authorship.
BODY_REF = re.compile(
    r"\b(?:refs?|close[sd]?|fix(?:e[sd])?|resolve[sd]?|part\s+of|towards?|addresses|implements)"
    r"\s*:?\s+(?:core)?#(\d+)",
    re.IGNORECASE,
)

FENCED = re.compile(r"```.*?```|~~~.*?~~~", re.DOTALL)
DEPT = re.compile(r"^\s*\[(\w+)\]")

# `Merge pull request #678 from datanika-io/dev` names a PULL REQUEST, not an issue. Issues
# and PRs share one number sequence on GitHub, so #678 can never also be an issue and this
# cannot mislabel anything -- but it does make a merge commit the "representative" for an
# issue, which loses the `[Dept]` tag the summary comment groups by. 116 of `master`'s 706
# commits are these. Blank the subject and keep the body, which is where a promotion's own
# generated `Closes #N` lives.
MERGE_SUBJECT = re.compile(r"^Merge (?:pull request|branch|remote-tracking branch)\b")

RECORD_SEP = "\x1e"
FIELD_SEP = "\x1f"


def run(*args: str) -> str:
    result = subprocess.run(args, capture_output=True, text=True, encoding="utf-8")
    if result.returncode != 0:
        print(f"  ! command failed: {' '.join(args[:4])}...\n    {(result.stderr or '')[:300]}")
        return ""
    return result.stdout


def gh_api(path: str, *extra: str) -> object:
    out = run("gh", "api", path, *extra)
    if not out.strip():
        return None
    try:
        return json.loads(out)
    except json.JSONDecodeError:
        return None


def strip_foreign(text: str) -> str:
    """Blank out cross-repository references so they cannot be read as local issue numbers."""
    return FOREIGN.sub(" ", text or "")


def refs_in_commit(subject: str, body: str) -> set[int]:
    """Issue numbers this commit claims to be work on."""
    subject = "" if MERGE_SUBJECT.match(subject or "") else strip_foreign(subject)
    body = strip_foreign(FENCED.sub(" ", body or ""))
    found = {int(n) for n in SUBJECT_REF.findall(subject)}
    found |= {int(n) for n in BODY_REF.findall(body)}
    return found


def dept_of(subject: str) -> str:
    match = DEPT.match(subject or "")
    return match.group(1) if match else "unlabelled"


def scan_commits(prod_ref: str) -> tuple[dict[int, list[tuple[str, str]]], int]:
    """Map issue number -> [(sha, subject)] over every commit reachable from `prod_ref`."""
    fmt = f"%H{FIELD_SEP}%s{FIELD_SEP}%b{RECORD_SEP}"
    raw = run("git", "log", prod_ref, f"--format={fmt}")
    references: dict[int, list[tuple[str, str]]] = {}
    scanned = 0
    for record in raw.split(RECORD_SEP):
        if not record.strip():
            continue
        parts = record.strip("\n").split(FIELD_SEP)
        if len(parts) < 3:
            continue
        scanned += 1
        sha, subject, body = parts[0].strip(), parts[1], parts[2]
        for number in refs_in_commit(subject, body):
            references.setdefault(number, []).append((sha[:8], subject))
    return references, scanned


def ensure_label(repo: str) -> bool:
    """Create the label if absent. Returns False if it still does not exist afterwards.

    This must fail CLOSED. On the first live run the create was rejected (HTTP 422, the
    description was 111 characters against a 100-character limit) and the old version of
    this function ignored the result. Every subsequent `gh issue edit` then failed with
    "'shipped-to-prod' not found", and `main` went on to comment on the promotion PR saying
    the issues were labelled. Nothing about the exit code or the comment said otherwise --
    a run that mutated nothing reported success, which is the exact shape this whole script
    exists to remove.
    """
    existing = gh_api(f"repos/{repo}/labels/{LABEL}")
    if isinstance(existing, dict) and existing.get("name") == LABEL:
        return True
    print(f"  creating label '{LABEL}'")
    run(
        "gh",
        "label",
        "create",
        LABEL,
        "--repo",
        repo,
        "--color",
        LABEL_COLOR,
        "--description",
        LABEL_DESC,
        "--force",
    )
    confirmed = gh_api(f"repos/{repo}/labels/{LABEL}")
    return isinstance(confirmed, dict) and confirmed.get("name") == LABEL


def open_issues(repo: str) -> list[dict]:
    out = run(
        "gh",
        "issue",
        "list",
        "--repo",
        repo,
        "--state",
        "open",
        "--limit",
        "1000",
        "--json",
        "number,title,labels",
    )
    try:
        return json.loads(out) if out.strip() else []
    except json.JSONDecodeError:
        return []


def promotion_pr(repo: str, sha: str) -> str | None:
    """The PR whose merge produced `sha` -- i.e. the promotion this deploy shipped."""
    if not sha:
        return None
    pulls = gh_api(f"repos/{repo}/commits/{sha}/pulls")
    if not isinstance(pulls, list):
        return None
    for pull in pulls:
        base = (pull.get("base") or {}).get("ref")
        head = (pull.get("head") or {}).get("ref")
        if pull.get("merged_at") and head == "dev" and base in ("master", "main"):
            return str(pull["number"])
    return None


def build_comment(newly: list[dict], prod_ref: str, sha: str) -> str:
    by_dept: dict[str, list[dict]] = {}
    for item in newly:
        by_dept.setdefault(item["dept"], []).append(item)

    lines = [
        START,
        "## Shipped to production and still open",
        "",
        f"These issues have at least one referencing commit in `{prod_ref}` "
        f"(`{sha[:8] if sha else prod_ref}`), which has now deployed. They are labelled "
        f"[`{LABEL}`](../../issues?q=is%3Aopen+label%3A{LABEL}).",
        "",
        "**Nothing here has been closed.** `refs #N` is the right convention for partial work, "
        "so whether an issue is *finished* is a judgement its owner makes — this only makes "
        '"live in production while the tracker says open" visible at the moment it becomes true.',
        "",
    ]
    for dept in sorted(by_dept):
        lines.append(f"**{dept}**")
        for item in sorted(by_dept[dept], key=lambda i: i["number"]):
            lines.append(f"- #{item['number']} — {item['title']} (`{item['sha']}`)")
        lines.append("")
    lines += [
        f"Standing queue, any time: `gh issue list --state open --label {LABEL}`",
        END,
    ]
    return "\n".join(lines)


def main() -> int:
    repo = os.environ["REPO"]
    prod_ref = os.environ.get("PROD_REF", "master")
    deploy_sha = os.environ.get("DEPLOY_SHA", "")
    dry_run = os.environ.get("DRY_RUN", "").lower() in ("1", "true", "yes")

    print(f"reconciling {repo} against {prod_ref} (deploy {deploy_sha[:8] or 'n/a'})")

    references, scanned = scan_commits(prod_ref)
    print(f"  scanned {scanned} commits, extracted {len(references)} distinct issue references")

    issues = open_issues(repo)
    print(f"  {len(issues)} open issues")

    # Doing nothing is an error, not a clean run. A workflow that reports success while it
    # scanned an empty range is exactly the signal this whole change exists to remove.
    if scanned == 0:
        print("FAIL: scanned 0 commits — is the checkout shallow? (needs fetch-depth: 0)")
        return 1
    if not references:
        print(f"FAIL: 0 issue references found across {scanned} commits — the parser is broken")
        return 1
    if not issues:
        print("FAIL: the API returned 0 open issues — treating as a failed query, not as a repo")
        print("      with nothing open. Re-run once the API is reachable.")
        return 1

    shipped_open = []
    for issue in issues:
        number = issue["number"]
        if number not in references:
            continue
        # Prefer a commit that carries a `[Dept]` tag: that is what the summary groups by,
        # and an untagged commit (a merge, or a pre-convention commit) would report the
        # whole group as "unlabelled".
        candidates = references[number]
        sha, subject = next(
            (c for c in candidates if DEPT.match(c[1] or "")),
            candidates[0],
        )
        shipped_open.append(
            {
                "number": number,
                "title": issue["title"],
                "sha": sha,
                "dept": dept_of(subject),
                "labelled": any(lbl["name"] == LABEL for lbl in issue.get("labels") or []),
            }
        )

    newly = [i for i in shipped_open if not i["labelled"]]
    print(
        f"  {len(shipped_open)} open issues are already in production; {len(newly)} newly detected"
    )

    if not newly:
        print("  nothing new to reconcile — tracker agrees with production")
        return 0

    if dry_run:
        for item in sorted(newly, key=lambda i: i["number"]):
            print(f"    would label #{item['number']} [{item['dept']}] {item['title'][:60]}")
        return 0

    if not ensure_label(repo):
        print(f"FAIL: label '{LABEL}' does not exist and could not be created.")
        print("      Not labelling and not commenting — a comment saying these issues are")
        print("      labelled, when they are not, is worse than no comment at all.")
        return 1

    for item in sorted(newly, key=lambda i: i["number"]):
        print(f"    labelling #{item['number']} [{item['dept']}]")
        run("gh", "issue", "edit", str(item["number"]), "--repo", repo, "--add-label", LABEL)

    number = promotion_pr(repo, deploy_sha)
    if number is None:
        print("  no promotion PR found for this deploy — labels applied, no comment posted")
        return 0

    body = build_comment(newly, prod_ref, deploy_sha)
    # A temp file, not the working tree: this runs inside a checkout, and dropping an
    # untracked .md next to the source is how a stray file ends up in someone's `git add`.
    with tempfile.NamedTemporaryFile("w", suffix=".md", delete=False, encoding="utf-8") as handle:
        handle.write(body)
        path = handle.name
    run("gh", "pr", "comment", number, "--repo", repo, "--body-file", path)
    os.unlink(path)
    print(f"  commented on promotion PR #{number}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
