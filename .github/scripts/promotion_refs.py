#!/usr/bin/env python3
"""Populate a promotion PR body with the `Closes #N` refs of the commits it promotes.

GitHub fires closing keywords only for PRs merged into the DEFAULT branch. Feature PRs
here merge into `dev`, so their own `Closes #N` never fires; the issue is closed only if
the promotion PR (dev -> master) carries the reference. WORKFLOW_RULES §8 makes that a
manual enumeration step, which is why issues leak.

References come in three kinds, and conflating them is what this script got wrong:

  * **closing** (`closes`/`fixes`/`resolves` #N) -- the department that owns the issue
    is declaring that this change fully delivers it. Rendered with the keyword, so
    merging the promotion closes it.
  * **tracking** (`refs`/`part of`/`towards` #N) -- touches it, does not finish it.
    Rendered WITHOUT a keyword, as a candidate the promoter reviews. `WORKFLOW_RULES`
    §4 tells departments to write this by default, because landing#273 showed what
    `closes` on a partial fix does.
  * **cross-repo** (`cloud#151`, `landing#343`) -- real, and not closable from here.

🚨 **The promoter must never guess which is which.** The keyword is the owning
department's declaration and this script only reports it. What it must NOT do is stay
silent about a commit it could not classify -- that is core#1040: promotion PR #1038
carried three commits, emitted one `Closes`, and dropped the other two (one cross-repo,
one referencing nothing) with no signal at all. An empty derivation looks exactly like a
correct one. So every promoted commit is now accounted for in the body, and the ones
nobody declared anything about are named.

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

# Prose that *talks about* closing keywords is not a closing reference. The first live
# run of this script harvested `(closes #142)` and `Closes #415.` out of its own PR body,
# where they were regex examples in backticks -- issues that were not being promoted at
# all. This is not cosmetic: GitHub parses the raw body, so a bogus `Closes #N` on a
# promotion PR really does close that issue on merge. Strip the constructs people use to
# quote or illustrate, and keep only declarations.
FENCED = re.compile(r"```.*?```|~~~.*?~~~", re.DOTALL)
INLINE_CODE = re.compile(r"`[^`]*`")
QUOTED_LINE = re.compile(r"^\s*>.*$", re.MULTILINE)


# A *declaration* starts its line (optionally as a list item). Prose embeds the keyword
# mid-sentence: "The first run harvested (closes #142) and closes #19 from its own body."
# Stripping code spans alone was not enough -- the same false positives came back through
# commit-message prose, which has no backticks to strip. Anchoring to line-start closes
# the whole class instead of one door at a time.
DECLARATION = re.compile(
    r"^\s*(?:[-*]\s*)?(?:close[sd]?|fix(?:e[sd])?|resolve[sd]?)\s*:?\s+#(\d+)\b",
    re.IGNORECASE | re.MULTILINE,
)


# -----------------------------------------------------------------------------------
# The NON-closing half. Ported from datanika-landing, where it shipped as landing#455
# after five consecutive promotions reported `success` while deriving nothing -- because
# the script matched only closing keywords and every commit wrote `refs #N`.
#
# That is not a convention to correct. WORKFLOW_RULES §4 records landing#273, where
# `closes #272` on a 4-of-36 partial fix retired the whole issue and 31 guides stopped
# existing as tracked work. Departments write `refs` *because they were told to*.
#
# So the answer is not to widen the closing regex -- that reintroduces #273 exactly. It
# is to derive the `refs` set as well and print it **without a keyword**, as candidates
# the promoter reviews. Both rules survive: the derivation is mechanical (which is what
# §8 automated) and the closure stays a judgement (which is what §4 protects).
#
# ⚠️ A bare `#N` in a PR body closes NOTHING -- only keyword+`#N` does. That is what
# makes the candidate list safe to render mechanically.
#
# `see` is deliberately absent: it marks background reading, not authorship.
TRACKING = re.compile(
    r"\b(?:refs?|part\s+of|towards?|addresses|implements)\s*:?\s+#(\d+)\b",
    re.IGNORECASE,
)

TRACKING_DECLARATION = re.compile(
    r"^\s*(?:[-*]\s*)?(?:refs?|part\s+of|towards?|addresses|implements)\s*:?\s+#(\d+)\b",
    re.IGNORECASE | re.MULTILINE,
)

# A reference to another repository's tracker: `refs cloud#151`, `closes landing#343`.
# Real work, and **not closable from this promotion** -- cloud ships inside the core
# image, so a cloud issue is closed by a core deploy verifying it on the serving
# container, not by this merge.
#
# The point of matching these is NOT to act on them. It is that `refs cloud#151` used to
# match nothing at all: the closing and tracking patterns both require whitespace before
# `#`, so a repo-qualified reference fell through both and the commit vanished from the
# body. A commit whose only reference points elsewhere is a commit we understood; a
# commit we dropped silently is not. (core#1040)
#
# 🚨 A KEYWORD is required here, exactly as it is for same-repo references. Found by
# rehearsing this against a real promotion: an unqualified `([A-Za-z][\w.-]*)#(\d+)`
# harvested `cloud#164` and `cloud#165` out of a commit body that merely CITED them as
# already-shipped background, and reported the batch 3/3 accounted. A bare mention is
# not a declaration -- that asymmetry is the entire basis of this script, and dropping
# it for the cross-repo case buys a flattering coverage number and nothing else.
CROSS_REPO = re.compile(
    r"\b(?:close[sd]?|fix(?:e[sd])?|resolve[sd]?|refs?|part\s+of|towards?|addresses"
    r"|implements)\s*:?\s+([A-Za-z][\w.-]*)#(\d+)\b",
    re.IGNORECASE,
)


def repo_aliases(repo: str) -> set[str]:
    """The spellings that mean *this* repository, lower-cased.

    `datanika-io/datanika-core` -> {"datanika-core", "core"}. Our prose writes `core#N`
    and `landing#N` constantly, and without this every such mention is rendered under
    "referenced in another repository" -- which is both wrong and the kind of confident
    wrong line that gets believed.
    """
    name = repo.split("/")[-1].lower()
    return {name, name.rsplit("-", 1)[-1]}


def strip_non_declarative(text: str) -> str:
    """Remove code blocks, inline code and block quotes before scanning for keywords."""
    text = FENCED.sub(" ", text)
    text = INLINE_CODE.sub(" ", text)
    text = QUOTED_LINE.sub(" ", text)
    return text


def find_refs(subject: str, body: str) -> set[int]:
    """Closing refs from a subject/title (a declaration by convention) and a body.

    The subject is scanned whole -- our convention is `[Dept] Title (closes #N)`. The body
    is scanned only for line-initial declarations, after code and quotes are stripped, so
    a paragraph *about* closing keywords is not mistaken for one.
    """
    found = {int(n) for n in KEYWORD.findall(subject or "")}
    found |= {int(n) for n in DECLARATION.findall(strip_non_declarative(body or ""))}
    return found


def find_tracking_refs(subject: str, body: str) -> set[int]:
    """Non-closing references (`refs`/`part of`/`towards`/…) -- candidates, not closures.

    Same subject/body asymmetry as `find_refs`, and for the same measured reason: the
    subject is short and deliberate so it is scanned whole, while a body is prose and is
    scanned only for line-initial declarations after code and quotes are stripped.
    Reusing that shape rather than inventing a second one means the false-positive class
    this script already closed stays closed on the new path too.
    """
    found = {int(n) for n in TRACKING.findall(subject or "")}
    found |= {int(n) for n in TRACKING_DECLARATION.findall(strip_non_declarative(body or ""))}
    return found


def find_cross_repo_refs(subject: str, body: str, mine: set[str]) -> set[str]:
    """Keyword-qualified `cloud#151`-style references, as `repo#number` strings.

    Deliberately NOT parsed into a number: these are not addressable in this repo and
    must never reach the closing or candidate lists. They exist so the coverage check
    can say *"this commit referenced something, elsewhere"* rather than *"this commit
    referenced nothing"* -- two different states that looked identical before core#1040.

    `mine` are the spellings that mean this repo; a self-qualified `core#N` is dropped
    here and handled by the same-repo patterns (or, if it was a bare mention, by not
    being a reference at all).
    """
    out = set()
    for blob in (subject or "", strip_non_declarative(body or "")):
        for owner, num in CROSS_REPO.findall(blob):
            if owner.lower() in mine:
                continue
            out.add(f"{owner}#{num}")
    return out


def run(*args: str) -> str:
    # `encoding="utf-8"` is not cosmetic. Without it `text=True` decodes with the
    # platform locale codec, which on a Windows dev box is cp1251: every em dash in an
    # issue title comes back as mojibake. On the ubuntu runner it happens to be right,
    # so the defect is invisible in CI and appears only in the local DRY_RUN rehearsal
    # below -- i.e. exactly where someone is checking the block before a promotion.
    result = subprocess.run(args, capture_output=True, text=True, encoding="utf-8")
    if result.returncode != 0:
        print(f"  ! command failed: {' '.join(args)}\n    {result.stderr.strip()[:300]}")
        return ""
    return result.stdout


def introduced_the_commit(pull: dict) -> bool:
    """Did this PR actually bring the commit in, or does its branch merely contain it?

    `GET /repos/{repo}/commits/{sha}/pulls` returns **every** pull whose branch
    contains the commit -- not just the one that introduced it. Any open feature
    branch cut from `dev` therefore comes back for every commit already on `dev`,
    and without this filter it donates its closing keywords to the promotion.

    That is core#635, caught on promotion PR #634 before merge: the block claimed
    `Closes #608 ... via #633` while #633 was still open and one commit ahead of
    `dev`. Merging would have retired an issue whose fix had not shipped.

    Direction matters here. This automation replaced hand-enumeration because that
    failed **open** -- issues left open after shipping (WORKFLOW_RULES.md section 8).
    The bug made it fail **closed**, which is worse: an open issue gets re-triaged,
    a closed one does not.

    Two independent reasons to skip, and neither subsumes the other:
      * not merged  -> the branch merely contains the commit
      * base master -> a previous promotion PR, which IS merged
    """
    if pull.get("merged_at") is None:
        return False
    return pull.get("base", {}).get("ref") != "master"


def gh_api(path: str) -> object:
    out = run("gh", "api", path)
    if not out.strip():
        return None
    try:
        return json.loads(out)
    except json.JSONDecodeError:
        return None


# A reference whose number does not exist in this repository. Distinct from `None`,
# which means "the lookup itself failed and we do not know" -- see `gh_issue`.
ISSUE_MISSING = object()


def run_capture(*args: str) -> tuple[int, str, str]:
    """`run()` but keeping stdout on a non-zero exit.

    `gh api` prints the API's own JSON body to **stdout** even when it exits 1, and
    that body is the only place the HTTP status appears in machine-readable form.
    `run()` throws it away, which is why a 404 and a network failure were
    indistinguishable.
    """
    result = subprocess.run(args, capture_output=True, text=True, encoding="utf-8")
    return result.returncode, result.stdout, result.stderr


def gh_issue(repo: str, num: int) -> object:
    """Look up one issue. Three outcomes, and conflating them is landing#493.

    Returns the issue dict, `ISSUE_MISSING` (the API said 404 -- the number does not
    exist here), or `None` (the lookup failed for some other reason, so we do not
    know either way).

    The distinction is the whole point. `landing 36048435` said `refs #676`, no such
    issue exists in `datanika-landing`, and the generator's
    `if not isinstance(issue, dict): continue` dropped the line -- while the commit
    still counted as *accounted for*, because a reference had been parsed from it.
    So the block asserted full coverage and named the commit nowhere.

    Reporting a *transient* failure as "does not exist" would be the same defect
    pointing the other way: a false accusation of a typo, printed into a promotion
    body, on a reference that is fine. Hence three states, not two.

    Measured against the real API (2026-09-04):
        existing issue      -> rc 0, stdout is the issue JSON
        nonexistent issue   -> rc 1, stdout {"message":"Not Found",...,"status":"404"}
                                     stderr `gh: Not Found (HTTP 404)`
    """
    rc, out, err = run_capture("gh", "api", f"repos/{repo}/issues/{num}")
    if rc == 0:
        try:
            parsed = json.loads(out)
        except json.JSONDecodeError:
            return None
        return parsed if isinstance(parsed, dict) and "number" in parsed else None
    try:
        payload = json.loads(out)
    except json.JSONDecodeError:
        payload = {}
    # Prefer the body's own status field; the stderr string is a fallback in case gh
    # changes how it renders the error. Both are checked because neither is a contract.
    if isinstance(payload, dict) and (
        str(payload.get("status")) == "404" or payload.get("message") == "Not Found"
    ):
        return ISSUE_MISSING
    if "HTTP 404" in err:
        return ISSUE_MISSING
    print(f"  ! could not check issue #{num}: {err.strip()[:200]}")
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
    tracking: dict[int, set[str]] = {}
    # sha -> what we derived from it. A sha whose set stays empty is one this script
    # could not classify, and core#1040 is that it used to disappear rather than say so.
    accounted: dict[str, set[str]] = {}
    subjects: dict[str, str] = {}
    mine = repo_aliases(repo)
    shas = []
    for entry in commits:
        sha, _, message = entry.strip().partition("\x1f")
        shas.append(sha)
        subject, _, msg_body = message.strip().partition("\n")
        subjects[sha] = subject.strip()
        accounted.setdefault(sha, set())
        for num in find_refs(subject, msg_body):
            refs.setdefault(num, set()).add(f"commit {sha[:7]}")
            accounted[sha].add(f"closes #{num}")
        for num in find_tracking_refs(subject, msg_body):
            tracking.setdefault(num, set()).add(f"commit {sha[:7]}")
            accounted[sha].add(f"refs #{num}")
        for ref in find_cross_repo_refs(subject, msg_body, mine):
            accounted[sha].add(ref)

    # Also consult the source PRs: a rebase-merge can leave the keyword only on the PR.
    for sha in shas:
        pulls = gh_api(f"repos/{repo}/commits/{sha}/pulls")
        if not isinstance(pulls, list):
            continue
        for pull in pulls:
            if not introduced_the_commit(pull):
                continue
            title, body_text = pull.get("title", ""), pull.get("body") or ""
            for num in find_refs(title, body_text):
                refs.setdefault(num, set()).add(f"#{pull['number']}")
                accounted[sha].add(f"closes #{num}")
            for num in find_tracking_refs(title, body_text):
                tracking.setdefault(num, set()).add(f"#{pull['number']}")
                accounted[sha].add(f"refs #{num}")
            for ref in find_cross_repo_refs(title, body_text, mine):
                accounted[sha].add(ref)

    # An issue that some commit genuinely closes is not also a candidate.
    for num in refs:
        tracking.pop(num, None)

    unaccounted = [s for s in shas if not accounted.get(s)]

    def _is_cross_repo(label: str) -> bool:
        """`cloud#151` yes; the `closes #12` / `refs #34` labels this repo owns, no."""
        return "#" in label and not label.startswith(("closes ", "refs "))

    elsewhere = sorted({r for s in shas for r in accounted.get(s, set()) if _is_cross_repo(r)})

    print(
        f"  closing: {len(refs)}  tracking: {len(tracking)}  "
        f"commits accounted: {len(shas) - len(unaccounted)}/{len(shas)}"
    )

    # -------------------------------------------------------------------------------
    # This check exists so the workflow can go RED (landing#455, ported).
    #
    # Reporting `success` on every run *was* the defect: five consecutive landing
    # promotions derived nothing, wrote an empty block and exited 0, which is
    # indistinguishable from a promotion that genuinely closes nothing. Nothing was
    # ever red, so nobody looked.
    #
    # Every commit in this project carries a `[Dept]` tag and, by convention, an issue
    # reference. Three or more commits yielding NO reference of any kind is a convention
    # breakdown or a broken parser, not a normal promotion.
    #
    # The threshold is deliberately conservative: a one- or two-commit hotfix promotion
    # with no issue is legitimate and must not go red. A *partially* unaccounted batch
    # is not red either -- it is REPORTED, in the body, which is core#1040's whole
    # point: the promoter needs to see it, not to be blocked by it.
    # 🚨 `elsewhere` belongs in this condition, and leaving it out was a FALSE RED.
    # A batch whose commits all carry `refs cloud#151` derives no same-repo reference of
    # either kind, so without this three such commits fail the job saying not one
    # reference was derived -- while every one of them referenced something. That is the
    # exact shape of the commit this work started from. A job that goes red when nothing
    # is wrong teaches people to merge past it, which costs more than the check earns.
    if not refs and not tracking and not elsewhere:
        if len(commits) >= 3:
            print(
                f"::error::{len(commits)} commits promoted and NOT ONE issue reference was "
                "derived. Either the commit convention has drifted or this parser is broken. "
                "Refusing to report success on an empty derivation (landing#455)."
            )
            return 1
        print("  no references found in a short promotion; leaving the body unchanged")
        return 0

    # Don't re-list issues that are already closed -- keeps the block honest about what
    # this promotion actually closes.
    # References this generator parsed but could not turn into a line (landing#493).
    # num -> (why, the labels that referenced it). Every one of these used to be a
    # bare `continue`, so the number vanished while its commit still counted toward
    # coverage -- an absence dressed as completeness, which is the exact failure
    # core#1040 exists to remove.
    unresolved: dict[int, tuple[str, set[str]]] = {}

    def _unresolvable(num: int, issue: object, via: set[str]) -> bool:
        if issue is ISSUE_MISSING:
            unresolved[num] = ("does not resolve in this repository", via)
            return True
        if not isinstance(issue, dict):
            unresolved[num] = ("could not be checked — the API lookup failed", via)
            return True
        if issue.get("pull_request"):
            unresolved[num] = ("resolves to a pull request, not an issue", via)
            return True
        return False

    lines = []
    for num in sorted(refs):
        issue = gh_issue(repo, num)
        if _unresolvable(num, issue, refs[num]):
            continue
        state = issue.get("state")
        title = (issue.get("title") or "").strip()
        via = ", ".join(sorted(refs[num]))
        if state == "closed":
            # Deliberately NOT the "Closes" keyword. Strikethrough is cosmetic -- GitHub
            # parses the raw text, so `~~Closes #N~~` still fires. An already-closed
            # issue needs no keyword, and omitting it means a stale or false-positive
            # reference cannot act on an issue this promotion does not own.
            lines.append(f"- #{num} — {title} _(already closed)_ · via {via}")
        else:
            lines.append(f"- Closes #{num} — {title} · via {via}")

    # The candidate half. NO closing keyword on any of these lines, by design: a bare
    # `#N` in a PR body closes nothing, which is exactly the property that lets this list
    # be generated mechanically without re-creating landing#273.
    candidate_lines = []
    suppressed_closed = 0
    for num in sorted(tracking):
        issue = gh_issue(repo, num)
        if _unresolvable(num, issue, tracking[num]):
            continue
        if issue.get("state") == "closed":
            # Already reconciled; re-listing it is noise. But it DID account for a
            # commit, so it is counted -- otherwise the coverage number below has no
            # visible explanation and reads like an arithmetic error.
            suppressed_closed += 1
            continue
        title = (issue.get("title") or "").strip()
        via = ", ".join(sorted(tracking[num]))
        candidate_lines.append(f"- #{num} — {title} · via {via}")

    # The "I could not tell" half (core#1040). Two distinct states, kept distinct:
    # a commit that referenced ANOTHER repo's tracker, and a commit that referenced
    # nothing at all. Both used to vanish; only one of them is a convention lapse.
    unaccounted_lines = []
    for sha in unaccounted:
        unaccounted_lines.append(f"- `{sha[:7]}` — {subjects.get(sha, '')}")

    # `unresolved` belongs in this condition. Without it, a promotion whose only
    # references are unresolvable renders NO block at all and exits 0 -- which is
    # landing#492 exactly: nine commits promoted, one of them citing `#676`, and the
    # body silently short of a commit.
    if (
        not lines
        and not candidate_lines
        and not unaccounted_lines
        and not elsewhere
        and not unresolved
    ):
        print("  references found, but none resolve to open issues; body unchanged")
        return 0

    sections = [START, ""]
    if lines:
        sections += [
            "### Issues closed by this promotion",
            "",
            "_Generated from the commits being promoted. Feature PRs merge into `dev`, "
            "which is not the default branch, so their own closing keywords never fire — "
            "these references are what actually closes the issues on merge._",
            "",
            *lines,
            "",
        ]
    if candidate_lines:
        sections += [
            "### Promoted, close by hand if complete",
            "",
            "_These commits reference the issues below with `refs` / `part of` / "
            "`towards`, which closes nothing **on purpose**: `WORKFLOW_RULES` §4 records "
            "landing#273, where `closes #272` on a 4-of-36 partial fix retired the whole "
            "issue. The list is derived mechanically and the closure stays a judgement — "
            "review each and close the ones whose acceptance criteria are fully met._",
            "",
            *candidate_lines,
            "",
        ]
    if elsewhere:
        sections += [
            "### Referenced in another repository — not closable from here",
            "",
            "_A cloud issue is closed by the **core** deploy that verifies it on the "
            "serving container, not by this merge; cloud ships inside the core image at "
            "a pinned `ref: master`. Listed so the commit is accounted for._",
            "",
            *[f"- `{r}`" for r in elsewhere],
            "",
        ]
    if unresolved:
        sections += [
            "### ⚠️ Referenced a number that does not resolve — closing nothing",
            "",
            "_These commits **did** carry a reference, so they are not in the section "
            "below — but the number could not be turned into an issue in this "
            "repository, so no line above speaks for them either. The usual cause is a "
            "cross-repo reference missing its prefix (`refs #676` where `core#676` was "
            "meant); `WORKFLOW_RULES` §4 requires the prefix for exactly this reason. "
            "Nothing here is closed, and nothing here is silently dropped — which is "
            "what used to happen (landing#493)._",
            "",
            *[
                f"- `#{num}` — {why} · via {', '.join(sorted(via))}"
                for num, (why, via) in sorted(unresolved.items())
            ],
            "",
        ]
    if unaccounted_lines:
        sections += [
            "### ⚠️ No issue reference derived — I could not tell",
            "",
            "_These commits carry no closing, tracking or cross-repo reference that this "
            "generator can parse, so **nothing above speaks for them**. That may be "
            "correct (a comment-only or tooling commit), or it may be a missed reference. "
            "It is stated rather than omitted because an absent line and a correct "
            "derivation used to look identical (core#1040)._",
            "",
            *unaccounted_lines,
            "",
        ]
    coverage = (
        f"_Coverage: **{len(shas) - len(unaccounted)} of {len(shas)}** promoted commits "
        "accounted for."
    )
    if suppressed_closed:
        coverage += (
            f" {suppressed_closed} reference(s) resolved to an already-closed issue and "
            "are not listed."
        )
    if unresolved:
        # Said out loud in the coverage line, not only in its own section. A commit
        # whose sole reference is unresolvable still counts as accounted -- it IS
        # named, in that section -- and without this sentence the count reads as if
        # everything were fine.
        coverage += (
            f" {len(unresolved)} reference(s) did not resolve to an issue here and "
            "close nothing — see the section above."
        )
    coverage += "_"
    sections += [coverage, "", END]
    block = "\n".join(sections)

    # An annotation as well as a body line. The body is read by whoever opens the PR;
    # the annotation is what shows on the run itself, and a mistyped reference is
    # worth noticing without opening anything. Deliberately a WARNING, not an error:
    # the commit message is already on `dev` and the promoter cannot fix it, and
    # core#1040's stated design is that a partially-unaccounted batch is reported
    # rather than blocked. A red here would only teach people to merge past it.
    for num, (why, via) in sorted(unresolved.items()):
        print(f"::warning::promotion body: #{num} {why} (via {', '.join(sorted(via))})")

    # Rehearsal path. The promoter can see the exact block a promotion would generate
    # BEFORE opening the PR -- the only moment at which noticing an empty derivation is
    # still cheap. Writes nothing.
    #
    #   REPO=datanika-io/datanika-core PR_NUMBER=0 \
    #   BASE_SHA=origin/master HEAD_SHA=origin/dev DRY_RUN=1 \
    #     python .github/scripts/promotion_refs.py
    if os.environ.get("DRY_RUN", "").lower() in ("1", "true", "yes"):
        print("  DRY RUN — the block that would be written:\n")
        print(block)
        return 0

    current = (
        run("gh", "pr", "view", pr_number, "--repo", repo, "--json", "body", "-q", ".body") or ""
    )
    if START in current and END in current:
        body = re.sub(re.escape(START) + r".*?" + re.escape(END), block, current, flags=re.DOTALL)
    else:
        body = (current.rstrip() + "\n\n" + block).lstrip()

    if body.strip() == current.strip():
        print("  body already up to date")
        return 0

    path = "/tmp/promotion-body.md"
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(body)
    run("gh", "pr", "edit", pr_number, "--repo", repo, "--body-file", path)
    print(
        f"  wrote {len(lines)} closing + {len(candidate_lines)} candidate reference(s), "
        f"{len(unaccounted_lines)} unaccounted commit(s):"
    )
    for line in lines + candidate_lines + unaccounted_lines:
        print(f"    {line}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
