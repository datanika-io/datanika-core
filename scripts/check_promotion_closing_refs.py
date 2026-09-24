"""Compare what GitHub WILL close on a promotion against what the body DECLARES (core#1541).

The defect
----------

A closing keyword in a **pull-request body** fires from prose, and nothing in this
organisation reads a PR body. A promotion body once read *"I will …close #1477… by hand
only after…"* -- a sentence written to say the issue would **not** close -- and the issue
closed two seconds after the merge. The outcome happened to be correct. It was correct by
luck, and luck is not a control.

Why there is no parser here
---------------------------

The obvious guard re-implements GitHub's closing-keyword grammar over the body and skips
the generated block, on the reasoning that the block's references are the intended ones.
**That design was specified, measured against the real corpus before being built, and was
wrong twice over.**

1. It would have been **blind to one of the two named instances.** ``#1188``'s keyword is
   *inside* the generated block: ``promotion_refs.py`` rendered a promoted issue's
   **title**, and that title contained a closing keyword and a number. The tooling was the
   author. (That injection path is core#1543; this check is what notices it from outside.)
2. A regex over the body is a **model** of GitHub's parser, which is the one thing it
   cannot be used to check (``QA_RULES`` §6). And a text scan of any kind is blind to a
   third route entirely: ``landing#530`` closes ``#395`` with **no closing keyword anywhere
   in its body**, because an issue can also be linked through the PR sidebar.

``closingIssuesReferences`` is GitHub's own computed answer to *"what does this PR close on
merge"*, available **before** the merge. Asking it makes the keyword list, the escaping
edge cases, the code-span stripping and the sidebar route all unnecessary at once.

🚨 The oracle is EMPTY BY CONSTRUCTION outside the default branch
-----------------------------------------------------------------

Found by Infra, re-derived here with controls in the same invocation before being acted on.
``closingIssuesReferences`` is GitHub's *"what closes when this merges"*, and a PR that does
not target the **default** branch closes nothing -- which is the same rule that makes
``Closes #N`` on a ``dev`` PR never fire::

    landing #674   base dev     default main     MERGED   totalCount=0   <- title declares a closure
    core    #1552  base dev     default master   OPEN     totalCount=0   <- body declares TWO
    core    #1519  base master  default master   MERGED   totalCount=2   <- positive control
    landing #676   base main    default main     MERGED   totalCount=1   <- positive control

🔑 **So this instrument is authoritative on a PROMOTION and structurally BLIND on a feature
PR** -- and a feature PR into ``dev`` is exactly the population a closing-keyword guard would
otherwise be pointed at. The two controls are in that table because Infra's first reading of
this returned ``0`` **with a positive control that also returned 0**, i.e. the zero had
measured nothing at all.

**Measured on the shipped code before this guard existed:** pointed at ``core#1552`` -- a PR
whose body carries two closing declarations -- it printed ``verdict : PASS`` and exited ``0``.
A confident clean reading over a population it cannot see. That is `QA_RULES` §31 arriving one
layer above the blindness this script was written to catch, and in this script.

So the base branch is checked **first**, and a PR that does not target the default branch is
``NO_VERDICT`` / exit ``2``, never a pass. In production the refusal should never fire --
``promotion-pr-refs.yml`` already scopes the job to ``branches: [master]`` with
``head_ref == 'dev'`` -- and that is the point: it is the guard that makes lifting this script
somewhere else safe, rather than a condition anybody expects to see.

⚠️ What the oracle does NOT answer, measured 2026-09-24
--------------------------------------------------------

It says what GitHub **links now**. It is not a record of what fired at some past merge. Of
the three already-merged PRs this check flags, one closed its undeclared issue (``#1526`` →
``#1477``, two seconds after the merge) and **two did not**: ``#1188`` → ``#1130`` and
``landing#530`` → ``#395`` are both still open while GitHub still reports the link. Why is
not established; the likeliest reading is that a link created *after* a merge cannot have
fired at it.

🔑 **This does not weaken the check, because the check runs BEFORE the merge** -- which is
exactly where the oracle is the forward-looking answer it claims to be. What it does mean is
that a reading taken *after* a merge must not be reported as a record of what fired, and that
this instrument errs toward over-reporting rather than under-reporting. On a warning that is
the safe direction, and the corpus says it is not noisy: 119 of the 125 block-carrying PRs
agree exactly.

⚠️ Deliberately **not** asserted by a test. It is a live fact about two issues, and a guard
pinning it would go red the day somebody closes one of them -- a criterion that fails on a
correct change (``QA_RULES`` §29, ``WORKFLOW_RULES`` §5a).

What is compared
----------------

* **will close** -- ``closingIssuesReferences``, the oracle.
* **declared** -- the ``- Closes #N —`` lines inside
  ``<!-- promotion-refs:start -->…<!-- promotion-refs:end -->``, which is this project's own
  generated format. Reading our own output is not modelling GitHub's grammar; the match is
  deliberately exact and case-sensitive, because it is the shape ``promotion_refs.py``
  emits and nothing else should satisfy it.

Both directions are reported. *Will close but not declared* is the defect. *Declared but
will not close* means the generated block no longer describes the merge.

🚨 Advisory by construction -- say so, do not write this up as a gate
--------------------------------------------------------------------

``master`` carries **no required checks and no strict mode**, and every promotion merges
with ``gh pr merge --merge --admin``, which bypasses. So a red here cannot stop anything.
It is a signal on the PR the promoter is about to read, and that is worth having -- but a
write-up claiming this is *"now gated"* would be false, and the next reader would rely on
it.

Three states, three exit codes
------------------------------

``0`` the sets agree · ``1`` they differ · ``2`` **nothing could be compared**.

Exit ``2`` is not a pass and not a failure. With no generated block there is no declaration
to compare against: measured over 309 merged PRs, **184 carry no block at all**, because
``promotion-pr-refs.yml`` only landed in 2026-07 and before it the promoter hand-wrote the
references in the narrative -- which *was* the declaration. Scoring those as findings would
flag a third of the corpus, and a guard that refuses everything is one careless repair away
from permitting everything.

Usage::

    python scripts/check_promotion_closing_refs.py --repo datanika-io/datanika-core --pr 1526
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
from dataclasses import dataclass, field

#: The generated block's fences. Duplicated from `.github/scripts/promotion_refs.py` by
#: value rather than by import: this script runs from `scripts/`, is type-checked, and must
#: not depend on a file under `.github/` being importable. `test_promotion_closing_refs.py`
#: pins the two together with a ROUND TRIP -- it drives the real generator and reads the
#: block back with the parser below -- so a format change on either side goes red, which a
#: shared constant alone would not catch.
START = "<!-- promotion-refs:start -->"
END = "<!-- promotion-refs:end -->"

#: What the generated block DECLARES. `promotion_refs.py` emits `- Closes #{num} — {title}`
#: for an open issue a promoted commit declares it closes. Exact and case-sensitive on
#: purpose: this reads our own output, and a looser pattern would start matching the block's
#: own explanatory prose -- which contains `closes #272` inside a code span, 65 times across
#: the measured corpus.
DECLARED_LINE = re.compile(r"^[ \t]*-[ \t]+Closes[ \t]+#(\d+)\b", re.MULTILINE)

#: One call for every fact the verdict depends on. `baseRefName` and `defaultBranchRef` are
#: not extras: without them the closing set's emptiness is unattributable, because "closes
#: nothing" and "cannot close anything from here" are the same empty list.
_GRAPHQL = """
query($owner:String!, $name:String!, $number:Int!) {
  repository(owner:$owner, name:$name) {
    defaultBranchRef { name }
    pullRequest(number:$number) {
      baseRefName
      body
      closingIssuesReferences(first:100) {
        totalCount
        nodes { number state }
      }
    }
  }
}
"""


@dataclass(frozen=True)
class Report:
    """A verdict and the population it was drawn from (``QA_RULES`` §31).

    The population fields are not decoration. A reader cannot tell a clean comparison from
    one that examined nothing, so the numbers are printed beside the verdict every time.
    """

    repo: str
    pr: int
    base_ref: str
    default_branch: str
    block_present: bool
    will_close: frozenset[int] = field(default_factory=frozenset)
    declared: frozenset[int] = field(default_factory=frozenset)

    @property
    def oracle_applies(self) -> bool:
        """Can ``closingIssuesReferences`` say anything at all about this PR?

        Only on the default branch. Everywhere else the answer is empty by construction, so
        reading it as *"closes nothing"* is reading a fact about the branch as a fact about
        the body.
        """
        return self.base_ref == self.default_branch

    @property
    def undeclared(self) -> frozenset[int]:
        """GitHub will close these and the block speaks for none of them. The defect."""
        return frozenset(self.will_close - self.declared)

    @property
    def unfired(self) -> frozenset[int]:
        """The block declares these and GitHub will not act on them."""
        return frozenset(self.declared - self.will_close)

    @property
    def verdict(self) -> str:
        # The population check comes FIRST and outranks everything below it. Without it this
        # returns PASS for every feature PR in the repository -- measured on core#1552.
        if not self.oracle_applies:
            return "NO_VERDICT"
        if not self.block_present:
            # Nothing closes and nothing is declared: consistent, and there is nothing this
            # check could have got wrong. Anything else with no block is unmeasurable here.
            return "PASS" if not self.will_close else "NO_VERDICT"
        return "FAIL" if (self.undeclared or self.unfired) else "PASS"

    @property
    def reason(self) -> str | None:
        """WHICH non-measurement this is. A single "unmeasured" word covers both, and the
        one it picks is the one nobody acts on (``QA_RULES`` §31 rule 2)."""
        if self.verdict != "NO_VERDICT":
            return None
        return "not-default-base" if not self.oracle_applies else "no-generated-block"

    @property
    def exit_code(self) -> int:
        return {"PASS": 0, "FAIL": 1, "NO_VERDICT": 2}[self.verdict]


def block_of(body: str) -> str | None:
    """The generated block's inner text, or ``None`` when the body carries no block."""
    if START not in body or END not in body:
        return None
    return body.split(START, 1)[1].split(END, 1)[0]


def declared_refs(body: str) -> frozenset[int]:
    """Issue numbers the generated block declares this promotion closes.

    Scans **only** inside the block. A closing keyword in the human narrative is exactly
    what this check exists to catch, so counting it as a declaration would make the guard
    agree with the defect.
    """
    block = block_of(body)
    if block is None:
        return frozenset()
    return frozenset(int(n) for n in DECLARED_LINE.findall(block))


def fetch_pr_facts(repo: str, pr: int) -> tuple[str, str, str, frozenset[int]] | None:
    """``(default_branch, base_ref, body, closing_refs)``. ``None`` means the lookup failed.

    ``None`` is deliberately distinct from an empty set: *"closes nothing"* and *"I could
    not find out"* need opposite responses, and folding them puts the reassuring reading in
    the default position.

    All four facts come from ONE call on purpose. Fetched separately, the closing set can be
    read before the branch it depends on is known -- which is the shape that let an empty
    list be reported as a clean verdict.
    """
    owner, _, name = repo.partition("/")
    result = subprocess.run(  # noqa: S603
        [  # noqa: S607
            "gh",
            "api",
            "graphql",
            "-f",
            f"query={_GRAPHQL}",
            "-F",
            f"owner={owner}",
            "-F",
            f"name={name}",
            "-F",
            f"number={pr}",
        ],
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    if result.returncode != 0:
        print(f"  ! closingIssuesReferences lookup failed: {result.stderr.strip()[:300]}")
        return None
    try:
        repository = json.loads(result.stdout)["data"]["repository"]
        default_branch = repository["defaultBranchRef"]["name"]
        pull = repository["pullRequest"]
        base_ref = pull["baseRefName"]
        body = pull["body"] or ""
        nodes = pull["closingIssuesReferences"]["nodes"]
    except (json.JSONDecodeError, KeyError, TypeError):
        print("  ! the PR lookup came back in a shape this script does not know")
        return None
    return default_branch, base_ref, body, frozenset(int(n["number"]) for n in nodes)


def compare(
    repo: str,
    pr: int,
    body: str,
    will_close: frozenset[int],
    base_ref: str,
    default_branch: str,
) -> Report:
    return Report(
        repo=repo,
        pr=pr,
        base_ref=base_ref,
        default_branch=default_branch,
        block_present=block_of(body) is not None,
        will_close=will_close,
        declared=declared_refs(body),
    )


def render(report: Report) -> str:
    """The message. States the mechanism and the rewrite; never tells anyone to be careful.

    In the measured instances the author knew the issue had to stay open and said so in the
    same sentence as the reference. A parser read four words and ignored the fifth.
    """
    out = [
        "",
        f"  promotion closing-reference check — {report.repo}#{report.pr}",
        f"    base branch             : {report.base_ref}  (default: {report.default_branch})",
        f"    oracle applies here     : {'yes' if report.oracle_applies else 'NO'}",
        f"    generated block present : {'yes' if report.block_present else 'NO'}",
        f"    GitHub will close       : {sorted(report.will_close) or '(nothing)'}",
        f"    the block declares      : {sorted(report.declared) or '(nothing)'}",
        f"    verdict                 : {report.verdict}"
        + (f"  ({report.reason})" if report.reason else ""),
        "",
    ]
    if report.reason == "not-default-base":
        out += [
            "  THIS INSTRUMENT CANNOT SEE THIS PR, and that is a fact about the branch",
            "  rather than about the body. `closingIssuesReferences` is GitHub's answer to",
            "  *what closes when this merges*, and a PR that does not target the default",
            f"  branch closes nothing — so it is empty for {report.base_ref!r} whatever the",
            "  body says.",
            "",
            "  Measured: core#1552 targets `dev`, its body carries TWO closing declarations,",
            "  and the oracle returns 0. Reading that as *closes nothing* is reading the",
            "  branch as the body — the mistake this check exists to catch, one level up.",
            "",
            f"  Point it at a PR based on {report.default_branch!r} (a promotion). For a",
            "  feature PR the commit-message gate is the one that applies:",
            "  scripts/check_closing_keyword_intent.py.",
            "",
        ]
        return "\n".join(out)
    if report.verdict == "NO_VERDICT":
        out += [
            "  NOTHING COULD BE COMPARED. This PR carries no generated promotion-refs block,",
            "  so there is no declaration to check GitHub's answer against — while GitHub says",
            f"  it will close {sorted(report.will_close)}. That is not a pass and not a failure:",
            "  it is an unmeasured promotion, and somebody should look at it by hand.",
            "",
        ]
        return "\n".join(out)
    if report.undeclared:
        out += [
            f"  WILL CLOSE BUT IS NOT DECLARED: {sorted(report.undeclared)}",
            "",
            "    GitHub will close these when this PR merges, and the generated block speaks",
            "    for none of them. A closing keyword anywhere in the body fires — in prose, in",
            "    a quoted issue title, or through an issue linked in the sidebar. The keyword",
            "    is read and the sentence around it is not.",
            "",
            "    Rewrite so no closing keyword sits directly before a reference:",
            "",
            "        backticks    `close #123` does not fire; **close #123** does",
            "        paraphrase   'the issue stays open' / 'this does not finish it'",
            "        elide        name the issue once, and not again in that sentence",
            "",
            "    ** Do NOT escape the hash. ** An HTML entity for `#` contains a hash followed",
            "       by digits, so escaping does not remove a reference — it silences the",
            "       closure AND plants a different one, pointing at whatever issue those",
            "       digits name. Measured; see scripts/check_closing_keyword_intent.py.",
            "",
            "    If the closure IS intended, declare it: the commit or its source PR should",
            "    say `Closes #N`, so the generated block carries it and intent matches effect.",
            "",
        ]
    if report.unfired:
        out += [
            f"  DECLARED BUT WILL NOT CLOSE: {sorted(report.unfired)}",
            "",
            "    The generated block says this promotion closes these and GitHub disagrees.",
            "    The block no longer describes the merge — usually a body edited by hand after",
            "    the generator ran, or a reference that was mangled. Re-run the generator.",
            "",
        ]
    return "\n".join(out)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="core#1541 promotion closing-reference check")
    parser.add_argument("--repo", required=True, help="owner/name")
    parser.add_argument("--pr", required=True, type=int)
    args = parser.parse_args(argv)

    facts = fetch_pr_facts(args.repo, args.pr)
    if facts is None:
        # An unreadable subject measures nothing, and reporting it as clean is the failure
        # this whole file is about. Exit 2, loudly.
        print(
            f"\n  promotion closing-reference check COULD NOT READ {args.repo}#{args.pr}.\n"
            "  This is NOT a pass — nothing was compared.\n"
        )
        return 2

    default_branch, base_ref, body, will_close = facts
    report = compare(args.repo, args.pr, body, will_close, base_ref, default_branch)
    print(render(report))
    return report.exit_code


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
