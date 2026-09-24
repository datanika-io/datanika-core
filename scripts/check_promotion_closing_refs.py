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

_GRAPHQL = """
query($owner:String!, $name:String!, $number:Int!) {
  repository(owner:$owner, name:$name) {
    pullRequest(number:$number) {
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
    block_present: bool
    will_close: frozenset[int] = field(default_factory=frozenset)
    declared: frozenset[int] = field(default_factory=frozenset)

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
        if not self.block_present:
            # Nothing closes and nothing is declared: consistent, and there is nothing this
            # check could have got wrong. Anything else with no block is unmeasurable here.
            return "PASS" if not self.will_close else "NO_VERDICT"
        return "FAIL" if (self.undeclared or self.unfired) else "PASS"

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


def fetch_closing_refs(repo: str, pr: int) -> frozenset[int] | None:
    """Ask GitHub what this PR closes. ``None`` means the lookup failed.

    ``None`` is deliberately distinct from an empty set: *"closes nothing"* and *"I could
    not find out"* need opposite responses, and folding them puts the reassuring reading in
    the default position.
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
        payload = json.loads(result.stdout)
        nodes = payload["data"]["repository"]["pullRequest"]["closingIssuesReferences"]["nodes"]
    except (json.JSONDecodeError, KeyError, TypeError):
        print("  ! closingIssuesReferences came back in a shape this script does not know")
        return None
    return frozenset(int(n["number"]) for n in nodes)


def fetch_body(repo: str, pr: int) -> str | None:
    result = subprocess.run(  # noqa: S603
        ["gh", "pr", "view", str(pr), "--repo", repo, "--json", "body", "-q", ".body"],  # noqa: S607
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    if result.returncode != 0:
        print(f"  ! could not read the PR body: {result.stderr.strip()[:300]}")
        return None
    return result.stdout


def compare(repo: str, pr: int, body: str, will_close: frozenset[int]) -> Report:
    return Report(
        repo=repo,
        pr=pr,
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
        f"    generated block present : {'yes' if report.block_present else 'NO'}",
        f"    GitHub will close       : {sorted(report.will_close) or '(nothing)'}",
        f"    the block declares      : {sorted(report.declared) or '(nothing)'}",
        f"    verdict                 : {report.verdict}",
        "",
    ]
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

    will_close = fetch_closing_refs(args.repo, args.pr)
    body = fetch_body(args.repo, args.pr)
    if will_close is None or body is None:
        # An unreadable subject measures nothing, and reporting it as clean is the failure
        # this whole file is about. Exit 2, loudly.
        print(
            f"\n  promotion closing-reference check COULD NOT READ {args.repo}#{args.pr}.\n"
            "  This is NOT a pass — nothing was compared.\n"
        )
        return 2

    report = compare(args.repo, args.pr, body, will_close)
    print(render(report))
    return report.exit_code


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
