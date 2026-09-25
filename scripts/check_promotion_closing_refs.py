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

🚨 The oracle is STALE for a body the previous step just wrote (core#1575)
--------------------------------------------------------------------------

``promotion-pr-refs.yml`` writes the generated block with ``gh pr edit`` in the step
**immediately before** this one, and GitHub recomputes ``closingIssuesReferences``
asynchronously. So the first look returns ``(nothing)`` while the block declares six issues,
and this script scored that as ``DECLARED BUT WILL NOT CLOSE`` -- a FAIL -- on **2 of 2**
promotions after the step shipped. On the measured instance GitHub later named **exactly the
six** the block declared: *the block was right the whole time; the check was early.*

⚠️ **The run history hid it, and that is the part worth keeping.** The workflow tallied
``98 success / 2 failure``, which reads as a fresh regression in a long-green generator --
but **all 98 greens predate the step's existence**. A workflow's conclusion history spans
versions of the workflow, so a tally taken without dating the step gives exactly the wrong
answer, and it is the reading anyone takes first.

``await_reparse`` re-reads while the oracle names nothing the block declares. Two things it
deliberately does **not** do: it does not poll until the sets *agree* (that is a check which
stops the moment it gets the answer it wants -- the real core#1541 defect,
``will_close - declared``, is left out of the loop entirely and evaluated afterwards); and it
does not report a pass on exhaustion. An exhausted wait is ``NO_VERDICT`` with reason
``oracle-silent-after-wait``, because *"GitHub named nothing"* and *"the block is wrong"* are
different claims and only the second is a finding.

Every run prints ``oracle reparse: N look(s) over Xs``. A check that waits and then agrees is
indistinguishable, from a green alone, from one that stopped looking -- and those numbers are
how ``REPARSE_WAIT_SECONDS`` gets tuned from data rather than from the guess it currently is.

Four states, three exit codes
-----------------------------

``0`` the sets agree · ``1`` they differ **and GitHub has answered** · ``2`` **nothing could
be compared** (not the default branch · no generated block · the oracle stayed silent for the
whole wait).

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
import time
from dataclasses import dataclass, field

#: How long to keep re-reading the oracle, and how often (core#1575).
#:
#: `promotion-pr-refs.yml` writes the generated block with `gh pr edit` in the step
#: IMMEDIATELY before this one, and GitHub has not recomputed `closingIssuesReferences` for
#: the new body by the time this runs. So the first look returns `(nothing)` while the block
#: declares six issues -- which this script scored as `DECLARED BUT WILL NOT CLOSE`, i.e. a
#: FAIL, on **2 of 2** promotions since the step shipped.
#:
#: ⚠️ The defaults are a starting point, not a measurement. The observed reparse had
#: completed by the time anyone looked again (~12 minutes later), but that is when a human
#: happened to re-read, not the latency. So `await_reparse` PRINTS how long it waited and on
#: which look the answer arrived: the next promotion measures the real latency for us, and
#: these numbers get tuned from data instead of from this guess.
REPARSE_WAIT_SECONDS = 300
REPARSE_POLL_SECONDS = 15

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

    #: Did a reparse wait run to exhaustion without the oracle ever answering (core#1575)?
    #:
    #: This is deliberately NOT the same thing as `oracle_silent`. A caller that waited the
    #: full window and still got nothing has established that it *cannot measure* this PR.
    #: A caller that never waited (`--wait-seconds 0`) has established nothing of the kind,
    #: and must keep the old FAIL -- otherwise opting out of the wait would silently convert
    #: every real mangled-block finding into an unmeasurable.
    reparse_exhausted: bool = False

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
    def oracle_silent(self) -> bool:
        """The block declares closures and GitHub links NOTHING AT ALL (core#1575).

        Distinguished from a partial disagreement on purpose. *"GitHub named a different
        set"* means it has answered and we disagree -- a finding. *"GitHub named nothing"*
        one second after the body was rewritten means it has not answered yet, which is not
        the same claim and must not be reported as one.
        """
        return bool(self.declared) and not self.will_close

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
        # core#1575. We waited the whole window and GitHub never named a single issue, so we
        # do not know whether the block is wrong or the reparse simply never landed. Saying
        # FAIL here asserts the first, which was wrong on 2 of 2 promotions.
        if self.oracle_silent and self.reparse_exhausted:
            return "NO_VERDICT"
        return "FAIL" if (self.undeclared or self.unfired) else "PASS"

    @property
    def reason(self) -> str | None:
        """WHICH non-measurement this is. A single "unmeasured" word covers both, and the
        one it picks is the one nobody acts on (``QA_RULES`` §31 rule 2)."""
        if self.verdict != "NO_VERDICT":
            return None
        if not self.oracle_applies:
            return "not-default-base"
        if self.oracle_silent and self.reparse_exhausted:
            return "oracle-silent-after-wait"
        return "no-generated-block"

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


@dataclass(frozen=True)
class Wait:
    """What the reparse poll actually did — printed, never inferred (core#1575).

    A check that waits and then agrees is indistinguishable, from a green alone, from one
    that stopped looking. So the number of looks and the elapsed seconds go in the output of
    every run, and the promotion after this one measures the real latency for us.
    """

    looks: int
    elapsed: float
    answered_on_look: int | None

    @property
    def exhausted(self) -> bool:
        return self.answered_on_look is None


def await_reparse(
    repo: str,
    pr: int,
    *,
    wait_seconds: float = REPARSE_WAIT_SECONDS,
    poll_seconds: float = REPARSE_POLL_SECONDS,
    fetch=None,
    sleep=time.sleep,
    now=time.monotonic,
) -> tuple[tuple[str, str, str, frozenset[int]] | None, Wait]:
    """Re-read the PR until every DECLARED issue is linked, or the window expires.

    🔑 **The stopping condition is the oracle's AVAILABILITY, not its agreement, and the
    difference is the whole design.** Polling "until the sets match" is a check that stops
    the moment it gets the answer it wants. This polls only while something the block
    declares is **not yet linked** -- the one state that is either a pending reparse or a
    mangled block -- and it leaves the actual core#1541 defect, `will_close - declared`
    (*GitHub will close something the block does not mention*), **out of the loop
    entirely.** That axis is evaluated once, afterwards, and no amount of waiting can
    suppress it.

    Two consequences worth stating because they are easy to get wrong:

    * When the oracle is **already current** -- a re-run on a body nobody just rewrote, or
      the post-merge invocation -- `declared <= will_close` holds on the first look and this
      returns immediately having waited **zero** seconds. The wait costs nothing in the case
      that does not need it.
    * When the block genuinely declares an issue GitHub will never link, this polls for the
      full window and then reports it. Slower, and still a finding.
    """
    # 🚨 Resolved HERE and not in the signature, and this is a correctness property of the
    # TESTS rather than of the check (core#1593). `fetch=fetch_pr_facts` as a default binds
    # the function OBJECT at definition time, while `monkeypatch.setattr(module,
    # "fetch_pr_facts", ...)` rebinds the module ATTRIBUTE -- so the patch never reached this
    # call and all four CLI-boundary tests drove the real `gh`. In a token-less CI job two of
    # them went red and two stayed GREEN asserting exit 2 for a reason that was not theirs.
    # A default argument is not an injection seam; a call-time lookup is.
    fetch = fetch_pr_facts if fetch is None else fetch
    deadline = now() + wait_seconds
    looks = 0
    started = now()
    facts = None
    while True:
        looks += 1
        facts = fetch(repo, pr)
        if facts is None:
            # An unreadable subject is not a pending reparse. Stop; `main` reports it.
            return None, Wait(looks, now() - started, None)
        _default, _base, body, will_close = facts
        if declared_refs(body) <= will_close:
            return facts, Wait(looks, now() - started, looks)
        if now() >= deadline:
            return facts, Wait(looks, now() - started, None)
        sleep(min(poll_seconds, max(0.0, deadline - now())))


def compare(
    repo: str,
    pr: int,
    body: str,
    will_close: frozenset[int],
    base_ref: str,
    default_branch: str,
    reparse_exhausted: bool = False,
) -> Report:
    return Report(
        repo=repo,
        pr=pr,
        base_ref=base_ref,
        default_branch=default_branch,
        block_present=block_of(body) is not None,
        will_close=will_close,
        declared=declared_refs(body),
        reparse_exhausted=reparse_exhausted,
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
    if report.reason == "oracle-silent-after-wait":
        out += [
            "  GITHUB NEVER ANSWERED, so nothing could be compared — and this is NOT a",
            "  finding about the body. The block declares",
            f"  {sorted(report.declared)} and `closingIssuesReferences` named NOTHING for the",
            "  whole wait.",
            "",
            "  The mechanism (core#1575): `promotion-pr-refs.yml` writes this block with",
            "  `gh pr edit` in the step immediately before this one, and GitHub recomputes",
            "  the closing set asynchronously. Asking it straight away reliably gets an empty",
            "  answer — which this check used to score as DECLARED BUT WILL NOT CLOSE, on 2 of",
            "  2 promotions after the step shipped.",
            "",
            "  ⚠️ Do NOT re-run the generator on the strength of this. It was almost",
            "  certainly right: on the measured instance GitHub later named exactly the six",
            "  the block declared. What to do instead:",
            "",
            "    * re-run THIS check on the same PR in a few minutes (the generator is",
            "      idempotent, so nothing is rewritten and the oracle will have caught up), or",
            "    * read `closingIssuesReferences` by hand before merging:",
            "      gh pr view <n> --json closingIssuesReferences",
            "",
            "  If it stays silent for much longer than the wait above, THAT is worth a look —",
            "  raise the wait rather than believing the empty set.",
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
            "    The generated block says this promotion closes these and GitHub named a",
            f"    DIFFERENT set: {sorted(report.will_close)}. So GitHub has answered — this is",
            "    not the core#1575 timing case, which shows up as an answer of NOTHING AT ALL",
            "    and is reported separately above.",
            "",
            "    Likeliest causes: a body edited by hand after the generator ran, or a",
            "    reference that was mangled. Re-running the generator is reasonable HERE,",
            "    because its output demonstrably no longer matches GitHub's reading.",
            "",
        ]
    return "\n".join(out)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="core#1541 promotion closing-reference check")
    parser.add_argument("--repo", required=True, help="owner/name")
    parser.add_argument("--pr", required=True, type=int)
    parser.add_argument(
        "--wait-seconds",
        type=float,
        default=REPARSE_WAIT_SECONDS,
        help=(
            "how long to keep re-reading while the oracle names nothing the block declares "
            "(core#1575: the generator rewrites the body in the previous step and GitHub "
            "reparses asynchronously). 0 disables the wait and restores the old behaviour"
        ),
    )
    parser.add_argument(
        "--poll-seconds", type=float, default=REPARSE_POLL_SECONDS, help="gap between looks"
    )
    args = parser.parse_args(argv)

    facts, waited = await_reparse(
        args.repo,
        args.pr,
        wait_seconds=args.wait_seconds,
        poll_seconds=args.poll_seconds,
    )
    # Printed on every run, pass or fail: a wait that is never reported cannot be told apart
    # from no wait at all, and the numbers are how the defaults get tuned from data.
    print(
        f"\n  oracle reparse: {waited.looks} look(s) over {waited.elapsed:.1f}s; "
        + (
            f"answered on look {waited.answered_on_look}"
            if waited.answered_on_look
            else "never answered within the wait"
        )
    )
    if facts is None:
        # An unreadable subject measures nothing, and reporting it as clean is the failure
        # this whole file is about. Exit 2, loudly.
        print(
            f"\n  promotion closing-reference check COULD NOT READ {args.repo}#{args.pr}.\n"
            "  This is NOT a pass — nothing was compared.\n"
        )
        return 2

    default_branch, base_ref, body, will_close = facts
    report = compare(
        args.repo,
        args.pr,
        body,
        will_close,
        base_ref,
        default_branch,
        reparse_exhausted=waited.exhausted and args.wait_seconds > 0,
    )
    print(render(report))
    return report.exit_code


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
