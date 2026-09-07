"""Refuse a commit whose body closes an issue its subject only `refs` (core#1162).

GitHub scans a commit message for ``close|closes|closed|fix|fixes|fixed|resolve|resolves|
resolved`` followed by an issue reference and closes that issue when the commit reaches the
**default branch**. It performs no negation analysis and does not care what the subject line
declared. So a commit whose body says *"Does not close"* next to a reference **closes it**.

Four measured instances, three repositories, in one week -- every one with a weak keyword
(``refs``) in the subject and a closing keyword in the body, on the same issue:

====================  ==========  =======================================================
commit                repository  what the body said
====================  ==========  =======================================================
``ebb268a3``          core        an explicit denial, in a sentence written to prevent it
``386d0a03``          core        "...is correct and does not clos" + "e" + the reference
``ca3fc58f``          core        "refuted the fix" + the reference -- **not a denial at
                                  all**, just prose in which a noun precedes a number
``3e8d3dae``          landing     an explicit denial; the promotion closed the issue one
                                  second after the merge
====================  ==========  =======================================================

🔑 **This is not a discipline problem, and a guard that assumes it is will be aimed at the wrong
thing.** In the landing case the author knew the issue had to stay open, said so explicitly, and
said so *in the same sentence as the reference*. The tooling closed it anyway. Nobody was
careless; a parser read four words and ignored the fifth. The message this script prints is
written accordingly -- it states what GitHub will do and how to phrase around it, and does not
tell anyone to be more careful.

Why *this* predicate
--------------------

Two heuristics were proposed, measured over 400 ``master`` commits, and **both withdrawn by their
authors**:

* a **negation window** (a negation word within ~40 characters before the keyword) --
  **29% precision and 67% recall.** It flags correct closures whose subject merely contains the
  word "not" somewhere, and it cannot see ``ca3fc58f`` at all, because "refuted the fix" contains
  no negation.
* a **loose separator** (``[^A-Za-z0-9]{0,5}`` between keyword and reference) -- 5 false
  positives, including two commits *explaining* that they are deliberately not closing something.

This predicate is **structural**: an internal contradiction between what the subject declares and
what the body triggers. No wordlist, no judgement about intent. **4 true positives, 0 false
positives** across the corpora measured.

Its limitation, stated rather than hidden: it only sees commits whose subject declares a weak
keyword. A denial in a commit whose subject declares nothing is missed. A narrow guard with zero
false positives is worth more than a wide one that gets switched off -- an over-firing guard is
not the safe direction here, because the cost of a false positive lands on a *correct* commit.

🚨 Escaping the ``#`` is not a repair -- measured, it does TWO things
--------------------------------------------------------------------

The finding arrived as *"escaping does not disarm the parser, it re-points it"*. Measured on the
real strings, it does **both**, to two different scans, in opposite directions:

* the **closing-keyword** grammar stops matching altogether -- the ``&`` sits between the keyword
  and the hash -- so an escaped denial closes nothing **and evades this guard**;
* a **bare-reference** scan, which is what the promotion-refs tooling runs, reads the entity's own
  hash-then-digits and returns an **unrelated issue**.

So the conclusion survives and the mechanism is sharper than reported: escaping silences the
closure while planting a reference nobody meant, in the one document whose generated reference
list is trusted *because* it is mechanical.

Therefore :func:`normalise` folds those forms back to ``#`` **before** the grammar runs -- without
it an escaped denial slips past this check -- and the error message never suggests escaping. The
safe repairs are to paraphrase or to elide the number.

Usage::

    python scripts/check_closing_keyword_intent.py --range origin/dev..HEAD
    python scripts/check_closing_keyword_intent.py --message-file .git/COMMIT_EDITMSG
    git log --format=%B -1 | python scripts/check_closing_keyword_intent.py --stdin

Exit ``0`` clean, ``1`` a contradiction was found, ``2`` the range could not be read (**not** a
pass -- an unreadable range measures nothing).
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from dataclasses import dataclass

#: GitHub's closing keywords, exactly.
_KEYWORD = r"clos(?:e|es|ed)|fix(?:|es|ed)|resolv(?:e|es|ed)"

#: GitHub's grammar: the keyword, an optional colon, REQUIRED whitespace, then a reference GitHub
#: will actually act on -- a bare ``#N``, or a fully-qualified ``owner/repo#N``.
#:
#: 🚨 ``core#N`` / ``landing#N`` is deliberately NOT accepted here, and that is a measurement
#: rather than a preference. It is this project's *prose* convention for naming an issue in
#: another repository; GitHub's cross-repo syntax is ``owner/repo#N``, so a bare ``word#N`` is
#: plain text to the parser and closes nothing. A first draft accepted it and produced the only
#: two false positives in 400 commits -- ``fixes core#1075`` and ``Closes core#719``. Both were
#: tested against reality rather than against judgement: **each of those issues was closed BY
#: HAND**, with no commit attributed, so GitHub demonstrably did not act on either.
#:
#: A looser *separator* is likewise rejected: it produced 5 false positives, two of them on
#: commits explaining that they were deliberately not closing something.
CLOSING = re.compile(rf"\b({_KEYWORD})\b:?[ \t]+(?:[\w.-]+/[\w.-]+)?#(\d+)", re.I)

#: The weak keywords, which close nothing. A run of references may follow one of them
#: (`refs #A, #B`) -- core#1168 is the case where those were meant for another repository.
_WEAK = r"refs?|references|part of|towards|toward|see"
WEAK_LEAD = re.compile(rf"\b(?:{_WEAK})\b\s*:?\s*", re.I)
REF_RUN = re.compile(r"(?:[\w.-]+/)?(?:[A-Za-z][\w-]*)?#(\d+)(?:\s*[,/]\s*)?")

#: Forms that FOLD BACK to `#`. Each contains a hash-then-digits shape of its own, which is why
#: escaping re-points a reference rather than removing it.
_ENTITIES = (
    (re.compile(r"&#0*35;", re.I), "#"),
    (re.compile(r"&#x0*23;", re.I), "#"),
    (re.compile(r"&num;", re.I), "#"),
    (re.compile(r"%23"), "#"),
)


def normalise(text: str) -> str:
    """Fold escaped hashes back to `#` before the grammar runs.

    🚨 Order matters and the reason is the finding: an entity for `#` contains a hash followed by
    digits. Applying the grammar first would read that inner shape as a reference to a DIFFERENT
    issue -- so a check that inspects rendered or unnormalised text can both miss the real
    reference and invent a false one in the same pass.
    """
    for pattern, repl in _ENTITIES:
        text = pattern.sub(repl, text)
    return text


#: Every commit here carries a `[Dept]` tag in its subject (WORKFLOW_RULES §4). One appearing at
#: the START of a later line means several messages have been concatenated.
_SUBJECT_TAG = re.compile(r"^\[(?:Engineering|QA|Growth|Product|Infra)\]", re.M)


def looks_concatenated(message: str) -> bool:
    """Several commit messages fed in as one.

    🚨 This manufactures the exact false positive the guard exists to avoid, and I did it to
    myself in the final check of the very session that shipped this. ``git log --format=%B
    <range> | ... --stdin`` is the natural thing to type; ``--stdin`` treats the whole stream as
    ONE message, so the first commit's subject becomes the subject and every later commit's
    subject becomes *body text*. A `refs` subject on one commit plus a `closes` subject on
    another then reads as a contradiction inside a single message. Neither commit was wrong;
    ``--range`` reported both clean.

    Refusing is right rather than merely warning: the alternative is a confident, specific,
    entirely fictional finding, which is worse than no answer.
    """
    return len(_SUBJECT_TAG.findall(message)) > 1


def subject_of(message: str) -> str:
    return message.splitlines()[0] if message.strip() else ""


def weak_refs(subject: str) -> set[str]:
    """Issue numbers the subject declares with a NON-closing keyword."""
    out: set[str] = set()
    for lead in WEAK_LEAD.finditer(subject):
        pos = lead.end()
        while (m := REF_RUN.match(subject, pos)) is not None:
            out.add(m.group(1))
            pos = m.end()
    return out


@dataclass(frozen=True)
class Finding:
    issue: str
    line_no: int
    line: str
    phrase: str


def findings(message: str) -> list[Finding]:
    """Closing references naming an issue the subject only `refs`.

    The contradiction is inside one message: the author declared the weaker intent where it is
    read, and triggered the stronger one where it is parsed.
    """
    text = normalise(message)
    declared = weak_refs(subject_of(text))
    if not declared:
        return []

    lines = text.splitlines()
    out: list[Finding] = []
    for m in CLOSING.finditer(text):
        issue = m.group(2)
        if issue not in declared:
            continue
        line_no = text[: m.start()].count("\n") + 1
        out.append(
            Finding(
                issue=issue,
                line_no=line_no,
                line=lines[line_no - 1].strip(),
                phrase=m.group(0).strip(),
            )
        )
    return out


def render(findings_: list[Finding], where: str) -> str:
    """The refusal text.

    Deliberately does NOT tell the author to be more careful: in the measured cases the author
    knew, said so, and said so beside the reference. It states the mechanism and the rewrite.
    """
    head = [
        "",
        f"  REFUSED: a commit message closes an issue its subject only `refs`  ({where})",
        "",
    ]
    for f in findings_:
        head += [
            f"    line {f.line_no}:  {f.line}",
            f"      -> GitHub reads `{f.phrase}` and will CLOSE that issue when this",
            "         reaches the default branch, whatever the surrounding sentence says.",
            "",
        ]
    head += [
        "  Nothing about this is carelessness -- in every measured instance the author knew the",
        "  issue had to stay open and said so in the same sentence. The parser reads the keyword",
        "  and the number and ignores the words between them.",
        "",
        "  Rewrite so no closing keyword sits directly before a reference:",
        "",
        "      paraphrase   'the issue stays open' / 'this does not finish it'",
        "      elide        name the issue once in the subject and not again in that sentence",
        "",
        "  ** Do NOT escape the hash. ** An HTML entity for `#` contains a hash followed by",
        "     digits, so escaping does not remove a reference -- it plants a different one,",
        "     pointing at whatever issue those digits name, and the promotion's generated",
        "     reference list will act on it. This check folds those forms back before reading.",
        "",
        "  Deliberate exception (rare -- the closure really is intended):",
        "      change the subject's `refs` to `closes`, so intent and effect agree.",
        "",
    ]
    return "\n".join(head)


def messages_in_range(rng: str) -> list[tuple[str, str]]:
    """(sha, message) for each commit in `rng`. Raises on an unreadable range."""
    sep = "\x1e"
    proc = subprocess.run(  # noqa: S603
        ["git", "log", f"--format=%H%x1f%B{sep}", rng],  # noqa: S607
        capture_output=True,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.decode("utf-8", "replace").strip()[:300])
    out: list[tuple[str, str]] = []
    for chunk in proc.stdout.decode("utf-8", "replace").split(sep):
        if "\x1f" not in chunk:
            continue
        sha, _, body = chunk.partition("\x1f")
        out.append((sha.strip(), body))
    return out


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="core#1162 closing-keyword intent check")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--range", dest="rng")
    g.add_argument("--message-file")
    g.add_argument("--stdin", action="store_true")
    args = ap.parse_args(argv)

    if args.stdin:
        raw = sys.stdin.read()
        if looks_concatenated(raw):
            print(
                "  closing-keyword check REFUSES this input: it carries more than one `[Dept]`\n"
                "  subject, so it is several commit messages concatenated. `--stdin` reads ONE\n"
                "  message, which would make an earlier commit's `closes` subject read as a later\n"
                "  commit's body and report a contradiction that exists in neither.\n"
                "\n"
                "  Use:  python scripts/check_closing_keyword_intent.py --range <range>",
                file=sys.stderr,
            )
            return 2
        items = [("(stdin)", raw)]
    elif args.message_file:
        with open(args.message_file, encoding="utf-8") as fh:
            items = [(args.message_file, fh.read())]
    else:
        try:
            items = messages_in_range(args.rng)
        except RuntimeError as exc:
            # An unreadable range measures nothing. It is not a pass.
            print(f"  closing-keyword check COULD NOT READ {args.rng}: {exc}", file=sys.stderr)
            return 2

    bad = 0
    for where, message in items:
        found = findings(message)
        if found:
            bad += 1
            print(render(found, where[:12] if len(where) == 40 else where))
    return 1 if bad else 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
