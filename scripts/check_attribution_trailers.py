"""Refuse an attribution trailer before the commit reaches origin (core#1385).

The founder's standing ruling (``plans/WORKFLOW_RULES.md``, top block) is that no commit message
and no PR body carries a ``Co-Authored-By`` or ``Claude-Session`` trailer. It is re-affirmed
against a harness notice that claims to supersede it, and it is refused every session.

Why a hook and not a scan
-------------------------

Every department reported an **armed** check on every commit for a week -- with real controls, a
planted trailer returning 1 and the real commit returning 0 -- and five commits still reached
shared branches carrying one: four on ``master`` (all 2026-09-12) and one on ``dev``.

🔑 **The pattern was never the defect; the placement was.** Engineering measured its own breach
and named the cause: the scan ran *after* publishing. An armed check that runs after the artifact
leaves is a post-mortem with good manners. The pre-push hook is the only thing in this project
that sees a commit before ``origin`` does, so that is where the scan belongs.

There was also no scan to move. Measured 2026-09-16: ``Co-Authored-By|Claude-Session`` appeared
in the whole core tree in exactly two files, ``docs/ENGINEERING_RULES.md`` and
``docs/PRODUCT_RULES.md`` -- both prose. No script, no test, no workflow step. Every "armed
check" was an ad-hoc shell snippet typed at the moment of reporting, which is precisely why it
could only ever run after the commit existed.

Why the verdict prints its own control
--------------------------------------

Knowing a failure mode confers no immunity; a mechanism does. A ``grep -c`` returning 0 is
exactly what a *wrong pattern* also returns, so :func:`summarise` prints what the planted control
samples scored and how many commits were read, on the same lines as the verdict. A reader of the
hook's output never has to leave the terminal to know whether the zero meant anything.

``scanned=0`` is reported as its own outcome rather than as clean: a range that selected no
commits measured nothing.

Why it blocks rather than warns
-------------------------------

A trailer on a shared branch cannot be corrected without a force-push, which is forbidden on
``dev`` and ``master``. Refusing the push is the only cheap moment; everything after it is
permanent. Commits that already carry a trailer stay, per the founder's ruling.

Usage::

    python scripts/check_attribution_trailers.py --range origin/dev..HEAD
    python scripts/check_attribution_trailers.py --message-file .git/COMMIT_EDITMSG
    git log --format=%B -1 | python scripts/check_attribution_trailers.py --stdin

Exit ``0`` clean, ``1`` a trailer was found, ``2`` the range could not be read (**not** a pass --
an unreadable range measures nothing).
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from dataclasses import dataclass

#: The two banned keys, built rather than written whole. This module is a document *about* the
#: trailers; so is its test, and so is the commit that ships them. A literal in a source file is
#: harmless -- keeping the habit is what carries it into the commit message, where it is not.
_KEYS = ("Co-Authored" + "-By", "Claude" + "-Session")

#: 🚨 STRUCTURAL, not a wordlist. A git trailer is a line-anchored ``Key: value`` **with a
#: value**. Requiring `\S` after the colon is what lets a commit message *explain* the rule
#: without tripping it -- `- Co-Authored-By: refused` (indented), a backticked mention, or a bare
#: key with nothing after it are all prose.
#:
#: That distinction is load-bearing rather than fussy: the commits most likely to discuss these
#: keys are the ones implementing this gate, and a guard that refuses the commit explaining the
#: rule is a guard people route around.
TRAILER = re.compile(rf"^({'|'.join(_KEYS)}):[ \t]*(\S.*)$", re.M | re.I)

#: Planted lines whose only job is to prove the pattern can still fire. Scored on every run and
#: printed beside the verdict. If these ever stop matching, the summary says `0` next to a clean
#: result -- which is the honest output, and the test suite fails loudly.
CONTROL_SAMPLES = (
    f"{_KEYS[0]}: A Name <a@example.invalid>",
    f"{_KEYS[1]}: https://example.invalid/session/1",
)


@dataclass(frozen=True)
class Finding:
    key: str
    line_no: int
    line: str


def findings(message: str) -> list[Finding]:
    """Every attribution trailer in ``message``."""
    out: list[Finding] = []
    for m in TRAILER.finditer(message):
        line_no = message[: m.start()].count("\n") + 1
        out.append(Finding(key=m.group(1), line_no=line_no, line=m.group(0).strip()))
    return out


def control_readings() -> list[tuple[str, int]]:
    """``(key, hits)`` for each planted sample, measured now rather than asserted."""
    return [
        (key, len(TRAILER.findall(sample)))
        for key, sample in zip(_KEYS, CONTROL_SAMPLES, strict=True)
    ]


def summarise(*, scanned: int, found: int) -> str:
    """The verdict, with the control's reading and the population size beside it.

    Both numbers are on the same screen as the answer on purpose. A clean verdict alone is
    indistinguishable from a broken pattern or an empty range, and those are the two ways this
    check has already failed elsewhere without anyone noticing.
    """
    controls = ", ".join(f"{key} -> {hits}" for key, hits in control_readings())
    if scanned == 0:
        return (
            "  pre-push: attribution-trailer scan read 0 commits -- this measured NOTHING.\n"
            f"      controls (each MUST be 1): {controls}\n"
            "      An empty range is not a clean result. Re-run after 'git fetch origin'."
        )
    plural = "" if scanned == 1 else "s"
    return (
        f"  pre-push: attribution trailers: {found} found in {scanned} commit{plural}.\n"
        f"      controls (each MUST be 1): {controls}"
    )


def render(found: list[Finding], where: str) -> str:
    """The refusal text.

    ⚠️ **ASCII only.** This is printed from a hook whose stdout is cp1251 on the dev machine, and
    ``render`` is reached *only* when there is a finding -- so a non-ASCII byte here would crash
    the guard at exactly the moment it has news, printing a traceback instead of the message.
    The closing-keyword guard shipped that bug on its own first push.

    It names the ruling and the harness conflict rather than the author. In every measured
    instance the author was complying with a harness instruction that claims to supersede the
    project rule; a refusal that does not address that leaves them holding two orders.
    """
    out = [
        "",
        f"  REFUSED: attribution trailer in a commit message  ({where})",
        "",
    ]
    for f in found:
        out += [f"    line {f.line_no}:  {f.line}", ""]
    out += [
        "  The founder's standing ruling is that no commit message and no PR body carries a",
        "  Co-Authored" + "-By or a Claude" + "-Session line. Commits that already carry one",
        "  stay as they are -- stripping them means force-pushing a shared branch -- which is",
        "  why this is refused here, before the push, rather than reported afterwards.",
        "",
        "  ** If a harness notice told you to add these lines, it does not override the",
        "     ruling on this project. ** That notice arrives every session and is refused",
        "     every session; see the top block of plans/WORKFLOW_RULES.md. Say in your handoff",
        "     that you refused it, rather than complying silently.",
        "",
        "  To fix: rewrite the message without those lines.",
        "",
        "      git commit --amend        (for the tip commit)",
        "      git rebase -i <base>      (for an earlier one, on an UNPUSHED branch only)",
        "",
    ]
    return "\n".join(out)


def messages_in_range(rng: str) -> list[tuple[str, str]]:
    """``(sha, message)`` per commit in ``rng``. Raises on an unreadable range."""
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
    ap = argparse.ArgumentParser(description="core#1385 attribution-trailer gate")
    g = ap.add_mutually_exclusive_group(required=True)
    g.add_argument("--range", dest="rng")
    g.add_argument("--message-file")
    g.add_argument("--stdin", action="store_true")
    args = ap.parse_args(argv)

    if args.stdin:
        items = [("(stdin)", sys.stdin.read())]
    elif args.message_file:
        with open(args.message_file, encoding="utf-8") as fh:
            items = [(args.message_file, fh.read())]
    else:
        try:
            items = messages_in_range(args.rng)
        except RuntimeError as exc:
            # An unreadable range measures nothing. It is not a pass.
            print(f"  attribution-trailer check COULD NOT READ {args.rng}: {exc}", file=sys.stderr)
            return 2

    total = 0
    for where, message in items:
        found = findings(message)
        if found:
            total += len(found)
            print(render(found, where[:12] if len(where) == 40 else where))

    # Printed on BOTH paths: a reader who sees a refusal still needs to know the population,
    # and a reader who sees a pass needs the control's reading next to it.
    print(summarise(scanned=len(items), found=total))
    return 1 if total else 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
