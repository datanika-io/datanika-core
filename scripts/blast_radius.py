#!/usr/bin/env python3
"""Report what a promotion touches, and REFUSE when asked about a path that does not exist.

core#1276. The promotion pre-flight used to be a handful of ad-hoc
``git diff --name-only BASE..HEAD -- <path> | wc -l`` invocations pasted into a
promotion body. Three of the four paths were wrong -- this repository's layout is
mixed (``datanika/migrations/versions`` lives inside the package, while
``docker-compose.yml``, ``Dockerfile`` and ``deploy/server`` sit at the root) and
one prefix was used across all of it.

``git diff -- <path that does not exist>`` prints **nothing** and exits **0**.
That is byte-identical to "this path did not change". So three assertions in two
promotion bodies -- *0 deploy/server, compose unchanged, Dockerfile unchanged* --
were assertions over nothing. Re-measured with correct paths both batches were
genuinely clean, so nothing shipped wrong: they were right by luck, not by
measurement, and the next batch carries a 38-line ``docker-compose.yml`` change.

The fix is NOT a corrected list of prefixes. A corrected list rots the moment a
directory moves, and rots silently in the reassuring direction. The fix is that a
path resolving to nothing is **impossible to mistake for a clean diff**: every
watched path is asserted to be tracked at BASE or HEAD *before* any diff is run,
and an unresolvable one exits non-zero with the reason.

Usage:
    python scripts/blast_radius.py origin/master origin/dev
    python scripts/blast_radius.py origin/master origin/dev --json
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from dataclasses import dataclass, field

#: What a promotion must never silently carry. The comment on each entry is the
#: reason it is watched, not decoration -- an entry nobody can justify should be
#: removed rather than left to pad the table.
WATCHED: dict[str, str] = {
    # Blue/green runs the new container's migrations while the OLD code still
    # serves, so anything non-expand/contract breaks production live.
    "migrations": "datanika/migrations/versions",
    # Applied by install-server-scripts.sh on every deploy (core#747). A change
    # here rewrites files that cron runs, including backup-offsite.sh.
    "server scripts": "deploy/server",
    # A new service here needs a deploy step, or it runs forever with no path to
    # a config change -- core#616, which hid for six weeks.
    "compose": "docker-compose.yml",
    # Rebuilt on the box; a broken build-time assertion fails the deploy.
    "image": "Dockerfile",
    # Changes what CI and CD themselves do, including the gates being relied on.
    "workflows": ".github/workflows",
}


class PathNotTrackedError(Exception):
    """A watched path matches no tracked file at either revision."""


@dataclass
class Entry:
    label: str
    path: str
    tracked_at_base: int
    tracked_at_head: int
    changed: list[str] = field(default_factory=list)

    @property
    def count(self) -> int:
        return len(self.changed)


def _git(*args: str) -> str:
    r = subprocess.run(
        ["git", *args],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    if r.returncode != 0:
        raise RuntimeError(f"git {' '.join(args)} failed: {r.stderr.strip()}")
    return r.stdout


def tracked_count(ref: str, path: str) -> int:
    """How many tracked files `path` matches at `ref`.

    This is the whole point of the module: a zero here means the QUESTION is
    wrong, and must never be reported alongside a zero that means the ANSWER is
    "nothing changed".
    """
    out = _git("ls-tree", "-r", "--name-only", ref, "--", path)
    return len([line for line in out.splitlines() if line.strip()])


def changed_files(base: str, head: str, path: str) -> list[str]:
    out = _git("diff", "--name-only", f"{base}..{head}", "--", path)
    return [line for line in out.splitlines() if line.strip()]


def collect(base: str, head: str, watched: dict[str, str] | None = None) -> list[Entry]:
    """Resolve every watched path, refusing before diffing if one is unresolvable.

    Resolution is checked at BOTH revisions on purpose: a directory added in this
    very batch is absent at BASE and present at HEAD, and a directory deleted by
    it is the reverse. Requiring it at BASE only would refuse a legitimate
    addition; requiring it at HEAD only would go quiet exactly when a watched
    directory was removed, which is the more dangerous of the two.
    """
    watched = WATCHED if watched is None else watched
    entries: list[Entry] = []
    unresolvable: list[str] = []

    for label, path in watched.items():
        at_base = tracked_count(base, path)
        at_head = tracked_count(head, path)
        if at_base == 0 and at_head == 0:
            unresolvable.append(f"{label!r} -> {path!r} (0 tracked files at {base} or {head})")
            continue
        entries.append(
            Entry(
                label=label,
                path=path,
                tracked_at_base=at_base,
                tracked_at_head=at_head,
                changed=changed_files(base, head, path),
            )
        )

    if unresolvable:
        raise PathNotTrackedError(
            "These watched paths match NO tracked file, so a diff over them would "
            "print nothing and exit 0 -- indistinguishable from 'unchanged':\n  "
            + "\n  ".join(unresolvable)
            + "\n\nFix the path. Do NOT read the empty diff as a clean result."
        )
    return entries


def render(entries: list[Entry], base: str, head: str) -> str:
    width = max(len(e.label) for e in entries)
    lines = [f"blast radius {base}..{head}"]
    for e in entries:
        mark = "  " if e.count == 0 else "**"
        lines.append(
            f"{mark} {e.label:<{width}}  changed={e.count:<3} "
            f"[{e.path} -> {max(e.tracked_at_base, e.tracked_at_head)} tracked]"
        )
        for f in e.changed:
            lines.append(f"       {f}")
    total = sum(e.count for e in entries)
    lines.append(f"   {total} file(s) changed across {len(entries)} watched path(s), all resolved")
    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("base")
    ap.add_argument("head")
    ap.add_argument("--json", action="store_true", dest="as_json")
    args = ap.parse_args(argv)

    try:
        entries = collect(args.base, args.head)
    except PathNotTrackedError as exc:
        print(f"::error::blast radius REFUSED\n{exc}", file=sys.stderr)
        return 2
    except RuntimeError as exc:
        print(f"::error::{exc}", file=sys.stderr)
        return 2

    if args.as_json:
        print(
            json.dumps(
                [
                    {
                        "label": e.label,
                        "path": e.path,
                        "changed": e.changed,
                        "count": e.count,
                    }
                    for e in entries
                ],
                indent=2,
            )
        )
    else:
        print(render(entries, args.base, args.head))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
