#!/usr/bin/env bash
# Read one blob out of git by `<rev>:<path>`, without that string ever reaching argv.
#
# core#1284. Two separate defects meet in `git show <rev>:<path>` on Git Bash:
#
#   1. MSYS argument conversion mangles the spec when the path is dot-prefixed:
#        git show origin/dev:.github/workflows/ci.yml
#        fatal: ambiguous argument 'origin\dev;.github\workflows\ci.yml'
#      The usual remedy is MSYS_NO_PATHCONV=1 -- but that same variable BREAKS
#      every POSIX path handed to a Windows binary (gh.exe, python.exe, git -C
#      <posix path>), so a script that sets it once at the top fixes one arm and
#      breaks the other. Measured: it bit six times in a single session, against
#      twenty existing documentation entries. A per-command remedy loses to a
#      habit, so this removes the need for the variable instead of restating it.
#
#   2. `git show` on a path that does not exist prints NOTHING and exits 0 for
#      several shapes, which is byte-identical to "the file is empty". That is
#      the same silent-empty class as core#1276.
#
# Both are avoided by asking git for the object id first: the spec travels on
# STDIN (never an argument, so nothing can rewrite it), `--batch-check` reports
# `missing` as a literal word rather than as emptiness, and the id that comes
# back is plain hex -- which no argument conversion can touch.
#
# Usage:  scripts/git-read-blob.sh 'origin/dev:.github/workflows/ci.yml'
# Exits:  0 blob written to stdout · 3 spec resolves to no object · 4 not a blob
set -euo pipefail

SPEC="${1:-}"
if [ -z "$SPEC" ]; then
  echo "usage: $0 '<rev>:<path>'" >&2
  exit 2
fi

# STDIN, deliberately. The whole point is that SPEC is never an argv entry.
CHECK="$(printf '%s\n' "$SPEC" | git cat-file --batch-check 2>/dev/null || true)"

if [ -z "$CHECK" ]; then
  echo "::error::git could not be asked about '$SPEC' (empty batch-check reply)." >&2
  echo "         This is NOT 'the file is empty' -- it is no answer at all." >&2
  exit 3
fi

case "$CHECK" in
  *" missing"|*" ambiguous")
    echo "::error::'$SPEC' resolves to no object in this repository ($CHECK)." >&2
    echo "         Reported explicitly rather than as empty output, because an" >&2
    echo "         empty read is indistinguishable from an empty file." >&2
    exit 3
    ;;
esac

OID="$(printf '%s' "$CHECK" | awk '{print $1}')"
TYPE="$(printf '%s' "$CHECK" | awk '{print $2}')"

if [ "$TYPE" != "blob" ]; then
  echo "::error::'$SPEC' is a $TYPE, not a blob." >&2
  exit 4
fi

# OID is plain hex: no colon, no dot, no slash -- immune to argument conversion
# in both directions, which is what makes this safe with or without the variable.
git cat-file -p "$OID"
