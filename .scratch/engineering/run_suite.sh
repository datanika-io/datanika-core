#!/usr/bin/env bash
# Run the suite and leave a verdict that CANNOT be misread as green.
#
# TWO defects are designed against here, both of which produced a WRONG answer in this project:
#
# 1. THE SENTINEL. An earlier version truncated the verdict file to empty at the start of a
#    24-minute run, so an incomplete run left 0 bytes -- and `grep -c failed` on an empty file
#    returns 0, which reads as "no failures". An absent measurement is not a passing one.
#    So: write `pytest_exit_code=UNKNOWN` FIRST, overwrite only on completion.
#
# 2. THE SHARED PATH. The version after that wrote to ONE fixed verdict path. Two concurrent
#    runs then shared it: run B's startup truncation landed inside run A's completed verdict,
#    and the result read `exit_code=1, failed_lines=0, summary=` -- a FALSE RED, which is the
#    same defect as a false green wearing the other colour.
#    So: every invocation gets its OWN directory, and there is NO "latest" pointer. A shared
#    mutable pointer is precisely the bug being fixed; re-adding one for convenience re-adds it.
#    The verdict path is printed on the FIRST line of stdout -- pass that exact path on.
#
# Read it with:  python .scratch/engineering/read_verdict.py <verdict path>
#   0 green   1 red   2 refuse-to-say  -- and 2 is NOT a pass.
set -u
W=/d/Projects/Datanika/worktrees/datanika-core-engineering
cd "$W"
S=/d/Temp/claude/D--Projects-Datanika/9028dc94-3f88-492f-97ae-6d467c75edae/scratchpad

RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-$$"
RUN_DIR="$S/runs/$RUN_ID"
mkdir -p "$RUN_DIR"
OUT="$RUN_DIR/suite.txt"
VERDICT="$RUN_DIR/verdict.txt"
echo "verdict_path=$VERDICT"

{
  echo "pytest_exit_code=UNKNOWN"
  echo "failed_lines=UNKNOWN"
  echo "error_lines=UNKNOWN"
  echo "summary=run did not complete"
} > "$VERDICT"

# NO_PROXY is an ENVIRONMENT CORRECTION, not a test silencer, and the distinction matters.
# This workstation has a system proxy in the Windows registry (127.0.0.1:10809). httpx reads it
# via urllib's getproxies() and routes even `http://127.0.0.1:9` through it; the proxy cannot
# connect and answers 503, so a test asserting httpx.ConnectError ("nothing listens on :9")
# sees an HTTPStatusError instead. curl does NOT read the registry, which is why a raw probe
# says "connection refused" while Python says 503. Linux CI has no such proxy.
# Scoped to loopback ONLY -- deliberately not `*` -- so a test that genuinely reaches an
# external host still behaves exactly as it would without this line.
export NO_PROXY="127.0.0.1,localhost,::1"
export no_proxy="$NO_PROXY"

# -rs reports SKIP reasons: the skip count moved 29 -> 151 between runs with no explanation,
# and a count with no reason attached cannot be told from tests silently ceasing to run.
./.venv/Scripts/python.exe -m pytest tests/ -q -rs > "$OUT" 2>&1
rc=$?

{
  echo "pytest_exit_code=$rc"
  echo "failed_lines=$(grep -c '^FAILED' "$OUT" || true)"
  echo "error_lines=$(grep -c '^ERROR' "$OUT" || true)"
  echo "summary=$(tail -1 "$OUT")"
} > "$VERDICT"
cat "$VERDICT"
