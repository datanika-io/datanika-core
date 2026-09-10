#!/usr/bin/env bash
# Run the suite and leave a verdict that CANNOT be misread as green.
#
# The sentinel is the whole point. The previous version truncated the verdict file to empty at
# the start of a 24-minute run, so an incomplete run left 0 bytes -- and `grep -c failed` on an
# empty file returns 0, which reads as "no failures". An absent measurement is not a passing
# one.
#
# So: write `pytest_exit_code=UNKNOWN` FIRST, overwrite only on completion. A killed, timed-out
# or still-running suite therefore leaves UNKNOWN, which `read_verdict.py` refuses on.
#
# Read it with:  python .scratch/engineering/read_verdict.py <verdict path>
#   0 green   1 red   2 refuse-to-say  -- and 2 is NOT a pass.
set -u
W=/d/Projects/Datanika/worktrees/datanika-core-engineering
cd "$W"
S=/d/Temp/claude/D--Projects-Datanika/9028dc94-3f88-492f-97ae-6d467c75edae/scratchpad
OUT="${1:-$S/suite.txt}"
VERDICT="${2:-$S/verdict.txt}"

{
  echo "pytest_exit_code=UNKNOWN"
  echo "failed_lines=UNKNOWN"
  echo "error_lines=UNKNOWN"
  echo "summary=run did not complete"
} > "$VERDICT"

./.venv/Scripts/python.exe -m pytest tests/ -q > "$OUT" 2>&1
rc=$?

{
  echo "pytest_exit_code=$rc"
  echo "failed_lines=$(grep -c '^FAILED' "$OUT" || true)"
  echo "error_lines=$(grep -c '^ERROR' "$OUT" || true)"
  echo "summary=$(tail -1 "$OUT")"
} > "$VERDICT"
cat "$VERDICT"
