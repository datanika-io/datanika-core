#!/usr/bin/env bash
# Fail unless this push's own pre-push run id is a whole line of the captured
# output (core#1293).
#
# A push that exits 0 and prints nothing from the hook reads as a clean push.
# scripts/hooks/pre-push prints `pre-push: ran id=<id>` before it does anything
# else. The caller exports the id, runs the push, and sends the combined
# output here:
#
#   export DATANIKA_PREPUSH_RUN_ID=<token>
#   out=$(git push ... 2>&1) || exit
#   printf '%s\n' "$out" | scripts/hooks/assert-pre-push-ran.sh
#
# Exit 0 only for an exact line `pre-push: ran id=$DATANIKA_PREPUSH_RUN_ID`.
# A different id is a different run. A sentence that merely mentions the line
# is not the line.
set -eu

id="${DATANIKA_PREPUSH_RUN_ID:-}"
if [ -z "$id" ]; then
  echo "assert-pre-push-ran: DATANIKA_PREPUSH_RUN_ID is empty — nothing to assert" >&2
  exit 2
fi
case "$id" in
  *[!A-Za-z0-9._-]*)
    echo "assert-pre-push-ran: run id must be one token (letters, digits, . _ -)" >&2
    exit 2
    ;;
esac

needle="pre-push: ran id=${id}"
# Drop CR so a Windows capture of the hook's LF line still matches whole-line.
if tr -d '\r' | grep -F -x -q -- "$needle"; then
  exit 0
fi
echo "assert-pre-push-ran: no line '${needle}'. A push with no hook output is not a success." >&2
exit 1
