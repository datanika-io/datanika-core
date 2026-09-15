#!/usr/bin/env bash
# The last thing between `dev` and production, and the only one that refuses on silence.
#
# core#1287. `verify_e2e_attribution.py` has crashed TWICE on a change to
# `e2e_tier_streak.py`'s contract (core#1205, core#1285). Both times it was found
# by a human mid-promotion, and both times the crash was survivable only because
# somebody happened to be reading the output.
#
# 🚨 An unhandled Python exception ALSO exits 1. So to anything reading the exit
# code — including a person under time pressure — "the gate refused" and "the gate
# broke" are the same signal. Measured, with a crash injected deliberately:
#
#     refusal : exit 1, verdict file written saying REFUSED
#     crash   : exit 1, NO verdict file
#     network : exit 1, NO verdict file
#
# The predicted failure mode this exists to stop is not exotic: *run it, see a
# traceback, decide it is tooling noise, promote anyway.* A human reading a stack
# trace is not a gate.
#
# So this consumes a POSITIVE ARTIFACT rather than the absence of a complaint, and
# distinguishes three outcomes the exit code alone cannot:
#
#     0  every gate produced a verdict and every verdict says OK
#     3  a gate produced NO VERDICT      -- it did not run, or it broke
#     4  a gate produced a verdict and that verdict says no
#     5  a verdict exists but describes a DIFFERENT commit
#
# Exit 5 matters as much as exit 3: a verdict file left from an earlier run would
# otherwise vouch for whatever head is promoted next. That is core#876's lesson
# ("a green that belongs to another commit is not evidence about this one") applied
# to the gate's own artifact instead of to the jobs it reads.
#
# Usage:  scripts/promotion_gate.sh <base-ref> <head-ref>
#         scripts/promotion_gate.sh origin/master origin/dev
set -uo pipefail

BASE="${1:-origin/master}"
HEAD_REF="${2:-origin/dev}"
PY="${PYTHON:-python}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

SHA="$(git rev-parse "$HEAD_REF" 2>/dev/null || true)"
if [ -z "$SHA" ]; then
  echo "::error::cannot resolve $HEAD_REF -- refusing rather than guessing a head." >&2
  exit 2
fi

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
VERDICT="$WORK/attribution.verdict"

echo "promotion gate for ${SHA:0:8} (${BASE}..${HEAD_REF})"
echo

# ── 1. blast radius ─────────────────────────────────────────────────────────────
# Fails closed on a path that matches no tracked file (core#1276): an empty diff
# over a mistyped path is byte-identical to "nothing changed".
echo "-- blast radius"
if ! "$PY" "$HERE/blast_radius.py" "$BASE" "$HEAD_REF"; then
  echo "::error::blast radius did not complete. No promotion." >&2
  exit 3
fi
echo

# ── 2. staging attribution, via its verdict file ────────────────────────────────
echo "-- staging attribution"
"$PY" "$HERE/verify_e2e_attribution.py" --sha "$SHA" --verdict-file "$VERDICT"
ATTR_RC=$?

if [ ! -f "$VERDICT" ]; then
  echo >&2
  echo "::error::NO VERDICT was produced by the attribution gate (exit $ATTR_RC)." >&2
  echo "         This is NOT 'the checks failed'. The gate did not reach a verdict --" >&2
  echo "         it crashed, or could not read GitHub. Its exit code is 1 either way," >&2
  echo "         which is exactly why this script looks for the artifact instead." >&2
  echo "         Read the output above. Do not treat a traceback as tooling noise." >&2
  exit 3
fi

TOKEN="$(cat "$VERDICT")"
echo "   verdict: $TOKEN"

TOKEN_SHA="$(printf '%s' "$TOKEN" | sed -n 's/.*sha=\([0-9a-f]*\).*/\1/p')"
if [ "$TOKEN_SHA" != "$SHA" ]; then
  echo "::error::the verdict describes ${TOKEN_SHA:0:8}, not ${SHA:0:8}." >&2
  echo "         A verdict about another commit is not evidence about this one." >&2
  exit 5
fi

case "$TOKEN" in
  *"PROMOTION-GATE OK "*) ;;
  *)
    echo "::error::the attribution gate reached a verdict and it is NOT ok." >&2
    echo "         This is a real refusal with a real reading behind it." >&2
    exit 4
    ;;
esac

echo
echo "promotion gate PASSED for ${SHA:0:8} -- every gate produced a verdict, and every"
echo "verdict describes this commit and says ok."
