#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# Is this run's commit still `dev`'s head?  (core#975, the residual.)
#
# Writes `is_head=true|false` to $GITHUB_OUTPUT (stdout when unset). The `staging`
# caller in ci.yml enters the `staging-deploy` group only on `true`.
#
# 🚨 WHY.  One group member per run closed the INTRA-run displacement core#975 was
# filed about.  It did not close INTER-run displacement under a burst: GitHub holds
# one running plus one PENDING entry per group, a newer waiter cancels the pending
# one, and "newer" means whichever run's tests finished later -- not whichever
# commit is newer.  Measured 2026-09-16: `0cd5b471` was pushed BEFORE `3a2d4147`,
# its `test` job finished 16 s AFTER, and it cancelled the head's pending staging
# entry.  The commit that became `dev`'s head got no staging verdict, and nothing
# anywhere was red.
#
# So a run whose commit is no longer the head does not enter the group.  The newer
# head's own run produces the reading that matters, and a skipped non-head commit
# reads `absent` in `scripts/verify_e2e_attribution.py` -- correct for a commit
# nobody can promote.
#
# 🚨 FAILS OPEN.  If the head cannot be read, the answer is `true`, which is exactly
# the behaviour before this check existed.  Answering `false` on a failed read would
# drop the head's verdict silently, the one outcome core#975 exists to prevent.
#
# The residual race points the safe way: a push landing after this check has read
# the head enters the group later still (its own tests take ~11 minutes), so it
# displaces THIS entry, which is by then no longer the head.
# ─────────────────────────────────────────────────────────────────────────────
set -uo pipefail

REPO="${REPO:?REPO is required, e.g. datanika-io/datanika-core}"
SHA="${SHA:?SHA is required: the commit this run is testing}"
BRANCH="${BRANCH:-dev}"
OUT="${GITHUB_OUTPUT:-/dev/stdout}"

head=$(gh api "repos/${REPO}/commits/${BRANCH}" --jq .sha 2>/dev/null)
rc=$?

if [ "${rc}" -ne 0 ] || [[ ! "${head}" =~ ^[0-9a-f]{40}$ ]]; then
  echo "::warning::could not read ${BRANCH}'s head (gh exit ${rc}); staging runs for ${SHA:0:8}, as it did before core#975's supersession check"
  echo "is_head=true" >> "${OUT}"
  exit 0
fi

if [ "${head}" = "${SHA}" ]; then
  echo "${SHA:0:8} is ${BRANCH}'s head: staging runs"
  echo "is_head=true" >> "${OUT}"
else
  echo "::notice::${SHA:0:8} is no longer ${BRANCH}'s head (${head:0:8} is): staging is skipped for this run, so it cannot displace the head's pending verdict (core#975)"
  echo "is_head=false" >> "${OUT}"
fi
