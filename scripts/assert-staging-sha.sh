#!/bin/bash
# Refuse to verify a staging stack that is not running THIS run's commit (core#876).
#
# Runs ON THE RUNNER, as the first real step of `smoke-staging`, `e2e-staging` and
# `e2e-sso`. Reads the stamp written by `staging-deploy-stamp.sh write` at the end of
# `deploy-staging` and compares it with the commit the current run is about.
#
#   EXPECTED_SHA=$GITHUB_SHA SSH_HOST=… SSH_KEY_PATH=~/.ssh/id_rsa \
#     bash scripts/assert-staging-sha.sh
#
# The defect it closes
# --------------------
# `staging-deploy`'s concurrency group serialises *access* to staging and never pins
# *identity*. Measured on `dev`:
#
#   1da0c21 deploy-staging  22:08:50 -> 22:12:44
#   87da585 deploy-staging  22:12:46 -> 22:16:48   (correctly queued behind)
#   1da0c21 e2e-staging     22:16:50 -> 22:24:05   <- ran against 87da585's build
#
# Nothing was concurrent. The lock did exactly what it promises. The verdict was still
# filed against the wrong commit — and 87da585's own E2E was cancelled, so the commit
# actually under test got no reading at all.
#
# 🚨 IT FAILS, IT NEVER SKIPS. A skipped verifier makes a run green having verified
# nothing, and a skip and a pass are the same colour. Every "cannot tell" answer below is
# an exit 1: no stamp, an unreadable stamp, an empty EXPECTED_SHA, an unreachable box.
# If this turns out to be noisy the fix is to re-deploy that SHA, never to soften the
# assertion — and ⚠️ NOT to re-deploy from inside the verifier, which would put a mutation
# inside the window the verifiers hold, i.e. core#753 reintroduced.
#
# Testing seam: set STAGING_STAMP_FILE to a file holding `report` output and no SSH is
# attempted. scripts/test-staging-sha-stamp.sh drives every branch through it.

set -u

EXPECTED_SHA=${EXPECTED_SHA:-}
STAMP_SCRIPT=${STAMP_SCRIPT:-scripts/staging-deploy-stamp.sh}

fail() {
  echo "::error::staging SHA assertion FAILED — $1"
  echo
  echo "  expected (this run) : ${EXPECTED_SHA:-<empty>}"
  echo "  stamped on staging  : ${stamp_sha:-<none>}"
  echo "  stamped run         : ${stamp_run_id:-<none>} attempt ${stamp_run_attempt:-<none>} at ${stamp_deployed_at:-<none>}"
  echo "  container stamped   : ${stamp_container:-<none>}"
  echo "  container live now  : ${live_container:-<none>}"
  echo
  echo "  A verdict from this job would describe a build this run did not deploy (core#876)."
  echo "  Re-run this commit's deploy so staging is running it, then re-run the verifier."
  echo "  Do NOT re-deploy from inside a verifier: that is core#753 reintroduced."
  exit 1
}

# ── 1. arm the assertion ────────────────────────────────────────────────────────────────
# An empty EXPECTED_SHA compared against an empty stamp is "equal". That is the vacuous
# shape this whole issue is about, so it is rejected before anything is read.
stamp_sha=""; stamp_run_id=""; stamp_run_attempt=""; stamp_deployed_at=""
stamp_container=""; stamp_image=""; live_container=""; live_image=""

case "$EXPECTED_SHA" in
  *[!0-9a-fA-F]* | "") fail "EXPECTED_SHA is empty or not a hex sha — the assertion is unarmed" ;;
esac
[ ${#EXPECTED_SHA} -ge 7 ] || fail "EXPECTED_SHA '$EXPECTED_SHA' is too short to identify a commit"

# ── 2. read the stamp ───────────────────────────────────────────────────────────────────
if [ -n "${STAGING_STAMP_FILE:-}" ]; then
  report=$(cat "$STAGING_STAMP_FILE" 2>/dev/null) || report=""
else
  : "${SSH_HOST:?SSH_HOST is required}"
  : "${SSH_KEY_PATH:=$HOME/.ssh/id_rsa}"
  [ -r "$STAMP_SCRIPT" ] || fail "$STAMP_SCRIPT is not readable on the runner"
  # `bash -s -- report` takes the script on stdin; the script itself is careful to give
  # every stdin-inheriting command a </dev/null (WORKFLOW_RULES §13 trap 2b).
  # stderr is deliberately NOT swallowed: when this fails it is usually ssh, and the
  # message saying why is the only thing that separates "the box is unreachable" from
  # "staging has no stamp" — which need different responses and would otherwise look alike.
  report=$(ssh -i "$SSH_KEY_PATH" -o BatchMode=yes "root@$SSH_HOST" \
             "bash -s -- report" < "$STAMP_SCRIPT") || report=""
fi

[ -n "$report" ] || fail "could not read the deploy stamp from staging at all (ssh or box failure)"

value_of() { printf '%s\n' "$report" | sed -n "s/^$1=//p" | tail -1; }

case "$report" in
  *stamp_missing=1*) fail "staging has NO deploy stamp — nothing recorded which commit it runs" ;;
esac

stamp_sha=$(value_of sha)
stamp_run_id=$(value_of run_id)
stamp_run_attempt=$(value_of run_attempt)
stamp_deployed_at=$(value_of deployed_at)
stamp_container=$(value_of app_container)
stamp_image=$(value_of app_image)
live_container=$(value_of live_container)
live_image=$(value_of live_image)

# ── 3. the verdict ──────────────────────────────────────────────────────────────────────
[ -n "$stamp_sha" ] || fail "the stamp carries no sha field — it cannot identify anything"

if [ "$stamp_sha" != "$EXPECTED_SHA" ]; then
  fail "staging is running a DIFFERENT COMMIT than this run deployed"
fi

# The SHA agreeing is not the same as the stack being the one that was stamped. A hand
# restart, or a deploy that raced e2e-sso (which holds a different lock, core#765), leaves
# the SHA correct and the containers replaced.
[ -n "$live_container" ] || fail "no staging app container is running — nothing to verify against"
[ "$live_container" = "$stamp_container" ] || fail "the staging app container was replaced after the stamp was written"
if [ -n "$stamp_image" ] && [ "$live_image" != "$stamp_image" ]; then
  fail "the staging app image changed after the stamp was written"
fi

echo "staging is running ${EXPECTED_SHA:0:8} — stamped by run ${stamp_run_id:-?} attempt ${stamp_run_attempt:-?} at ${stamp_deployed_at:-?}"
echo "container ${live_container:0:12} matches the stamp; this job's verdict belongs to this commit."
