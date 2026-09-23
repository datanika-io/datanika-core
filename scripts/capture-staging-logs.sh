#!/usr/bin/env bash
# Capture staging's APP and WORKER logs for the E2E test window (core#1528).
#
# A gating E2E failure preserves the browser's view — trace, screenshot, video — and **nothing
# the server said**. core#1296's own failure had already localised itself to the app tier
# ("This is the APP, not the worker.") and eight days later nothing survived that could say
# which app path it was: the only `docker logs` anywhere in staging.yml is deploy-staging's
# health wait, on a container that never became healthy, and staging is recreated on the next
# push to dev, so the window closes in minutes.
#
# 🚨 THIS SCRIPT MUST FAIL LOUDLY RATHER THAN UPLOAD NOTHING. An `if-no-files-found: ignore` on
# an empty capture is a step that always succeeds and never records anything — the same defect
# one level up from the one it is fixing. Every refusal below is a refusal to pretend.
#
# ⚠️ It runs ONLY when the gate failed or the flaky detector fired, so on a gating failure the
# job is already red and this adds no new colour. On a FLAKY run the job is otherwise green and
# this can turn it red — deliberately. A flaky detector that files a durable issue pointing at
# evidence that was never captured is exactly core#1296, and the classifier keys on
# `steps.gating.outcome`, so a capture failure is never claimed as a spec failure.
set -euo pipefail

SINCE="${SINCE:?the log window's opening timestamp, taken from the BOX's clock}"
SSH_KEY_PATH="${SSH_KEY_PATH:-$HOME/.ssh/id_rsa}"
OUT_DIR="${OUT_DIR:-e2e/staging-logs}"
PROJECT="${STAGING_PROJECT:-datanika-staging}"
APP_CMD="${APP_CMD:-/opt/datanika-staging/active-app.sh}"

mkdir -p "$OUT_DIR"

# ⚠️ The transport is injectable ON PURPOSE, and it is not a testing convenience bolted on.
# `e2e-staging` runs only on a push to `dev`, so this script can never be exercised from a pull
# request — the one place a change to it would be reviewed. With `CAPTURE_LOCAL=1` the same code
# path runs against a real local container, which is how the refusals below were shown to fire.
# A recipe belongs where it can be tested; a recipe nobody can run is prose.
if [ "${CAPTURE_LOCAL:-0}" = "1" ]; then
  remote() { sh -c "$*"; }
else
  SSH_HOST="${SSH_HOST:?}"
  remote() {
    ssh -q -i "$SSH_KEY_PATH" -o StrictHostKeyChecking=accept-new "root@$SSH_HOST" "$@"
  }
fi

# ── Name the containers the way the DEPLOY does ───────────────────────────────────────────
# Never a hardcoded colour. `active-app.sh` is what deploy-staging's own health wait and the
# seed command call, so this follows the swap instead of guessing at it — core#622 is the case
# where a `name=~` regex that omitted `(-b)?` watched nothing for as long as the other colour
# served, for five weeks, with everything green.
APP=$(remote "$APP_CMD" | tr -d '\r')
[ -n "$APP" ] || { echo "REFUSE: $APP_CMD named no container"; exit 1; }

# The worker has no blue/green script because it is not blue/green. Derive it from the compose
# labels rather than from a container name, and require EXACTLY ONE: two would mean a stale
# container is also consuming this broker, and zero would mean the capture is about to be
# silently empty for the tier the assertion sends you to.
WORKER=$(remote "docker ps -q --filter 'label=com.docker.compose.project=$PROJECT' --filter 'label=com.docker.compose.service=celery'" | tr -d '\r')
COUNT=$(printf '%s\n' "$WORKER" | grep -c . || true)
if [ "$COUNT" != "1" ]; then
  echo "REFUSE: expected exactly one $PROJECT celery container, found $COUNT:"
  printf '  %s\n' $WORKER
  exit 1
fi

echo "app    container: $APP"
echo "worker container: $WORKER"
echo "window opens at : $SINCE (box clock)"

# ── Bounded. Never the whole container life ───────────────────────────────────────────────
# An unbounded dump of a container that has served a full dev day is not evidence, it is a
# haystack. `-t` is not decoration: it is what makes the window assertion below possible.
capture() {
  local cid="$1" dest="$2"
  remote "docker logs -t --since '$SINCE' '$cid' 2>&1" > "$dest"
}

capture "$APP" "$OUT_DIR/app.log"
capture "$WORKER" "$OUT_DIR/worker.log"

# ── Anti-vacuity: the two ways this step can succeed having preserved nothing ──────────────
fail=0
for f in "$OUT_DIR/app.log" "$OUT_DIR/worker.log"; do
  if [ ! -s "$f" ]; then
    echo "REFUSE: $f is empty. The E2E suite drove this stack for minutes; a silent container"
    echo "        is a finding about WHICH container was captured, not an absence of news."
    fail=1
    continue
  fi

  FIRST=$(head -1 "$f" | cut -d' ' -f1)
  # ⚠️ Compare the first 19 characters only. `docker logs -t` emits
  # `2026-09-23T22:00:00.123456789Z` while the marker is `2026-09-23T22:00:00Z`, and a plain
  # string compare puts `...00.123456789Z` BEFORE `...00Z` — '.' sorts below 'Z' — so a line
  # logged in the marker's own second would read as predating the window and this assertion
  # would fail on a correct capture.
  if [ "${FIRST:0:19}" \< "${SINCE:0:19}" ]; then
    echo "REFUSE: $f begins at $FIRST, before the window opened at $SINCE."
    echo "        --since did not bound this capture, so it is the container's whole life."
    fail=1
  fi
  echo "ok: $f  $(wc -l < "$f") lines, first entry $FIRST"
done

[ "$fail" = "0" ] || exit 1
echo "captured the test window for both tiers"
