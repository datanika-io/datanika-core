#!/bin/bash
# Record / report WHICH COMMIT the staging stack is currently running (core#876).
#
# Runs ON THE BOX. Both modes are sent over stdin by ci.yml:
#
#   ssh root@$HOST "bash -s -- write $SHA $RUN_ID $RUN_ATTEMPT" < scripts/staging-deploy-stamp.sh
#   ssh root@$HOST "bash -s -- report"                          < scripts/staging-deploy-stamp.sh
#
# Why this exists
# ---------------
# The `staging-deploy` concurrency group (core#753) serialises *access* to staging. It does
# not pin *identity*. Two runs can be perfectly serialised and still test the wrong
# artifacts: run A's `e2e-staging` starts seconds after run B's `deploy-staging` finished,
# so it exercises B's build and files the verdict against A. Observed three times in two
# days, and the attribution comes out crossed — B's own E2E is typically cancelled, so the
# commit that was actually under test gets no verdict at all.
#
# ⚠️ THE SCRIPT ARRIVES ON STDIN, so every command in it that would otherwise inherit stdin
# gets `</dev/null` (WORKFLOW_RULES §13 trap 2b): a single stdin-consuming call eats the
# remainder of the script, ssh still exits 0, and the truncation is indistinguishable from
# the script having finished normally.

set -u

STAMP=${STAGING_STAMP_PATH:-/opt/datanika-staging/deploy-stamp}
ACTIVE_APP=${STAGING_ACTIVE_APP:-/opt/datanika-staging/active-app.sh}

# Live identity of whatever is serving staging right now. Never fails the caller: an empty
# value is a *finding* the asserter must reject, not an error to swallow here.
live_ids() {
  local name id image
  name=$("$ACTIVE_APP" 2>/dev/null </dev/null) || name=""
  if [ -n "$name" ]; then
    id=$(docker inspect --format '{{.Id}}' "$name" 2>/dev/null </dev/null) || id=""
    image=$(docker inspect --format '{{.Image}}' "$name" 2>/dev/null </dev/null) || image=""
  fi
  printf 'live_container=%s\nlive_image=%s\n' "${id:-}" "${image:-}"
}

case "${1:-}" in
  write)
    SHA=${2:-}
    RUN_ID=${3:-}
    RUN_ATTEMPT=${4:-}
    # A stamp with no SHA is worse than no stamp: it is the shape that makes an empty
    # comparison look like agreement. Refuse to write one.
    case "$SHA" in
      [0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f]*) ;;
      *) echo "staging-deploy-stamp: refusing to write a stamp with no usable sha ('$SHA')" >&2
         exit 1 ;;
    esac

    ids=$(live_ids)
    container=$(printf '%s\n' "$ids" | sed -n 's/^live_container=//p')
    image=$(printf '%s\n' "$ids" | sed -n 's/^live_image=//p')
    if [ -z "$container" ]; then
      echo "staging-deploy-stamp: no running staging app to stamp (active-app.sh gave nothing)" >&2
      exit 1
    fi

    # Write via a temp file in the same directory + `mv`, so `report` never reads a
    # half-written stamp. The deploy holds the lock, but e2e-sso does not (core#765).
    tmp=$(mktemp "${STAMP}.XXXXXX") || exit 1
    {
      printf 'sha=%s\n' "$SHA"
      printf 'run_id=%s\n' "$RUN_ID"
      printf 'run_attempt=%s\n' "$RUN_ATTEMPT"
      printf 'deployed_at=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
      printf 'app_container=%s\n' "$container"
      printf 'app_image=%s\n' "$image"
    } > "$tmp" || { rm -f "$tmp"; exit 1; }
    mv -f "$tmp" "$STAMP" || { rm -f "$tmp"; exit 1; }
    echo "staging-deploy-stamp: wrote $STAMP for $SHA (container ${container:0:12})"
    ;;

  report)
    # Deliberately exits 0 whatever it finds. The asserter on the runner owns the verdict;
    # a reporter that fails on a missing stamp would make "no stamp" and "ssh broke" the
    # same signal, and they need different messages.
    if [ -s "$STAMP" ]; then
      cat "$STAMP"
    else
      echo 'stamp_missing=1'
    fi
    live_ids
    ;;

  *)
    echo "usage: staging-deploy-stamp.sh write <sha> <run_id> <run_attempt> | report" >&2
    exit 2
    ;;
esac
