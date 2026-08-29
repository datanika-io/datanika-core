#!/usr/bin/env bash
#
# Blue/green swap for the Datanika app container. (A3, core#425)
#
# The problem it solves
# ---------------------
# `docker compose up -d app` RECREATES the container: the old one stops, the new one
# boots, and every request 502s in between. Measured on prod 2026-07-21: a 60s window,
# and the frontend is down longer than the backend because it boots slower.
#
# How this avoids it
# ------------------
# Only ONE container serves at a time (so no split Reflex session state), but the NEW one
# is fully healthy before Apache is repointed:
#
#   start inactive colour -> wait for its healthcheck -> assert the backend routes on its
#   OWN port -> rewrite the 2-line Define include -> apachectl configtest -> apachectl
#   graceful -> verify through the proxy -> stop the old colour
#
# `graceful` finishes in-flight requests rather than cutting them. Any failure before the
# swap leaves Apache pointing at the OLD container, which is still serving.
#
# ⚠️ Requires expand/contract migrations. The new container runs `alembic upgrade head`
# while the old one is still serving, so the old code meets the new schema. See
# plans/infra/SPEC_EXPAND_CONTRACT_MIGRATIONS.md — a rename/drop here is an outage.
#
# Failure handling — why the recovery hangs off EXIT and not ERR (core#603)
# ------------------------------------------------------------------------
# This script used to arm `trap 'rollback' ERR` and then signal every failure as
#
#     [ "$CODE" = 200 ] || { log "FATAL: ..."; exit 1; }
#
# **bash does not run an ERR trap for an explicit `exit`.** Nor for a command that fails
# on the left of `||`. Nor for a signal. So on every path that actually mattered the trap
# was armed and unreachable — verified on bash 5.2:
#
#     exit 1 ................................ ERR trap: NOT fired   EXIT trap: fired
#     [ 200 = 401 ] || { log; exit 1; } ..... ERR trap: NOT fired   EXIT trap: fired
#     SIGTERM ............................... ERR trap: NOT fired   EXIT trap: fired (with a TERM trap)
#
# On 2026-08-29 the post-swap `/mcp` assertion failed against an image that could not
# import `datanika_mcp` (core#602). The rollback written to prevent exactly that never
# ran, and production was left on the NEW colour with `/mcp` down. It was undone by hand.
#
# So: recovery is hung off `EXIT`, which covers `set -e`, explicit `exit`, and signals;
# HUP/INT/TERM are trapped so a cancelled CD job (which SIGHUPs this script when the SSH
# channel closes) unwinds instead of abandoning a half-finished swap; and `fatal()` is the
# only permitted way to abort. `tests/test_deploy/test_bluegreen_rollback.py` runs this
# file against a fake tree with injected failures and asserts the rollback actually fires
# — reading the script is how the last one passed review.
#
# Usage:  deploy-bluegreen.sh [--env staging|prod] [--dry-run]

set -euo pipefail

# State the exit trap reads. Initialised before the trap is installed so that an early
# abort (bad argument, unreadable include) cannot trip `set -u` inside the handler.
BACKUP=""
ROLLBACK_ARMED=0
ROLLED_BACK=0

ENVIRONMENT="staging"
DRY_RUN=0
while [ $# -gt 0 ]; do
  case "$1" in
    --env) ENVIRONMENT="$2"; shift 2 ;;
    --dry-run) DRY_RUN=1; shift ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

# Test-harness hook. Empty in production, so every path below is absolute exactly as it
# has always been. Set only by tests/test_deploy/test_bluegreen_rollback.py, which runs
# this file byte-identically against a fake tree — a rewritten copy would prove the copy.
ROOT="${BLUEGREEN_TEST_ROOT:-}"

case "$ENVIRONMENT" in
  staging)
    PROJECT=datanika-staging
    COMPOSE_DIR="${ROOT}/opt/datanika-staging"
    INCLUDE="${ROOT}/etc/apache2/conf-enabled/datanika-staging-active.conf"
    VAR_BE=DATANIKA_STG_BE
    VAR_FE=DATANIKA_STG_FE
    HOST_HEADER=staging-app.datanika.io
    # colour -> "service container be_port fe_port"
    BLUE="app  datanika-staging-app    8100 3100"
    GREEN="app_b datanika-staging-app-b 8110 3110"
    ;;
  prod)
    PROJECT=datanika
    COMPOSE_DIR="${ROOT}/opt/datanika/datanika"
    INCLUDE="${ROOT}/etc/apache2/conf-enabled/datanika-prod-active.conf"
    VAR_BE=DATANIKA_BE
    VAR_FE=DATANIKA_FE
    HOST_HEADER=app.datanika.io
    BLUE="app  datanika-app    8000 3000"
    GREEN="app_b datanika-app-b 8010 3010"
    ;;
  *) echo "unknown env: $ENVIRONMENT" >&2; exit 2 ;;
esac

log() { printf '%s %s\n' "$(date -u +%H:%M:%S)" "$*"; }

# One MCP initialize body, used both pre-repoint (against the target backend) and
# post-swap (through Cloudflare). Defined once so the two probes cannot drift apart.
MCP_BODY='{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"swap","version":"1"}}}'

field() { echo "$1" | awk -v n="$2" '{print $n}'; }

# The ONLY permitted way to abort. Never write a bare `exit 1`: see the header — that is
# the defect core#603 fixed, and the test asserts this file contains exactly one of them.
fatal() { log "FATAL: $*"; exit 1; }

# --- rollback -------------------------------------------------------------------------
rollback() {
  [ "$ROLLED_BACK" = 1 ] && return 0
  ROLLED_BACK=1
  log "ROLLBACK: returning ${ENVIRONMENT} to ${ACTIVE_NAME} (${A_CTR})"

  # Apache routing first — restore the path back to the live colour before retiring
  # anything. Only touch Apache if we actually changed the file: this box also serves a
  # webdav co-tenant and the founder's VPN path, so a needless reload is not free.
  if [ -s "$BACKUP" ] && ! cmp -s "$BACKUP" "$INCLUDE"; then
    cp "$BACKUP" "$INCLUDE"
    log "  restored ${INCLUDE} -> ${ACTIVE_NAME}"
    if apachectl configtest >/dev/null 2>&1; then
      if apachectl graceful; then log "  apache gracefully reloaded"; else log "  !! apachectl graceful FAILED"; fi
    else
      log "  !! the RESTORED config fails configtest — not reloading. Apache is still"
      log "  !! running the pre-swap config from memory, but the next reload breaks it."
    fi
  else
    log "  ${INCLUDE} unchanged — no Apache reload needed"
  fi

  # Then retire the colour we were deploying, so its ports are free for the next attempt.
  if [ "$(docker inspect -f '{{.State.Running}}' "$T_CTR" 2>/dev/null || echo false)" = "true" ]; then
    if (cd "$COMPOSE_DIR" && set -a && . ./.env.docker && set +a \
          && docker compose -p "$PROJECT" --profile bluegreen stop "$T_SVC" >/dev/null 2>&1) \
       || docker stop "$T_CTR" >/dev/null 2>&1; then
      log "  stopped ${T_CTR}"
    else
      log "  !! could not stop ${T_CTR} — it is still up on :${T_BE} / :${T_FE}"
    fi
  else
    log "  ${T_CTR} is not running — nothing to stop"
  fi

  # Report what is TRUE, not what was intended. The old line said "rolled back; X still
  # serving" without ever asking, which is the same defect as a green deploy check that
  # asserts the right bytes arrived rather than that the thing works.
  local code
  code=$(curl -sk -o /dev/null -w '%{http_code}' --max-time 10 \
         -H "Host: ${HOST_HEADER}" "https://127.0.0.1/healthz" || echo 000)
  if [ "$code" = 200 ]; then
    log "ROLLBACK COMPLETE: ${A_CTR} is serving (/healthz -> 200)"
  else
    log "!! ROLLBACK INCOMPLETE: /healthz through the proxy returned ${code}"
    log "!! ${ENVIRONMENT} MAY BE DOWN. Check ${A_CTR} and ${INCLUDE} now."
  fi
}

on_exit() {
  local rc=$1
  # Disarm before doing anything: rollback must never re-enter through its own failures,
  # and `set -e` inside a trap handler would abandon the recovery halfway.
  trap - EXIT HUP INT TERM
  set +e
  if [ "$rc" -ne 0 ] && [ "$ROLLBACK_ARMED" = 1 ]; then
    rollback
  fi
  [ -n "$BACKUP" ] && rm -f "$BACKUP"
  exit "$rc"
}

# EXIT covers `set -e`, `fatal`, and the signal traps below in one place. There is
# deliberately no ERR trap: it fires on a strict subset of these and its presence is what
# made an unreachable rollback look like a working one for five weeks.
trap 'on_exit $?' EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

# --- which colour is live right now? --------------------------------------------------
CUR_BE=$(sed -nE "s/^Define ${VAR_BE} ([0-9]+).*/\1/p" "$INCLUDE")
if [ -z "$CUR_BE" ]; then
  fatal "cannot read ${VAR_BE} from ${INCLUDE}"
fi

if [ "$CUR_BE" = "$(field "$BLUE" 3)" ]; then
  ACTIVE="$BLUE"; TARGET="$GREEN"; ACTIVE_NAME=blue; TARGET_NAME=green
else
  ACTIVE="$GREEN"; TARGET="$BLUE"; ACTIVE_NAME=green; TARGET_NAME=blue
fi

A_SVC=$(field "$ACTIVE" 1); A_CTR=$(field "$ACTIVE" 2)
T_SVC=$(field "$TARGET" 1); T_CTR=$(field "$TARGET" 2)
T_BE=$(field "$TARGET" 3);  T_FE=$(field "$TARGET" 4)

log "active=${ACTIVE_NAME} (${A_CTR}, be ${CUR_BE})  ->  target=${TARGET_NAME} (${T_CTR}, be ${T_BE})"
if [ "$DRY_RUN" = 1 ]; then log "dry run — stopping here"; exit 0; fi

# --- arm the rollback BEFORE the first mutation ---------------------------------------
BACKUP=$(mktemp)
cp "$INCLUDE" "$BACKUP"
# A rollback that restores a truncated file is worse than no rollback: the result passes
# configtest with no Define at all, and every vhost then fails to resolve ${VAR_BE}.
# Check the recovery is usable while it is still cheap to refuse.
# The patterns deliberately mirror what the reader above accepts, so a file this script
# can parse can never be one it refuses to back up.
grep -qE "^Define ${VAR_BE} ${CUR_BE}([[:space:]]|$)" "$BACKUP" \
  && grep -qE "^Define ${VAR_FE} [0-9]+([[:space:]]|$)" "$BACKUP" \
  || fatal "backup of ${INCLUDE} lacks both Define lines — refusing to start a swap we cannot undo"
ROLLBACK_ARMED=1

# --- 1. start the target colour -------------------------------------------------------
log "starting ${T_CTR}"
cd "$COMPOSE_DIR"
set -a && . ./.env.docker && set +a
docker compose -p "$PROJECT" --profile bluegreen up -d "$T_SVC"

# --- 2. wait for it to be healthy (the whole point) ----------------------------------
log "waiting for ${T_CTR} healthcheck"
for i in $(seq 1 60); do
  STATUS=$(docker inspect --format '{{.State.Health.Status}}' "$T_CTR" 2>/dev/null || echo missing)
  [ "$STATUS" = healthy ] && break
  if [ "$i" = 60 ]; then
    docker logs --tail 40 "$T_CTR" || true
    fatal "${T_CTR} never became healthy (last: ${STATUS})"
  fi
  sleep 5
done
log "${T_CTR} healthy"

# Belt and braces: ask the new container directly, not just Docker's opinion.
DIRECT=$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:${T_BE}/healthz" || echo 000)
[ "$DIRECT" = 200 ] || fatal "direct /healthz on :${T_BE} returned ${DIRECT}"
log "direct /healthz on :${T_BE} = 200"

# --- 2b. assert the backend's OWN routes before Apache is touched ---------------------
# /mcp and the OAuth 2.1 AS are Starlette routes on the BACKEND, so they can be asserted
# against the target's own port while the live colour keeps serving. Checking them only
# after the swap — as this script used to — means a broken image is discovered with
# production already pointing at it. That is exactly what core#602 did on 2026-08-29:
# `datanika_mcp` failed to import, /mcp was never mounted, and the assertion that caught
# it fired one step too late to matter.
#
# Prod-only, mirroring the post-swap block: staging is not wired to this script (#596),
# so the expectations below have never been exercised there.
if [ "$ENVIRONMENT" = prod ]; then
  BE_MCP=$(curl -s -o /dev/null -w '%{http_code}' --max-time 15 -X POST "http://127.0.0.1:${T_BE}/mcp" \
           -H 'Content-Type: application/json' -H 'Accept: application/json, text/event-stream' \
           -d "$MCP_BODY" || echo 000)
  log "  target /mcp unauth on :${T_BE} -> ${BE_MCP} (expect 401)"
  [ "$BE_MCP" = 401 ] || fatal "/mcp on the target backend returned ${BE_MCP} — the image did not mount MCP. Production is untouched and still on ${ACTIVE_NAME}."
  for WK in oauth-authorization-server oauth-protected-resource; do
    BE_WK=$(curl -s -o /dev/null -w '%{http_code}' --max-time 15 \
            "http://127.0.0.1:${T_BE}/.well-known/${WK}" || echo 000)
    log "  target /.well-known/${WK} on :${T_BE} -> ${BE_WK} (expect 200)"
    [ "$BE_WK" = 200 ] || fatal "/.well-known/${WK} on the target backend returned ${BE_WK}"
  done
fi

# --- 3. repoint Apache ---------------------------------------------------------------
log "repointing Apache -> ${TARGET_NAME} (be ${T_BE}, fe ${T_FE})"
cat > "$INCLUDE" <<CONF
# Active ${ENVIRONMENT} backend/frontend ports — rewritten by deploy-bluegreen.sh.
# Parsed before sites-enabled/*, where the vhost consumes \${${VAR_BE}} / \${${VAR_FE}}.
Define ${VAR_BE} ${T_BE}
Define ${VAR_FE} ${T_FE}
CONF

# Never reload an untested config: this box also serves a webdav co-tenant and the
# founder's VPN path, so a bad reload is not contained to Datanika. Note the include has
# ALREADY been rewritten at this point — a failure here that did not roll back would leave
# a broken config on disk to be discovered by whoever next reloads Apache, for any reason.
apachectl configtest >/dev/null 2>&1 || { apachectl configtest || true; fatal "configtest failed on the rewritten include"; }
apachectl graceful
sleep 2

# --- 4. verify through the proxy, before retiring the old colour ---------------------
for path in /healthz /readyz /; do
  CODE=$(curl -sk -o /dev/null -w '%{http_code}' -H "Host: ${HOST_HEADER}" "https://127.0.0.1${path}" || echo 000)
  log "  proxy ${path} -> ${CODE}"
  [ "$CODE" = 200 ] || fatal "${path} returned ${CODE} after the swap"
done

# Prod is reached by real users through Cloudflare, so a local Host-header check proves
# Apache routing, not what users receive. The /mcp + OAuth checks are repeated here on
# purpose: 2b proved the BACKEND mounts them, this proves the VHOST routes them. They are
# different failure modes — a missing vhost entry silently serves the Reflex SPA instead.
if [ "$ENVIRONMENT" = prod ]; then
  for path in /healthz /; do
    CODE=$(curl -s -o /dev/null -w '%{http_code}' --max-time 15 "https://${HOST_HEADER}${path}" || echo 000)
    log "  public ${path} -> ${CODE}"
    [ "$CODE" = 200 ] || fatal "public ${path} returned ${CODE}"
  done
  MCP=$(curl -s -o /dev/null -w '%{http_code}' --max-time 15 -X POST "https://${HOST_HEADER}/mcp" \
        -H 'Content-Type: application/json' -H 'Accept: application/json, text/event-stream' \
        -d "$MCP_BODY" || echo 000)
  log "  public /mcp unauth -> ${MCP} (expect 401; 200 means it fell through to the SPA)"
  [ "$MCP" = 401 ] || fatal "public /mcp returned ${MCP}"
  OAUTH=$(curl -s -o /dev/null -w '%{http_code}' --max-time 15 \
          "https://${HOST_HEADER}/.well-known/oauth-authorization-server" || echo 000)
  log "  public OAuth discovery -> ${OAUTH} (expect 200)"
  [ "$OAUTH" = 200 ] || fatal "public OAuth discovery returned ${OAUTH}"
fi

# --- 5. retire the old colour ---------------------------------------------------------
# Point of no return: the new colour is verified and serving through the proxy. Failing to
# stop the old container is not a reason to undo a good swap, so disarm before touching it.
ROLLBACK_ARMED=0
log "stopping ${A_CTR}"
docker compose -p "$PROJECT" --profile bluegreen stop "$A_SVC" >/dev/null
log "swap complete: ${TARGET_NAME} is live"
