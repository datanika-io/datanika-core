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
#   start inactive colour -> wait for its healthcheck -> rewrite the 2-line Define
#   include -> apachectl configtest -> apachectl graceful -> verify through the proxy
#   -> stop the old colour
#
# `graceful` finishes in-flight requests rather than cutting them. Any failure before the
# swap leaves Apache pointing at the OLD container, which is still serving.
#
# ⚠️ Requires expand/contract migrations. The new container runs `alembic upgrade head`
# while the old one is still serving, so the old code meets the new schema. See
# plans/infra/SPEC_EXPAND_CONTRACT_MIGRATIONS.md — a rename/drop here is an outage.
#
# Usage:  deploy-bluegreen.sh [--env staging|prod] [--dry-run]

set -euo pipefail

ENVIRONMENT="staging"
DRY_RUN=0
while [ $# -gt 0 ]; do
  case "$1" in
    --env) ENVIRONMENT="$2"; shift 2 ;;
    --dry-run) DRY_RUN=1; shift ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

case "$ENVIRONMENT" in
  staging)
    PROJECT=datanika-staging
    COMPOSE_DIR=/opt/datanika-staging
    INCLUDE=/etc/apache2/conf-enabled/datanika-staging-active.conf
    VAR_BE=DATANIKA_STG_BE
    VAR_FE=DATANIKA_STG_FE
    HOST_HEADER=staging-app.datanika.io
    # colour -> "service container be_port fe_port"
    BLUE="app  datanika-staging-app    8100 3100"
    GREEN="app_b datanika-staging-app-b 8110 3110"
    ;;
  prod)
    PROJECT=datanika
    COMPOSE_DIR=/opt/datanika/datanika
    INCLUDE=/etc/apache2/conf-enabled/datanika-prod-active.conf
    VAR_BE=DATANIKA_BE
    VAR_FE=DATANIKA_FE
    HOST_HEADER=app.datanika.io
    BLUE="app  datanika-app    8000 3000"
    GREEN="app_b datanika-app-b 8010 3010"
    ;;
  *) echo "unknown env: $ENVIRONMENT" >&2; exit 2 ;;
esac

log() { printf '%s %s\n' "$(date -u +%H:%M:%S)" "$*"; }

field() { echo "$1" | awk -v n="$2" '{print $n}'; }

# --- which colour is live right now? -----------------------------------------------
CUR_BE=$(sed -nE "s/^Define ${VAR_BE} ([0-9]+).*/\1/p" "$INCLUDE")
if [ -z "$CUR_BE" ]; then
  log "FATAL: cannot read ${VAR_BE} from ${INCLUDE}"; exit 1
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

# --- rollback ------------------------------------------------------------------------
BACKUP=$(mktemp)
cp "$INCLUDE" "$BACKUP"
ROLLED_BACK=0
rollback() {
  [ "$ROLLED_BACK" = 1 ] && return
  ROLLED_BACK=1
  log "ROLLBACK: restoring Apache to ${ACTIVE_NAME} and stopping ${T_CTR}"
  cp "$BACKUP" "$INCLUDE"
  if apachectl configtest >/dev/null 2>&1; then
    apachectl graceful || true
  else
    log "FATAL: restored config fails configtest — NOT reloading. Investigate now."
  fi
  (cd "$COMPOSE_DIR" && set -a && . ./.env.docker && set +a \
     && docker compose -p "$PROJECT" --profile bluegreen stop "$T_SVC" >/dev/null 2>&1) || true
  log "rolled back; ${A_CTR} still serving"
}
trap 'rollback' ERR

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
    log "FATAL: ${T_CTR} never became healthy (last: ${STATUS})"
    docker logs --tail 40 "$T_CTR" || true
    exit 1
  fi
  sleep 5
done
log "${T_CTR} healthy"

# Belt and braces: ask the new container directly, not just Docker's opinion.
DIRECT=$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:${T_BE}/healthz" || echo 000)
[ "$DIRECT" = 200 ] || { log "FATAL: direct /healthz on :${T_BE} returned ${DIRECT}"; exit 1; }
log "direct /healthz on :${T_BE} = 200"

# --- 3. repoint Apache ---------------------------------------------------------------
log "repointing Apache -> ${TARGET_NAME} (be ${T_BE}, fe ${T_FE})"
cat > "$INCLUDE" <<CONF
# Active ${ENVIRONMENT} backend/frontend ports — rewritten by deploy-bluegreen.sh.
# Parsed before sites-enabled/*, where the vhost consumes \${${VAR_BE}} / \${${VAR_FE}}.
Define ${VAR_BE} ${T_BE}
Define ${VAR_FE} ${T_FE}
CONF

# Never reload an untested config: this box also serves a webdav co-tenant and the
# founder's VPN path, so a bad reload is not contained to Datanika.
apachectl configtest >/dev/null 2>&1 || { log "FATAL: configtest failed"; apachectl configtest || true; exit 1; }
apachectl graceful
sleep 2

# --- 4. verify through the proxy, before retiring the old colour ---------------------
for path in /healthz /readyz /; do
  CODE=$(curl -sk -o /dev/null -w '%{http_code}' -H "Host: ${HOST_HEADER}" "https://127.0.0.1${path}" || echo 000)
  log "  proxy ${path} -> ${CODE}"
  [ "$CODE" = 200 ] || { log "FATAL: ${path} returned ${CODE} after the swap"; exit 1; }
done

# Prod is reached by real users through Cloudflare, so a local Host-header check proves
# Apache routing, not what users receive. Also assert the backend-routed endpoints that
# live outside /api/ and have silently fallen through to the SPA before — a swap is
# exactly when that would recur unnoticed.
if [ "$ENVIRONMENT" = prod ]; then
  for path in /healthz /; do
    CODE=$(curl -s -o /dev/null -w '%{http_code}' --max-time 15 "https://${HOST_HEADER}${path}" || echo 000)
    log "  public ${path} -> ${CODE}"
    [ "$CODE" = 200 ] || { log "FATAL: public ${path} returned ${CODE}"; exit 1; }
  done
  MCP=$(curl -s -o /dev/null -w '%{http_code}' --max-time 15 -X POST "https://${HOST_HEADER}/mcp" \
        -H 'Content-Type: application/json' -H 'Accept: application/json, text/event-stream' \
        -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"swap","version":"1"}}}' || echo 000)
  log "  public /mcp unauth -> ${MCP} (expect 401; 200 means it fell through to the SPA)"
  [ "$MCP" = 401 ] || { log "FATAL: /mcp returned ${MCP}"; exit 1; }
  OAUTH=$(curl -s -o /dev/null -w '%{http_code}' --max-time 15 \
          "https://${HOST_HEADER}/.well-known/oauth-authorization-server" || echo 000)
  log "  public OAuth discovery -> ${OAUTH} (expect 200)"
  [ "$OAUTH" = 200 ] || { log "FATAL: OAuth discovery returned ${OAUTH}"; exit 1; }
fi

# --- 5. retire the old colour ---------------------------------------------------------
trap - ERR
log "stopping ${A_CTR}"
docker compose -p "$PROJECT" --profile bluegreen stop "$A_SVC" >/dev/null
log "swap complete: ${TARGET_NAME} is live"
