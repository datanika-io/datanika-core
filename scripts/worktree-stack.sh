#!/usr/bin/env bash
#
# Run a local stack from an agent WORKTREE without touching anyone else's (core#1197).
#
# Why
# ---
# On 2026-09-15 two agents' local stacks collided three ways, and every one was silent: one shared
# image tag that the next build re-pointed (on a containerd image store a moved tag cannot be put
# back); fixed container names and default host ports, so a stack revived by the Docker daemon
# answered another agent's health check; and Reflex's default api_url, which sends a second stack's
# browser websocket to whichever backend holds host port 8000. Details: scripts/worktree_stack.py.
#
# What it does
# ------------
# 1. Derives this worktree's identity: the department from `datanika-core-<dept>`, the image tag
#    `:worktree-<dept>` that build-from-worktree.sh builds, the compose project `wt-<dept>`, and a
#    host-port band (qa 1xxxx, growth 2xxxx, infra 3xxxx, engineering 4xxxx, product 5xxxx).
# 2. Renders compose's own configuration and derives an overlay from it: container names under
#    `wt-<dept>-`, every published port moved into the band on 127.0.0.1, and REFLEX_API_URL and
#    REFLEX_DEPLOY_URL pointed at this stack's own ports.
# 3. Renders the merged configuration and REFUSES to start anything unless every service is isolated.
# The overlay reaches compose on stdin (`-f -`) and is never written to disk (core#1197 AC4).
#
# Usage
#   bash scripts/build-from-worktree.sh                  # first: builds :worktree-<dept>
#   bash scripts/worktree-stack.sh config                # show the overlay and run the check
#   bash scripts/worktree-stack.sh up [service...]       # check, then `up -d --no-build`
#   bash scripts/worktree-stack.sh ps | logs [service...] | down
#
#   --port-offset N   the band for a worktree with none assigned (a multiple of 10000, up to 50000)
#   --env-file PATH   variables for interpolation (default: .env.docker in this worktree)
#
set -euo pipefail

OFFSET_ARGS=()
ENV_FILE=""
while [ $# -gt 0 ]; do
  case "$1" in
    --port-offset) OFFSET_ARGS=(--port-offset "${2:?--port-offset needs a value}"); shift 2 ;;
    --env-file)    ENV_FILE="${2:?--env-file needs a value}"; shift 2 ;;
    -h|--help)     sed -n '2,32p' "$0"; exit 0 ;;
    *) break ;;
  esac
done
COMMAND="${1:-}"
if [ $# -gt 0 ]; then shift; fi
case "$COMMAND" in
  config|up|ps|logs|down) ;;
  *)
    echo "usage: bash scripts/worktree-stack.sh [--port-offset N] [--env-file PATH] config|up|ps|logs|down" >&2
    exit 2
    ;;
esac

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CORE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$CORE_DIR"

PY="${PYTHON:-}"
if [ -z "$PY" ]; then
  for candidate in .venv/Scripts/python.exe .venv/bin/python; do
    if [ -x "$candidate" ]; then PY="$candidate"; break; fi
  done
fi
if [ -z "$PY" ]; then PY="$(command -v python3 || command -v python || true)"; fi
[ -n "$PY" ] || { echo "FATAL: no python found; set PYTHON=/path/to/python" >&2; exit 1; }

IDENTITY="$("$PY" scripts/worktree_stack.py identity "$CORE_DIR" "${OFFSET_ARGS[@]}")"
field() { printf '%s\n' "$IDENTITY" | sed -n "s/^$1=//p"; }
AGENT="$(field agent)"
TAG="$(field tag)"
PROJECT="$(field project)"
OFFSET="$(field offset)"

ENV_FILE="${ENV_FILE:-$CORE_DIR/.env.docker}"
if [ ! -f "$ENV_FILE" ]; then
  echo "FATAL: no env file at $ENV_FILE -- compose needs its variables to render; pass --env-file" >&2
  exit 1
fi

FILES=(-f docker-compose.yml -f docker-compose.local.yml)
export DATANIKA_IMAGE_TAG="${TAG##*:}"

BASE="$(docker compose "${FILES[@]}" --profile '*' config --no-interpolate --format json)"
OVERLAY="$(printf '%s' "$BASE" | "$PY" scripts/worktree_stack.py overlay --agent "$AGENT" --offset "$OFFSET")"

stack() {
  printf '%s' "$OVERLAY" | docker compose --env-file "$ENV_FILE" -p "$PROJECT" "${FILES[@]}" -f - "$@"
}

isolated() {
  local full
  full="$(stack --profile '*' config --format json)"
  if ! printf '%s' "$full" | "$PY" scripts/worktree_stack.py check --agent "$AGENT" --offset "$OFFSET"; then
    echo "worktree-stack: REFUSED -- the rendered configuration is not isolated; nothing was started." >&2
    return 1
  fi
}

echo "worktree-stack: dept=$AGENT project=$PROJECT image=$TAG host ports $OFFSET-$((OFFSET + 9999))"
case "$COMMAND" in
  config)
    isolated
    echo "--- overlay (streamed to compose; never written)"
    printf '%s' "$OVERLAY"
    ;;
  up)
    isolated
    [ $# -gt 0 ] || set -- postgres redis app celery scheduler proxy
    if ! docker image inspect "$TAG" >/dev/null 2>&1; then
      echo "FATAL: no image $TAG -- build it first: bash scripts/build-from-worktree.sh" >&2
      exit 1
    fi
    stack up -d --no-build "$@"
    echo
    echo "worktree-stack: up. One origin, through the proxy: http://localhost:$((OFFSET + 3100))"
    echo "                frontend http://localhost:$((OFFSET + 3000))  backend http://localhost:$((OFFSET + 8000))"
    ;;
  ps) stack ps ;;
  logs) stack logs --tail 100 "$@" ;;
  # Never `down -v`: this project's volumes are its own and may hold a walk's data.
  down) stack down ;;
esac
