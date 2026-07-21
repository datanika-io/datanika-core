#!/bin/bash
# Print the staging app container that is CURRENTLY serving traffic.
#
# Blue/green alternates the live container's name, so anything hardcoding
# `datanika-staging-app` breaks whenever green is active — this broke dev CI on
# 2026-07-21, and the failure looked like an unrelated PR's fault. Apache's active-ports
# include is the single source of truth for which colour is live, so resolve from it.
#
# Deployed to /opt/datanika-staging/active-app.sh
# Usage: docker exec "$(/opt/datanika-staging/active-app.sh)" <cmd>
set -u
INC=/etc/apache2/conf-enabled/datanika-staging-active.conf
BE=$(sed -nE 's/^Define DATANIKA_STG_BE ([0-9]+).*/\1/p' "$INC" 2>/dev/null)
case "$BE" in
  8100) NAME=datanika-staging-app   ;;
  8110) NAME=datanika-staging-app-b ;;
  *)    NAME=datanika-staging-app   ;;  # fallback to the historical default
esac
# Report only a RUNNING container, so callers fail loudly instead of exec'ing a corpse.
if [ "$(docker inspect --format '{{.State.Running}}' "$NAME" 2>/dev/null)" != "true" ]; then
  echo "active-app.sh: $NAME is not running (Apache says be=$BE)" >&2
  exit 1
fi
echo "$NAME"
