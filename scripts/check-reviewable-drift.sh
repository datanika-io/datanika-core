#!/usr/bin/env bash
# Reviewable-settings drift (datanika-io/datanika-cloud#141).
#
# .env.docker is preserved by the deploy, so a flag set only there never
# appears in a pull request. This prints each reviewable override the repo
# declaration does not list, by name and by the value the settings object
# resolved.
#
# Secret-pattern names (*_KEY *_SECRET *_PASSWORD *_TOKEN *_DSN *_URL) are
# not passed to the probe and are never printed. The probe is
# scripts/read_reviewable_settings.py, executed by the container's own
# interpreter. An environment listing is not the check (core#646).
#
# --report-only exits 0 when drift is present. The production deploy passes
# it so flags already in the preserved file do not fail the deploy. Without
# it, drift exits 1. The synthetic staging test is that failure.

set -euo pipefail

ENV_NAME=""
REPORT_ONLY=0
while [ $# -gt 0 ]; do
  case "$1" in
    --env)
      ENV_NAME="${2:-}"
      shift 2
      ;;
    --report-only)
      REPORT_ONLY=1
      shift
      ;;
    *)
      echo "usage: check-reviewable-drift.sh --env prod|staging [--report-only]" >&2
      exit 2
      ;;
  esac
done

case "$ENV_NAME" in
  prod|staging) ;;
  *)
    echo "reviewable-drift: --env must be prod or staging" >&2
    exit 2
    ;;
esac

ROOT=$(cd "$(dirname "$0")/.." && pwd)
DECL="${DATANIKA_DRIFT_DECLARATION:-${ROOT}/deploy/reviewable-settings.${ENV_NAME}.env}"
PROBE="${DATANIKA_DRIFT_PROBE:-${ROOT}/scripts/read_reviewable_settings.py}"
CONTAINER="${DATANIKA_DRIFT_CONTAINER:-}"

if [ "$ENV_NAME" = "prod" ]; then
  DEFAULT_ENV="/opt/datanika/datanika/.env.docker"
else
  DEFAULT_ENV="/opt/datanika-staging/datanika/.env.docker"
fi
ENV_FILE="${DATANIKA_DRIFT_ENV_FILE:-${DEFAULT_ENV}}"

[ -f "$DECL" ] || { echo "reviewable-drift: declaration missing" >&2; exit 1; }
[ -f "$ENV_FILE" ] || { echo "reviewable-drift: env file missing" >&2; exit 1; }
[ -f "$PROBE" ] || { echo "reviewable-drift: probe missing" >&2; exit 1; }

if [ -z "$CONTAINER" ]; then
  if [ "$ENV_NAME" != "prod" ]; then
    echo "reviewable-drift: set DATANIKA_DRIFT_CONTAINER for staging" >&2
    exit 2
  fi
  ports="/etc/apache2/conf-enabled/datanika-prod-active.conf"
  BE=$(sed -nE 's/^Define DATANIKA_BE ([0-9]+).*/\1/p' "$ports")
  case "$BE" in
    8000) CONTAINER="datanika-app" ;;
    8010) CONTAINER="datanika-app-b" ;;
    *)
      echo "reviewable-drift: cannot determine serving container" >&2
      exit 1
      ;;
  esac
fi

is_secret() {
  case "$1" in
    *_KEY|*_SECRET|*_PASSWORD|*_TOKEN|*_DSN|*_URL) return 0 ;;
    *) return 1 ;;
  esac
}

names_file=$(mktemp)
probe_out=$(mktemp)
trap 'rm -f "$names_file" "$probe_out"' EXIT

declare -A declared

add_name() {
  local name="$1"
  is_secret "$name" && return 0
  case "$name" in
    [A-Z]*) printf '%s\n' "$name" >> "$names_file" ;;
    *) return 0 ;;
  esac
}

upper_name() {
  printf '%s' "$1" | tr '[:lower:]' '[:upper:]'
}

while IFS= read -r line || [ -n "$line" ]; do
  line=${line%$'\r'}
  case "$line" in
    ''|\#*) continue ;;
  esac
  case "$line" in
    *=*) ;;
    *) continue ;;
  esac
  raw=${line%%=*}
  raw=${raw#"${raw%%[![:space:]]*}"}
  raw=${raw%"${raw##*[![:space:]]}"}
  name=$(upper_name "$raw")
  value=${line#*=}
  value=${value%$'\r'}
  is_secret "$name" && continue
  declared["$name"]=$value
  add_name "$name"
done < "$DECL"

while IFS= read -r line || [ -n "$line" ]; do
  line=${line%$'\r'}
  case "$line" in
    ''|\#*) continue ;;
    export\ *) line=${line#export } ;;
  esac
  case "$line" in
    *=*) ;;
    *) continue ;;
  esac
  raw=${line%%=*}
  raw=${raw#"${raw%%[![:space:]]*}"}
  raw=${raw%"${raw##*[![:space:]]}"}
  name=$(upper_name "$raw")
  add_name "$name"
done < "$ENV_FILE"

ask=()
while IFS= read -r name; do
  [ -n "$name" ] || continue
  ask+=("$name")
done < <(sort -u "$names_file")

if [ "${#ask[@]}" -eq 0 ]; then
  echo "reviewable-drift 0"
  exit 0
fi

docker exec -i "$CONTAINER" /app/.venv/bin/python - "${ask[@]}" < "$PROBE" > "$probe_out"

drift=()
while IFS= read -r line || [ -n "$line" ]; do
  line=${line%$'\r'}
  case "$line" in
    '') continue ;;
    UNREAD\ *)
      name=${line#UNREAD }
      drift+=("DRIFT ${name} unresolved")
      ;;
    *=*)
      name=${line%%=*}
      value=${line#*=}
      if [ -z "${declared[$name]+x}" ] || [ "${declared[$name]}" != "$value" ]; then
        drift+=("DRIFT ${name}=${value}")
      fi
      ;;
  esac
done < "$probe_out"

if [ "${#drift[@]}" -eq 0 ]; then
  echo "reviewable-drift 0"
  exit 0
fi

printf 'reviewable-drift %s\n' "${#drift[@]}"
printf '%s\n' "${drift[@]}"
if [ "$REPORT_ONLY" -eq 1 ]; then
  exit 0
fi
exit 1
