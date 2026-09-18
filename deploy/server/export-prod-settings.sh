#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# Export the resolved value of security-relevant production settings as
# Prometheus metrics, so that a setting living in the box's `.env.docker` —
# the file CD *preserves* rather than ships — is visible from outside the box.
#
# 🚨 WHY THIS EXISTS.  `.env.docker` is not in the tarball the deploy transfers.
# Nothing reviews it, nothing diffs it, and no promotion disturbs it.  Two
# billing flags (`DATANIKA_BYTES_QUOTA_ENFORCE`, `DATANIKA_OVERAGE_CHARGE_ENABLE`)
# were live in production for five weeks while every handoff file in the org
# asserted they were off, because their *code* defaults are False and the box
# overrides both (cloud#117).  **A setting CD preserves is a setting nothing
# reviews.**  This turns that class of setting into a scraped series.
#
# 🚨 READ THE INTERPRETER, NEVER THE ENV VAR.  core#646 is the precedent: Reflex
# reads `REFLEX_REDIS_URL`, `REDIS_URL` was set, and checking the env var said
# everything was fine while the framework never read that name.  Every value
# below is resolved by calling the container's own Python, exactly as
# CLAUDE.md's billing-flag and hooks probes do.
#
# 🚨 ABSENT IS NOT COMPLIANT.  If a setting does not exist on the deployed code
# (older image, renamed field), this reports `absent` and counts a violation.
# The alternative — treating a missing attribute as its required value — is the
# `noDataState: OK` mistake in a different costume.
#
# 🚨 ONLY RUNNING CONTAINERS ARE GRADED — AT READ TIME, NOT ONLY AT LOOP START
# (core#1421).  A blue/green swap stops the colour it retires, and a container that
# is running when the loop filters and stopping by the time `docker exec` reaches it
# fails the read.  An empty read is `error`, `error` never equals `require`, and the
# critical `Production Setting Violation` rule paged for a container that served
# nothing (2026-09-17, 06:17:40Z, every live container compliant).  So a container
# whose read ERRORED is asked again whether it is still running, boundedly: if it
# stopped, its lines are discarded; if it is still running, `error` is graded
# exactly as before.  `absent` is never re-checked — it is a property of the code,
# not of a stop, and it stays a violation.
#
# Canonical copy: datanika/deploy/server/export-prod-settings.sh
# ✅ THIS FILE IS INSTALLED BY THE DEPLOY.  `scripts/install-server-scripts.sh`
# copies it to the box on every push to `master` and asserts sha256 against this
# repo copy.  Change it here, merge, promote.  (core#747, shipped 2026-09-04.)
#
# 🚨 DO NOT HAND-INSTALL IT — this banner said to until core#1117.  The issue
# named three stale banners; this was a fourth, found while fixing them.
#
# Install:
#   install -m 0755 export-prod-settings.sh /opt/datanika/scripts/
#   ( crontab -l; echo '*/5 * * * * /opt/datanika/scripts/export-prod-settings.sh >/dev/null 2>&1' ) | crontab -
# ─────────────────────────────────────────────────────────────────────────────
set -uo pipefail

# The ONLY dir node-exporter reads.  Overridable so the behavioural test can run this
# exact script against a fake `docker`; nothing on the box sets the variable.
TEXTFILE_DIR="${DATANIKA_PROD_SETTINGS_TEXTFILE_DIR:-/opt/datanika/node_textfile}"
OUT="${TEXTFILE_DIR}/datanika_prod_settings.prom"
TMP="${OUT}.$$"
CBUF="${TMP}.container"   # one container's lines, held until they are known to be gradable
PY=/app/.venv/bin/python

# core#1421: how long a container whose read errored may take to finish stopping.
# The swap stops the retiring colour with `docker compose stop`, whose default
# timeout is 10 s before SIGKILL (the retired colour reads `Exited (137)`, so it used
# all of it).  TRIES x INTERVAL must outlast that.  Overridable for the test only.
RECHECK_TRIES="${DATANIKA_PROD_SETTINGS_RECHECK_TRIES:-30}"
RECHECK_INTERVAL="${DATANIKA_PROD_SETTINGS_RECHECK_INTERVAL:-1}"

# ── The manifest.  `require=` is the value production must resolve to; a
# setting with require=- is RECORDED but not graded (we want it visible, but
# the correct value is a founder decision, not ours).
#
#   <module path>|<attribute>|<require: true|false|->
MANIFEST="
datanika.config:settings|datanika_allow_local_file_paths|false
datanika_cloud.billing.config:cloud_settings|bytes_quota_enforce|-
datanika_cloud.billing.config:cloud_settings|overage_charge_enable|-
"

# Prod containers only.  Staging runs from /opt/datanika-staging with its own
# .env.docker and is deliberately NOT graded here.
CONTAINERS="datanika-app datanika-app-b datanika-celery datanika-beat"

mkdir -p "${TEXTFILE_DIR}"

{
  echo "# HELP datanika_prod_setting Resolved value of a production setting (1=true, 0=false, -1=absent), read from the container's own interpreter."
  echo "# TYPE datanika_prod_setting gauge"
  echo "# HELP datanika_prod_setting_violation 1 when a REQUIRED production setting does not resolve to its required value (an absent setting counts as a violation)."
  echo "# TYPE datanika_prod_setting_violation gauge"
  echo "# HELP datanika_prod_settings_scrape_success 1 when this exporter completed and probed at least one running container."
  echo "# TYPE datanika_prod_settings_scrape_success gauge"
  echo "# HELP datanika_prod_settings_last_run_timestamp_seconds Unix time this exporter last completed."
  echo "# TYPE datanika_prod_settings_last_run_timestamp_seconds gauge"
} > "${TMP}"

# `grep -x … >/dev/null`, not `grep -qx`: under `pipefail`, `-q` exits on the first
# match, and a `docker ps` still writing then dies of SIGPIPE, which makes the whole
# pipeline read "not running" for a container that is.
is_running() {
  docker ps --format '{{.Names}}' | grep -x "$1" >/dev/null
}

# Exit 0 if the container is still running after the bounded wait, 1 if it stopped.
still_running_after_wait() {
  local tries=0
  while [ "${tries}" -lt "${RECHECK_TRIES}" ]; do
    is_running "$1" || return 1
    sleep "${RECHECK_INTERVAL}"
    tries=$((tries + 1))
  done
  is_running "$1"
}

probed_any=0

for C in ${CONTAINERS}; do
  is_running "${C}" || continue

  : > "${CBUF}"
  errored=0
  while IFS='|' read -r modspec attr require; do
    [ -z "${modspec:-}" ] && continue
    mod="${modspec%%:*}"
    obj="${modspec##*:}"

    raw=$(docker exec "${C}" "${PY}" -c "
import sys
try:
    m = __import__('${mod}', fromlist=['${obj}'])
    o = getattr(m, '${obj}')
except Exception:
    print('IMPORTERR'); sys.exit(0)
v = getattr(o, '${attr}', '__ABSENT__')
print('ABSENT' if v == '__ABSENT__' else str(v))
" 2>/dev/null | tail -1)

    case "${raw}" in
      True|true)   val=1; state=true    ;;
      False|false) val=0; state=false   ;;
      ABSENT)      val=-1; state=absent ;;
      *)           val=-1; state=error; errored=1 ;;
    esac

    echo "datanika_prod_setting{container=\"${C}\",setting=\"${attr}\",state=\"${state}\"} ${val}" >> "${CBUF}"

    if [ "${require}" != "-" ]; then
      viol=1
      [ "${state}" = "${require}" ] && viol=0
      echo "datanika_prod_setting_violation{container=\"${C}\",setting=\"${attr}\",require=\"${require}\"} ${viol}" >> "${CBUF}"
    fi
  done <<< "${MANIFEST}"

  # core#1421: an errored read in a container that has since STOPPED is not a
  # production container being non-compliant -- it is a swap retiring a colour.
  # Discard its lines.  Still running after the wait: grade it, `error` and all.
  if [ "${errored}" = 1 ] && ! still_running_after_wait "${C}"; then
    continue
  fi
  cat "${CBUF}" >> "${TMP}"
  probed_any=1
done
rm -f "${CBUF}"

echo "datanika_prod_settings_scrape_success ${probed_any}" >> "${TMP}"
echo "datanika_prod_settings_last_run_timestamp_seconds $(date +%s)" >> "${TMP}"

# Atomic: node-exporter must never read a half-written file.
mv "${TMP}" "${OUT}"
chmod 0644 "${OUT}"
