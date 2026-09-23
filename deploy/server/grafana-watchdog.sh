#!/usr/bin/env bash
# Assert from OUTSIDE Grafana that Grafana can still evaluate alert rules (core#1477).
#
# DO NOT HAND-INSTALL THIS FILE. `scripts/install-server-scripts.sh` runs on every deploy,
# copies it to /opt/datanika/scripts/ atomically, and asserts sha256 against the repo copy
# (core#747). Unlike every sibling in this directory, its CRON ENTRY is in git too —
# `deploy/server/datanika.cron` -> `/etc/cron.d/datanika`. That is deliberate and is the
# reason this script exists at all; see the CRON section at the bottom.
#
# THE PROBLEM THIS CLOSES
# -----------------------
# The eight `Watchdog: … missing` rules cannot report a broken Grafana, because they ARE
# Grafana alert rules. On 2026-09-20 Grafana could not evaluate any rule for ~25 minutes,
# emitted 176 false alerts from 11 rules, and THE FOUNDER REPORTED IT BEFORE ANY AGENT
# NOTICED. An observer sharing a failure domain with its subject observes nothing at the
# moment it matters.
#
# The reading that would have named it was available the whole time and unread:
# `/api/prometheus/grafana/api/v1/rules` returned `{"groups":[]}` — ZERO rules — while 44
# rules existed and the alerts endpoint still listed 44 instances. That contradiction is a
# one-call statement that *Grafana is broken, not production*.
#
# `deploy-pointer.yml`'s last step already makes exactly this call, and it was correct: it
# read 44/44/ok at 19:56Z, because the failure began at 20:46Z. The instrument was right;
# it simply runs once, at deploy time. A one-shot post-deploy check cannot see a condition
# that starts an hour later. This is that same check, on a schedule.
#
# WHY THE OBVIOUS FIX IS NOT ENOUGH — the trap one level down
# -----------------------------------------------------------
# Exporting "Grafana can list its rules" into node-exporter's textfile directory (the pattern
# `export-prod-settings.sh` and `export-watchdog-freshness.sh` use) puts the fact in
# Prometheus, a different process with different storage. But an alert rule on that metric is
# STILL A GRAFANA RULE, so it fails in precisely the case it exists to report.
#
# 🔑 So the metric below is history and defence in depth. It is NOT the reporting path.
# The reporting path is a direct POST to api.telegram.org, which touches nothing on this box
# but curl. `TELEGRAM_BOT_TOKEN` / `TELEGRAM_CHAT_ID` are already in `.env.docker` (Grafana
# reads them today), so this adds no credential and no sub-processor.
#
# WHY THE EXPECTED COUNT IS READ FROM A FILE AND NOT FROM GRAFANA
# ---------------------------------------------------------------
# Comparing the rules API against Grafana's PROVISIONING API would reproduce the 2026-09-20
# contradiction directly, and it is tempting. It is also two calls into the failing component.
# The provisioning YAML on disk is the same number with no dependency on Grafana being alive,
# so that is what this reads. If the file cannot be read, EXPECTED is 0 and the count-equality
# clause is skipped rather than manufacturing a failure — an unreadable file is not evidence
# about Grafana. The other three clauses still catch the 2026-09-20 shape.
#
# DEBOUNCE, THEN EDGE-TRIGGER — the part that decides whether this is an improvement
# ----------------------------------------------------------------------------------
# The 2026-09-20 storm was 18–25 messages per minute. A watchdog that behaves the same way is
# not an improvement, it is a second source of the noise it was written to prevent. So:
#
#   * BROKEN only after ${FAIL_THRESHOLD} consecutive failing reads. Grafana's rules API
#     returned HTTP 500 once on 2026-09-22 (15:25:44Z, 11.3 s, `context canceled`) while its
#     SQLite hit `max-retries-reached`, and recovered on the next read. That single 500 must
#     NOT page — and with a threshold of 2 it does not.
#   * One message on TRANSITION — ok->broken and broken->ok — plus at most one reminder every
#     ${REMIND_SECONDS}s while broken. Ten consecutive failures send one message, not ten.
#
# ⚠️ `notified_state` is tracked SEPARATELY from `state`, and that is not bookkeeping. If the
# Telegram POST fails, the transition stays un-notified and the next run retries it. Folding
# the two together would mean one unreachable-Telegram moment silently swallows the single
# edge-triggered message forever — a watchdog that reports nothing and looks fine, which is
# the exact defect class this whole file is about.
#
# SEEN FAILING BEFORE TRUSTED
# ---------------------------
# `tests/test_deploy/test_grafana_watchdog.py` drives this against a fake Grafana and a fake
# Telegram: healthy -> 0 messages, 1 bad read -> 0, 2 -> 1, 10 -> still 1, recovery -> 1.
# `DRY_RUN=1` proves detection on the box without sending anything and without touching
# production's Grafana.
#
# IDEMPOTENT. Safe to run on any schedule. Writes one state file and one metrics file.

set -uo pipefail

ENV_FILE="${ENV_FILE:-/opt/datanika/datanika/.env.docker}"
GRAFANA_URL="${GRAFANA_URL:-http://127.0.0.1:3001}"
RULES_PATH="${RULES_PATH:-/api/prometheus/grafana/api/v1/rules}"
PROVISIONING_FILE="${PROVISIONING_FILE:-/opt/datanika/datanika/monitoring/grafana/provisioning/alerting/alerts.yml}"
STATE_FILE="${STATE_FILE:-/opt/datanika/state/grafana-watchdog.state}"
TEXTFILE_DIR="${TEXTFILE_DIR:-/opt/datanika/node_textfile}"
OUT="${OUT:-${TEXTFILE_DIR}/datanika_grafana_watchdog.prom}"
TELEGRAM_API="${TELEGRAM_API:-https://api.telegram.org}"
FAIL_THRESHOLD="${FAIL_THRESHOLD:-2}"
REMIND_SECONDS="${REMIND_SECONDS:-21600}"
DRY_RUN="${DRY_RUN:-0}"
HOSTLABEL="${HOSTLABEL:-app.datanika.io}"

NOW="$(date +%s)"
PY="${PYTHON:-python3}"

# Read one key out of the env file WITHOUT sourcing it. Sourcing production's whole
# environment into a cron job is a larger blast radius than this needs, and a stray backtick
# in any value would execute.
read_env() {
    local k="$1" v=""
    [ -r "${ENV_FILE}" ] || return 0
    v="$(sed -n "s/^[[:space:]]*${k}=//p" "${ENV_FILE}" | head -1)"
    v="${v%\"}"; v="${v#\"}"
    v="${v%\'}"; v="${v#\'}"
    printf '%s' "${v}"
}

GF_USER="$(read_env GRAFANA_ADMIN_USER)"; GF_USER="${GF_USER:-admin}"
GF_PASS="$(read_env GRAFANA_ADMIN_PASSWORD)"
TG_TOKEN="$(read_env TELEGRAM_BOT_TOKEN)"
TG_CHAT="$(read_env TELEGRAM_CHAT_ID)"

BODY="$(mktemp)" || exit 1
trap 'rm -f "${BODY}"' EXIT

# ---------------------------------------------------------------------------------------
# 1. The reading.
# ---------------------------------------------------------------------------------------
HTTP="$(curl -sS --max-time 20 -u "${GF_USER}:${GF_PASS}" \
        -o "${BODY}" -w '%{http_code}' "${GRAFANA_URL}${RULES_PATH}" 2>/dev/null)" || HTTP="000"

# 🚨 NOT `GROUPS`. `GROUPS` is a bash BUILT-IN array holding the caller's group ids; bash
# silently discards assignments to it, so `${GROUPS}` would read a real gid (197121 on the
# dev machine) no matter what this script computed. That is never 0, so the "zero rule
# groups" clause below — the exact 2026-09-20 shape — could not fire. Measured, not
# reasoned: it survived every test except the one that removes the redundant count-equality
# clause, which is why that test exists.
RGROUPS=-1; RULES=-1; ERRORS=-1
if [ "${HTTP}" = "200" ]; then
    # ⚠️ The body reaches Python on STDIN, and the program comes from `-c`. Passing the temp
    # path as argv instead would be the MSYS path-mangling trap (`WORKFLOW_RULES` §1): this
    # file is exercised by `tests/test_deploy/`, which runs in the pre-push hook on Windows,
    # where a `/tmp/...` argument handed to a native interpreter arrives mangled. A shell
    # redirect is translated correctly; an argv string is not.
    PARSED="$("${PY}" -c '
import json, sys
try:
    d = json.load(sys.stdin)
    # Grafana wraps this endpoint as {"status":..., "data":{"groups":[...]}}. The 2026-09-20
    # capture in core#1477 shows a bare {"groups":[]}. Accept both rather than depending on
    # which shape a broken Grafana happens to emit.
    root = d.get("data") if isinstance(d.get("data"), dict) else d
    groups = root.get("groups") or []
    rules = [r for g in groups for r in (g.get("rules") or [])]
    errors = sum(1 for r in rules if str(r.get("health", "")).lower() == "error")
    print(len(groups), len(rules), errors)
except Exception:
    print(-1, -1, -1)
' < "${BODY}" 2>/dev/null)"
    # shellcheck disable=SC2086
    set -- ${PARSED:-}
    RGROUPS="${1:--1}"; RULES="${2:--1}"; ERRORS="${3:--1}"
    case "${RGROUPS}${RULES}${ERRORS}" in *[!0-9-]*|'') RGROUPS=-1; RULES=-1; ERRORS=-1 ;; esac
fi

# Expected rule count, from disk — no dependency on Grafana being alive.
EXPECTED=0
if [ -r "${PROVISIONING_FILE}" ]; then
    EXPECTED="$(grep -cE '^[[:space:]]+- uid:' "${PROVISIONING_FILE}" 2>/dev/null)"
    case "${EXPECTED}" in ''|*[!0-9]*) EXPECTED=0 ;; esac
fi

# ---------------------------------------------------------------------------------------
# 2. The verdict. Every clause names WHY, because the message has to be actionable at 03:00.
# ---------------------------------------------------------------------------------------
HEALTHY=1; REASON="ok"
if [ "${HTTP}" != "200" ]; then
    HEALTHY=0; REASON="rules API returned HTTP ${HTTP}"
elif [ "${RULES}" -lt 0 ] 2>/dev/null; then
    HEALTHY=0; REASON="rules API returned 200 but the body did not parse"
elif [ "${RGROUPS}" -eq 0 ] 2>/dev/null; then
    # THE 2026-09-20 SHAPE. Grafana answers, cheerfully, with nothing.
    HEALTHY=0; REASON="rules API returned 200 with ZERO rule groups (${EXPECTED} are provisioned) — Grafana cannot evaluate"
elif [ "${EXPECTED}" -gt 0 ] 2>/dev/null && [ "${RULES}" -ne "${EXPECTED}" ] 2>/dev/null; then
    HEALTHY=0; REASON="rules API lists ${RULES} rules, ${EXPECTED} are provisioned on disk"
elif [ "${ERRORS}" -gt 0 ] 2>/dev/null; then
    HEALTHY=0; REASON="${ERRORS} of ${RULES} rules are in Error health"
fi

# ---------------------------------------------------------------------------------------
# 3. State: debounce, then edge-trigger.
# ---------------------------------------------------------------------------------------
state=ok; notified_state=ok; fails=0; since="${NOW}"; last_notified=0
if [ -r "${STATE_FILE}" ]; then
    # shellcheck disable=SC1090
    . "${STATE_FILE}" 2>/dev/null || true
fi
case "${fails}" in ''|*[!0-9]*) fails=0 ;; esac
case "${last_notified}" in ''|*[!0-9]*) last_notified=0 ;; esac
case "${since}" in ''|*[!0-9]*) since="${NOW}" ;; esac
[ "${state}" = broken ] || state=ok
[ "${notified_state}" = broken ] || notified_state=ok

prev_state="${state}"
if [ "${HEALTHY}" = "1" ]; then
    fails=0
    state=ok
else
    fails=$((fails + 1))
    [ "${fails}" -ge "${FAIL_THRESHOLD}" ] && state=broken
fi
[ "${state}" != "${prev_state}" ] && since="${NOW}"

# Notify on a transition we have not successfully reported, or on the reminder interval.
send=0; kind=""
if [ "${state}" != "${notified_state}" ]; then
    send=1; kind="transition"
elif [ "${state}" = broken ] && [ $((NOW - last_notified)) -ge "${REMIND_SECONDS}" ]; then
    send=1; kind="reminder"
fi

# ---------------------------------------------------------------------------------------
# 4. Delivery. Direct to Telegram — this path does not traverse Grafana.
# ---------------------------------------------------------------------------------------
sent=0
if [ "${send}" = "1" ]; then
    if [ "${state}" = broken ]; then
        MSG="🔴 Grafana cannot evaluate alert rules on ${HOSTLABEL}

${REASON}

Failing reads: ${fails} (threshold ${FAIL_THRESHOLD}). Broken since $(date -u -d "@${since}" +%FT%TZ 2>/dev/null || echo "${since}").
🔑 Alert rules are NOT reporting right now. Silence from Grafana means nothing until this clears.
core#1477 — check: docker logs datanika-grafana | tail; docker compose up -d --force-recreate grafana"
    else
        MSG="🟢 Grafana is evaluating alert rules again on ${HOSTLABEL}

${RULES} rules listed, ${ERRORS} in Error health. Recovered at $(date -u +%FT%TZ).
core#1477"
    fi

    if [ "${DRY_RUN}" = "1" ]; then
        # Detection proven without sending. Never print the token.
        echo "grafana-watchdog: DRY RUN — would send (${kind}) to ${TELEGRAM_API}:"
        echo "--- 8< ---"; echo "${MSG}"; echo "--- >8 ---"
        sent=1
    elif [ -n "${TG_TOKEN}" ] && [ -n "${TG_CHAT}" ]; then
        if curl -fsS --max-time 20 -o /dev/null \
             -X POST "${TELEGRAM_API}/bot${TG_TOKEN}/sendMessage" \
             --data-urlencode "chat_id=${TG_CHAT}" \
             --data-urlencode "text=${MSG}" 2>/dev/null; then
            sent=1
        else
            # Leave notified_state alone so the next run retries this transition.
            echo "grafana-watchdog: Telegram send FAILED — transition stays unreported, will retry" >&2
        fi
    else
        echo "grafana-watchdog: no TELEGRAM_BOT_TOKEN/TELEGRAM_CHAT_ID in ${ENV_FILE} — cannot report" >&2
    fi

    if [ "${sent}" = "1" ]; then
        notified_state="${state}"
        last_notified="${NOW}"
    fi
fi

# ---------------------------------------------------------------------------------------
# 5. Persist state, then the metric.
# ---------------------------------------------------------------------------------------
mkdir -p "$(dirname "${STATE_FILE}")" 2>/dev/null
STMP="${STATE_FILE}.tmp.$$"
{
    echo "state=${state}"
    echo "notified_state=${notified_state}"
    echo "fails=${fails}"
    echo "since=${since}"
    echo "last_notified=${last_notified}"
} > "${STMP}" && mv -f "${STMP}" "${STATE_FILE}"

if mkdir -p "${TEXTFILE_DIR}" 2>/dev/null; then
    MTMP="${OUT}.tmp.$$"
    {
        echo "# HELP datanika_grafana_rule_eval_healthy 1 when Grafana's rules API answered AND listed the provisioned rules with none in Error health. NOT the reporting path — see core#1477; an alert rule on this metric would share Grafana's failure domain."
        echo "# TYPE datanika_grafana_rule_eval_healthy gauge"
        echo "datanika_grafana_rule_eval_healthy ${HEALTHY}"
        echo "# HELP datanika_grafana_rules_listed Rules the rules API listed (-1 when the call failed or the body did not parse). Distinguishes an empty list from a failed call."
        echo "# TYPE datanika_grafana_rules_listed gauge"
        echo "datanika_grafana_rules_listed ${RULES}"
        echo "# HELP datanika_grafana_rules_provisioned Rules counted in the provisioning YAML on disk (0 when unreadable)."
        echo "# TYPE datanika_grafana_rules_provisioned gauge"
        echo "datanika_grafana_rules_provisioned ${EXPECTED}"
        echo "# HELP datanika_grafana_rules_error_health Rules the API reports in Error health (-1 when unknown)."
        echo "# TYPE datanika_grafana_rules_error_health gauge"
        echo "datanika_grafana_rules_error_health ${ERRORS}"
        echo "# HELP datanika_grafana_watchdog_http_code Last HTTP status from Grafana's rules API (000 = no response)."
        echo "# TYPE datanika_grafana_watchdog_http_code gauge"
        echo "datanika_grafana_watchdog_http_code ${HTTP}"
        echo "# HELP datanika_grafana_watchdog_last_run_timestamp_seconds Unix time this watchdog last completed. Its own liveness, separate from its subject's."
        echo "# TYPE datanika_grafana_watchdog_last_run_timestamp_seconds gauge"
        echo "datanika_grafana_watchdog_last_run_timestamp_seconds ${NOW}"
    } > "${MTMP}" && mv -f "${MTMP}" "${OUT}" && chmod 0644 "${OUT}"
fi

echo "grafana-watchdog: http=${HTTP} groups=${RGROUPS} rules=${RULES}/${EXPECTED} errors=${ERRORS} healthy=${HEALTHY} state=${state} fails=${fails} sent=${sent} (${REASON})"

# Exit 0 whether or not Grafana is healthy: this is a REPORTER, and cron mailing a non-zero
# exit would be a second, unthrottled notification channel for the condition it just
# throttled. A non-zero exit here means the watchdog itself could not run.
exit 0

# CRON — installed from git, unlike every sibling in this directory
# -----------------------------------------------------------------
#   deploy/server/datanika.cron  ->  /etc/cron.d/datanika   (installed by
#   scripts/install-server-scripts.sh on every deploy, 0644 root:root)
#
# Every other box-side job here carries its cron line in a COMMENT and was added to root's
# crontab by hand — `export-watchdog-freshness.sh` says so in its own header: "NOTHING in
# this repository installs a crontab ENTRY". That is the one part of the box's configuration
# with no copy in git, and it is exactly the surface where "installed, verified, and never
# runs" hides. Adding a fourth hand-made line would have made that worse, so this one is in
# git from the start.
