#!/usr/bin/env bash
# Export the freshness of `scheduled-workflow-watchdog.yml`'s own scheduled runs (core#1264).
#
# DO NOT HAND-INSTALL THIS FILE. `scripts/install-server-scripts.sh` runs on every deploy,
# copies it to /opt/datanika/scripts/ atomically, and asserts sha256 against the repo copy
# (core#747, core#1117). Editing the copy on the box is drift the next deploy silently
# reverts; edit it here and promote. ⚠️ The deploy installs the FILE — it does NOT install
# the crontab entry. See the CRON note below, which is the one step this cannot automate.
#
# THE PROBLEM THIS CLOSES
# -----------------------
# `scheduled-workflow-watchdog.yml` auto-discovers every workflow carrying a `schedule:` key —
# including itself. That is vacuous for the exact failure it exists to catch:
#
#   * its check is "has this workflow produced a scheduled run within min(cron) + 14h?"
#   * answering that question REQUIRES a run to execute
#   * so if its OWN schedule is auto-disabled (`disabled_inactivity`) or silently stops, there
#     is no run, no check, no issue, and NOTHING ANYWHERE IS RED
#
# Since core#1260 the watchdog's filing path ends in `exit 1`, so a red run is what a CORRECT
# detection looks like. The failure modes therefore read: red = found something, and
# SILENCE = HEALTHY. Silence is also what total failure looks like.
#
# WHY THIS RUNS ON THE BOX AND NOT IN ACTIONS
# -------------------------------------------
# Every in-Actions observer inherits the failure it is meant to observe. This box does not: it is
# not GitHub Actions, it is not subject to `disabled_inactivity`, and its cron is not GitHub's
# scheduler. That independence is the entire point — an observer sharing fate with its subject
# observes nothing at the moment it matters.
#
# `cve-watch.yml` is NOT a second observer. It names the watchdog only in a comment explaining
# that it gets auto-discovered. A cross-reference is not coverage.
#
# ANTI-VACUITY — the part that makes this different from the thing it watches
# --------------------------------------------------------------------------
# "API returned an empty list" and "the API call failed" are both `len() == 0` and mean OPPOSITE
# things. An HTTP error yielding an empty parse reads exactly like a silent cron. So:
#
#   * `datanika_watchdog_scrape_success` is 1 ONLY when the HTTP call returned 200 AND the body
#     parsed. It has its own alert rule. A network failure is never allowed to masquerade as a
#     finding about the watchdog.
#   * `datanika_watchdog_last_scheduled_run_timestamp_seconds` is ALWAYS emitted. When the API
#     answered but listed no runs, it is 0 — which is ancient, so the staleness rule fires
#     loudly rather than selecting nothing. A rule that selects nothing reads healthy here:
#     every rule on this box is `noDataState: OK`.
#   * On a failed call the PREVIOUS timestamp is carried forward, so a transient network blip
#     does not manufacture a fake staleness alert, while `scrape_success` still goes 0.
#
# NOT graded by punctuality. Measured honoured times for that cron run +1h52m to +2h53m past the
# declared `41 16 * * *` and drift about an hour a day. The question is CADENCE, not lateness.
#
# Public repo — no token, deliberately. A credential is another thing that expires silently.
#
# IDEMPOTENT. Safe to run on any schedule. Writes one file atomically.
#
# CRON — AND THE ONE STEP THIS CHANGE CANNOT AUTOMATE
# ---------------------------------------------------
#   ( crontab -l; echo '*/10 * * * * /opt/datanika/scripts/export-watchdog-freshness.sh >/dev/null 2>&1' ) | crontab -
#
# 🚨 `install-server-scripts.sh` installs and sha256-verifies this FILE, but NOTHING in this
# repository installs a crontab ENTRY — `export-prod-settings.sh` carries its cron line in a
# comment exactly like this one, and the production crontab is hand-maintained. So until that
# line is added once by hand, this script is installed, verified, and never runs.
#
# That is the same vacuity core#1264 is about, one level over: a guard that is present and
# does not execute. It is why `watchdog-freshness-exporter-gone` alerts on the ABSENCE of this
# exporter's own output rather than trusting that it is scheduled — if the cron entry is never
# added, or is dropped by a later `crontab -` rewrite, that rule is what says so.
#
# */10 rather than */5: the subject's cadence is daily, so a ten-minute resolution is already
# 144x finer than the thing being measured, and it halves the API calls against an unauthenticated
# rate limit shared with everything else on this box.

set -uo pipefail

REPO="${REPO:-datanika-io/datanika-core}"
WORKFLOW="${WORKFLOW:-scheduled-workflow-watchdog.yml}"
TEXTFILE_DIR="${TEXTFILE_DIR:-/opt/datanika/node_textfile}"
OUT="${OUT:-${TEXTFILE_DIR}/datanika_watchdog_freshness.prom}"
API="https://api.github.com/repos/${REPO}/actions/workflows/${WORKFLOW}/runs?event=schedule&per_page=1"

mkdir -p "${TEXTFILE_DIR}" || { echo "cannot create ${TEXTFILE_DIR}" >&2; exit 1; }
TMP="$(mktemp "${TEXTFILE_DIR}/.watchdog.XXXXXX")" || exit 1
trap 'rm -f "${TMP}"' EXIT

# Previous value, so a failed call carries forward rather than inventing staleness.
PREV=0
if [ -r "${OUT}" ]; then
    # `[ {]` is load-bearing: this metric is emitted WITH labels, so the line reads
    # `name{workflow="..."} 123` and a pattern anchored on `name ` (trailing space) matches
    # nothing — it would silently read 0 and manufacture a staleness alert out of a network
    # blip. Caught by this script's own negative control, not by review. `$NF` rather than `$2`
    # for the same reason: the value is the last field whether labels are present or not.
    PREV="$(awk '/^datanika_watchdog_last_scheduled_run_timestamp_seconds[ {]/{print $NF; exit}' "${OUT}" 2>/dev/null)"
    case "${PREV}" in ''|*[!0-9]*) PREV=0 ;; esac
fi

BODY="$(mktemp)" || exit 1
trap 'rm -f "${TMP}" "${BODY}"' EXIT

HTTP="$(curl -sS --max-time 25 -o "${BODY}" -w '%{http_code}' \
        -H 'Accept: application/vnd.github+json' "${API}" 2>/dev/null)" || HTTP="000"

SUCCESS=0
RUNS=-1
TS="${PREV}"

if [ "${HTTP}" = "200" ]; then
    # `total_count` is the API's own count; the array length is what we actually read. Taking
    # both means "API answered but listed nothing" is distinguishable from "API answered".
    RUNS="$(python3 - "${BODY}" <<'PY' 2>/dev/null
import json, sys
try:
    with open(sys.argv[1]) as fh:
        d = json.load(fh)
    print(len(d.get("workflow_runs", [])))
except Exception:
    print(-1)
PY
)"
    case "${RUNS}" in ''|*[!0-9-]*) RUNS=-1 ;; esac

    if [ "${RUNS}" -ge 0 ] 2>/dev/null; then
        SUCCESS=1
        if [ "${RUNS}" -gt 0 ]; then
            EPOCH="$(python3 - "${BODY}" <<'PY' 2>/dev/null
import json, sys, datetime
try:
    with open(sys.argv[1]) as fh:
        d = json.load(fh)
    r = d["workflow_runs"][0]
    # created_at is when the run was CREATED by the scheduler, which is the cadence question.
    t = r.get("run_started_at") or r["created_at"]
    print(int(datetime.datetime.strptime(t, "%Y-%m-%dT%H:%M:%SZ")
              .replace(tzinfo=datetime.timezone.utc).timestamp()))
except Exception:
    print(0)
PY
)"
            case "${EPOCH}" in ''|*[!0-9]*) EPOCH=0 ;; esac
            [ "${EPOCH}" -gt 0 ] && TS="${EPOCH}"
        else
            # API answered and listed NO scheduled runs at all. That is the finding, not an
            # error: 0 is ancient, so the staleness rule fires rather than selecting nothing.
            TS=0
        fi
    fi
fi

{
    echo "# HELP datanika_watchdog_last_scheduled_run_timestamp_seconds Unix time of the most recent SCHEDULED run of the workflow named in the label. 0 means the API answered and listed none."
    echo "# TYPE datanika_watchdog_last_scheduled_run_timestamp_seconds gauge"
    echo "datanika_watchdog_last_scheduled_run_timestamp_seconds{workflow=\"${WORKFLOW}\",repo=\"${REPO}\"} ${TS}"
    echo "# HELP datanika_watchdog_scrape_success 1 when the GitHub API returned 200 AND the body parsed. 0 means this exporter could not reach a verdict — NOT that the watchdog is stale."
    echo "# TYPE datanika_watchdog_scrape_success gauge"
    echo "datanika_watchdog_scrape_success{workflow=\"${WORKFLOW}\"} ${SUCCESS}"
    echo "# HELP datanika_watchdog_runs_returned Number of scheduled runs the API listed (-1 when the call failed or the body did not parse). Distinguishes an empty list from a failed call."
    echo "# TYPE datanika_watchdog_runs_returned gauge"
    echo "datanika_watchdog_runs_returned{workflow=\"${WORKFLOW}\"} ${RUNS}"
    echo "# HELP datanika_watchdog_freshness_http_code Last HTTP status from the GitHub API (000 = no response)."
    echo "# TYPE datanika_watchdog_freshness_http_code gauge"
    echo "datanika_watchdog_freshness_http_code{workflow=\"${WORKFLOW}\"} ${HTTP}"
    echo "# HELP datanika_watchdog_freshness_last_run_timestamp_seconds Unix time THIS exporter last completed. Its own liveness, separate from its subject's."
    echo "# TYPE datanika_watchdog_freshness_last_run_timestamp_seconds gauge"
    echo "datanika_watchdog_freshness_last_run_timestamp_seconds $(date +%s)"
} > "${TMP}"

# Atomic: node-exporter must never read a half-written file.
mv "${TMP}" "${OUT}" || exit 1
chmod 0644 "${OUT}"
trap - EXIT
rm -f "${BODY}"

echo "watchdog freshness: http=${HTTP} success=${SUCCESS} runs=${RUNS} last_scheduled=${TS}"
