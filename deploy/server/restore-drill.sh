#!/bin/bash
# Restore drill — proves the OFF-SITE backups actually restore (an untested backup
# is not a backup). Pulls the latest dump from Aweb, DECRYPTS it, restores it into
# a throwaway postgres container, verifies row counts against the LIVE database,
# tears everything down. Never touches the prod postgres/volume.
#
# Deployed to:   /opt/datanika/scripts/restore-drill.sh on 185.25.22.188
# Cron (monthly): 0 5 1 * * /opt/datanika/scripts/restore-drill.sh >> /var/log/datanika-restore-drill.log 2>&1
# Canonical copy: deploy/server/restore-drill.sh (datanika-core)
# Invariants pinned by: tests/test_deploy/test_backup_encryption.py
#
# ⚠️ NOTHING DEPLOYS THIS FILE (core#747) — the copy that runs is hand-installed.
# After changing it, install it and compare sha256 against git.
#
# ⚠️ The assertion is the whole value of this script, and its first version was
# `plans >= 5`. That is seed data written by seed_v2_plans.py, never by a
# customer, and `pg_dump` emits COPY blocks ALPHABETICALLY -- which puts `plans`
# at line ~1609 of 2649 and `users` LAST in the file. So `users` is the first
# thing any truncation destroys and the last thing a plans-only check would
# notice. Measured 2026-08-31 (core#725): a dump truncated immediately after the
# plans block restores with **0 users, 0 runs, 0 uploads**, exits 0, logs zero
# errors, passes `gzip -t`, clears the >1000-byte size gate in
# backup-offsite.sh -- and printed "RESTORE DRILL PASS (plans=5)".
#
# So: compare every table against the live database instead. No hardcoded list,
# because a hardcoded list is the same mistake with more rows.
#
# ── Encryption (core#675, 2026-08-31) ────────────────────────────────────────
# The off-site artifact is now GPG ciphertext. This drill is therefore also the
# only thing that regularly proves the encryption round-trips — if it ever stops
# decrypting, we find out on a schedule instead of during a recovery.
# It additionally FAILS if any plaintext dump is found off-site, so a silent
# revert to unencrypted backups becomes a loud monthly failure rather than a
# quiet reappearance of core#675.

set -euo pipefail

REMOTE=root@185.226.65.96
REMOTE_DIR=/opt/datanika-backups
SSH_KEY=/root/.ssh/aweb_backup
SSH_OPTS="-i ${SSH_KEY} -o StrictHostKeyChecking=accept-new -o ConnectTimeout=20"
WORK=/tmp/restore-drill.$$
CONTAINER=datanika-restore-test
PROD_CONTAINER=datanika-postgres
TEXTFILE_DIR=/opt/datanika/node_textfile   # the ONLY dir node-exporter reads
STAMP_FILE=/opt/datanika/monitoring/restore-drill-last-success.txt
export GNUPGHOME="${GNUPGHOME:-/root/.gnupg}"   # overridable: cron leaves it unset, but a test must be able to remove the key

cleanup() { docker rm -f "${CONTAINER}" >/dev/null 2>&1 || true; rm -rf "${WORK}"; }
trap cleanup EXIT

mkdir -p "${WORK}"
START_EPOCH=$(date +%s)

# Guard: encryption must not have silently regressed. '*.sql.gz' does not match
# '*.sql.gz.gpg', so this counts plaintext dumps only.
echo "[$(date)] checking no plaintext dumps remain off-site..."
PLAINTEXT_COUNT=$(ssh ${SSH_OPTS} "${REMOTE}" "ls -1 ${REMOTE_DIR}/*.sql.gz 2>/dev/null | wc -l")
if [ "${PLAINTEXT_COUNT}" != 0 ]; then
    echo "[$(date)] RESTORE DRILL FAIL — ${PLAINTEXT_COUNT} UNENCRYPTED dump(s) on the off-site host."
    echo "  That is core#675 reappearing. Check backup-offsite.sh on this box is the encrypting version."
    exit 1
fi

echo "[$(date)] fetching latest off-site dump from ${REMOTE}..."
LATEST=$(ssh ${SSH_OPTS} "${REMOTE}" "ls -t ${REMOTE_DIR}/*.sql.gz.gpg 2>/dev/null | head -1")
if [ -z "${LATEST}" ]; then echo "[$(date)] ERROR: no off-site dump found"; exit 1; fi
echo "[$(date)] latest: ${LATEST}"
rsync -az -e "ssh ${SSH_OPTS}" "${REMOTE}:${LATEST}" "${WORK}/"
ENC="${WORK}/$(basename "${LATEST}")"
DUMP="${WORK}/$(basename "${LATEST}" .gpg)"

# Decrypt. A failure here is the single most important thing this drill can
# report: it means the off-site copies are unreadable and the backup is not a
# backup. Do not add a fallback to plaintext — a fallback would mask exactly this.
echo "[$(date)] decrypting off-site artifact..."
if ! gpg --batch --quiet --yes --decrypt "${ENC}" > "${DUMP}" 2>"${WORK}/gpg.log"; then
    echo "[$(date)] RESTORE DRILL FAIL — could not decrypt ${ENC}"
    echo "  The off-site backups cannot be read with the key on this box."
    echo "  Escrowed copy: secrets/datanika-backup-privkey.asc (founder dev machine)."
    tail -10 "${WORK}/gpg.log" || true
    exit 1
fi
echo "[$(date)] decrypted ok ($(stat -c%s "${DUMP}") bytes)"

# Cheap pre-flight the size gate cannot do: a complete pg_dump ends with its own
# terminator. A dump cut off mid-COPY does not, and is otherwise a valid gzip.
if ! gunzip -c "${DUMP}" | tail -5 | grep -qE '^(\\unrestrict|-- PostgreSQL database dump complete)'; then
    echo "[$(date)] ERROR: dump has no end-of-dump terminator — truncated in transfer or at source"
    exit 1
fi

echo "[$(date)] starting throwaway postgres..."
docker rm -f "${CONTAINER}" >/dev/null 2>&1 || true
docker run -d --name "${CONTAINER}" \
    -e POSTGRES_USER=datanika -e POSTGRES_PASSWORD=drill -e POSTGRES_DB=datanika \
    postgres:16-alpine >/dev/null
# Wait until the target DB actually answers (pg_isready alone can return true during
# the entrypoint's temporary init server, before the datanika DB exists).
READY=0
for i in $(seq 1 45); do
    if docker exec "${CONTAINER}" psql -U datanika -d datanika -c 'SELECT 1' >/dev/null 2>&1; then
        READY=1; break
    fi
    sleep 1
done
if [ "${READY}" != 1 ]; then echo "[$(date)] ERROR: throwaway postgres never became ready"; exit 1; fi

echo "[$(date)] restoring dump..."
RESTORE_LOG="${WORK}/restore.log"
RESTORE_RC=0
gunzip -c "${DUMP}" | docker exec -i "${CONTAINER}" psql -U datanika -d datanika -v ON_ERROR_STOP=1 -q \
    > "${RESTORE_LOG}" 2>&1 || RESTORE_RC=$?

ERRS=$(grep -ci "error" "${RESTORE_LOG}" 2>/dev/null || true)
ERRS=${ERRS:-0}
if [ "${RESTORE_RC}" != 0 ] || [ "${ERRS}" != 0 ]; then
    echo "[$(date)] RESTORE DRILL FAIL — psql exit=${RESTORE_RC}, ${ERRS} error line(s). Tail:"
    tail -20 "${RESTORE_LOG}"
    exit 1
fi

# One statement, built from the catalogue, so no table is forgotten as the schema
# grows. Run against both databases and compare.
COUNT_SQL="select string_agg(format('select %L as t, count(*) as n from public.%I', tablename, tablename), ' union all ' order by tablename) from pg_tables where schemaname='public'"

counts() {  # $1 = container
    local inner
    inner=$(docker exec "$1" psql -U datanika -d datanika -At -c "${COUNT_SQL}")
    [ -n "${inner}" ] || return 1
    docker exec "$1" psql -U datanika -d datanika -At -F'|' -c "${inner}" | sort
}

counts "${PROD_CONTAINER}" > "${WORK}/live.txt"   || { echo "[$(date)] ERROR: could not read live counts"; exit 1; }
counts "${CONTAINER}"      > "${WORK}/restored.txt" || { echo "[$(date)] ERROR: could not read restored counts"; exit 1; }

LIVE_POPULATED=$(awk -F'|' '($2+0)>0' "${WORK}/live.txt" | wc -l)
if [ "${LIVE_POPULATED}" -lt 1 ]; then
    echo "[$(date)] RESTORE DRILL FAIL — the live database reported no populated tables."
    echo "  Refusing a verdict: a comparison against nothing passes trivially."
    exit 1
fi

# The assertion: every table that has rows in production must have rows in the
# restore. Deliberately not equality — prod moves on between 03:00 and 05:00.
MISSING=$(join -t'|' -a1 -e MISSING -o 0,1.2,2.2 "${WORK}/live.txt" "${WORK}/restored.txt" \
    | awk -F'|' '($2+0)>0 && ($3=="MISSING" || ($3+0)==0) {printf "%s(live=%s) ", $1, $2}')

RESTORED_ROWS=$(awk -F'|' '{s+=$2} END {print s+0}' "${WORK}/restored.txt")
ELAPSED=$(( $(date +%s) - START_EPOCH ))
echo "[$(date)] restored ${RESTORED_ROWS} rows across $(wc -l < "${WORK}/restored.txt") tables; ${LIVE_POPULATED} tables populated in prod; ${ELAPSED}s"

if [ -n "${MISSING}" ]; then
    echo "[$(date)] RESTORE DRILL FAIL — populated in prod but EMPTY in the restore: ${MISSING}"
    exit 1
fi

echo "[$(date)] RESTORE DRILL PASS (${LIVE_POPULATED} populated tables all present, ${RESTORED_ROWS} rows, decrypted off-site copy, ${ELAPSED}s)"

# Freshness metric. Must land in TEXTFILE_DIR as *.prom or node-exporter never
# sees it: the first version wrote a .txt into /opt/datanika/monitoring, which is
# not the collector directory and not the collector's format, so a drill that
# stopped running produced no metric and no alert could exist.
mkdir -p "${TEXTFILE_DIR}" "$(dirname "${STAMP_FILE}")"
TMP="${TEXTFILE_DIR}/datanika_restore_drill.prom.$$"
{
    echo "# HELP datanika_restore_drill_last_success_timestamp_seconds Unix time of last passing restore drill"
    echo "# TYPE datanika_restore_drill_last_success_timestamp_seconds gauge"
    echo "datanika_restore_drill_last_success_timestamp_seconds $(date +%s)"
    echo "# HELP datanika_restore_drill_rows_restored Rows restored across all tables in the last passing drill"
    echo "# TYPE datanika_restore_drill_rows_restored gauge"
    echo "datanika_restore_drill_rows_restored ${RESTORED_ROWS}"
    echo "# HELP datanika_restore_drill_duration_seconds Wall-clock duration of the last passing drill"
    echo "# TYPE datanika_restore_drill_duration_seconds gauge"
    echo "datanika_restore_drill_duration_seconds ${ELAPSED}"
    echo "# HELP datanika_restore_drill_decrypted_offsite Whether the last passing drill decrypted a GPG off-site artifact"
    echo "# TYPE datanika_restore_drill_decrypted_offsite gauge"
    echo "datanika_restore_drill_decrypted_offsite 1"
} > "${TMP}"
mv "${TMP}" "${TEXTFILE_DIR}/datanika_restore_drill.prom"
date -u +%Y-%m-%dT%H:%M:%SZ > "${STAMP_FILE}"
