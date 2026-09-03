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
#
# ── Volume artifacts (core#970, 2026-09-03) ──────────────────────────────────
# The drill now also pulls, decrypts, member-counts and extracts both volume
# tarballs, and cross-checks the uploads volume against the restored dump. See
# the block after the table comparison.
#
# 🚨 A REAL RESTORE MUST LAND ARCHIVES AT THEIR ORIGINAL ABSOLUTE PATH.
# `uploaded_files.archive_path` holds an ABSOLUTE path on production
# (`/app/uploaded_files/archives/<sha256>.tar.gz`), and
# `UploadService.resolve_archive_path` early-returns on an absolute path — it
# does no rebasing. So restoring the dump and unpacking the tarball somewhere
# else yields a database whose every row points at a file that is not there,
# with no error until a user asks for a download.
#
# The two supported recoveries are therefore:
#   1. extract the archive into a volume mounted at /app/uploaded_files in the
#      restored container — which is what the backup's `tar czf … -C "${MP}" .`
#      shape is designed for; or
#   2. rewrite the column, deliberately, as part of the restore.
# Do not improvise a third. This drill exercises (1) by construction: it compares
# `basename(archive_path)`, so it proves the BYTES survived, and it deliberately
# does not prove the path would resolve — that is a property of how you restore,
# not of the backup. Stated here because the runbook describes the dangling paths
# without saying which of the two you must pick.

set -euo pipefail

REMOTE=root@185.226.65.96
# Overridable for the same reason GNUPGHOME below is (core#970): the negative
# controls for the volume assertions need a directory holding a DELIBERATELY
# broken artifact, and pointing them at the real off-site backups to prove a
# check works is not a trade anyone should make. Cron leaves it unset.
REMOTE_DIR="${REMOTE_DIR:-/opt/datanika-backups}"
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

# ── Volume artifacts (core#970) ──────────────────────────────────────────────
# backup-offsite.sh ships two encrypted volume tarballs beside the dump. Until
# core#970 this drill touched neither, so the dump was proven monthly and the
# user data it REFERENCES was proven never. The tarballs have creation-time proof
# — each ciphertext is round-tripped through `gpg --decrypt | cmp` before it ships
# — but that proves the bytes were good on this box at 03:00. It says nothing
# about whether the copy sitting on Aweb 29 days later still decrypts, which is
# the exact failure this drill was built for after core#725.
VOLUMES="datanika_uploaded_files datanika_dbt_projects"
BACKUP_METRICS="${TEXTFILE_DIR}/datanika_backup.prom"

echo "[$(date)] checking no plaintext volume archives remain off-site..."
PLAIN_TARS=$(ssh ${SSH_OPTS} "${REMOTE}" "ls -1 ${REMOTE_DIR}/*.tar.gz 2>/dev/null | wc -l")
if [ "${PLAIN_TARS}" != 0 ]; then
    echo "[$(date)] RESTORE DRILL FAIL — ${PLAIN_TARS} UNENCRYPTED volume archive(s) off-site."
    echo "  '*.tar.gz' and '*.tar.gz.gpg' are disjoint globs, so this counts plaintext only."
    exit 1
fi

VOL_DRILL_METRICS=""
for VOL in ${VOLUMES}; do
    LATEST_V=$(ssh ${SSH_OPTS} "${REMOTE}" "ls -t ${REMOTE_DIR}/${VOL}_*.tar.gz.gpg 2>/dev/null | head -1")
    if [ -z "${LATEST_V}" ]; then
        echo "[$(date)] RESTORE DRILL FAIL — no off-site archive for ${VOL}."
        echo "  backup-offsite.sh ships one nightly; its absence means the volume half of the"
        echo "  backup is not arriving, which no dump-only check can see."
        exit 1
    fi
    rsync -az -e "ssh ${SSH_OPTS}" "${REMOTE}:${LATEST_V}" "${WORK}/"
    VENC="${WORK}/$(basename "${LATEST_V}")"
    VTAR="${WORK}/$(basename "${LATEST_V}" .gpg)"

    # Same reasoning as the dump: no fallback to plaintext. A fallback would mask
    # exactly the condition being tested.
    if ! gpg --batch --quiet --yes --decrypt "${VENC}" > "${VTAR}" 2>"${WORK}/gpg-${VOL}.log"; then
        echo "[$(date)] RESTORE DRILL FAIL — could not decrypt ${VENC}"
        echo "  The off-site copy of ${VOL} is unreadable with the key on this box."
        tail -10 "${WORK}/gpg-${VOL}.log" || true
        exit 1
    fi

    # Regular-file members, compared against what the BACKUP recorded — not
    # against a constant, and not against anything derived from this same archive.
    # `grep -c` exits 1 on zero matches, which `set -e` would turn into an abort
    # that reads like a transfer failure, so capture it the same way the backup
    # script does.
    if ! MEMBERS=$(tar tvzf "${VTAR}" 2>/dev/null | grep -c '^-'); then
        MEMBERS=0
    fi
    EXPECTED=$(sed -n "s/^datanika_backup_last_files_count{volume=\"${VOL}\"} //p" \
        "${BACKUP_METRICS}" 2>/dev/null | tail -1)
    if [ -z "${EXPECTED}" ]; then
        echo "[$(date)] RESTORE DRILL FAIL — no datanika_backup_last_files_count for ${VOL}"
        echo "  in ${BACKUP_METRICS}. Refusing a verdict: with no recorded expectation there is"
        echo "  nothing to compare the archive against, and 'the tar extracts' is satisfied by"
        echo "  a tar containing no files at all. That is core#725's plans>=5, one layer down."
        exit 1
    fi
    if [ "${MEMBERS}" != "${EXPECTED}" ]; then
        echo "[$(date)] RESTORE DRILL FAIL — ${VOL}: archive holds ${MEMBERS} regular files,"
        echo "  the backup recorded ${EXPECTED}."
        echo "  archive:   $(basename "${LATEST_V}")"
        echo "  metric written: $(sed -n 's/^datanika_backup_last_success_timestamp_seconds //p' "${BACKUP_METRICS}" 2>/dev/null | tail -1)"
        echo "  If the archive is OLDER than that timestamp the off-site copy is stale (the"
        echo "  rsync is failing); if they are the same run, files were lost between tar and"
        echo "  transfer. The two call for opposite responses."
        exit 1
    fi

    mkdir -p "${WORK}/x/${VOL}"
    tar xzf "${VTAR}" -C "${WORK}/x/${VOL}"
    echo "[$(date)] ${VOL}: decrypted, ${MEMBERS} files, extracted"
    VOL_DRILL_METRICS="${VOL_DRILL_METRICS}datanika_restore_drill_volume_files{volume=\"${VOL}\"} ${MEMBERS}
"
done

# ⚠️ THE assertion, and the one that is NOT satisfied by "the tar extracts".
# A tar truncated after a directory header still extracts, still exits 0, and
# still contains a directory — core#725 all over again. So cross-check the two
# artifacts against each other: every non-deleted uploaded_files row in the
# RESTORED dump must have its bytes in the extracted tree. Neither artifact can
# fake that alone.
UPX="${WORK}/x/datanika_uploaded_files"
docker exec "${CONTAINER}" psql -U datanika -d datanika -At \
    -c "select archive_path from uploaded_files where deleted_at is null and archive_path <> ''" \
    </dev/null > "${WORK}/upload_paths.txt"

# `done < file`, never `... | while read`: a pipeline runs the loop in a subshell
# and the counters below would not survive it — the loop would report 0 checked
# and 0 missing, which is a clean pass from a check that measured nothing.
UP_CHECKED=0
UP_MISSING=""
while IFS= read -r P; do
    [ -n "${P}" ] || continue
    UP_CHECKED=$((UP_CHECKED + 1))
    if [ -z "$(find "${UPX}" -type f -name "$(basename "${P}")" -print -quit 2>/dev/null)" ]; then
        UP_MISSING="${UP_MISSING} $(basename "${P}")"
    fi
done < "${WORK}/upload_paths.txt"

if [ "${UP_CHECKED}" -lt 1 ]; then
    echo "[$(date)] RESTORE DRILL FAIL — 0 uploaded_files rows examined."
    echo "  Refusing a verdict rather than reporting a pass: a cross-check over an empty row"
    echo "  set succeeds against any tree, including an empty one. If production genuinely"
    echo "  has no uploads this needs a deliberate decision, not a silent green."
    exit 1
fi
if [ -n "${UP_MISSING}" ]; then
    echo "[$(date)] RESTORE DRILL FAIL — uploaded_files rows whose bytes are NOT in the"
    echo "  off-site archive (${UP_CHECKED} checked):${UP_MISSING}"
    echo "  The dump restored and the archive extracted; they do not agree. A restore from"
    echo "  these two artifacts would produce rows pointing at files that do not exist."
    exit 1
fi
echo "[$(date)] uploaded_files cross-check: ${UP_CHECKED} row(s), 0 missing"

echo "[$(date)] RESTORE DRILL PASS (${LIVE_POPULATED} populated tables all present, ${RESTORED_ROWS} rows, decrypted off-site copy, ${UP_CHECKED} upload(s) cross-checked, ${ELAPSED}s)"

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
    # core#970. Per-volume, because the freshness timestamp above is written on
    # the same successful run and would stay green through a volume that had
    # silently stopped being captured.
    echo "# HELP datanika_restore_drill_volume_files Regular files in the last restored per-volume archive"
    echo "# TYPE datanika_restore_drill_volume_files gauge"
    printf '%s' "${VOL_DRILL_METRICS}"
    echo "# HELP datanika_restore_drill_uploads_verified Non-deleted uploaded_files rows whose bytes were found in the restored archive"
    echo "# TYPE datanika_restore_drill_uploads_verified gauge"
    echo "datanika_restore_drill_uploads_verified ${UP_CHECKED}"
} > "${TMP}"
mv "${TMP}" "${TEXTFILE_DIR}/datanika_restore_drill.prom"
date -u +%Y-%m-%dT%H:%M:%SZ > "${STAMP_FILE}"
