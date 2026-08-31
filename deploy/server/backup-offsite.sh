#!/bin/bash
# Nightly Postgres backup for the pointer.gr prod box, WITH off-site copy to Aweb.
#
# Root-cause fix for the 2026-07 outage: the old Hetzner setup kept every backup
# ON Hetzner, so the account termination wiped the DB *and* all backups. This
# script keeps a local copy AND ships each dump to a second host (Aweb).
#
# Deployed to:   /opt/datanika/scripts/backup-offsite.sh on 185.25.22.188
# Cron (nightly): 0 3 * * * /opt/datanika/scripts/backup-offsite.sh >> /var/log/datanika-backup.log 2>&1
# Canonical copy: plans/infra/scripts/backup-offsite.sh (this file)

set -euo pipefail

LOCAL_DIR=/opt/datanika/backups
REMOTE=root@185.226.65.96          # Aweb (separate provider — the off-site leg)
REMOTE_DIR=/opt/datanika-backups
SSH_KEY=/root/.ssh/aweb_backup
SSH_OPTS="-i ${SSH_KEY} -o StrictHostKeyChecking=accept-new -o ConnectTimeout=20"
DB_USER=datanika
DB_NAME=datanika
LOCAL_KEEP_DAYS=7
REMOTE_KEEP_DAYS=30
TEXTFILE_DIR=/opt/datanika/node_textfile   # node-exporter textfile collector (backup freshness metric)

STAMP=$(date +%Y-%m-%d_%H%M%S)
FILE="${LOCAL_DIR}/${DB_NAME}_${STAMP}.sql.gz"
mkdir -p "${LOCAL_DIR}"

echo "[$(date)] dumping ${DB_NAME} from container..."
# docker exec (NOT a compose-based path — the old setup silently wrote 20-byte
# empty files because the compose path was wrong).
docker exec datanika-postgres pg_dump -U "${DB_USER}" -d "${DB_NAME}" \
    --no-owner --no-privileges | gzip > "${FILE}"

# Sanity gate: a real dump of even an empty schema is > 1 KB gzipped. Fail loud
# rather than silently shipping an empty backup (the trap that bit us before).
SIZE=$(stat -c%s "${FILE}")
if [ "${SIZE}" -lt 1000 ]; then
    echo "[$(date)] ERROR: dump is only ${SIZE} bytes — aborting, NOT overwriting off-site copies"
    exit 1
fi
echo "[$(date)] local dump ok: ${FILE} (${SIZE} bytes)"

# Off-site leg — ship to Aweb.
echo "[$(date)] shipping off-site to ${REMOTE}:${REMOTE_DIR}..."
ssh ${SSH_OPTS} "${REMOTE}" "mkdir -p ${REMOTE_DIR}"
rsync -az -e "ssh ${SSH_OPTS}" "${FILE}" "${REMOTE}:${REMOTE_DIR}/"
echo "[$(date)] off-site copy delivered"

# Retention.
find "${LOCAL_DIR}" -name '*.sql.gz' -mtime +${LOCAL_KEEP_DAYS} -delete 2>/dev/null || true
ssh ${SSH_OPTS} "${REMOTE}" "find ${REMOTE_DIR} -name '*.sql.gz' -mtime +${REMOTE_KEEP_DAYS} -delete 2>/dev/null || true"

# Prometheus freshness metric (node-exporter textfile collector). Only reached on
# full success (set -e aborts earlier on any failure), so a stale timestamp ==
# backups have stopped -> the "Backup Stale" Grafana alert fires after 26h.
mkdir -p "${TEXTFILE_DIR}"
TMP="${TEXTFILE_DIR}/datanika_backup.prom.$$"
{
    echo "# HELP datanika_backup_last_success_timestamp_seconds Unix time of last successful off-site backup"
    echo "# TYPE datanika_backup_last_success_timestamp_seconds gauge"
    echo "datanika_backup_last_success_timestamp_seconds $(date +%s)"
    echo "# HELP datanika_backup_last_size_bytes Gzipped size of the last dump"
    echo "# TYPE datanika_backup_last_size_bytes gauge"
    echo "datanika_backup_last_size_bytes ${SIZE}"
} > "${TMP}"
mv "${TMP}" "${TEXTFILE_DIR}/datanika_backup.prom"

echo "[$(date)] backup complete (local keep ${LOCAL_KEEP_DAYS}d, off-site keep ${REMOTE_KEEP_DAYS}d)"
