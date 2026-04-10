#!/bin/bash
# Daily PostgreSQL backup script with retention policy.
#
# Retention:
#   - Daily backups: kept for 7 days
#   - Weekly backups (Sunday): kept for 4 weeks
#   - Monthly backups (1st of month): kept for 3 months
#
# Usage: Run via cron or docker exec:
#   docker exec datanika-postgres /scripts/backup-postgres.sh
#
# Environment variables (from .env.docker):
#   POSTGRES_USER, POSTGRES_DB, BACKUP_DIR (default: /backups)

set -euo pipefail

BACKUP_DIR="${BACKUP_DIR:-/backups}"
DATE=$(date +%Y-%m-%d_%H%M%S)
DAY_OF_WEEK=$(date +%u)  # 1=Monday, 7=Sunday
DAY_OF_MONTH=$(date +%d)

DB_USER="${POSTGRES_USER:-datanika}"
DB_NAME="${POSTGRES_DB:-datanika}"

# Ensure backup directory structure
mkdir -p "${BACKUP_DIR}/daily" "${BACKUP_DIR}/weekly" "${BACKUP_DIR}/monthly"

DAILY_FILE="${BACKUP_DIR}/daily/${DB_NAME}_${DATE}.sql.gz"

echo "[$(date)] Starting backup of ${DB_NAME}..."

# Create compressed backup
pg_dump -U "${DB_USER}" -d "${DB_NAME}" --no-owner --no-privileges | gzip > "${DAILY_FILE}"

FILESIZE=$(du -h "${DAILY_FILE}" | cut -f1)
echo "[$(date)] Backup created: ${DAILY_FILE} (${FILESIZE})"

# Copy to weekly on Sundays
if [ "${DAY_OF_WEEK}" = "7" ]; then
    cp "${DAILY_FILE}" "${BACKUP_DIR}/weekly/${DB_NAME}_weekly_${DATE}.sql.gz"
    echo "[$(date)] Weekly backup copied"
fi

# Copy to monthly on 1st of month
if [ "${DAY_OF_MONTH}" = "01" ]; then
    cp "${DAILY_FILE}" "${BACKUP_DIR}/monthly/${DB_NAME}_monthly_${DATE}.sql.gz"
    echo "[$(date)] Monthly backup copied"
fi

# --- Retention cleanup ---

# Daily: keep 7 days
find "${BACKUP_DIR}/daily" -name "*.sql.gz" -mtime +7 -delete 2>/dev/null && \
    echo "[$(date)] Cleaned daily backups older than 7 days" || true

# Weekly: keep 4 weeks (28 days)
find "${BACKUP_DIR}/weekly" -name "*.sql.gz" -mtime +28 -delete 2>/dev/null && \
    echo "[$(date)] Cleaned weekly backups older than 28 days" || true

# Monthly: keep 3 months (90 days)
find "${BACKUP_DIR}/monthly" -name "*.sql.gz" -mtime +90 -delete 2>/dev/null && \
    echo "[$(date)] Cleaned monthly backups older than 90 days" || true

echo "[$(date)] Backup complete"
