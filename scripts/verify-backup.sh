#!/bin/bash
# Verify the latest PostgreSQL backup by restoring to a temporary database.
#
# Usage: docker exec datanika-postgres /scripts/verify-backup.sh

set -euo pipefail

BACKUP_DIR="${BACKUP_DIR:-/backups}"
DB_USER="${POSTGRES_USER:-datanika}"
VERIFY_DB="backup_verify_test"

# Find latest daily backup
LATEST=$(ls -t "${BACKUP_DIR}/daily/"*.sql.gz 2>/dev/null | head -1)

if [ -z "${LATEST}" ]; then
    echo "[ERROR] No backup files found in ${BACKUP_DIR}/daily/"
    exit 1
fi

echo "[$(date)] Verifying backup: ${LATEST}"

# Create temp database
psql -U "${DB_USER}" -d postgres -c "DROP DATABASE IF EXISTS ${VERIFY_DB};" 2>/dev/null
psql -U "${DB_USER}" -d postgres -c "CREATE DATABASE ${VERIFY_DB};"

# Restore
if gunzip -c "${LATEST}" | psql -U "${DB_USER}" -d "${VERIFY_DB}" > /dev/null 2>&1; then
    # Count tables to verify
    TABLE_COUNT=$(psql -U "${DB_USER}" -d "${VERIFY_DB}" -t -c \
        "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public';")
    echo "[$(date)] Restore OK — ${TABLE_COUNT} tables found"
    RESULT=0
else
    echo "[$(date)] Restore FAILED"
    RESULT=1
fi

# Cleanup
psql -U "${DB_USER}" -d postgres -c "DROP DATABASE IF EXISTS ${VERIFY_DB};" 2>/dev/null

exit ${RESULT}
