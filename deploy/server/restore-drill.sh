#!/bin/bash
# Restore drill — proves the OFF-SITE backups actually restore (an untested backup
# is not a backup). Pulls the latest dump from Aweb, restores it into a throwaway
# postgres container, verifies row counts, tears everything down. Never touches
# the prod postgres/volume.
#
# Deployed to:   /opt/datanika/scripts/restore-drill.sh on 185.25.22.188
# Cron (monthly): 0 5 1 * * /opt/datanika/scripts/restore-drill.sh >> /var/log/datanika-restore-drill.log 2>&1
# Canonical copy: plans/infra/scripts/restore-drill.sh

set -euo pipefail

REMOTE=root@185.226.65.96
REMOTE_DIR=/opt/datanika-backups
SSH_KEY=/root/.ssh/aweb_backup
SSH_OPTS="-i ${SSH_KEY} -o StrictHostKeyChecking=accept-new -o ConnectTimeout=20"
WORK=/tmp/restore-drill.$$
CONTAINER=datanika-restore-test
STAMP_FILE=/opt/datanika/monitoring/restore-drill-last-success.txt

cleanup() { docker rm -f "${CONTAINER}" >/dev/null 2>&1 || true; rm -rf "${WORK}"; }
trap cleanup EXIT

mkdir -p "${WORK}"

echo "[$(date)] fetching latest off-site dump from ${REMOTE}..."
LATEST=$(ssh ${SSH_OPTS} "${REMOTE}" "ls -t ${REMOTE_DIR}/*.sql.gz 2>/dev/null | head -1")
if [ -z "${LATEST}" ]; then echo "[$(date)] ERROR: no off-site dump found"; exit 1; fi
echo "[$(date)] latest: ${LATEST}"
rsync -az -e "ssh ${SSH_OPTS}" "${REMOTE}:${LATEST}" "${WORK}/"
DUMP="${WORK}/$(basename "${LATEST}")"

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
gunzip -c "${DUMP}" | docker exec -i "${CONTAINER}" psql -U datanika -d datanika -v ON_ERROR_STOP=0 -q \
    > "${RESTORE_LOG}" 2>&1 || true

# Verify: plan rows must have restored (prod is near-empty pre-launch, so plans is
# the canonical signal — 5 V2 rows). Fail the drill if the count is wrong.
PLANS=$(docker exec "${CONTAINER}" psql -U datanika -d datanika -t -A -c "SELECT count(*) FROM plans;" 2>/dev/null || echo 0)
ORGS=$(docker exec "${CONTAINER}" psql -U datanika -d datanika -t -A -c "SELECT count(*) FROM organizations;" 2>/dev/null || echo "?")
USERS=$(docker exec "${CONTAINER}" psql -U datanika -d datanika -t -A -c "SELECT count(*) FROM users;" 2>/dev/null || echo "?")
ERRS=$(grep -ci "error" "${RESTORE_LOG}" 2>/dev/null || echo 0)

echo "[$(date)] restored: plans=${PLANS} organizations=${ORGS} users=${USERS} | restore-log errors=${ERRS}"

if [ "${PLANS}" -ge 5 ] 2>/dev/null; then
    echo "[$(date)] RESTORE DRILL PASS (plans=${PLANS})"
    mkdir -p "$(dirname "${STAMP_FILE}")"
    date -u +%Y-%m-%dT%H:%M:%SZ > "${STAMP_FILE}"
else
    echo "[$(date)] RESTORE DRILL FAIL — plans=${PLANS} (expected >= 5). Restore-log tail:"
    tail -20 "${RESTORE_LOG}"
    exit 1
fi
