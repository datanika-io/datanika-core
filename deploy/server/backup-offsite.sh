#!/bin/bash
# Nightly Postgres backup for the pointer.gr prod box, WITH off-site copy to Aweb.
#
# Root-cause fix for the 2026-07 outage: the old Hetzner setup kept every backup
# ON Hetzner, so the account termination wiped the DB *and* all backups. This
# script keeps a local copy AND ships each dump to a second host (Aweb).
#
# Deployed to:   /opt/datanika/scripts/backup-offsite.sh on 185.25.22.188
# Cron (nightly): 0 3 * * * /opt/datanika/scripts/backup-offsite.sh >> /var/log/datanika-backup.log 2>&1
# Canonical copy: deploy/server/backup-offsite.sh (datanika-core)
# Invariants pinned by: tests/test_deploy/test_backup_encryption.py
#
# ⚠️ NOTHING DEPLOYS THIS FILE. core#747: `deploy/server/` is referenced by no
# workflow. The copy that runs is the hand-installed one at the path above, and
# the two drift silently. After changing this file, install it and verify the
# box and git agree byte-for-byte:
#     sha256sum /opt/datanika/scripts/backup-offsite.sh
#
# ── Encryption at rest (core#675, 2026-08-31) ────────────────────────────────
# The off-site leg is encrypted; the local copy is NOT. That asymmetry is
# deliberate and is the whole design:
#
#   * Aweb is a shared, general-purpose box — a VPN endpoint, three unrelated
#     bots, a public web server and Plausible — and it is unpatched
#     (landing#389). It has no business being able to read our users' rows.
#     It receives ciphertext only, and holds NO key of any kind.
#   * The app box already holds the live database and every secret in
#     .env.docker. Encrypting a local dump *here* protects nothing, and it
#     would cost the fast local restore path. So the local copy stays plaintext.
#
# The private key lives on this box (root-only keyring) so the monthly restore
# drill can decrypt unattended — an encrypted backup nobody ever decrypts is
# exactly the "green that proves nothing" this project keeps getting burned by.
# 🚨 Do NOT "harden" this by removing the private key from the box: that breaks
# the drill, and the threat this closes is an *Aweb* compromise, not a compromise
# of the box that already has the live database.
#
# 🚨 The private key is ALSO escrowed off-box (secrets/datanika-backup-privkey.asc
# on the founder's dev machine). Without that, losing this box makes every
# off-site copy permanently unreadable — which would recreate core#748 in a
# worse form. See docs/runbooks/RUNBOOK_RESTORE_PREREQUISITES.md.

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
GPG_RECIPIENT=backup@datanika.io           # fingerprint BA853D54E247C0B99FD06116306A85F70420B8AA
export GNUPGHOME="${GNUPGHOME:-/root/.gnupg}"   # overridable: cron leaves it unset, but a test must be able to remove the key

STAMP=$(date +%Y-%m-%d_%H%M%S)
FILE="${LOCAL_DIR}/${DB_NAME}_${STAMP}.sql.gz"
ENC="${FILE}.gpg"
mkdir -p "${LOCAL_DIR}"

# The local ciphertext is a shipping artifact, not something we keep — the
# plaintext next to it is the local restore path. Remove it however we exit.
cleanup() { rm -f "${ENC}"; }
trap cleanup EXIT

# Pre-flight: fail BEFORE dumping if we cannot encrypt. Discovering a missing
# key after a 5-minute pg_dump wastes the dump; more importantly, a run that
# aborts here leaves the freshness metric stale, which is what raises the alarm.
if ! gpg --list-keys "${GPG_RECIPIENT}" >/dev/null 2>&1; then
    echo "[$(date)] ERROR: no gpg key for ${GPG_RECIPIENT} in ${GNUPGHOME} — refusing to run."
    echo "  The off-site leg must be encrypted (core#675). Import the public key:"
    echo "    gpg --import /opt/datanika/scripts/backup-pubkey.asc"
    exit 1
fi

echo "[$(date)] dumping ${DB_NAME} from container..."
# docker exec (NOT a compose-based path — the old setup silently wrote 20-byte
# empty files because the compose path was wrong).
docker exec datanika-postgres pg_dump -U "${DB_USER}" -d "${DB_NAME}" \
    --no-owner --no-privileges | gzip > "${FILE}"

# Sanity gate: a real dump of even an empty schema is > 1 KB gzipped. Fail loud
# rather than silently shipping an empty backup (the trap that bit us before).
#
# ⚠️ This deliberately measures the PLAINTEXT, before encryption. core#675 asked
# for exactly this: gpg adds a ~600-byte header, so an encrypted empty dump is
# NOT obviously small and a floor applied to the ciphertext would be a weaker
# check wearing the same number.
SIZE=$(stat -c%s "${FILE}")
if [ "${SIZE}" -lt 1000 ]; then
    echo "[$(date)] ERROR: dump is only ${SIZE} bytes — aborting, NOT overwriting off-site copies"
    exit 1
fi
echo "[$(date)] local dump ok: ${FILE} (${SIZE} bytes, plaintext — this box holds the live DB anyway)"

# Encrypt for the off-site leg. --trust-model always because the key is ours and
# unsigned by any web of trust; without it gpg refuses in batch mode.
echo "[$(date)] encrypting off-site copy to ${GPG_RECIPIENT}..."
gpg --batch --yes --quiet --trust-model always \
    --recipient "${GPG_RECIPIENT}" --output "${ENC}" --encrypt "${FILE}"

# 🚨 Round-trip the ciphertext BEFORE shipping it. An encryption step that
# silently produced garbage would otherwise ship nightly for 30 days and only be
# discovered during a recovery — the exact failure mode the July outage taught
# us. This costs ~0.1s on a 27 KB dump.
if ! gpg --batch --quiet --decrypt "${ENC}" 2>/dev/null | cmp -s - "${FILE}"; then
    echo "[$(date)] ERROR: ciphertext does not decrypt back to the dump — NOT shipping."
    exit 1
fi
ENC_SIZE=$(stat -c%s "${ENC}")
echo "[$(date)] encryption verified by round-trip: ${ENC} (${ENC_SIZE} bytes)"

# Off-site leg — ship the CIPHERTEXT to Aweb. Aweb never receives a key.
echo "[$(date)] shipping off-site to ${REMOTE}:${REMOTE_DIR}..."
ssh ${SSH_OPTS} "${REMOTE}" "mkdir -p ${REMOTE_DIR} && chmod 700 ${REMOTE_DIR}"
rsync -az -e "ssh ${SSH_OPTS}" "${ENC}" "${REMOTE}:${REMOTE_DIR}/"
echo "[$(date)] off-site copy delivered (encrypted)"

# Retention.
# ⚠️ '*.sql.gz' and '*.sql.gz.gpg' are DISJOINT globs — a name ending in .gpg is
# not matched by '*.sql.gz'. Both patterns are therefore needed off-site, and the
# plaintext sweep is what removes any legacy pre-encryption dump that is still
# there. It is a safety net, not the migration: the 31 legacy plaintext dumps
# were converted in place on 2026-08-31.
find "${LOCAL_DIR}" -name '*.sql.gz' -mtime +${LOCAL_KEEP_DAYS} -delete 2>/dev/null || true
ssh ${SSH_OPTS} "${REMOTE}" "find ${REMOTE_DIR} -name '*.sql.gz.gpg' -mtime +${REMOTE_KEEP_DAYS} -delete 2>/dev/null || true"
ssh ${SSH_OPTS} "${REMOTE}" "find ${REMOTE_DIR} -name '*.sql.gz' -delete 2>/dev/null || true"

# Prometheus freshness metric (node-exporter textfile collector). Only reached on
# full success (set -e aborts earlier on any failure), so a stale timestamp ==
# backups have stopped -> the "Backup Stale" Grafana alert fires after 26h.
mkdir -p "${TEXTFILE_DIR}"
TMP="${TEXTFILE_DIR}/datanika_backup.prom.$$"
{
    echo "# HELP datanika_backup_last_success_timestamp_seconds Unix time of last successful off-site backup"
    echo "# TYPE datanika_backup_last_success_timestamp_seconds gauge"
    echo "datanika_backup_last_success_timestamp_seconds $(date +%s)"
    echo "# HELP datanika_backup_last_size_bytes Gzipped size of the last dump (plaintext, pre-encryption)"
    echo "# TYPE datanika_backup_last_size_bytes gauge"
    echo "datanika_backup_last_size_bytes ${SIZE}"
    echo "# HELP datanika_backup_last_encrypted_size_bytes Size of the encrypted artifact actually shipped off-site"
    echo "# TYPE datanika_backup_last_encrypted_size_bytes gauge"
    echo "datanika_backup_last_encrypted_size_bytes ${ENC_SIZE}"
} > "${TMP}"
mv "${TMP}" "${TEXTFILE_DIR}/datanika_backup.prom"

echo "[$(date)] backup complete (local keep ${LOCAL_KEEP_DAYS}d plaintext, off-site keep ${REMOTE_KEEP_DAYS}d encrypted)"
