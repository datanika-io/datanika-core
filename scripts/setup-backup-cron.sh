#!/bin/bash
# Install cron job for daily PostgreSQL backups on the host.
# Run this once on the Hetzner server:
#   bash /opt/datanika/datanika/scripts/setup-backup-cron.sh
#
# Creates:
#   - Daily backup at 3:00 AM UTC
#   - Weekly verification at 4:00 AM UTC on Sundays

set -euo pipefail

CRON_BACKUP='0 3 * * * docker exec datanika-postgres /bin/bash /scripts/backup-postgres.sh >> /var/log/datanika-backup.log 2>&1'
CRON_VERIFY='0 4 * * 0 docker exec datanika-postgres /bin/bash /scripts/verify-backup.sh >> /var/log/datanika-backup.log 2>&1'

# Add cron entries if not already present
(crontab -l 2>/dev/null || true) | grep -v "backup-postgres.sh" | grep -v "verify-backup.sh" > /tmp/datanika-cron
echo "${CRON_BACKUP}" >> /tmp/datanika-cron
echo "${CRON_VERIFY}" >> /tmp/datanika-cron
crontab /tmp/datanika-cron
rm /tmp/datanika-cron

echo "Backup cron jobs installed:"
crontab -l | grep datanika
