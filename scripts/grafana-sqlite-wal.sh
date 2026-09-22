#!/usr/bin/env bash
# Convert Grafana's SQLite database to write-ahead logging, ONCE, while Grafana is stopped.
#
#   bash scripts/grafana-sqlite-wal.sh          # on the box, from /opt/datanika/datanika
#
# core#1476. Grafana's SQLite runs in rollback-journal mode, where readers and writers block each
# other exclusively; under this box's IO load (a 9-20 % full-stall share all day) that produces
# `SQLITE_BUSY` bursts which, when three retries are exhausted, turn into failed rule evaluations
# and — through `execErrState: Alerting`, deliberately — into alerts. On 2026-09-22 the retry
# ceiling was reached four times in half an hour.
#
# WHY A CONVERSION AND NOT THE SETTING. `GF_DATABASE_WAL=true` is set and is INERT: Grafana 13.1.0
# logs `Using SQLite driver driver=modernc.org/sqlite`, and a fresh database created with the
# setting on is `journal_mode=delete` (probe on the exact production image, `PRAGMA page_size` 4096
# as the control). But journal_mode=WAL is PERSISTENT IN THE FILE HEADER, and the same probe
# showed Grafana 13.1.0 keeps it: a database converted while stopped runs with `-wal`/`-shm`
# present and still reads `wal` after Grafana stops — with the setting on AND with it off.
#
# Safe by construction:
#   * refuses while the Grafana container is running (changing the mode needs exclusive access);
#   * idempotent — reads the mode first and does nothing if it is already `wal`;
#   * backs the file up (`grafana.db.pre-wal`) before converting, and converts only after that;
#   * verifies `PRAGMA quick_check` and re-reads the mode in a FRESH process, because the mode is a
#     property of the file, not of the connection that set it (coordinator rule 17);
#   * hands every grafana.db* file back to Grafana's uid (472, gid 0) — this runs as root.
# The caller (deploy-pointer.yml) treats any non-zero exit as a WARNING and starts Grafana anyway,
# so the worst case is today's behaviour, never a Grafana that does not come back.
#
# Uses the app image already on the box (python3 + the stdlib sqlite3 module), so it pulls
# nothing. Exit: 0 converted or already WAL · 3 refused · 4 failed (file left as it was).
set -uo pipefail

C="${GRAFANA_CONTAINER:-datanika-grafana}"
IMG="${PY_IMAGE:-ghcr.io/datanika-io/datanika-core:${DATANIKA_IMAGE_TAG:-latest}}"
DB=/g/grafana.db

running="$(docker inspect -f '{{.State.Running}}' "$C" 2>/dev/null)" || {
  echo "REFUSING: no container named $C"; exit 3; }
if [ "$running" != "false" ]; then
  echo "REFUSING: $C is running ($running) — the journal mode can only be changed with exclusive access"
  exit 3
fi
VOL="$(docker inspect -f '{{range .Mounts}}{{if eq .Destination "/var/lib/grafana"}}{{.Name}}{{end}}{{end}}' "$C")"
[ -n "$VOL" ] || { echo "REFUSING: $C has no named volume at /var/lib/grafana"; exit 3; }

py() { docker run --rm --network none --user 0:0 -v "$VOL":/g --entrypoint python3 "$IMG" -c "$1"; }

before="$(py "import sqlite3; print(sqlite3.connect('$DB').execute('PRAGMA journal_mode').fetchone()[0])")" || {
  echo "FAILED: could not read the journal mode of $DB in $VOL"; exit 4; }
echo "grafana.db journal_mode before: $before (volume $VOL)"
if [ "$before" = "wal" ]; then
  echo "already WAL — nothing to do"
  exit 0
fi

py "import shutil; shutil.copy2('$DB', '$DB.pre-wal')" || { echo "FAILED: backup — not converting"; exit 4; }
echo "backed up to grafana.db.pre-wal"

result="$(py "
import sqlite3
c = sqlite3.connect('$DB')
mode = c.execute('PRAGMA journal_mode=WAL').fetchone()[0]
check = c.execute('PRAGMA quick_check').fetchone()[0]
c.close()
print(mode, check)")" || { echo "FAILED: conversion raised"; exit 4; }
echo "conversion: $result"
[ "$result" = "wal ok" ] || { echo "FAILED: conversion did not verify ($result)"; exit 4; }

py "
import glob, os
for p in glob.glob('$DB*'):
    os.chown(p, 472, 0)
print('chowned', len(glob.glob('$DB*')), 'file(s) to 472:0')" || { echo "FAILED: chown"; exit 4; }

after="$(py "import sqlite3; print(sqlite3.connect('$DB').execute('PRAGMA journal_mode').fetchone()[0])")"
echo "journal_mode read back in a fresh process: $after"
[ "$after" = "wal" ] || { echo "FAILED: the file does not read back as wal"; exit 4; }
echo "grafana.db is now in WAL mode"
