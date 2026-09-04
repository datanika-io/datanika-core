#!/usr/bin/env bash
# Install the box-side scripts that live in `deploy/server/` (core#747).
#
# THE PROBLEM THIS CLOSES
# -----------------------
# `deploy/server/` was applied by NO workflow. The deploy tarball ships the whole tree to
# `/opt/datanika/datanika/deploy/server/`, which makes it *worse* rather than better: the
# correct content sits on the box at a path nothing reads, beside the stale copy that
# actually runs from cron. On 2026-09-04 that was measured — `backup-offsite.sh` on the box
# was sha `874c8a3c…` while `origin/master` had `58baca17…`, i.e. core#1017 had been merged,
# promoted, and was NOT running. Three files were hand-installed that day; hand-installing is
# the thing this script replaces.
#
# This is the third recorded instance of "config that no deploy step applies is config that
# does not apply" — after `postgres-exporter`/`cadvisor`/`node-exporter` running six weeks
# unreachable by any config change (core#616), and the restore-drill fix needing a hand
# install (core#725).
#
# WHY AN EXPLICIT LIST AND NOT A GLOB
# -----------------------------------
# Installing a root-owned script that cron executes on the production box is a deliberate
# act. A glob makes it a side effect of adding a file — and `deploy/server/` legitimately
# holds files that must NOT be installed there: `deploy-pointer.sh` is a dev-machine
# fallback, and `networkd-99-datanika-dns.conf` is hand-applied on purpose, because a pushed
# network config that fails leaves the box unreachable with no way in to revert it.
#
# `tests/test_deploy/test_server_script_coverage.py` requires every file in `deploy/server/`
# to be either in INSTALL below or in that test's documented exemption map. So adding a file
# and forgetting it fails CI; it does not silently do nothing.
#
# IDEMPOTENT. Safe to run on every deploy — it is a copy plus a hash comparison.

set -uo pipefail

SRC_DIR="${SRC_DIR:-/opt/datanika/datanika/deploy/server}"
DEST_DIR="${DEST_DIR:-/opt/datanika/scripts}"

# Files this script installs, and nothing else. Destination basename == source basename.
INSTALL=(
    backup-offsite.sh
    restore-drill.sh
    rebuild-parity-drill.sh
    export-prod-settings.sh
)

# Installed but NOT executable — read as data by the scripts above, never run.
INSTALL_DATA=(
    backup-pubkey.asc
)

fail() { echo "install-server-scripts: ERROR: $*" >&2; exit 1; }

[ -d "${SRC_DIR}" ] || fail "source directory ${SRC_DIR} does not exist — did the transfer step run?"
mkdir -p "${DEST_DIR}" || fail "cannot create ${DEST_DIR}"

installed=0
changed=0

install_one() {
    local name="$1" mode="$2"
    local src="${SRC_DIR}/${name}" dest="${DEST_DIR}/${name}"

    [ -f "${src}" ] || fail "${name} is listed for installation but is not in ${SRC_DIR}"
    [ -s "${src}" ] || fail "${name} is EMPTY in ${SRC_DIR} — refusing to install it over a working copy"

    local before="absent"
    [ -f "${dest}" ] && before=$(sha256sum "${dest}" | cut -d' ' -f1)
    local want
    want=$(sha256sum "${src}" | cut -d' ' -f1)

    # Write to a temp file in the SAME directory and rename, so a cron job that fires
    # mid-install sees either the whole old file or the whole new one — never a half-written
    # script. `install` alone is not atomic and `backup-offsite.sh` runs unattended at 03:00.
    local tmp="${dest}.install.$$"
    cp "${src}" "${tmp}" || fail "copy of ${name} failed"
    chmod "${mode}" "${tmp}" || fail "chmod ${mode} on ${name} failed"
    mv -f "${tmp}" "${dest}" || fail "atomic rename of ${name} failed"

    # Assert the OUTCOME, not the exit code of the copy. A silent truncation, a full disk or
    # a filesystem that quietly rejected the write all produce a successful `cp`.
    local after
    after=$(sha256sum "${dest}" | cut -d' ' -f1)
    [ "${after}" = "${want}" ] || fail "${name} installed but its hash does not match the repo copy (repo=${want} box=${after})"

    installed=$((installed + 1))
    if [ "${before}" = "${want}" ]; then
        echo "  unchanged  ${name}  ${want:0:8}"
    else
        changed=$((changed + 1))
        echo "  INSTALLED  ${name}  ${before:0:8} -> ${want:0:8}"
    fi
}

echo "install-server-scripts: ${SRC_DIR} -> ${DEST_DIR}"
for f in "${INSTALL[@]}"; do install_one "$f" 0755; done
for f in "${INSTALL_DATA[@]}"; do install_one "$f" 0644; done

# An empty run means the list was emptied or the loop stopped matching — the shape that
# turns this step into a no-op that reports success forever (the same failure this script
# exists to end). Refuse it.
[ "${installed}" -ge 5 ] || fail "only ${installed} file(s) installed; expected at least 5 — the INSTALL list looks truncated"

echo "install-server-scripts: ${installed} file(s) verified, ${changed} changed"
