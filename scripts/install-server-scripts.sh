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
CRON_DIR="${CRON_DIR:-/etc/cron.d}"
CRONTAB="${CRONTAB:-crontab}"

# Files this script installs, and nothing else. Destination basename == source basename.
INSTALL=(
    backup-offsite.sh
    restore-drill.sh
    rebuild-parity-drill.sh
    export-prod-settings.sh
    export-watchdog-freshness.sh
    grafana-watchdog.sh
)

# Installed but NOT executable — read as data by the scripts above, never run.
INSTALL_DATA=(
    backup-pubkey.asc
)

# Cron files (core#1477). These go to CRON_DIR, not DEST_DIR, 0644 root, and the destination
# basename DIFFERS from the source: `datanika.cron` -> `datanika`.
#
# 🚨 The rename is not cosmetic. Cron silently ignores any file in /etc/cron.d whose name
# contains a dot, so installing `datanika.cron` there would leave a file that is present,
# correct, readable and never executed — "installed, verified, and never runs", which is the
# same defect class as the stale `backup-offsite.sh` that made this whole script necessary.
# The source keeps the suffix so the file reads as a cron file in the repo.
INSTALL_CRON=(
    datanika.cron
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

# A cron file names commands. Installing it while root's crontab schedules the SAME command
# runs that command twice — two concurrent `backup-offsite.sh` at 03:00, two `restore-drill.sh`
# on the 1st. Nothing downstream would report that; both runs succeed and race.
#
# So this refuses, before writing anything, and names the offenders. Today it passes trivially
# (the cron file holds one command that is in nobody's crontab); it is armed for the migration
# of the five hand-made entries, where the mistake is to add a line here and forget to remove
# it there. Fail-closed: a `crontab` that cannot be read is treated as EMPTY, because refusing
# every deploy on a missing binary would be a guard that only ever costs, and the duplicate it
# protects against requires an entry to exist.
assert_no_duplicate_schedule() {
    local src="$1" existing dupes=""
    existing="$("${CRONTAB}" -l 2>/dev/null)" || existing=""
    [ -n "${existing}" ] || return 0

    # Take the basename of the COMMAND FIELD ONLY — field 7, or field 3 on an `@daily` line.
    #
    # 🚨 The first version grepped every `/`-prefixed token on the line, which on
    # `*/2 * * * * root /opt/.../grafana-watchdog.sh >/dev/null 2>&1` yields `2` (from `*/2`)
    # and `null` (from `/dev/null`) as well as the real command. The live crontab contains
    # `2>&1` on three lines, so the bare substring `2` matched and this guard would have
    # REFUSED EVERY DEPLOY — a guard that always refuses, one careless repair away from
    # permitting the thing it guards. It passed its own test only because that test's fake
    # crontab happened to contain no digit 2. Now driven by the real crontab's shape.
    local cmd
    while read -r cmd; do
        [ -n "${cmd}" ] || continue
        if printf '%s\n' "${existing}" | grep -Fq -- "${cmd}"; then
            dupes="${dupes} ${cmd}"
        fi
    done <<EOF
$(awk '
    /^[[:space:]]*(#|$)/                   { next }
    /^[[:space:]]*[A-Za-z_][A-Za-z0-9_]*=/ { next }
    {
        if ($1 ~ /^@/) c = $3; else c = $7
        if (c ~ /^\//) { n = split(c, p, "/"); print p[n] }
    }
' "${src}" | sort -u)
EOF

    [ -z "${dupes}" ] || fail \
"$(basename "${src}") names command(s) root's crontab ALSO schedules:${dupes}
Installing it would run each of them TWICE. Remove the line(s) from root's crontab first:
  crontab -l | grep -v '<script-name>' | crontab -
then re-run this deploy. See deploy/server/datanika.cron for the two-step migration."
}

install_cron() {
    local name="$1"
    local src="${SRC_DIR}/${name}"
    # `datanika.cron` -> `datanika`. Cron ignores a dotted name in /etc/cron.d, silently.
    local dest_name="${name%.cron}"
    local dest="${CRON_DIR}/${dest_name}"

    [ -f "${src}" ] || fail "${name} is listed for installation but is not in ${SRC_DIR}"
    [ -s "${src}" ] || fail "${name} is EMPTY in ${SRC_DIR} — refusing to install it"
    case "${dest_name}" in
        *.*) fail "cron destination '${dest_name}' contains a dot — cron would ignore it silently" ;;
        *[!A-Za-z0-9_-]*) fail "cron destination '${dest_name}' has a character cron rejects" ;;
    esac

    # Every cron.d line needs a USER field between the schedule and the command. Without it
    # cron logs a parse error and skips the line — present, readable, never executed.
    #
    # 🚨 COUNTING FIELDS DOES NOT WORK, and the first version of this check did exactly that.
    # `*/2 * * * * /path/cmd >/dev/null 2>&1` has EIGHT whitespace-separated fields with no
    # user at all, so a ">= 6 fields" test passes it. The redirections supply the missing
    # field. Assert the SHAPE instead: field 6 must look like a username and field 7 must be
    # an absolute path. Seen failing in test_a_cron_line_missing_its_user_field_is_refused.
    local bad
    bad="$(awk '
        /^[[:space:]]*(#|$)/                       { next }
        /^[[:space:]]*[A-Za-z_][A-Za-z0-9_]*=/     { next }
        {
            if ($1 ~ /^@/) { user = $2; cmd = $3; n = 3 }
            else           { user = $6; cmd = $7; n = 7 }
            if (NF < n || user !~ /^[a-z_][a-z0-9_-]*$/ || cmd !~ /^\//)
                printf "line %d: %s\n", NR, $0
        }
    ' "${src}")"
    [ -z "${bad}" ] || fail "${name} has schedule line(s) with no user field between the schedule and the command — cron skips those silently:
${bad}"

    assert_no_duplicate_schedule "${src}"

    mkdir -p "${CRON_DIR}" || fail "cannot create ${CRON_DIR}"
    local before="absent"
    [ -f "${dest}" ] && before=$(sha256sum "${dest}" | cut -d' ' -f1)
    local want
    want=$(sha256sum "${src}" | cut -d' ' -f1)

    local tmp="${dest}.install.$$"
    cp "${src}" "${tmp}" || fail "copy of ${name} failed"
    chmod 0644 "${tmp}" || fail "chmod 0644 on ${name} failed"
    mv -f "${tmp}" "${dest}" || fail "atomic rename of ${name} failed"

    local after
    after=$(sha256sum "${dest}" | cut -d' ' -f1)
    [ "${after}" = "${want}" ] || fail "${name} installed but its hash does not match the repo copy (repo=${want} box=${after})"

    installed=$((installed + 1))
    if [ "${before}" = "${want}" ]; then
        echo "  unchanged  ${dest_name}  ${want:0:8}  (${CRON_DIR})"
    else
        changed=$((changed + 1))
        echo "  INSTALLED  ${dest_name}  ${before:0:8} -> ${want:0:8}  (${CRON_DIR})"
    fi
}

echo "install-server-scripts: ${SRC_DIR} -> ${DEST_DIR}"
for f in "${INSTALL[@]}"; do install_one "$f" 0755; done
for f in "${INSTALL_DATA[@]}"; do install_one "$f" 0644; done
for f in "${INSTALL_CRON[@]}"; do install_cron "$f"; done

# An empty run means the list was emptied or the loop stopped matching — the shape that
# turns this step into a no-op that reports success forever (the same failure this script
# exists to end). Refuse it.
[ "${installed}" -ge 5 ] || fail "only ${installed} file(s) installed; expected at least 5 — the INSTALL list looks truncated"

echo "install-server-scripts: ${installed} file(s) verified, ${changed} changed"
