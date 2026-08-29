#!/usr/bin/env bash
#
# Sync the Apache vhosts from the repo onto the box. (core#607)
#
# Extracted from the inline `Sync Apache vhosts from the repo` step in
# .github/workflows/deploy-pointer.yml. It lives in a file for the same reason
# scripts/deploy-bluegreen.sh does: a heredoc inside a workflow cannot be executed by a
# test, and "we read it and it looked right" is exactly how core#603 stayed broken for
# five weeks.
#
# Why production routing is deployed at all
# -----------------------------------------
# It used to exist only on the box and in an untracked plans/ folder. When the previous
# host was terminated (2026-07-14) the server, data and backups went together; the vhost
# survived only because someone happened to have a local copy.
#
# Never touches webdav.conf -- that vhost belongs to a co-tenant on this shared box, which
# also carries the founder's VPN -- and never restarts Apache, only graceful.
#
# The defect this rewrite fixes (core#607)
# ----------------------------------------
# `sync_one` overwrote a vhost in place after backing it up, and was called twice. Its
# guards aborted with `exit 1`. The restore loop lived ONLY inside the `configtest`-failed
# branch. So:
#
#     sync_one app.datanika.io.conf ...        -> prod vhost REPLACED on disk
#     sync_one staging-app.datanika.io.conf .. -> guard fails, exit 1
#     (configtest never runs, so the restore never runs)
#
# ...leaves the production vhost replaced and unrestored. **Apache keeps serving from
# memory**, so nothing looks wrong. The damage surfaces at the next reload for an
# unrelated reason -- certbot renewal, a logrotate postrotate, the next deploy -- at which
# point a half-synced routing table is loaded and nobody connects it to this step.
#
# Same shape as core#603: recovery that exists, is correct, and is unreachable from the
# paths that need it. As there: recovery hangs off a single **EXIT** trap, which covers
# `set -e`, an explicit `exit`, and signals; HUP/INT/TERM are trapped so a cancelled CD job
# unwinds; there is deliberately **no ERR trap**, because it fires on a strict subset and
# its presence is what makes an unreachable recovery look like a working one. `fatal()` is
# the only permitted abort.
#
# Two further defects found on the box while fixing this
# ------------------------------------------------------
# 1. Backups went to a PREDICTABLE `/tmp/<name>.bak`, and the restore loop trusted whatever
#    was there with `[ -f "/tmp/$d.bak" ]`. A backup left by an EARLIER deploy would be
#    restored over a vhost this run never touched -- reverting a good file using stale
#    content. Backups now go to a fresh `mktemp -d` and only files written by THIS run are
#    ever restored.
# 2. `/tmp/zapp-datanika-io.conf.bak` on prod was a **dangling relative symlink** dated
#    2026-07-17 -- the fossil of the `cp -a` bug the old comment says was fixed. `[ -f ]`
#    follows symlinks, so it was false, so the restore silently skipped the production
#    vhost and printed nothing. The one path that was supposed to work had already been
#    a no-op for six weeks. A private directory plus an explicit regular-file check closes
#    both.
#
# Usage:  sync-vhosts.sh
# Exit :  0 = vhosts current or applied; 1 = aborted (and anything changed was restored)

set -euo pipefail

# --- state the exit trap reads --------------------------------------------------------
# Initialised before the trap is installed so an early abort cannot trip `set -u` inside
# the handler.
BACKUP_DIR=""
RESTORE_ARMED=0
RESTORED=0
RELOAD_ATTEMPTED=0
CHANGED=0
SYNCED=()

# Test-harness hook. Empty in production, so every path below is absolute exactly as it
# has always been. Set only by tests/test_deploy/test_vhost_sync.py, which runs this file
# byte-identically against a fake tree -- a rewritten copy would prove the copy.
ROOT="${VHOST_SYNC_TEST_ROOT:-}"

SRC="${ROOT}/opt/datanika/datanika/deploy/apache"
ENABLED="${ROOT}/etc/apache2/sites-enabled"

log() { printf '%s %s\n' "$(date -u +%H:%M:%S)" "$*"; }

# The ONLY permitted way to abort. Never write a bare `exit 1`: that is the core#603
# defect, and the test asserts this file contains exactly one of them.
fatal() { log "FATAL: $*"; exit 1; }

# --- recovery -------------------------------------------------------------------------
restore() {
  [ "$RESTORED" = 1 ] && return 0
  RESTORED=1
  log "RESTORE: returning ${#SYNCED[@]} vhost(s) to their pre-sync contents"

  local entry name target backup ok=0 failed=0
  for entry in "${SYNCED[@]}"; do
    IFS=$'\t' read -r name target backup <<<"$entry"
    # An explicit regular-file test, NOT `[ -f ]` alone: `[ -f ]` follows symlinks, which
    # is how a dangling /tmp backup made the old restore a silent no-op.
    if [ -f "$backup" ] && [ ! -L "$backup" ]; then
      if cp "$backup" "$target"; then
        log "  restored ${name} (${target})"
        ok=1
      else
        log "  !! FAILED to restore ${name} -> ${target}"
        failed=1
      fi
    else
      log "  !! backup for ${name} is missing or not a regular file: ${backup}"
      failed=1
    fi
  done

  if [ "$ok" = 0 ]; then
    log "!! nothing was restored"
    return 0
  fi

  # Report whether the box is now syntactically sane. Never assert "it must be fine
  # because we put the old file back" -- check.
  if apachectl configtest >/dev/null 2>&1; then
    log "  apachectl configtest: Syntax OK after restore"
  else
    log "  !! apachectl configtest STILL FAILS after restore -- inspect ${ENABLED} now"
    failed=1
  fi

  # Only reload if we had already asked Apache to load the new config. If configtest
  # rejected it we never reloaded, so Apache's memory still holds the good config and a
  # needless reload on a box with a webdav co-tenant is not free.
  if [ "$RELOAD_ATTEMPTED" = 1 ]; then
    if apachectl graceful; then
      log "  apache gracefully reloaded onto the restored vhosts"
    else
      log "  !! apachectl graceful FAILED after restore -- Apache may be serving the bad config"
    fi
  fi

  [ "$failed" = 0 ] && log "RESTORE COMPLETE" || log "!! RESTORE INCOMPLETE -- manual check required"
}

on_exit() {
  local rc=$1
  # Disarm before doing anything: restore must never re-enter through its own failures,
  # and `set -e` inside a trap handler would abandon the recovery halfway.
  trap - EXIT HUP INT TERM
  set +e
  if [ "$rc" -ne 0 ] && [ "$RESTORE_ARMED" = 1 ]; then
    restore
  fi
  [ -n "$BACKUP_DIR" ] && rm -rf "$BACKUP_DIR"
  exit "$rc"
}

# EXIT covers `set -e`, `fatal`, and the signal traps below in one place. There is
# deliberately no ERR trap -- see the header.
trap 'on_exit $?' EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

# --- sync one vhost -------------------------------------------------------------------
sync_one() {
  local repo_name="$1" enabled_name="$2"
  local src="$SRC/$repo_name"
  local link="$ENABLED/$enabled_name"

  [ -f "$src" ] || fatal "missing in repo: $src"

  # sites-enabled holds SYMLINKS into sites-available. Resolve once and work on the real
  # file: `cp -a` on the link would back up the LINK, leaving a dangling relative symlink
  # and no content -- a restore that silently restores nothing.
  local target
  target=$(readlink -f "$link") || fatal "cannot resolve $link"
  [ -n "$target" ] || fatal "cannot resolve $link"
  [ -f "$target" ] || fatal "vhost target missing: $link -> $target"

  cmp -s "$src" "$target" && return 0

  local backup="$BACKUP_DIR/$enabled_name"
  cp "$target" "$backup" || fatal "cannot back up $target"
  # Arm recovery only after the backup is VERIFIED, and before the file is touched. A
  # backup that was never checked is not a backup; a backup taken after the overwrite is
  # a copy of the damage.
  cmp -s "$backup" "$target" || fatal "backup verification failed for $enabled_name"
  SYNCED+=("${enabled_name}"$'\t'"${target}"$'\t'"${backup}")
  RESTORE_ARMED=1

  cp "$src" "$target" || fatal "cannot write $target"
  CHANGED=1
  log "vhost changed: $enabled_name ($target)"
}

# --- run ------------------------------------------------------------------------------
BACKUP_DIR=$(mktemp -d) || fatal "cannot create a backup directory"

sync_one app.datanika.io.conf         zapp-datanika-io.conf
sync_one staging-app.datanika.io.conf zstaging-app-datanika-io.conf

if [ "$CHANGED" = 0 ]; then
  log "vhosts already current"
  exit 0
fi

CT_LOG="$BACKUP_DIR/configtest.log"
# Deliberately not `configtest | tee | grep -q`: `grep -q` exits on first match and can
# SIGPIPE `tee`, which under `set -o pipefail` makes a SUCCESSFUL configtest look failed.
apachectl configtest >"$CT_LOG" 2>&1 || true

if grep -qi 'Syntax OK' "$CT_LOG"; then
  RELOAD_ATTEMPTED=1
  apachectl graceful || fatal "apachectl graceful failed after a passing configtest"
  # Point of no return: the new config is loaded and serving. Restoring the old files now
  # would put disk and memory out of step in the other direction.
  RESTORE_ARMED=0
  log "vhosts applied, Apache gracefully reloaded"
else
  log "CONFIGTEST FAILED:"
  cat "$CT_LOG"
  fatal "apache configtest rejected the synced vhosts"
fi
