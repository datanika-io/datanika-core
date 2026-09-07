#!/usr/bin/env bash
# Remove files a previous deploy shipped and the current one no longer ships.
#
# WHY THIS EXISTS (core#1178)
# ---------------------------
# deploy-pointer.yml ships source as a tarball and the box runs `tar xzf -`.
# `tar x` overwrites what the archive holds and removes NOTHING it omits. So a
# file retired in git keeps running in production forever, with every check green.
#
# Measured on 2026-09-07, right after master fe0a9823:
#   822 files shared, 0 missing, 11 on the box and absent from git.
# Five of those eleven MUST NEVER BE DELETED:
#   .env.docker                      the live config, preserved rather than shipped
#   .env.docker.bak.pre-cutover…     operator backup of it
#   .env.docker.bak.pre-localfile…   operator backup of it
#   .secrets/client_secret_*.json    a Google OAuth client secret
#   reflex.db                        runtime state
# and a sixth, assets/llms.txt, was never in git at all.
#
# 🚨 THAT IS WHY THIS KEYS ON WHAT WE SHIPPED, NOT ON WHAT GIT HAS.
# A "delete anything absent from the repo" rule — the obvious fix, and the one
# this script exists to foreclose — deletes a production credential and the live
# config on its first run. The only paths this script can ever remove are paths
# a PREVIOUS MANIFEST says we ourselves put there. Everything the box owns is
# structurally unreachable, not merely deny-listed.
#
# Usage: reconcile-deployed-tree.sh <new-manifest> [--apply]
#   without --apply it reports and changes nothing (the default; dry run)
set -euo pipefail

NEW_MANIFEST="${1:?usage: reconcile-deployed-tree.sh <new-manifest> [--apply]}"
APPLY="${2:-}"
ROOT="${DATANIKA_ROOT:-/opt/datanika}"
STATE="$ROOT/.deploy-manifest"

# Trees this script is allowed to touch at all. A path outside these is ignored
# even if it somehow reached a manifest.
ALLOWED_PREFIXES='^(datanika|datanika-cloud)/'

# Defence in depth. Nothing here can be in a manifest (none of it is shipped),
# so this list should never actually fire — it exists so that a future change
# which accidentally starts shipping one of these cannot then delete it.
DENY='(^|/)\.env(\.|$)|(^|/)\.env\.docker|(^|/)\.secrets/|(^|/)reflex\.db$|(^|/)backups?/|(^|/)dbt_projects/'

# Floors. An empty or truncated manifest is the catastrophic failure mode:
# `prev - new` would then be EVERY file, and this script would delete the
# application. Fail closed, loudly, rather than reconcile against nonsense.
MIN_MANIFEST=500
MAX_REMOVALS="${DATANIKA_MAX_REMOVALS:-50}"

die() { echo "reconcile: FATAL: $*" >&2; exit 1; }

[ -f "$NEW_MANIFEST" ] || die "manifest not found: $NEW_MANIFEST"

# Normalise: regular files only, strip ./ prefix, drop directory entries.
LC_ALL=C grep -vE '/$' "$NEW_MANIFEST" | sed 's|^\./||' | LC_ALL=C sort -u > /tmp/reconcile.new
NEW_COUNT=$(wc -l < /tmp/reconcile.new)

if [ "$NEW_COUNT" -lt "$MIN_MANIFEST" ]; then
  die "new manifest has $NEW_COUNT entries, floor is $MIN_MANIFEST. Refusing to
  reconcile — a truncated manifest would delete everything the last one listed."
fi

# --- bootstrap graveyard (core#1178) ------------------------------------
# The manifest can only remove what a PREVIOUS manifest recorded, so files
# retired before this mechanism existed are invisible to it. This list names
# them once. It runs on the seed pass too, which is the entire point.
# Removing an absent file is a no-op, so this is idempotent and self-cleaning.
# Guarded by tests/test_deploy/test_retired_paths.py: every entry must be
# absent from the tree AND recorded in git history as deleted.
RETIRED_LIST="$ROOT/datanika/scripts/retired-paths.txt"
if [ -f "$RETIRED_LIST" ]; then
  while IFS= read -r rel; do
    case "$rel" in ''|'#'*) continue ;; esac
    echo "$rel" | LC_ALL=C grep -qE "$DENY" && { echo "reconcile: SKIP retired (deny-listed): $rel"; continue; }
    T="$ROOT/datanika/$rel"
    [ -f "$T" ] || continue
    if [ "$APPLY" = "--apply" ]; then
      rm -f -- "$T" && echo "reconcile: REMOVED retired $rel"
    else
      echo "reconcile: would remove retired $rel"
    fi
  done < "$RETIRED_LIST"
fi

if [ ! -f "$STATE" ]; then
  echo "reconcile: no previous manifest — seeding $STATE with $NEW_COUNT entries."
  echo "reconcile: removing nothing on a seed run, by design."
  [ "$APPLY" = "--apply" ] && cp /tmp/reconcile.new "$STATE"
  exit 0
fi

LC_ALL=C sort -u "$STATE" > /tmp/reconcile.prev
PREV_COUNT=$(wc -l < /tmp/reconcile.prev)

# The candidate set: shipped before, not shipped now.
LC_ALL=C comm -23 /tmp/reconcile.prev /tmp/reconcile.new > /tmp/reconcile.candidates

# Filter: allowed tree, not deny-listed, and actually present as a regular file.
: > /tmp/reconcile.remove
while IFS= read -r p; do
  [ -n "$p" ] || continue
  echo "$p" | LC_ALL=C grep -qE "$ALLOWED_PREFIXES" || { echo "reconcile: skip (outside allowed trees): $p"; continue; }
  echo "$p" | LC_ALL=C grep -qE "$DENY" && { echo "reconcile: SKIP (deny-listed): $p"; continue; }
  [ -f "$ROOT/$p" ] || continue          # already gone: nothing to do
  [ -L "$ROOT/$p" ] && { echo "reconcile: skip (symlink): $p"; continue; }
  echo "$p" >> /tmp/reconcile.remove
done < /tmp/reconcile.candidates

N=$(wc -l < /tmp/reconcile.remove)
echo "reconcile: prev=$PREV_COUNT new=$NEW_COUNT candidates=$(wc -l < /tmp/reconcile.candidates) to-remove=$N"

if [ "$N" -gt "$MAX_REMOVALS" ]; then
  die "$N removals exceeds cap $MAX_REMOVALS. Refusing. Re-run with
  DATANIKA_MAX_REMOVALS raised only after reading the list above."
fi

if [ "$N" -eq 0 ]; then
  echo "reconcile: nothing to remove."
else
  while IFS= read -r p; do
    if [ "$APPLY" = "--apply" ]; then
      rm -f -- "$ROOT/$p" && echo "reconcile: REMOVED $p"
    else
      echo "reconcile: would remove $p"
    fi
  done < /tmp/reconcile.remove
fi

if [ "$APPLY" = "--apply" ]; then
  cp /tmp/reconcile.new "$STATE"
  echo "reconcile: manifest updated ($NEW_COUNT entries)."
else
  echo "reconcile: dry run — manifest NOT updated."
fi
