#!/usr/bin/env bash
#
# Bound docker's disk growth on the prod box, without destroying the blue/green
# rollback. Run at the START of a deploy, before anything is built. core#666.
#
# WHAT ACTUALLY GROWS THE DISK
# ----------------------------
# `docker compose build` on the box produces a new image every deploy; the previous
# one loses its tag. After five weeks that was 270-of-283 images `<none>` and
# `docker system df` reporting 53 GB "reclaimable".
#
# ⚠️ That figure is not a claim about disk. Running `docker image prune` against it
# was measured on 2026-08-30: 275 image records disappeared, the Build Cache row grew
# by ~52 GB, and `df` did not move by a single byte — **total reclaimed 0 B**. Those
# layers are BuildKit cache records that the dangling images merely also referenced.
# Dropping the second reference frees nothing while the builder still holds the first.
#
# So the consumer to bound is the BUILD CACHE, and the only thing that touches it is
# `docker builder prune`. The image prune is kept for tidiness — it stops the record
# count growing without bound and keeps `docker images` readable — but it is not the
# thing that moves `df`, and this script must not be read as claiming otherwise.
#
# WHY CAPPED, NEVER `-af`
# -----------------------
# `docker builder prune -af` empties the cache, which makes the very next deploy a
# fully cold build (the box compiles lxml/xmlsec from source — 5-8 silent minutes).
# A cap keeps recent layers warm and still bounds the total. The cap is the point.
#
# WHY THIS IS SAFE FOR THE ROLLBACK, AND WHY IT IS CHECKED ANYWAY
# ---------------------------------------------------------------
# The blue/green rollback is an `Exited (137)` container plus the image it references
# — and that image is itself dangling. `docker image prune` (no `-a`) skips it,
# because the exited container holds a reference. `builder prune` touches cache
# records, not images.
#
# So both commands are safe by construction. The check below exists anyway, because
# the hazard is one edit away and every unsafe variant is shorter to type:
#
#   docker container prune      <- unreferences the rollback image, and then any
#                                  image prune deletes the only artifact that lets
#                                  us roll back without a rebuild
#   docker system prune -a      <- same, plus it takes the staging image
#   docker image prune -a       <- takes every image no *running* container uses
#
# NEVER any of those on this box. It is shared with co-tenants (the founder's VPN,
# an Apache webdav vhost), so an exited container is somebody's deliberate state, not
# garbage — which is why the guard below protects images held by *every* container on
# the host and not just ours.
#
# Usage: prune-docker-cache.sh [keep-storage] [min-free-gb]

set -euo pipefail

KEEP="${1:-20GB}"
MIN_FREE_GB="${2:-5}"

say() { printf '%s\n' "$*"; }

# ---------------------------------------------------------------- before
disk_used_gb()  { df -B1 --output=used  / | tail -1 | tr -d ' ' | awk '{printf "%.1f", $1/1073741824}'; }
disk_avail_gb() { df -B1 --output=avail / | tail -1 | tr -d ' ' | awk '{printf "%.1f", $1/1073741824}'; }
cache_bytes()   { docker builder du 2>/dev/null | awk '/^Total:/ {print $2 $3}' | tail -1; }
image_count()   { docker images -aq | wc -l | tr -d ' '; }

BEFORE_USED=$(disk_used_gb)
BEFORE_AVAIL=$(disk_avail_gb)
BEFORE_IMAGES=$(image_count)
BEFORE_CACHE=$(cache_bytes)

# Every image referenced by every container on this host, running or not. Derived,
# never hardcoded: the colours alternate on every deploy, so the rollback image id is
# different each time. An earlier note in core#666 recorded a specific id and it was
# already stale by the time anyone acted on it.
PROTECTED=$(docker ps -aq | xargs -r docker inspect -f '{{.Image}}' | sort -u)
PROTECTED_N=$(printf '%s\n' "$PROTECTED" | grep -c . || true)

say "=== docker prune (core#666) ==="
say "keep-storage        : $KEEP"
say "images before       : $BEFORE_IMAGES"
say "build cache before  : ${BEFORE_CACHE:-unknown}"
say "disk before         : ${BEFORE_USED} GiB used, ${BEFORE_AVAIL} GiB free"
say "container-held images (protected): $PROTECTED_N"

# ---------------------------------------------------------------- prune
# Non-fatal: a failed prune leaves the box exactly as it was, and blocking a deploy
# on a maintenance step would be the wrong trade. Loud, though — silence is the
# failure mode this whole issue is about (same shape as core#615/#616).
prune_builder() {
  if docker builder prune -f --keep-storage "$KEEP"; then return 0; fi
  # `--keep-storage` is accepted by docker 28 but is on its way out; the replacement
  # spelling is `--max-used-space`. Try it rather than silently skipping the only
  # command that moves `df`.
  say "::warning::--keep-storage rejected, retrying with --max-used-space"
  docker builder prune -f --max-used-space "$KEEP"
}

if ! prune_builder; then
  say "::warning::builder prune failed — build cache is NOT bounded this run"
fi

if ! docker image prune -f; then
  say "::warning::image prune failed"
fi

# ---------------------------------------------------------------- verify
# The load-bearing assertion. If a future edit turns this into `-a`, adds a
# `container prune`, or reaches for `system prune -a`, the deploy fails HERE —
# before the blue/green swap — rather than at 2 a.m. when someone needs the rollback
# and finds the image gone.
MISSING=""
for img in $PROTECTED; do
  docker image inspect "$img" >/dev/null 2>&1 || MISSING="$MISSING $img"
done
if [ -n "$MISSING" ]; then
  say "::error::prune removed image(s) still referenced by a container:$MISSING"
  say "The blue/green rollback and/or a co-tenant container has been broken."
  exit 1
fi

# ---------------------------------------------------------------- after
AFTER_USED=$(disk_used_gb)
AFTER_AVAIL=$(disk_avail_gb)
AFTER_IMAGES=$(image_count)
AFTER_CACHE=$(cache_bytes)

say ""
say "images  : $BEFORE_IMAGES -> $AFTER_IMAGES"
say "cache   : ${BEFORE_CACHE:-unknown} -> ${AFTER_CACHE:-unknown}"
say "disk    : ${BEFORE_USED} -> ${AFTER_USED} GiB used, ${BEFORE_AVAIL} -> ${AFTER_AVAIL} GiB free"
say "rollback images intact: $PROTECTED_N/$PROTECTED_N"

# Two Grafana rules DO watch disk on this box — `disk-space-warning` and
# `disk-space-critical` — and they fire at exactly the two numbers below (core#727;
# `tests/test_deploy/test_disk_thresholds_agree.py` keeps the three in step). This check
# is defence in depth, not the only coverage: it looks at the one moment the alert cannot
# help with, which is immediately before a build that needs the headroom.
#
# ⚠️ This comment used to read "No alert rule watches disk on this box". That was FALSE
# and was carried for weeks — a rule existed, was evaluated, and discriminated. It is
# corrected rather than deleted because the false version actively mis-answered a triage.
AVAIL_INT=${AFTER_AVAIL%%.*}
if [ "$AVAIL_INT" -lt "$MIN_FREE_GB" ]; then
  say "::error::only ${AFTER_AVAIL} GiB free after pruning (floor ${MIN_FREE_GB} GiB)."
  say "The build that follows needs headroom; failing now beats failing mid-build."
  exit 1
fi
if [ "$AVAIL_INT" -lt 20 ]; then
  say "::warning::only ${AFTER_AVAIL} GiB free after pruning — investigate before it bites"
fi
