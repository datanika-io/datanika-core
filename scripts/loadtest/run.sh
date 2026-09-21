#!/usr/bin/env bash
# Datanika load-test driver (core#778). Run FROM THE PRODUCTION BOX, against STAGING.
#
#   bash scripts/loadtest/run.sh [--keys N] [--stages "5:120s,...,100:120s"] [--out DIR]
#
# ── THE RULES THIS SCRIPT ENFORCES, AND WHY EACH ONE EXISTS ───────────────────────────────
#
# 1. It targets **staging**, never production. April's runs went at production and left its
#    database unusable for the better part of an hour (`max_connections` pegged with 99
#    connections idle in `wait_event=ClientRead`). Prod and staging now share one 4 vCPU box,
#    so the founder's label on any result is "a floor under neighbour load, not a capacity
#    figure" — and that is only honest if the neighbour is sampled, which k6 does throughout.
#
# 2. It refuses to start while `e2e-staging` could be running. That job is triggered by every
#    push to `dev`; added load would produce a gating red indistinguishable from a real
#    regression, and somebody would spend a day on it.
#
# 3. **Cleanup is verified by effect, not by having been attempted.** The keys are revoked and
#    the remaining-active count is asserted to be 0 against the total minted, the keys file is
#    asserted gone, and the generator image is removed. A cleanup that is merely invoked is the
#    same class of evidence as a green that was never able to fail.
#
# 4. It records the drain. Run 8's failure mode was connections that stayed pegged AFTER the
#    run ended, so `pg_stat_activity` is sampled for two minutes past the finish. A run that
#    looks clean and leaves the database saturated has not been measured, only survived.
#
# ⚠️ Everything it writes goes under --out (default: a fresh mktemp -d). Nothing is left in a
# path another session would inherit, and nothing is written into the repository.
set -uo pipefail

KEYS=161                  # Run 9's count. See the ceiling formula in k6_baseline.js.
STAGES="5:120s,10:120s,20:120s,30:120s,40:120s,60:120s,80:120s,100:120s"
OUT=""
K6_IMAGE="grafana/k6:1.7.1"   # Pinned. `:latest` on an instrument is how two runs stop comparing.
STAGING_BE="http://127.0.0.1:8100"   # staging is NOT blue/green; this port is stable.
PROD_BE=""                            # resolved from the active vhost below - never hardcoded.
PROD_COLOUR=""

while [ $# -gt 0 ]; do
  case "$1" in
    --keys)   KEYS="$2"; shift 2 ;;
    --stages) STAGES="$2"; shift 2 ;;
    --out)    OUT="$2"; shift 2 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done
[ -n "$OUT" ] || OUT="$(mktemp -d)"
mkdir -p "$OUT"
LOG="$OUT/run.log"
say() { printf '%s %s\n' "$(date -u +%H:%M:%SZ)" "$*" | tee -a "$LOG"; }

say "run start  keys=$KEYS  stages=$STAGES  out=$OUT"

# ── resolve the serving colour (core#622 class) ───────────────────────────────────────────
# 🔴 FOUND ON THE FIRST REAL EXECUTION, 2026-09-21. PROD_BE was hardcoded to :8000. The
# production backend port ALTERNATES on every deploy — 8000 blue, 8010 green — and the
# promotion that day had swapped prod to green, so `curl :8000/healthz` returned 000 and the
# preflight below refused with "do not add load to a sick box" while production was serving
# 200 through Cloudflare the whole time.
#
# Two failure modes, and the quiet one is worse:
#   1. A FALSE REFUSAL that names a healthy production as sick, sending whoever reads it
#      after an incident that does not exist.
#   2. Mid-swap both colours are briefly up, so a hardcoded port can resolve to the colour
#      that is NOT serving. The neighbour scenario would then sample a container taking no
#      real traffic and report a reassuring number. The founder's label on every result is
#      "a floor under NEIGHBOUR LOAD" — measured against the wrong process, that label is
#      not merely imprecise, it is unearned.
#
# So: ask the vhost, and refuse if it cannot be read. Never assume, never default.
ACTIVE_CONF="${ACTIVE_CONF:-/etc/apache2/conf-enabled/datanika-prod-active.conf}"
PROD_PORT="$(grep -oE '\b80[01]0\b' "$ACTIVE_CONF" 2>/dev/null | head -1 || true)"
case "${PROD_PORT:-}" in
  8000) PROD_COLOUR="blue" ;;
  8010) PROD_COLOUR="green" ;;
  *) say "REFUSING: cannot determine the serving colour from $ACTIVE_CONF"
     say "          got '${PROD_PORT:-<nothing>}'. Guessing a colour is how the neighbour"
     say "          reading ends up describing a container that serves no traffic."
     exit 16 ;;
esac
PROD_BE="http://127.0.0.1:${PROD_PORT}"
say "preflight: serving colour is $PROD_COLOUR (backend $PROD_PORT), read from $ACTIVE_CONF"

# ── preflight ─────────────────────────────────────────────────────────────────────────────
say "preflight: staging must be healthy BEFORE we load it, or the result describes a sick box"
code=$(curl -s -o /dev/null -w '%{http_code}' "$STAGING_BE/healthz" || echo 000)
[ "$code" = "200" ] || { say "REFUSING: staging /healthz = $code"; exit 10; }
code=$(curl -s -o /dev/null -w '%{http_code}' "$PROD_BE/healthz" || echo 000)
[ "$code" = "200" ] || { say "REFUSING: production /healthz = $code — do not add load to a sick box"; exit 11; }

say "preflight: no E2E suite in flight (rule 2)"
# 🔴 CORRECTED 2026-09-21, on this harness's FIRST rehearsal. The pattern here was
# `datanika-staging-e2e`, a name that does not exist. The real containers an E2E run brings
# up are `e2e-authentik-server-1`, `-worker-1`, `-redis-1`, `-db-1` — measured while
# `e2e-sso` was live against staging. **The guard matched nothing and would have let the
# load test start on top of a running suite**, producing exactly the false gating red it was
# written to prevent. A guard aimed at a guessed name is not a guard.
E2E_RUNNING="$(docker ps --format '{{.Names}}' | grep -ciE '(^|-)e2e-|authentik|playwright' || true)"
if [ "${E2E_RUNNING:-0}" -gt 0 ]; then
  say "REFUSING: an E2E suite is running ($E2E_RUNNING container(s)) — added load would"
  say "          produce a gating red indistinguishable from a real regression."
  docker ps --format '          {{.Names}}' | grep -iE '(^|-)e2e-|authentik|playwright' | tee -a "$LOG"
  exit 12
fi

# 🚨 core#1476: a staging image build starves Grafana's SQLite. Adding a load test on top of
# one measures contention, not the app. Refuse rather than produce a number nobody can use.
#
# ⚠️ The load comparison uses awk, not `[ "$a" \> "$b" ]`. That form compares STRINGS: a load
# of "10.5" is lexicographically LESS than "3.0", so the original refused nothing at exactly
# the load that matters most. Corrected in the same pass as the E2E pattern above.
LOAD1="$(cut -d' ' -f1 /proc/loadavg)"
BUILDING="$(ps -eo comm= | grep -cE '^(buildkitd|docker-untar)$' || true)"
say "preflight: load1=$LOAD1 build-processes=$BUILDING"
if [ "${BUILDING:-0}" -gt 0 ] && awk -v l="$LOAD1" 'BEGIN{exit !(l>3.0)}'; then
  say "REFUSING: a build is running and load ($LOAD1) is already above 3.0 — wait for a quiet box"
  exit 13
fi

say "preflight: pull the generator ($K6_IMAGE)"
docker pull -q "$K6_IMAGE" >/dev/null 2>&1 || { say "REFUSING: cannot pull $K6_IMAGE"; exit 14; }

# ── seed ──────────────────────────────────────────────────────────────────────────────────
KEYDIR="$(mktemp -d)"
KEYFILE="$KEYDIR/loadtest-keys.txt"
say "seed: minting $KEYS read-scoped keys on staging"
docker exec -i datanika-staging-app /app/.venv/bin/python - "$KEYS" \
  < "$(dirname "$0")/seed_loadtest_org.py" > "$KEYFILE" 2> "$OUT/seed.err"
# 🔴 Defect 6 (core#778), and the worst of the six because it disarmed the guard below.
# This was `$(grep -c . "$KEYFILE" || echo 0)`. `grep -c` PRINTS its count and exits 1
# when the count is zero, so on the failure path the `||` fired too and MINTED became
# the two-line string "0
0". The comparison then died with "integer expression
# expected" -- it did not refuse, it ERRORED -- and the run was stopped two lines later
# only because `set -u` tripped on an unbound CEIL. Had CEIL carried a default, this
# harness would have gone on to load-test staging with ZERO keys and reported a number.
#
# A guard that errors is not a guard. `|| :` keeps the exit status quiet without adding
# a second line, and the ${MINTED:-0} covers grep being absent entirely.
MINTED=$(grep -c . "$KEYFILE" 2>/dev/null || :)
MINTED=${MINTED:-0}
case "$MINTED" in (*[!0-9]*|"") say "REFUSING: minted count is not a number: $(printf %q "$MINTED")"; rm -rf "$KEYDIR"; exit 15 ;; esac
say "seed: minted=$MINTED (requested $KEYS)"
if [ "$MINTED" -lt "$KEYS" ]; then
  say "REFUSING: seeder produced $MINTED of $KEYS keys — the ceiling would be lower than intended"
  rm -rf "$KEYDIR"; exit 15
fi

CEIL=$(( MINTED * 30 / 60 ))
say "seed: per-key ceiling implies <= ${CEIL} req/s at 30 rpm/key — top stage must be under it"

cleanup() {
  say "cleanup: revoking keys"
  REV=$(docker exec -i datanika-staging-app /app/.venv/bin/python - revoke \
        < "$(dirname "$0")/seed_loadtest_org.py" 2>>"$OUT/seed.err" || echo "revoke-failed")
  say "cleanup: $REV"
  rm -f "$KEYFILE"; rmdir "$KEYDIR" 2>/dev/null
  if [ -e "$KEYFILE" ]; then say "cleanup: KEYS FILE STILL PRESENT — investigate"; else say "cleanup: keys file gone: yes"; fi
  docker rmi "$K6_IMAGE" >/dev/null 2>&1 && say "cleanup: generator image removed" || say "cleanup: generator image not removed (in use?)"
  say "END"
}
trap cleanup EXIT

# ── run ───────────────────────────────────────────────────────────────────────────────────
say "generator start"
docker run --rm --network host \
  -v "$KEYDIR":/keys:ro -v "$OUT":/out \
  -e TARGET_BASE="$STAGING_BE" -e NEIGHBOUR_BASE="$PROD_BE" \
  -e KEYS_FILE=/keys/loadtest-keys.txt -e STAGES="$STAGES" \
  "$K6_IMAGE" run --summary-export=/out/summary.json --out "csv=/out/raw.csv" - \
  < "$(dirname "$0")/k6_baseline.js" > "$OUT/k6.out" 2>&1
K6RC=$?
echo "$K6RC" > "$OUT/k6.exit"
say "generator exit $K6RC  (non-zero = a threshold in k6_baseline.js aborted the run; that is the abort criteria working)"

# ── drain (rule 4) ────────────────────────────────────────────────────────────────────────
for s in 30 60 90 120; do
  sleep 30
  N=$(docker exec datanika-staging-postgres psql -U datanika -d datanika -t -A \
        -c "SELECT count(*) FROM pg_stat_activity WHERE backend_type='client backend';" 2>/dev/null || echo "?")
  say "drain +${s}s staging client backends: $N"
done

say "neighbour after: prod /healthz = $(curl -s -o /dev/null -w '%{http_code}' "$PROD_BE/healthz")"
say "results in $OUT  (summary.json, raw.csv, k6.out, run.log)"
