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
STAGING_BE="http://127.0.0.1:8100"
PROD_BE="http://127.0.0.1:8000"

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

# ── preflight ─────────────────────────────────────────────────────────────────────────────
say "preflight: staging must be healthy BEFORE we load it, or the result describes a sick box"
code=$(curl -s -o /dev/null -w '%{http_code}' "$STAGING_BE/healthz" || echo 000)
[ "$code" = "200" ] || { say "REFUSING: staging /healthz = $code"; exit 10; }
code=$(curl -s -o /dev/null -w '%{http_code}' "$PROD_BE/healthz" || echo 000)
[ "$code" = "200" ] || { say "REFUSING: production /healthz = $code — do not add load to a sick box"; exit 11; }

say "preflight: no e2e-staging in flight (rule 2)"
if docker ps --format '{{.Names}}' | grep -q 'datanika-staging-e2e'; then
  say "REFUSING: an e2e-staging container is running"; exit 12
fi

# 🚨 core#1476: a staging image build starves Grafana's SQLite. Adding a load test on top of
# one measures contention, not the app. Refuse rather than produce a number nobody can use.
if pgrep -f 'buildkitd|docker-untar' >/dev/null 2>&1 && [ "$(uptime | sed 's/.*average: //' | cut -d, -f1 | tr -d ' ')" \> "3.0" ]; then
  say "REFUSING: a build appears to be running and load is already above 3.0 — wait for a quiet box"
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
MINTED=$(grep -c . "$KEYFILE" 2>/dev/null || echo 0)
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
