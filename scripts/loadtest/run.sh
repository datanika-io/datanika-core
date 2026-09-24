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

# 🔴 core#1556: this was 161 — Run 9's count — while STAGES below tops out at 100 req/s. 161
# keys is a 80 req/s ceiling, so the script's own defaults asked for a ladder its own fixture
# could not deliver, and the last two stages would have measured the RATE LIMITER. 300 is the
# count `preflight.sh`'s own closing line already tells the operator to pass (ceiling 150).
KEYS=300
STAGES="5:120s,10:120s,20:120s,30:120s,40:120s,60:120s,80:120s,100:120s"
# Ramp inserted before each new rate so the rate that follows is HELD rather than merely touched
# (core#1560). Only the hold is a measurement; see expand_stages below.
RAMP="30s"
OUT=""
K6_IMAGE="grafana/k6:1.7.1"   # Pinned. `:latest` on an instrument is how two runs stop comparing.
STAGING_BE="http://127.0.0.1:8100"   # staging is NOT blue/green; this port is stable.
PROD_BE=""                            # resolved from the active vhost below - never hardcoded.
PROD_COLOUR=""

while [ $# -gt 0 ]; do
  case "$1" in
    --keys)   KEYS="$2"; shift 2 ;;
    --stages) STAGES="$2"; shift 2 ;;
    --ramp)   RAMP="$2"; shift 2 ;;
    --out)    OUT="$2"; shift 2 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done
[ -n "$OUT" ] || OUT="$(mktemp -d)"
mkdir -p "$OUT"
LOG="$OUT/run.log"
say() { printf '%s %s\n' "$(date -u +%H:%M:%SZ)" "$*" | tee -a "$LOG"; }

# ── stage expansion (core#1560) ───────────────────────────────────────────────────────────
# `ramping-arrival-rate` INTERPOLATES. A segment whose target differs from the rate we are
# already at is a RAMP, and its achieved rate is the mean of that ramp, not its label — run 10's
# `60:120s` delivered 49.92 = (40+60)/2 while k6's console printed `60.00 iters/s` beside it.
# A rate is HELD only where two consecutive segments share a target.
#
# So each requested rung becomes "ramp to R, then HOLD R". Idempotent by construction: a spec
# that already contains a hold is passed through untouched, so expanding twice is the same as
# expanding once, and a hand-written expanded spec is never double-ramped.
already_holds() {   # $1 = spec. returns 0 if any two consecutive segments share a target.
  ah_prev=""
  for ah_part in $(printf '%s' "$1" | tr ',' ' '); do
    ah_rate="${ah_part%%:*}"
    if [ "$ah_rate" = "$ah_prev" ]; then return 0; fi
    ah_prev="$ah_rate"
  done
  return 1
}  # end already_holds

expand_stages() {   # $1 = spec, $2 = ramp duration -> prints the effective spec
  es_spec="$1"; es_ramp="$2"; es_out=""; es_prev=""
  if already_holds "$es_spec"; then printf '%s\n' "$es_spec"; return 0; fi
  for es_part in $(printf '%s' "$es_spec" | tr ',' ' '); do
    es_rate="${es_part%%:*}"
    # The first rung needs no ramp: startRate == the first target, so it is already flat.
    if [ -n "$es_prev" ] && [ "$es_rate" != "$es_prev" ]; then
      es_out="${es_out:+$es_out,}${es_rate}:${es_ramp}"
    fi
    es_out="${es_out:+$es_out,}${es_part}"
    es_prev="$es_rate"
  done
  printf '%s\n' "$es_out"
}  # end expand_stages

# ── the limiter-ceiling GATE (core#1556) ──────────────────────────────────────────────────
# This used to be a SENTENCE. The script computed the ceiling, printed "top stage must be under
# it", and proceeded regardless — a report wearing a gate's clothes. It failed quietly in the
# direction that matters, because a limiter-bound stage does not error: it returns plausible,
# LOWER numbers, which then get published as application throughput.
#
# There is deliberately NO override. The correct action is always available — mint more keys, or
# lower the top stage — so an override could only ever be a way to skip the check. A run that
# deliberately measures the limiter is a DIFFERENT run and must be labelled one.
ceiling_gate() {   # $1 = minted keys, $2 = stage spec, $3 = rpm per key (default 30)
  cg_minted="$1"; cg_spec="$2"; cg_rpm="${3:-30}"
  cg_top="$(printf '%s' "$cg_spec" | tr ',' '\n' | cut -d: -f1 | grep -E '^[0-9]+$' | sort -n | tail -1)"
  if [ -z "${cg_top:-}" ]; then
    say "REFUSING: no numeric stage rate could be parsed out of '$cg_spec'"
    return 18
  fi
  cg_ceil=$(( cg_minted * cg_rpm / 60 ))
  if [ "$cg_top" -lt "$cg_ceil" ]; then
    say "gate: top stage ${cg_top} req/s is below the ${cg_ceil} req/s limiter ceiling"
    say "      (${cg_minted} keys x ${cg_rpm} rpm / 60) — permitted"
    return 0
  fi
  # Ceiling division, and the +1 is load-bearing: the requirement is strict. 201 keys yields a
  # ceiling of exactly 100 for a top stage of 100, which is AT the ceiling, not under it.
  cg_need=$(( ( (cg_top + 1) * 60 + cg_rpm - 1 ) / cg_rpm ))
  say "REFUSING: top stage ${cg_top} req/s is NOT below the ${cg_ceil} req/s limiter ceiling"
  say "          (${cg_minted} keys x ${cg_rpm} rpm / 60). The upper stages would measure the"
  say "          RATE LIMITER and report it as application throughput — quietly, because a"
  say "          limiter-bound stage does not error, it just returns lower numbers."
  say "          Remedy: --keys ${cg_need} (or more), or lower the top stage below ${cg_ceil}."
  return 17
}  # end ceiling_gate

EFFECTIVE_STAGES="$(expand_stages "$STAGES" "$RAMP")"
say "run start  keys=$KEYS  stages=$STAGES  out=$OUT"
say "stages: effective $EFFECTIVE_STAGES"
say "stages: ramp $RAMP precedes each new rate; ONLY THE HOLD IS A MEASUREMENT (core#1560)"

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

# core#1556: this is now a GATE, not a caption. It is evaluated against the EFFECTIVE spec,
# because that is what k6 will actually offer.
# ⚠️ `|| { rm ...; exit $?; }` would exit with the status of `rm`, not of the gate — so a refusal
# would leave with 0. Capture the status first; this is the same "assert the outcome, not the
# exit code of the last command" trap the harness has already been bitten by twice.
CG_RC=0
ceiling_gate "$MINTED" "$EFFECTIVE_STAGES" || CG_RC=$?
if [ "$CG_RC" -ne 0 ]; then rm -rf "$KEYDIR"; exit "$CG_RC"; fi

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
# Defect 7 (core#778): `-i` is load-bearing and was missing. The k6 script is PIPED into
# this command, but `docker run` does not attach stdin without it, so k6 received an EMPTY
# script and died with "no exported functions in script" -- an error pointing at the JS
# file, which parses fine and exports three symbols. The `docker exec -i` used by the
# seeder above has it; this one did not. Nothing short of executing the harness finds it.
# Defect 8 (core#778): the generator image runs as uid 12345 (`k6`), and KEYDIR is a
# `mktemp -d` -- 0700, owned by root -- so the container could not stat the key file:
#     GoError: stat /keys/loadtest-keys.txt: permission denied
#
# The obvious fix is `chmod 0755 $KEYDIR; chmod 0644 $KEYFILE`. REJECTED: that file holds
# live staging API keys in cleartext, and this box has co-tenants (an Apache webdav vhost,
# and the founder's VPN unit which is currently inactive but can be started at any time).
# Widening host permissions on a secret to satisfy a container is the wrong trade when the
# container can simply be told who to be. `--user 0:0` changes nothing outside this
# ephemeral --rm container and leaves the key file readable only by root.
docker run --rm -i --user 0:0 --network host \
  -v "$KEYDIR":/keys:ro -v "$OUT":/out \
  -e TARGET_BASE="$STAGING_BE" -e NEIGHBOUR_BASE="$PROD_BE" \
  -e KEYS_FILE=/keys/loadtest-keys.txt -e STAGES="$EFFECTIVE_STAGES" \
  "$K6_IMAGE" run --summary-export=/out/summary.json --out "csv=/out/raw.csv" - \
  < "$(dirname "$0")/k6_baseline.js" > "$OUT/k6.out" 2>&1
K6RC=$?
echo "$K6RC" > "$OUT/k6.exit"
# ⚠️ Non-zero now has TWO meanings and they call for opposite responses, so do not collapse them:
#   - an abortOnFail criterion stopped the run   -> the abort criteria working
#   - a `http_reqs{rung:N}` threshold was missed -> that rung did NOT deliver its requested rate
# The second is a finding about the target or the fixture, not a broken harness. Read which one
# it was in summary.json rather than inferring it from the exit status.
say "generator exit $K6RC  (non-zero = an abort criterion fired, OR a rung did not deliver its rate — read summary.json)"

# ── drain (rule 4) ────────────────────────────────────────────────────────────────────────
for s in 30 60 90 120; do
  sleep 30
  N=$(docker exec datanika-staging-postgres psql -U datanika -d datanika -t -A \
        -c "SELECT count(*) FROM pg_stat_activity WHERE backend_type='client backend';" 2>/dev/null || echo "?")
  say "drain +${s}s staging client backends: $N"
done

say "neighbour after: prod /healthz = $(curl -s -o /dev/null -w '%{http_code}' "$PROD_BE/healthz")"
say "results in $OUT  (summary.json, raw.csv, k6.out, run.log)"
