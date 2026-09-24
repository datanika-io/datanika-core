#!/usr/bin/env bash
# Datanika load-test OPERATOR-SIDE preflight (core#778). Run FROM THE DEV MACHINE, before run.sh.
#
#   bash scripts/loadtest/preflight.sh [--duration-min N] [--cooldown-min N] [--self-check]
#
# Exit 0  -> the operator-side gates are clear; go and invoke run.sh on the box.
# Exit >0 -> do not start. The refusal names the gate and prints the reading behind it.
#
# ── WHY THIS EXISTS, AND WHY IT IS A SEPARATE SCRIPT FROM run.sh ──────────────────────────
#
# `run.sh` runs ON THE PRODUCTION BOX and its preflight is therefore box-shaped: staging
# /healthz, production /healthz, an E2E container census, load1. Those are necessary and they
# are not sufficient, because **the box cannot see GitHub.**
#
# 🔑 The measured finding this closes (Infra, rounds 13-14, core#778):
#
#     A GUARD IS LEAST INFORMATIVE EXACTLY WHEN THE RISK IS HIGHEST.
#
#     run.sh's container census asks "is an E2E suite running RIGHT NOW". A 25-minute ladder
#     needs "will one START during my run". Those differ, and they differ WORST in the gap
#     between a dev run's unit jobs finishing and its staging jobs starting: in that window
#     the census reads a confident zero and a staging deploy is minutes away.
#
#     Measured twice, on two different sessions: every box-side signal clean -- E2E containers
#     0, staging `Up 3 hours`, load1 0.46 -- while the Actions API showed an in-flight dev run
#     whose staging jobs did not exist yet. All four box-side checks would have PASSED and
#     staging would have been rebuilt underneath the run.
#
# So the question this script asks is not "is anything running" but **"is anything ABLE TO
# START"**. Four things can rebuild staging during a run, and none is visible from the box:
#
#   G1  an in-flight or queued Actions run on `dev`   -> its staging job may not exist yet
#   G2  a merge-queue entry on `dev`                  -> it merges, pushes dev, deploys staging
#   G3  an open PR into `dev`                         -> five departments self-merge, any time
#   G4  a dev run that only just finished             -> staging is cold, not collided-with
#
# ── WHAT THIS SCRIPT DOES NOT PROMISE ─────────────────────────────────────────────────────
#
# It cannot see the future. A department can open and merge a PR ninety seconds from now and
# nothing here would have known. What a clear reading buys is that **no already-existing
# trigger is pointed at staging** -- it lowers the probability, it does not remove it.
# Anything stronger would be the over-claim that makes a guard worse than none.
#
# ── EVERY GATE CARRIES ITS OWN POSITIVE CONTROL, IN THE SAME RUN ──────────────────────────
#
# Coordinator rule 26: an instrument that cannot see part of its population reports that part
# as clean. Each gate below therefore reads TWICE -- the filtered population it grades, and an
# unfiltered one that must be non-empty. A filtered zero beside an unfiltered zero is a broken
# query, not a quiet repository, and it REFUSES.
#
# This is not hypothetical here. On 2026-09-23 the merge-queue GraphQL query was called with a
# doubled owner (`datanika-io/datanika-io/datanika-landing`), returned an `errors` array and no
# count, and printed nothing at all -- which reads exactly like an empty queue. Same class as
# run.sh's own defect 6: **a guard that errors is not a guard.**
set -uo pipefail

GH="${GH:-/d/Tools/gh/bin/gh.exe}"
OWNER=datanika-io
REPO=datanika-core
DURATION_MIN=25          # Run 9's ladder: 8 stages x 120s plus drain sampling.
COOLDOWN_MIN=10          # G4: how recently a dev run may have finished.
SELF_CHECK=0

while [ $# -gt 0 ]; do
  case "$1" in
    --duration-min) DURATION_MIN="$2"; shift 2 ;;
    --cooldown-min) COOLDOWN_MIN="$2"; shift 2 ;;
    --self-check)   SELF_CHECK=1; shift ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

say() { printf '%s %s\n' "$(date -u +%H:%M:%SZ)" "$*"; }

# ── the decision, as a pure function ──────────────────────────────────────────────────────
# Kept separate from the measuring so that `--self-check` can drive it with populations the
# repository does not currently contain. Coordinator rule 10's inverse: a guard that refuses
# EVERYTHING is not discriminating, and the obvious repair for "it refuses everything" is to
# loosen it. Driving both populations is the only thing that tells them apart.
#
# args: runs queue prs cooled     ("cooled" = 1 when the cooldown has elapsed)
# echoes: "PASS" | "REFUSE:<gate>:<reason>"
verdict() {
  local runs="$1" queue="$2" prs="$3" cooled="$4"
  # A reading that is not a number is a broken instrument, never a zero.
  case "$runs"   in (*[!0-9]*|"") echo "REFUSE:G1:unreadable in-flight-run count: '$runs'"; return ;; esac
  case "$queue"  in (*[!0-9]*|"") echo "REFUSE:G2:unreadable merge-queue count: '$queue'"; return ;; esac
  case "$prs"    in (*[!0-9]*|"") echo "REFUSE:G3:unreadable open-PR count: '$prs'"; return ;; esac
  case "$cooled" in (0|1) ;; (*) echo "REFUSE:G4:unreadable cooldown state: '$cooled'"; return ;; esac

  [ "$runs"  -gt 0 ] && { echo "REFUSE:G1:$runs in-flight/queued run(s) on dev — a staging deploy may not have started yet"; return; }
  [ "$queue" -gt 0 ] && { echo "REFUSE:G2:$queue merge-queue entry/entries on dev — each merges and redeploys staging"; return; }
  [ "$prs"   -gt 0 ] && { echo "REFUSE:G3:$prs open PR(s) into dev — each is a future staging deploy, mergeable at any moment"; return; }
  [ "$cooled" -eq 0 ] && { echo "REFUSE:G4:a dev run finished inside the cooldown — staging is cold, not quiet"; return; }
  echo "PASS"
}

# ── --self-check: prove the decision DISCRIMINATES before trusting any reading ────────────
if [ "$SELF_CHECK" = "1" ]; then
  fails=0
  check() { # expected-prefix, args...
    local want="$1"; shift
    local got; got="$(verdict "$@")"
    if [ "${got#"$want"}" = "$got" ]; then
      echo "  FAIL  want=${want}* got=$got   (args: $*)"; fails=$((fails+1))
    else
      echo "  ok    $got"
    fi
  }
  echo "self-check: the decision must answer DIFFERENT populations DIFFERENTLY"
  check "PASS"       0 0 0 1
  check "REFUSE:G1"  1 0 0 1
  check "REFUSE:G1"  9 9 9 0
  check "REFUSE:G2"  0 1 0 1
  check "REFUSE:G3"  0 0 1 1
  check "REFUSE:G4"  0 0 0 0
  check "REFUSE:G1"  x 0 0 1
  check "REFUSE:G2"  0 "" 0 1
  check "REFUSE:G3"  0 0 "-1" 1
  check "REFUSE:G4"  0 0 0 2
  if [ "$fails" -gt 0 ]; then echo "self-check: $fails case(s) wrong"; exit 25; fi
  echo "self-check: all 10 cases correct — it passes a clear board and refuses each gate on its own"
  exit 0
fi

say "preflight: window = now + ${DURATION_MIN}m   cooldown = ${COOLDOWN_MIN}m   repo = $OWNER/$REPO"
command -v "$GH" >/dev/null 2>&1 || { say "REFUSING: gh not found at $GH (set GH=...)"; exit 24; }

# ── G1: in-flight / queued Actions runs on dev ────────────────────────────────────────────
# Control: the SAME endpoint, unfiltered. `dev` is written to several times an hour by five
# departments, so a repository with zero runs in its last 50 is not a quiet repository, it is
# an endpoint that did not answer.
RUNS_ALL="$("$GH" api "repos/$OWNER/$REPO/actions/runs?branch=dev&per_page=50" --jq '.workflow_runs|length' 2>/dev/null || echo ERR)"
RUNS="$(  "$GH" api "repos/$OWNER/$REPO/actions/runs?branch=dev&per_page=50" \
          --jq '[.workflow_runs[]|select(.status=="in_progress" or .status=="queued")]|length' 2>/dev/null || echo ERR)"
say "G1  in-flight/queued dev runs = $RUNS   (control: $RUNS_ALL recent dev runs readable)"
case "$RUNS_ALL" in (*[!0-9]*|""|0) say "REFUSING: G1 control failed — the runs endpoint returned '$RUNS_ALL'. A filtered zero beside an unreadable population is not evidence."; exit 24 ;; esac
if [ "$RUNS" != "ERR" ] && [ "${RUNS:-0}" -gt 0 ] 2>/dev/null; then
  "$GH" api "repos/$OWNER/$REPO/actions/runs?branch=dev&per_page=50" \
    --jq '.workflow_runs[]|select(.status=="in_progress" or .status=="queued")|"      \(.name) \(.status) \(.head_sha[0:8]) \(.html_url)"' 2>/dev/null
fi

# ── G2: merge-queue entries on dev ────────────────────────────────────────────────────────
# Control: assert the FIELD RESOLVED, not that a count parsed. A GraphQL error returns
# `repository: null` with the count absent, and an absent count formats as empty -- which is
# what a quiet queue also looks like.
QJSON="$("$GH" api graphql -f query="query { repository(owner:\"$OWNER\", name:\"$REPO\") {
  mergeQueue(branch:\"dev\") { entries(first:50) { totalCount nodes { position state pullRequest { number } } } } } }" 2>/dev/null || echo '{}')"
QRESOLVED="$(printf '%s' "$QJSON" | grep -c '"mergeQueue"' || true)"
QUEUE="$(printf '%s' "$QJSON" | sed -n 's/.*"totalCount":\([0-9]*\).*/\1/p' | head -1)"
say "G2  merge-queue entries on dev = ${QUEUE:-<absent>}   (control: mergeQueue field present = $QRESOLVED)"
[ "$QRESOLVED" = "1" ] || { say "REFUSING: G2 control failed — the mergeQueue field did not resolve. An erroring query reads exactly like an empty queue."; exit 24; }
printf '%s' "$QJSON" | grep -o '"number":[0-9]*' | sed 's/^/      queued PR /' || true

# ── G3: open PRs targeting dev ────────────────────────────────────────────────────────────
# Control: the unfiltered open-PR listing must be readable (it may legitimately be 0, so the
# control is that the CALL succeeded, asserted separately from the count).
PRS_RAW="$("$GH" pr list --repo "$OWNER/$REPO" --base dev --state open --json number,title 2>/dev/null || echo ERR)"
[ "$PRS_RAW" = "ERR" ] && { say "REFUSING: G3 control failed — could not list open PRs."; exit 24; }
PRS="$(printf '%s' "$PRS_RAW" | grep -o '"number":' | wc -l | tr -d ' ')"
say "G3  open PRs into dev = $PRS"
printf '%s' "$PRS_RAW" | grep -o '"number":[0-9]*' | sed 's/^/      open PR /' || true

# ── G4: how long since a dev run last finished ────────────────────────────────────────────
# This one is about MEASUREMENT QUALITY, not collision: a staging stack recreated four minutes
# ago has cold caches and an empty connection pool, and a ladder against it describes the cold
# start rather than the box. Stated as its own gate so it is not mistaken for a safety check.
LAST="$("$GH" api "repos/$OWNER/$REPO/actions/runs?branch=dev&status=completed&per_page=1" \
        --jq '.workflow_runs[0].updated_at // empty' 2>/dev/null || echo "")"
if [ -z "$LAST" ]; then
  say "REFUSING: G4 control failed — no completed dev run readable, so 'recently' cannot be evaluated."
  exit 24
fi
LAST_EPOCH="$(date -u -d "$LAST" +%s 2>/dev/null || echo "")"
NOW_EPOCH="$(date -u +%s)"
case "$LAST_EPOCH" in (*[!0-9]*|"") say "REFUSING: G4 — could not parse '$LAST' as a time."; exit 24 ;; esac
AGE_MIN=$(( (NOW_EPOCH - LAST_EPOCH) / 60 ))
COOLED=0; [ "$AGE_MIN" -ge "$COOLDOWN_MIN" ] && COOLED=1
say "G4  newest completed dev run finished ${AGE_MIN}m ago (need >= ${COOLDOWN_MIN}m) -> cooled=$COOLED"

# ── decide ────────────────────────────────────────────────────────────────────────────────
V="$(verdict "$RUNS" "${QUEUE:-ERR}" "$PRS" "$COOLED")"
if [ "$V" = "PASS" ]; then
  say "PREFLIGHT PASS — no existing trigger is pointed at staging."
  say "  This lowers the probability of a collision; it does not remove it. A department can"
  say "  merge ninety seconds from now. Re-read this before the ladder's top stage, and stop"
  say "  the run rather than reasoning about a staging deploy that started mid-ladder."
  say "  Next: on the box, bash scripts/loadtest/run.sh --keys 300"
  exit 0
fi
GATE="$(printf '%s' "$V" | cut -d: -f2)"
say "PREFLIGHT REFUSED at $GATE"
say "  $(printf '%s' "$V" | cut -d: -f3-)"
say "  NOT taking the run is a legitimate outcome. A number measured while staging was being"
say "  rebuilt underneath it is not a floor, and the founder's label demands better."
case "$GATE" in
  G1) exit 20 ;; G2) exit 21 ;; G3) exit 22 ;; G4) exit 23 ;; *) exit 24 ;;
esac
