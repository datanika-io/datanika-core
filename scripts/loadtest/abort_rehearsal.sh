#!/usr/bin/env bash
# Prove the load test's ABORT CRITERIA discriminate — before any number from them is trusted.
#
#   bash scripts/loadtest/abort_rehearsal.sh
#
# core#778 gap 1. `k6_baseline.js` delays the LATENCY abort until the first stage is complete
# (`delayAbortEval`), because a percentile over the opening seconds of an open-model ladder is
# a percentile over a handful of samples. A delay that has never been seen doing anything is
# the same non-evidence as a guard that has never been seen refusing, so this drives the REAL
# generator script against a synthetic target in three populations, and requires three
# DIFFERENT answers:
#
#   A  every request slow (1.5 s)      -> MUST abort, and not before the first stage ends
#   B  an opening tail of 3 slow ones  -> must NOT abort; the ladder runs to completion
#   C  B again, with the delay removed -> MUST abort early: the pre-fix behaviour, i.e. gap 1
#
# C is what makes B mean something. Without it, "B did not abort" is equally what a threshold
# that can never fire would print.
#
# Needs only docker. Touches nothing it did not create: its own network and two containers,
# removed on exit. Safe to run anywhere, including beside production — the "target" is a
# 30-line Python server on a private network and the load is 5-10 req/s against it.
set -uo pipefail
export MSYS_NO_PATHCONV=1   # Git Bash would rewrite KEYS_FILE=/tmp/... into a Windows path.

HERE="$(cd "$(dirname "$0")" && pwd)"
K6_IMAGE="grafana/k6:1.7.1"          # the generator's own pin (run.sh)
PY_IMAGE="python:3.12-alpine"
STAGES="5:20s,10:20s"                # first stage 20 s => the latency abort may fire from 20 s
FIRST_S=20
TOTAL_S=40
TAG="lt-rehearsal-$$"
NET="$TAG-net"

cleanup() {
  docker rm -f "$TAG-target" >/dev/null 2>&1 || true
  docker network rm "$NET" >/dev/null 2>&1 || true
}
trap cleanup EXIT

SERVER='
import os, threading, time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
MODE = os.environ["MODE"]; SLOW_N = int(os.environ.get("SLOW_N", "3")); SLOW_S = 1.5
lock = threading.Lock(); seen = [0]
class H(BaseHTTPRequestHandler):
    def do_GET(self):
        delay = 0.0
        if not self.path.startswith("/healthz"):
            with lock:
                seen[0] += 1; n = seen[0]
            delay = SLOW_S if (MODE == "slow" or (MODE == "tail" and n <= SLOW_N)) else 0.005
        time.sleep(delay)
        body = b"{}"
        self.send_response(200); self.send_header("Content-Length", str(len(body))); self.end_headers()
        self.wfile.write(body)
    def log_message(self, *a): pass
ThreadingHTTPServer(("0.0.0.0", 8080), H).serve_forever()
'

GEN="$(cat "$HERE/k6_baseline.js")"
NODELAY="${GEN/, delayAbortEval: first.duration/}"
if [ "$NODELAY" = "$GEN" ]; then
  echo "REFUSING: could not find the delayAbortEval clause to remove for case C —"
  echo "          the rehearsal would compare the script with itself and prove nothing."
  exit 2
fi

run_case() {   # $1=label $2=MODE $3=script-text  -> prints "rc elapsed"
  docker rm -f "$TAG-target" >/dev/null 2>&1 || true
  docker run -d --name "$TAG-target" --network "$NET" -e MODE="$2" -e SRV="$SERVER" \
    "$PY_IMAGE" sh -c 'printf "%s" "$SRV" > /s.py && exec python /s.py' >/dev/null
  for _ in $(seq 1 30); do   # wait until the target answers, from inside the network
    docker run --rm --network "$NET" "$PY_IMAGE" python -c \
      "import urllib.request; urllib.request.urlopen('http://$TAG-target:8080/healthz', timeout=2)" \
      >/dev/null 2>&1 && break
    sleep 1
  done
  local t0 t1 rc
  t0=$(date +%s)
  printf '%s' "$3" | docker run --rm -i --network "$NET" \
    -e TARGET_BASE="http://$TAG-target:8080" -e NEIGHBOUR_BASE="http://$TAG-target:8080" \
    -e KEYS_FILE=/tmp/keys.txt -e STAGES="$STAGES" -e NEIGHBOUR_DURATION="${TOTAL_S}s" \
    -e PRE_VUS=20 -e MAX_VUS=40 --entrypoint sh "$K6_IMAGE" \
    -c 'printf "k%s\n" 1 2 3 4 5 6 7 8 > /tmp/keys.txt && exec k6 run --quiet -' \
    > "/tmp/$TAG-$1.out" 2>&1
  rc=$?
  t1=$(date +%s)
  echo "$rc $((t1 - t0))"
}

docker network create "$NET" >/dev/null || { echo "REFUSING: cannot create $NET"; exit 3; }
docker pull -q "$K6_IMAGE" >/dev/null && docker pull -q "$PY_IMAGE" >/dev/null || { echo "cannot pull images"; exit 3; }

FAIL=0
read -r rcA tA < <(run_case A slow "$GEN")
read -r rcB tB < <(run_case B tail "$GEN")
read -r rcC tC < <(run_case C tail "$NODELAY")

verdict() { if eval "$2"; then echo "  PASS  $1"; else echo "  FAIL  $1"; FAIL=1; fi; }
echo "rehearsal: stages=$STAGES (latency abort may fire from ${FIRST_S}s), k6 $K6_IMAGE"
echo "  A every request slow        : k6 exit $rcA after ${tA}s"
echo "  B opening tail of 3 slow    : k6 exit $rcB after ${tB}s"
echo "  C tail, delay removed       : k6 exit $rcC after ${tC}s"
# k6 exits 99 when a threshold with abortOnFail is crossed.
verdict "A aborted on its thresholds (exit 99)"              '[ "$rcA" = 99 ]'
verdict "A did not abort before the first stage ended"       '[ "$tA" -ge "$FIRST_S" ]'
verdict "A aborted before the ladder finished"               '[ "$tA" -lt "$TOTAL_S" ]'
verdict "B ran to completion with every threshold met (0)"   '[ "$rcB" = 0 ]'
verdict "C aborted (exit 99) — the pre-fix behaviour, gap 1" '[ "$rcC" = 99 ]'
verdict "C aborted inside the first stage"                   '[ "$tC" -lt "$FIRST_S" ]'
[ "$FAIL" = 0 ] && echo "ABORT CRITERIA DISCRIMINATE" || {
  echo "ABORT CRITERIA DID NOT BEHAVE — k6 output for each case:"
  for c in A B C; do echo "---- case $c"; tail -25 "/tmp/$TAG-$c.out" 2>/dev/null; done
}
rm -f "/tmp/$TAG-A.out" "/tmp/$TAG-B.out" "/tmp/$TAG-C.out"
exit "$FAIL"
