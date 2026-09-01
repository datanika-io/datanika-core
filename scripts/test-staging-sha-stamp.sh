#!/bin/bash
# Controls for the staging SHA attribution assertion (core#876).
#
# Gated in ci.yml's `lint` job. Every case below is a shape that has occurred or that the
# assertion is claimed to reject — including the two that matter most:
#
#   * the stamp is MISSING ENTIRELY (not merely disagreeing), and
#   * EXPECTED_SHA is empty, so an unarmed assertion compares nothing with nothing.
#
# Both are the vacuous-green shape this whole issue is about. A guard nobody has watched
# fail is not evidence, so the refusals are asserted by OUTCOME (exit status + the message
# naming the reason), never by "it ran without error".

set -u
cd "$(dirname "$0")/.."

ASSERT=scripts/assert-staging-sha.sh
WRITER=scripts/staging-deploy-stamp.sh
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT

PASS=0
FAIL=0
A=0

ok() { PASS=$((PASS + 1)); A=$((A + 1)); printf '  ok   %s\n' "$1"; }
no() { FAIL=$((FAIL + 1)); A=$((A + 1)); printf '  FAIL %s\n' "$1"; }

# Run the asserter against a canned `report` payload. Never SSHes.
run_assert() { # <expected sha> <report text>
  printf '%s\n' "$2" > "$TMP/report"
  EXPECTED_SHA="$1" STAGING_STAMP_FILE="$TMP/report" bash "$ASSERT" > "$TMP/out" 2>&1
  echo $?
}

GOOD_SHA=1da0c21b6d3f4a5e8c7b9a0d2e4f6a8b0c1d2e3f
OTHER_SHA=87da585f0e1d2c3b4a5968778695a4b3c2d1e0f9
CID=aa11bb22cc33dd44ee55ff6677889900aa11bb22cc33dd44ee55ff6677889900
IMG=sha256:99887766554433221100ffeeddccbbaa99887766554433221100ffeeddccbbaa

matching_report() {
  cat <<EOF
sha=$GOOD_SHA
run_id=17
run_attempt=1
deployed_at=2026-09-01T10:00:00Z
app_container=$CID
app_image=$IMG
live_container=$CID
live_image=$IMG
EOF
}

expect_pass() { # <label> <expected sha> <report>
  rc=$(run_assert "$2" "$3")
  if [ "$rc" = 0 ]; then ok "$1"; else no "$1 (exit $rc)"; sed 's/^/       /' "$TMP/out"; fi
}

expect_fail() { # <label> <expected sha> <report> <substring the message must contain>
  rc=$(run_assert "$2" "$3")
  if [ "$rc" = 0 ]; then
    no "$1 — ACCEPTED when it must refuse"
  elif grep -qF "$4" "$TMP/out"; then
    ok "$1"
  else
    no "$1 — refused, but for the wrong reason (wanted: $4)"; sed 's/^/       /' "$TMP/out"
  fi
}

echo "== the assertion must PASS only when staging really is running this commit =="
expect_pass "1  matching sha, matching container"  "$GOOD_SHA" "$(matching_report)"

echo "== the three shapes measured on dev =="
# The original core#876 instance: 1da0c21's e2e ran two seconds after 87da585's deploy.
expect_fail "2  staging runs another commit" "$GOOD_SHA" \
  "$(matching_report | sed "s/^sha=.*/sha=$OTHER_SHA/")" "DIFFERENT COMMIT"
# The crossed half: the commit that WAS under test asking about itself must also refuse,
# because the stamp names the other one.
expect_fail "3  the overtaken commit asking about itself" "$OTHER_SHA" \
  "$(matching_report)" "DIFFERENT COMMIT"

echo "== unarmed-assertion shapes: these are the ones that read as green =="
expect_fail "4  no stamp on the box at all" "$GOOD_SHA" \
  "stamp_missing=1
live_container=$CID
live_image=$IMG" "NO deploy stamp"
expect_fail "5  EXPECTED_SHA empty (assertion not armed)" "" "$(matching_report)" "unarmed"
expect_fail "6  EXPECTED_SHA empty AND stamp empty — nothing equals nothing" "" \
  "stamp_missing=1" "unarmed"
expect_fail "7  EXPECTED_SHA is not a sha" "refs/heads/dev" "$(matching_report)" "unarmed"
expect_fail "8  EXPECTED_SHA too short to identify a commit" "1da0c" "$(matching_report)" "too short"
expect_fail "9  stamp present but carries no sha field" "$GOOD_SHA" \
  "run_id=17
app_container=$CID
live_container=$CID" "no sha field"
expect_fail "10 report unreadable (ssh or box failure)" "$GOOD_SHA" "" "could not read"

echo "== the sha agreeing is not the stack being the stamped one =="
expect_fail "11 container replaced after the stamp" "$GOOD_SHA" \
  "$(matching_report | sed "s/^live_container=.*/live_container=deadbeef${CID:8}/")" \
  "container was replaced"
expect_fail "12 no app container running at all" "$GOOD_SHA" \
  "$(matching_report | sed 's/^live_container=.*/live_container=/')" \
  "no staging app container is running"
expect_fail "13 image changed under a stable container id" "$GOOD_SHA" \
  "$(matching_report | sed 's/^live_image=.*/live_image=sha256:0000/')" "image changed"

echo "== the writer refuses to create the unarmed shape =="
STAMP="$TMP/stamp"
out=$(STAGING_STAMP_PATH="$STAMP" bash "$WRITER" write "" 17 1 2>&1); rc=$?
if [ "$rc" != 0 ] && [ ! -e "$STAMP" ]; then ok "14 writer refuses an empty sha and writes nothing"
else no "14 writer wrote a stamp with no sha (rc=$rc): $out"; fi

# active-app.sh failing must not produce a stamp either: a stamp naming a container that
# is not running is a claim the asserter would then have to disprove.
printf '#!/bin/bash\nexit 1\n' > "$TMP/no-app.sh"; chmod +x "$TMP/no-app.sh"
out=$(STAGING_STAMP_PATH="$STAMP" STAGING_ACTIVE_APP="$TMP/no-app.sh" \
        bash "$WRITER" write "$GOOD_SHA" 17 1 2>&1); rc=$?
if [ "$rc" != 0 ] && [ ! -e "$STAMP" ]; then ok "15 writer refuses when no app is running"
else no "15 writer stamped a stack it could not identify (rc=$rc): $out"; fi

echo "== report mode on a box with no stamp says so, and exits 0 =="
out=$(STAGING_STAMP_PATH="$TMP/absent" STAGING_ACTIVE_APP="$TMP/no-app.sh" \
        bash "$WRITER" report 2>&1); rc=$?
if [ "$rc" = 0 ] && printf '%s' "$out" | grep -q 'stamp_missing=1'; then
  ok "16 report exits 0 and reports the absence (the runner owns the verdict)"
else no "16 report mode wrong (rc=$rc): $out"; fi

echo "== an unknown mode is an error, not a silent no-op =="
STAGING_STAMP_PATH="$STAMP" bash "$WRITER" verify >/dev/null 2>&1
if [ $? -ne 0 ]; then ok "17 unknown mode exits non-zero"; else no "17 unknown mode exited 0"; fi

echo
echo "assertions: $A   passed: $PASS   failed: $FAIL"
[ "$A" -ge 17 ] || { echo "harness ran only $A assertions — it has been gutted"; exit 1; }
[ "$FAIL" -eq 0 ] || exit 1
echo "staging SHA attribution controls: all good"
