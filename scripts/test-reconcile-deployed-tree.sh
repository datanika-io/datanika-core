#!/usr/bin/env bash
# Proof harness for reconcile-deployed-tree.sh (core#1178).
#
# Proves BOTH DIRECTIONS IN ONE RUN, which is the whole requirement:
#   - it DOES remove a file the new archive omits, and
#   - it does NOT touch the preserved set,
# plus the two fail-closed floors that stop a truncated manifest from deleting
# production, and a negative control for every one of them.
set -uo pipefail
HERE="$(cd "$(dirname "$0")" && pwd)"
SUT="$HERE/reconcile-deployed-tree.sh"
PASS=0; FAIL=0
ok()   { PASS=$((PASS+1)); echo "  ok   - $1"; }
bad()  { FAIL=$((FAIL+1)); echo "  FAIL - $1"; }
check(){ if [ "$2" = "$3" ]; then ok "$1"; else bad "$1 (expected '$3', got '$2')"; fi; }

newfix() {                       # $1 = dir
  rm -rf "$1"; mkdir -p "$1/datanika/scripts" "$1/datanika/.secrets" "$1/datanika-cloud"
  # 520 shipped files, to clear the 500-entry floor honestly
  for i in $(seq 1 520); do echo "x" > "$1/datanika/f$i.py"; done
  echo "retired"  > "$1/datanika/scripts/backup-postgres.sh"
  echo "SECRET"   > "$1/datanika/.env.docker"
  echo "SECRET"   > "$1/datanika/.secrets/client_secret.json"
  echo "runtime"  > "$1/datanika/reflex.db"
  ( cd "$1" && find datanika datanika-cloud -type f | LC_ALL=C sort ) > "$1/all.txt"
}
# manifest of everything EXCEPT the box-owned set (mirrors the real tarball excludes)
shipped() { LC_ALL=C grep -vE '\.env\.docker|\.secrets/|reflex\.db' "$1/all.txt"; }

echo "== 1. seed run removes nothing and writes a manifest =="
T=$(mktemp -d); newfix "$T"; shipped "$T" > "$T/m1.txt"
DATANIKA_ROOT="$T" bash "$SUT" "$T/m1.txt" --apply >/dev/null 2>&1
check "seed writes manifest"          "$([ -f "$T/.deploy-manifest" ] && echo y || echo n)" "y"
check "seed keeps the retired file"   "$([ -f "$T/datanika/scripts/backup-postgres.sh" ] && echo y || echo n)" "y"

echo "== 2. BOTH DIRECTIONS, one run: removes the omitted file, spares the preserved set =="
LC_ALL=C grep -v 'scripts/backup-postgres.sh' "$T/m1.txt" > "$T/m2.txt"
OUT=$(DATANIKA_ROOT="$T" bash "$SUT" "$T/m2.txt" --apply 2>&1)
check "REMOVES the file the archive omits" "$([ -f "$T/datanika/scripts/backup-postgres.sh" ] && echo present || echo gone)" "gone"
check "SPARES .env.docker"                 "$([ -f "$T/datanika/.env.docker" ] && echo present || echo gone)" "present"
check "SPARES .secrets/client_secret.json" "$([ -f "$T/datanika/.secrets/client_secret.json" ] && echo present || echo gone)" "present"
check "SPARES reflex.db"                   "$([ -f "$T/datanika/reflex.db" ] && echo present || echo gone)" "present"
check "removed exactly one"                "$(echo "$OUT" | grep -c '^reconcile: REMOVED')" "1"

echo "== 3. NEGATIVE CONTROL: the removal assertion can fail =="
T2=$(mktemp -d); newfix "$T2"; shipped "$T2" > "$T2/m1.txt"
DATANIKA_ROOT="$T2" bash "$SUT" "$T2/m1.txt" --apply >/dev/null 2>&1
DATANIKA_ROOT="$T2" bash "$SUT" "$T2/m1.txt" --apply >/dev/null 2>&1   # same manifest: nothing omitted
check "unchanged manifest removes nothing" "$([ -f "$T2/datanika/scripts/backup-postgres.sh" ] && echo present || echo gone)" "present"

echo "== 4. NEGATIVE CONTROL: deny-list fires even if the path IS in the manifest =="
T3=$(mktemp -d); newfix "$T3"; cp "$T3/all.txt" "$T3/m1.txt"          # manifest WRONGLY includes secrets
DATANIKA_ROOT="$T3" bash "$SUT" "$T3/m1.txt" --apply >/dev/null 2>&1
LC_ALL=C grep -vE '\.env\.docker|\.secrets/|reflex\.db' "$T3/m1.txt" > "$T3/m2.txt"
OUT3=$(DATANIKA_ROOT="$T3" bash "$SUT" "$T3/m2.txt" --apply 2>&1)
check "deny-list spares .env.docker"  "$([ -f "$T3/datanika/.env.docker" ] && echo present || echo gone)" "present"
check "deny-list spares the secret"   "$([ -f "$T3/datanika/.secrets/client_secret.json" ] && echo present || echo gone)" "present"
check "deny-list announced itself"    "$(echo "$OUT3" | grep -c 'SKIP (deny-listed)')" "3"

echo "== 5. truncated manifest FAILS CLOSED (the catastrophic case) =="
T4=$(mktemp -d); newfix "$T4"; shipped "$T4" > "$T4/m1.txt"
DATANIKA_ROOT="$T4" bash "$SUT" "$T4/m1.txt" --apply >/dev/null 2>&1
head -3 "$T4/m1.txt" > "$T4/trunc.txt"
OUT4=$(DATANIKA_ROOT="$T4" bash "$SUT" "$T4/trunc.txt" --apply 2>&1); RC4=$?
check "truncated manifest exits non-zero" "$([ $RC4 -ne 0 ] && echo y || echo n)" "y"
check "truncated manifest deleted nothing" "$(find "$T4/datanika" -name 'f*.py' | wc -l | tr -d ' ')" "520"
check "and said why"                       "$(echo "$OUT4" | grep -c 'floor is 500')" "1"

echo "== 6. removal cap FAILS CLOSED =="
T5=$(mktemp -d); newfix "$T5"; shipped "$T5" > "$T5/m1.txt"
DATANIKA_ROOT="$T5" bash "$SUT" "$T5/m1.txt" --apply >/dev/null 2>&1
head -505 "$T5/m1.txt" > "$T5/m2.txt"                                  # omits ~19 -> under cap
LC_ALL=C grep -c . "$T5/m2.txt" >/dev/null
OUT5=$(DATANIKA_MAX_REMOVALS=5 DATANIKA_ROOT="$T5" bash "$SUT" "$T5/m2.txt" --apply 2>&1); RC5=$?
check "cap breach exits non-zero"  "$([ $RC5 -ne 0 ] && echo y || echo n)" "y"
check "cap breach deleted nothing" "$(find "$T5/datanika" -name 'f*.py' | wc -l | tr -d ' ')" "520"

echo "== 7. dry run is the default and mutates nothing =="
T6=$(mktemp -d); newfix "$T6"; shipped "$T6" > "$T6/m1.txt"
DATANIKA_ROOT="$T6" bash "$SUT" "$T6/m1.txt" --apply >/dev/null 2>&1
LC_ALL=C grep -v 'scripts/backup-postgres.sh' "$T6/m1.txt" > "$T6/m2.txt"
OUT6=$(DATANIKA_ROOT="$T6" bash "$SUT" "$T6/m2.txt" 2>&1)              # no --apply
check "dry run keeps the file"     "$([ -f "$T6/datanika/scripts/backup-postgres.sh" ] && echo present || echo gone)" "present"
check "dry run says 'would remove'" "$(echo "$OUT6" | grep -c 'would remove')" "1"

echo "== 8. bootstrap graveyard removes pre-manifest orphans, even on the SEED run =="
T7=$(mktemp -d); newfix "$T7"; shipped "$T7" > "$T7/m1.txt"
printf '# comment
scripts/backup-postgres.sh
.env.docker
' > "$T7/datanika/scripts/retired-paths.txt"
OUT7=$(DATANIKA_ROOT="$T7" bash "$SUT" "$T7/m1.txt" --apply 2>&1)   # seed run
check "graveyard removes the orphan on seed" "$([ -f "$T7/datanika/scripts/backup-postgres.sh" ] && echo present || echo gone)" "gone"
check "graveyard REFUSES a deny-listed entry" "$([ -f "$T7/datanika/.env.docker" ] && echo present || echo gone)" "present"
check "and said it refused"                   "$(echo "$OUT7" | grep -c 'SKIP retired (deny-listed)')" "1"
check "comments ignored"                      "$(echo "$OUT7" | grep -c 'REMOVED retired')" "1"

echo "== 9. NEGATIVE CONTROL: graveyard is a no-op when the file is already gone =="
OUT8=$(DATANIKA_ROOT="$T7" bash "$SUT" "$T7/m1.txt" --apply 2>&1)
check "idempotent second run removes nothing" "$(echo "$OUT8" | grep -c 'REMOVED retired')" "0"

rm -rf "$T" "$T2" "$T3" "$T4" "$T5" "$T6" "$T7"
echo
echo "=== $PASS passed, $FAIL failed ==="
[ "$FAIL" -eq 0 ] || exit 1
