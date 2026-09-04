#!/bin/bash
# Rebuild-parity drill — answers ONE question the restore drill structurally cannot:
#
#     "If we rebuilt production from source today, would we get production's plans back?"
#
# The restore drill (restore-drill.sh) proves the BACKUPS restore. That is a different
# recovery path from the one we have actually performed: on 2026-07-14 the Hetzner box was
# terminated with its data AND its backups, and on 2026-07-17 prod was rebuilt from source.
# In that path there is no dump to restore — the database comes from `alembic upgrade head`
# plus whatever a human does next, and nothing anywhere checks the result against what
# production was serving.
#
# core#1060 is that gap with a number on it: a from-scratch build creates the `free` plan and
# NOTHING ELSE, so every paid row is created out of band, and a paid row created without
# naming four columns silently takes FREE-TIER values — seats 2, connections 5, runs 500,
# extra seat $12 — on a plan sold at $399/mo. core#928 is the same class.
#
# 🔑 Why this cannot be a migration, and why it is a drill instead. Engineering's own first
# fix was a correction migration, and they retracted it: a correction migration runs at the
# same point in the chain as the UPDATE it corrects and matches ZERO rows for exactly the
# same reason — the paid slugs still do not exist yet. It inherits the defect. So the only
# thing that catches this is measuring the rebuilt database against the live one.
#
# Deployed to:   /opt/datanika/scripts/rebuild-parity-drill.sh on 185.25.22.188
# Cron (monthly): 30 5 1 * * /opt/datanika/scripts/rebuild-parity-drill.sh >> /var/log/datanika-rebuild-parity.log 2>&1
# Canonical copy: deploy/server/rebuild-parity-drill.sh (datanika-core)
# Invariants pinned by: tests/test_deploy/test_rebuild_parity_drill.py
#
# ⚠️ NOTHING DEPLOYS THIS FILE (core#747) — the copy that runs is hand-installed.
# After changing it, install it and compare sha256 against git.
#
# ── Safety ───────────────────────────────────────────────────────────────────
# The throwaway database sits on its OWN docker network. Production's postgres is on
# `datanika_default` and is NOT reachable from it. So if the DATABASE_URL override ever
# stopped taking effect, alembic errors out — it does not quietly migrate production.
# That is deliberate: fail-closed, because the failure this guards against is unrecoverable.
# The pre-flight below additionally asks the IMAGE'S OWN settings object which URL it
# resolved, and aborts unless the answer names the throwaway host. Reading the env var we
# just set would prove nothing (core#646: a value set under a name its consumer does not
# read is invisible in every direction).
#
# This script only ever READS the production database.

set -uo pipefail

IMAGE="${IMAGE:-ghcr.io/datanika-io/datanika-core:latest}"
ENV_FILE="${ENV_FILE:-/opt/datanika/datanika/.env.docker}"
PROD_CONTAINER="${PROD_CONTAINER:-datanika-postgres}"
NET=datanika-rebuild-parity-net
DB=datanika-rebuild-parity-db
WORK=/tmp/rebuild-parity.$$
TEXTFILE_DIR="${TEXTFILE_DIR:-/opt/datanika/node_textfile}"   # the ONLY dir node-exporter reads

# Columns that legitimately differ between a fresh build and production, with the reason.
# Keep this list SHORT and justified — every name here is an assertion switched off.
#   id, created_at, updated_at : per-row, per-instance. Never comparable.
#   paddle_*                   : vendor ids, environment-specific by construction.
IGNORE_COLS="id created_at updated_at paddle_product_id paddle_price_id"

# ── The pinned expectation (core#1060 / core#928) ────────────────────────────
# A from-scratch build does not create the paid plans. That is a KNOWN, TRACKED defect, so
# this drill does not go red for it every month — it pins the exact shape of the gap and
# fails when the shape CHANGES, in either direction.
#
# 🚨 If this drill fails on the fingerprint, do NOT just paste the new value in.
#    - fingerprint SHRANK  -> the defect is being fixed. Confirm against core#1060, update
#                             this line in the same commit that fixes it, and close the issue.
#    - fingerprint GREW    -> a plan or a column was added that a rebuild will not reproduce.
#                             That is a NEW instance of core#928's class. File it.
# The gap itself is printed in full on every run regardless, so it is never only a hash.
#
# Pinned 2026-09-04 against `:latest` (master a9c4e2b7d5f3): 4 missing slugs, 30 wrong-by-default
# column values. 🔔 THE NEXT RUN AFTER THE 2026-09-04 PROMOTION IS EXPECTED TO GO RED, and that
# is this check's arming proof rather than a defect: core#1047 (`b4d8f1a2c6e9`) drops the
# `max_schedules` server default, which removes 4 of the 30 lines. Re-pin then, having read the
# gap — do not pre-empt it by pinning a number nobody has measured.
#
# ⚠️ `${VAR-default}`, NOT `${VAR:-default}`. With `:-` an explicitly EMPTY override falls back to
# the pinned value, which made the "nothing pinned" branch below structurally unreachable — a
# branch that can never fire, found by running the arm that was supposed to exercise it. With `-`
# only an UNSET variable takes the default, so `EXPECTED_GAP= ` reaches the bootstrap path and the
# guard is testable.
EXPECTED_GAP="${EXPECTED_GAP-d40ef6fd71337e53}"

cleanup() {
    docker rm -f "${DB}" >/dev/null 2>&1 || true
    docker network rm "${NET}" >/dev/null 2>&1 || true
    rm -rf "${WORK}"
}
trap cleanup EXIT HUP INT TERM

mkdir -p "${WORK}"
START_EPOCH=$(date +%s)
FAILURES=""
note_fail() { FAILURES="${FAILURES}
  - $1"; }

psql_prod() { docker exec "${PROD_CONTAINER}" psql -U datanika -d datanika -At -F'|' -c "$1" </dev/null; }
psql_fresh() { docker exec "${DB}" psql -U datanika -d datanika -At -F'|' -c "$1" </dev/null; }

echo "[$(date)] rebuild-parity drill starting (image=${IMAGE})"

# ── 1. throwaway postgres on an isolated network ─────────────────────────────
docker rm -f "${DB}" >/dev/null 2>&1 || true
docker network rm "${NET}" >/dev/null 2>&1 || true
docker network create "${NET}" >/dev/null || { echo "[$(date)] REBUILD PARITY ABORT — could not create network"; exit 1; }
docker run -d --name "${DB}" --network "${NET}" \
    -e POSTGRES_USER=datanika -e POSTGRES_PASSWORD=paritydrill -e POSTGRES_DB=datanika \
    postgres:16-alpine >/dev/null || { echo "[$(date)] REBUILD PARITY ABORT — could not start throwaway postgres"; exit 1; }

READY=0
for _ in $(seq 1 45); do
    # `</dev/null` on every docker exec, not only the ones whose output we read: this
    # script is run over ssh, and an exec that inherits stdin eats the rest of the script.
    if docker exec "${DB}" psql -U datanika -d datanika -c 'select 1' </dev/null >/dev/null 2>&1; then READY=1; break; fi
    sleep 1
done
[ "${READY}" = 1 ] || { echo "[$(date)] REBUILD PARITY ABORT — throwaway postgres never became ready"; exit 1; }

SYNC_URL="postgresql://datanika:paritydrill@${DB}:5432/datanika"
ASYNC_URL="postgresql+asyncpg://datanika:paritydrill@${DB}:5432/datanika"
RUN_IMG=(docker run --rm --network "${NET}" --env-file "${ENV_FILE}"
         -e DATABASE_URL_SYNC="${SYNC_URL}" -e DATABASE_URL="${ASYNC_URL}" -w /app "${IMAGE}")

# ── 2. pre-flight: ask the image which URL it RESOLVED, not which one we set ──
RESOLVED=$("${RUN_IMG[@]}" /app/.venv/bin/python -c \
    "from datanika.config import settings; print(settings.database_url_sync)" 2>"${WORK}/preflight.err" | tail -1)
case "${RESOLVED}" in
    *"${DB}:5432"*) echo "[$(date)] pre-flight OK — the image resolves the throwaway host" ;;
    *) echo "[$(date)] REBUILD PARITY ABORT — the image did not resolve the throwaway database."
       echo "  Refusing to continue: the whole safety argument is that alembic cannot reach production."
       echo "  resolved: $(printf '%s' "${RESOLVED}" | sed 's#://[^@]*@#://***@#')"
       tail -5 "${WORK}/preflight.err" 2>/dev/null || true
       exit 1 ;;
esac

# ── 3. the rebuild itself: alembic upgrade head, from the SERVING image ──────
echo "[$(date)] alembic upgrade head into the throwaway database..."
"${RUN_IMG[@]}" /app/.venv/bin/alembic upgrade head > "${WORK}/alembic.log" 2>&1
ALEMBIC_RC=$?
if [ "${ALEMBIC_RC}" != 0 ]; then
    echo "[$(date)] REBUILD PARITY FAIL — alembic upgrade head exited ${ALEMBIC_RC} on an EMPTY database."
    echo "  This is the strongest possible finding: the from-scratch recovery path is broken."
    tail -25 "${WORK}/alembic.log"
    exit 1
fi
echo "[$(date)] rebuild complete, head=$(psql_fresh 'select version_num from alembic_version')"

# ── 4. anti-vacuity: a comparison against nothing passes against anything ────
LIVE_ROWS=$(psql_prod "select count(*) from plans")
FRESH_ROWS=$(psql_fresh "select count(*) from plans")
if [ "${LIVE_ROWS:-0}" -lt 1 ]; then
    echo "[$(date)] REBUILD PARITY ABORT — the live database reports 0 plans."
    echo "  Refusing a verdict rather than reporting a pass. That is core#725's plans>=5 inverted:"
    echo "  every comparison below would succeed trivially."
    exit 1
fi
if [ "${FRESH_ROWS:-0}" -lt 1 ]; then
    echo "[$(date)] REBUILD PARITY FAIL — the rebuilt database has 0 plans."
    echo "  A rebuild that seeds no plan at all cannot serve anyone, free tier included."
    exit 1
fi
echo "[$(date)] live plans: ${LIVE_ROWS}   from-scratch plans: ${FRESH_ROWS}"

# ── 5. schema parity on `plans` ──────────────────────────────────────────────
COLS_SQL="select string_agg(column_name, ' ' order by column_name) from information_schema.columns where table_name='plans'"
LIVE_COLS=$(psql_prod "${COLS_SQL}")
FRESH_COLS=$(psql_fresh "${COLS_SQL}")
if [ "${LIVE_COLS}" != "${FRESH_COLS}" ]; then
    note_fail "plans SCHEMA differs between production and a from-scratch build.
      only in prod:  $(comm -23 <(tr ' ' '\n' <<<"${LIVE_COLS}" | sort) <(tr ' ' '\n' <<<"${FRESH_COLS}" | sort) | tr '\n' ' ')
      only in fresh: $(comm -13 <(tr ' ' '\n' <<<"${LIVE_COLS}" | sort) <(tr ' ' '\n' <<<"${FRESH_COLS}" | sort) | tr '\n' ' ')
      A migration reached one and not the other, or prod is mid expand/contract."
fi

# Compare only columns present on BOTH sides, minus the justified ignores.
COMPARE_COLS=""
for c in ${FRESH_COLS}; do
    case " ${LIVE_COLS} " in *" ${c} "*) ;; *) continue ;; esac
    case " ${IGNORE_COLS} " in *" ${c} "*) continue ;; esac
    COMPARE_COLS="${COMPARE_COLS} ${c}"
done
NCOMPARE=$(wc -w <<<"${COMPARE_COLS}")
if [ "${NCOMPARE}" -lt 5 ]; then
    echo "[$(date)] REBUILD PARITY ABORT — only ${NCOMPARE} comparable column(s) on plans."
    echo "  Refusing a verdict: a column-wise check over almost no columns is not a check."
    exit 1
fi
echo "[$(date)] comparing ${NCOMPARE} column(s) per slug"

# ── 6. slug parity, and per-slug column divergence where both sides have a row ──
dump_slugs() { local q="select slug"; for c in ${COMPARE_COLS}; do q="${q}, coalesce(${c}::text,'<null>')"; done
               echo "${q} from plans order by slug"; }
psql_prod  "$(dump_slugs)" | sort > "${WORK}/live.txt"
psql_fresh "$(dump_slugs)" | sort > "${WORK}/fresh.txt"

cut -d'|' -f1 "${WORK}/live.txt"  > "${WORK}/live.slugs"
cut -d'|' -f1 "${WORK}/fresh.txt" > "${WORK}/fresh.slugs"
MISSING_SLUGS=$(comm -23 "${WORK}/live.slugs" "${WORK}/fresh.slugs" | tr '\n' ' ')
EXTRA_SLUGS=$(comm -13 "${WORK}/live.slugs" "${WORK}/fresh.slugs" | tr '\n' ' ')

# Shared slugs must agree exactly. There is NO pinned expectation for this — a slug a rebuild
# does create and gets wrong is a straightforward defect with nothing to negotiate.
DIVERGED=0
while IFS= read -r slug; do
    [ -n "${slug}" ] || continue
    L=$(grep -m1 "^${slug}|" "${WORK}/live.txt")
    F=$(grep -m1 "^${slug}|" "${WORK}/fresh.txt")
    [ -n "${F}" ] || continue
    i=1
    for c in ${COMPARE_COLS}; do
        i=$((i + 1))
        lv=$(cut -d'|' -f${i} <<<"${L}"); fv=$(cut -d'|' -f${i} <<<"${F}")
        if [ "${lv}" != "${fv}" ]; then
            DIVERGED=$((DIVERGED + 1))
            note_fail "slug '${slug}' column '${c}': production=${lv}  from-scratch=${fv}"
        fi
    done
done < "${WORK}/live.slugs"

# ── 7. the core#1060 finding, DERIVED rather than restated ───────────────────
# For each slug a rebuild does not create, report what a bare out-of-band INSERT would take
# from the column defaults, next to what production actually serves. Computed from the live
# catalogue, so it stays true as columns are added — the point of core#1060 is precisely that
# these two disagree and nothing says so.
: > "${WORK}/gap.txt"
psql_prod "select column_name || '=' || column_default from information_schema.columns
           where table_name='plans' and column_default is not null
             and column_default not like 'nextval%' and column_default <> 'now()'
           order by column_name" > "${WORK}/defaults.txt"
for slug in ${MISSING_SLUGS}; do
    L=$(grep -m1 "^${slug}|" "${WORK}/live.txt")
    while IFS= read -r d; do
        [ -n "${d}" ] || continue
        col=${d%%=*}; def=${d#*=}
        def=${def%%::*}; def=$(sed "s/^'//; s/'$//" <<<"${def}")
        case " ${COMPARE_COLS} " in *" ${col} "*) ;; *) continue ;; esac
        i=1; for c in ${COMPARE_COLS}; do i=$((i + 1)); [ "${c}" = "${col}" ] && break; done
        lv=$(cut -d'|' -f${i} <<<"${L}")
        [ "${lv}" = "${def}" ] || echo "${slug}.${col}: production=${lv} default_would_give=${def}" >> "${WORK}/gap.txt"
    done < "${WORK}/defaults.txt"
done
GAP_LINES=$(wc -l < "${WORK}/gap.txt")

echo
echo "[$(date)] ── what a from-scratch rebuild would NOT give you ──"
echo "  plan slugs production serves but a rebuild does not create: ${MISSING_SLUGS:-<none>}"
[ -n "${EXTRA_SLUGS}" ] && echo "  plan slugs a rebuild creates but production does not have: ${EXTRA_SLUGS}"
if [ "${GAP_LINES}" -gt 0 ]; then
    echo "  columns where an out-of-band INSERT would silently take a wrong value (core#1060):"
    sed 's/^/    /' "${WORK}/gap.txt"
fi
echo

# ── 8. the pinned expectation ────────────────────────────────────────────────
GAP_FP=$( (echo "slugs:${MISSING_SLUGS}"; sort "${WORK}/gap.txt") | sha256sum | cut -c1-16)
echo "[$(date)] gap fingerprint: ${GAP_FP} (${GAP_LINES} column(s), $(wc -w <<<"${MISSING_SLUGS}") missing slug(s))"
if [ -z "${EXPECTED_GAP}" ]; then
    note_fail "no EXPECTED_GAP pinned in this script. Set it to ${GAP_FP} after reading the gap above."
elif [ "${GAP_FP}" != "${EXPECTED_GAP}" ]; then
    note_fail "gap fingerprint changed: expected ${EXPECTED_GAP}, measured ${GAP_FP}.
      SHRANK  -> core#1060 is being fixed. Update EXPECTED_GAP in the same commit and close it.
      GREW    -> a new instance of core#928's class. File it; do not just re-pin."
fi

# ── 9. metrics, written on BOTH paths ────────────────────────────────────────
# Deliberately before the verdict: 'how far a rebuild is from production' is the number worth
# graphing, and it is most interesting exactly when this script exits non-zero.
ELAPSED=$(( $(date +%s) - START_EPOCH ))
OK=1; [ -n "${FAILURES}" ] && OK=0
mkdir -p "${TEXTFILE_DIR}"
TMP="${TEXTFILE_DIR}/datanika_rebuild_parity.prom.$$"
{
    echo "# HELP datanika_rebuild_parity_last_run_timestamp_seconds Unix time of the last rebuild-parity drill, pass or fail"
    echo "# TYPE datanika_rebuild_parity_last_run_timestamp_seconds gauge"
    echo "datanika_rebuild_parity_last_run_timestamp_seconds $(date +%s)"
    echo "# HELP datanika_rebuild_parity_ok Whether a from-scratch build reproduced production's plans as expected"
    echo "# TYPE datanika_rebuild_parity_ok gauge"
    echo "datanika_rebuild_parity_ok ${OK}"
    echo "# HELP datanika_rebuild_parity_slugs_missing Plan slugs production serves that a from-scratch build does not create"
    echo "# TYPE datanika_rebuild_parity_slugs_missing gauge"
    echo "datanika_rebuild_parity_slugs_missing $(wc -w <<<"${MISSING_SLUGS}")"
    echo "# HELP datanika_rebuild_parity_columns_wrong_by_default Columns an out-of-band paid-plan INSERT would take a wrong value for"
    echo "# TYPE datanika_rebuild_parity_columns_wrong_by_default gauge"
    echo "datanika_rebuild_parity_columns_wrong_by_default ${GAP_LINES}"
    echo "# HELP datanika_rebuild_parity_columns_diverged Columns disagreeing on a slug BOTH sides have"
    echo "# TYPE datanika_rebuild_parity_columns_diverged gauge"
    echo "datanika_rebuild_parity_columns_diverged ${DIVERGED}"
    echo "# HELP datanika_rebuild_parity_duration_seconds Wall-clock duration of the last rebuild-parity drill"
    echo "# TYPE datanika_rebuild_parity_duration_seconds gauge"
    echo "datanika_rebuild_parity_duration_seconds ${ELAPSED}"
} > "${TMP}"
mv "${TMP}" "${TEXTFILE_DIR}/datanika_rebuild_parity.prom"

# ── 10. verdict ──────────────────────────────────────────────────────────────
if [ -n "${FAILURES}" ]; then
    echo "[$(date)] REBUILD PARITY FAIL — a from-scratch rebuild does not reproduce production:${FAILURES}"
    exit 1
fi
echo "[$(date)] REBUILD PARITY PASS (${LIVE_ROWS} live plans, ${NCOMPARE} columns compared, gap ${GAP_FP} as expected, ${ELAPSED}s)"
