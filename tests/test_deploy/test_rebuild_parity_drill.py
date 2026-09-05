"""The rebuild-parity drill must stay fail-closed and non-vacuous (core#1060).

The restore drill proves the *backups* restore. It cannot answer the question this
one exists for:

    "If we rebuilt production from source today, would we get production's plans back?"

That is not a hypothetical recovery path — it is the one we performed. The Hetzner
box was terminated on 2026-07-14 with its data *and* its backups, and prod was
rebuilt from source on 2026-07-17. In that path there is no dump; the database comes
from `alembic upgrade head` plus whatever a human does next.

MEASURED ON THE PRODUCTION BOX, 2026-09-04, against the serving image
(`ghcr.io/datanika-io/datanika-core:latest`, alembic head `a9c4e2b7d5f3`), into a
throwaway postgres on an isolated docker network:

    from-scratch `plans` rows                                     1  (`free`)
    rows production serves                                        5
    slugs a rebuild does not create   pro-monthly pro-annual enterprise-monthly enterprise-annual
    column values an out-of-band INSERT would get wrong          30

core#1060 named four of those columns on one slug. The drill derives them from the
live catalogue instead of restating them, and found **thirty across four slugs** —
including three the issue never mentioned, two of which change *behaviour* rather
than a displayed number:

    enterprise-*.sso_enabled     production=true   default_would_give=false
    *.hard_cap_bytes             production=false  default_would_give=true
    enterprise-*.max_parallel_runs  production=20  default_would_give=5

`hard_cap_bytes` is the sharp one: a rebuilt production would **block** Pro and
Enterprise on bytes mid-cycle, which is the exact behaviour the published FAQ
promises it will not do.

UPDATE 2026-09-05 (core#1071) — THE MEASUREMENT ABOVE HAS CHANGED, and the block
above is kept as the dated record that produced the decision rather than edited.
Core migration `e8b3d5c7f2a9` sets `plans.hard_cap_bytes`' server default to
`false`, matching its sibling gate `hard_cap_runs`. So the four
`<paid slug>.hard_cap_bytes: production=false default_would_give=true` lines leave
the gap and it becomes **22 columns**, still across the same 4 missing slugs.

    the pinned fingerprint therefore SHRANK, and that is the designed signal

🚨 The new fingerprint CANNOT be computed from this repository. It is a sha256 over
the live catalogue joined to production's own rows, so only a run on the box
produces it. Infra re-measures against the `:staging` image once this is on `dev` —
the same route that caught core#1047's four `max_schedules` lines before promotion —
and re-pins `EXPECTED_GAP` in that commit. Until then the drill is expected to fail
on the fingerprint with `SHRANK`, which the script's own message tells the reader to
confirm against core#1060 rather than paste over. **Do not re-pin from a prediction.**

`RECORDED_DEFAULT_WOULD_GIVE` below holds the three behaviour-changing columns and is
compared against the migration chain by
`test_the_recorded_defaults_still_match_the_migration_tree`, so this narrative cannot
outlive the tree it describes — which is how the defect survived in the first place
(`datanika-cloud/tests/test_bytes_migration_roundtrip.py` said "default False" while
the DDL said True, and asserted nothing).

🔑 AND THE MECHANISM IS ALREADY LIVE, not merely predicted. Staging carries a plan
`e2e-pro` with `seats_included=2, max_connections=5, runs_included=500,
rate_limit_rpm=60` — every one the core default, i.e. Free-tier values on a row
named "E2E Pro". It is built by `datanika_cloud/billing/e2e_admin.py:145`, whose
`Plan(...)` names the byte columns deliberately and every quota column not at all.

WHY THIS CANNOT BE A MIGRATION, so nobody re-files it as one: Engineering's own
first proposal was a correction migration and they retracted it. A correction
migration runs at the same point in the chain as the UPDATE it corrects and matches
**zero rows** for exactly the same reason — the paid slugs still do not exist yet.
It inherits the defect it is correcting. Measuring the rebuilt database against the
live one is the only thing that catches this.
"""

from __future__ import annotations

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "deploy" / "server" / "rebuild-parity-drill.sh"

#: The three behaviour-changing columns from the header's measurement, and what the
#: **migration chain** gives a row that omits them. Not prose: compared against
#: ``server_defaults()`` by ``test_the_recorded_defaults_still_match_the_migration_tree``.
#:
#: A dated narrative is only useful while it is true, and the failure mode of this whole
#: class is a comment that names the *safe* value and terminates the search. Pinning the
#: values here means a change to any of the three reds at PR time and has to be recorded
#: above in the same commit — rather than being noticed a month later by a drill whose
#: fingerprint moved for a reason nobody could name.
RECORDED_DEFAULT_WOULD_GIVE = {
    # Enterprise is sold with SSO; a rebuild silently does not have it.
    "sso_enabled": "false",
    # core#1071 / `e8b3d5c7f2a9`, 2026-09-05: was "true", which blocked Pro and Enterprise
    # on volume mid-cycle against the published FAQ. Now agrees with `hard_cap_runs`.
    "hard_cap_bytes": "false",
    # Enterprise pays for 20; a rebuild gives 5.
    "max_parallel_runs": "5",
}


def _strip_comments(text: str) -> list[str]:
    """Executable lines only.

    Load-bearing, not tidiness: this script *documents* the defect it guards in a
    long header, so a check reading raw text would be satisfied by the explanation
    rather than by the code. That is core#1055's finding, and it cost eight nights
    of a green nightly reporting `12 failed`.
    """
    return [ln for ln in text.splitlines() if not ln.strip().startswith("#")]


# ── The predicates, at module level so the suite can arm them in-suite ────────
# A guard proved discriminating once by an external harness is a claim about a past
# session. These run against a mutated copy of the REAL script every time CI does.


def isolated_network(code: list[str]) -> bool:
    """The throwaway postgres and every image run share a private network.

    This is the whole safety argument: production's postgres is on
    `datanika_default`, so if the DATABASE_URL override ever stopped taking effect
    alembic cannot reach it and errors out instead of migrating production.
    """
    joined = "\n".join(code)
    return (
        "docker network create" in joined
        and len(re.findall(r'--network "\$\{NET\}"', joined)) >= 2
        and re.search(r'docker run -d --name "\$\{DB\}" --network "\$\{NET\}"', joined) is not None
    )


def preflight_aborts(code: list[str]) -> bool:
    """The pre-flight asks the IMAGE which URL it resolved, and exits on a mismatch.

    Reading back the env var we just set would prove nothing — core#646 is an S1
    that lived for weeks because a value was set under a name its consumer never
    read. So the assertion must go through `settings`, and the non-matching branch
    must actually exit.

    Asserting the tokens is not enough (core#1055): a `case` whose default arm no
    longer exits leaves every token in place and can never fail. So require the
    exit *inside* the fallback arm.
    """
    joined = "\n".join(code)
    if "settings.database_url_sync" not in joined:
        return False
    m = re.search(r"case \"\$\{RESOLVED\}\" in(.*?)esac", joined, re.S)
    if not m:
        return False
    body = m.group(1)
    # The wildcard arm is the abort path; it must both refuse and exit non-zero.
    tail = body.split("*)")[-1]
    return "REBUILD PARITY ABORT" in tail and re.search(r"exit 1", tail) is not None


def refuses_vacuous_verdict(code: list[str]) -> bool:
    """A comparison against an empty production database passes against anything.

    Two guards, and both must branch-then-act: zero live plan rows, and too few
    comparable columns. This is core#725's `plans >= 5` one layer down — that check
    printed PASS beside `users=0`.
    """
    joined = "\n".join(code)
    checks = [
        (r'\[ "\$\{LIVE_ROWS:-0\}" -lt 1 \]', "live database reports 0 plans"),
        (r'\[ "\$\{NCOMPARE\}" -lt 5 \]', "comparable column"),
    ]
    for pattern, _ in checks:
        m = re.search(pattern + r"(.{0,600})", joined, re.S)
        if not m or "exit 1" not in m.group(1):
            return False
    return True


def metrics_are_emitted(code: list[str]) -> bool:
    """The metric names must be WRITTEN, not merely mentioned.

    🚨 Earned the hard way twice. A guard asserting `"datanika_backup_files_torn" in
    body` stayed green against a mutation that deleted the metric entirely, because
    the script also names it in a warning telling the operator what to watch. So
    require each series name to appear on an `echo` whose output is redirected into
    the .prom file, and require the block to be written *before* the verdict — the
    gap is most worth graphing exactly when the drill exits non-zero.
    """
    joined = "\n".join(code)
    required = [
        "datanika_rebuild_parity_ok",
        "datanika_rebuild_parity_slugs_missing",
        "datanika_rebuild_parity_columns_wrong_by_default",
        "datanika_rebuild_parity_columns_diverged",
        "datanika_rebuild_parity_last_run_timestamp_seconds",
    ]
    for name in required:
        if not re.search(rf'^\s*echo "{name} ', joined, re.M):
            return False
    if not re.search(r'mv "\$\{TMP\}" "\$\{TEXTFILE_DIR\}/datanika_rebuild_parity\.prom"', joined):
        return False
    # Written before the verdict, so a failing run still publishes the numbers.
    return joined.index("datanika_rebuild_parity.prom") < joined.rindex("REBUILD PARITY FAIL")


def verdict_can_fail(code: list[str]) -> bool:
    """A non-empty failure list must exit non-zero.

    `^\\s*exit\\s+[1-9]` is not enough on its own — `echo "..."; exit 1` on one line
    is how such an edit actually gets written, and a line-anchored pattern is blind
    to the realistic mutation while catching only the tidy one.
    """
    joined = "\n".join(code)
    m = re.search(r'if \[ -n "\$\{FAILURES\}" \]; then(.*?)\nfi', joined, re.S)
    return bool(m) and "REBUILD PARITY FAIL" in m.group(1) and re.search(r"\bexit 1\b", m.group(1))


def bootstrap_branch_is_reachable(code: list[str]) -> bool:
    """`${VAR-default}`, never `${VAR:-default}`, for the pinned expectation.

    Found by running the control that was supposed to exercise the unpinned path:
    with `:-` an explicitly empty override falls back to the pinned value, so the
    "nothing pinned" branch could never execute. A branch that cannot fire is this
    project's signature defect, and here it was in the guard's own escape hatch.
    """
    joined = "\n".join(code)
    return (
        re.search(r'EXPECTED_GAP="\$\{EXPECTED_GAP-[0-9a-f]{16}\}"', joined) is not None
        and "${EXPECTED_GAP:-" not in joined
    )


def stdin_is_never_stolen(code: list[str]) -> bool:
    """No `docker exec -i`, and every `docker exec` reads from /dev/null.

    This script is run over ssh. A `docker exec -i` in a script arriving on stdin
    consumes the remainder of the script: the run prints its first result and stops,
    with no error and exit 0 — indistinguishable from "the drill finished".
    """
    joined = "\n".join(code)
    if re.search(r"docker exec\s+-i\b", joined):
        return False
    execs = [ln for ln in code if "docker exec" in ln]
    return bool(execs) and all("</dev/null" in ln for ln in execs)


def production_is_only_ever_read(code: list[str]) -> bool:
    """Every `psql_prod` call is a SELECT.

    The drill's entire licence to touch production is that it reads. This is
    deliberately *not* a substring sweep for "insert" over the whole file — the
    script prints the phrase "an out-of-band INSERT would silently take a wrong
    value" in its own report, and a loose detector matches that. Same shape as the
    nightly-smoke alarm that failed a green job because a new diagnostic line
    contained `skipped=0`. Anchor on the call sites, not on the prose.
    """
    joined = "\n".join(code)
    writes = re.compile(
        r"\b(insert\s+into|update\s+\w+\s+set|delete\s+from|alter|drop|truncate)\b", re.I
    )
    calls = re.findall(r'psql_prod\s+"(.*?)"', joined, re.S)
    if not calls:
        return False
    for arg in calls:
        stripped = arg.strip()
        if writes.search(stripped):
            return False
        if stripped.lower().startswith("select"):
            continue
        # Indirect: `"${COLS_SQL}"` or `"$(dump_slugs)"`. Resolve the name and check
        # its definition — an indirection that is not followed is an assertion that
        # stops at the first variable it meets.
        name = re.match(r"\$[{(]([A-Za-z_][A-Za-z0-9_]*)", stripped)
        if not name:
            return False
        ident = name.group(1)
        defn = re.search(rf'^{ident}="(.*?)"$', joined, re.S | re.M) or re.search(
            rf"^{ident}\(\) \{{(.*?)^\}}", joined, re.S | re.M
        )
        if not defn or "select" not in defn.group(1).lower() or writes.search(defn.group(1)):
            return False
    return True


def defaults_come_from_the_rebuild(code: list[str]) -> bool:
    """`column_default` must be read from the REBUILT database, never from production.

    🚨 This guard exists because the first version got it backwards, and only a measurement
    found it. The drill models: *we rebuild from source, then someone creates the paid rows
    out of band in the NEW database*. The defaults that apply are therefore the rebuilt
    schema's. Reading production's catalogue answers a different question and makes the
    headline finding track **production's** schema drift instead of the rebuild's.

    How it surfaced: running the drill against the staging image — which already carries
    core#1047, the migration that DROPs the `max_schedules` default — still reported
    `max_schedules default_would_give=10` and an unchanged fingerprint, because the number
    came from prod's catalogue where #1047 had not landed. **The drill was structurally
    blind to the exact class of fix it exists to notice**, while passing.
    """
    joined = "\n".join(code)
    m = re.search(
        r"(psql_prod|psql_fresh)\s+\"select column_name \|\| '=' \|\| column_default", joined
    )
    return bool(m) and m.group(1) == "psql_fresh"


PREDICATES = {
    "isolated_network": isolated_network,
    "preflight_aborts": preflight_aborts,
    "refuses_vacuous_verdict": refuses_vacuous_verdict,
    "metrics_are_emitted": metrics_are_emitted,
    "verdict_can_fail": verdict_can_fail,
    "bootstrap_branch_is_reachable": bootstrap_branch_is_reachable,
    "stdin_is_never_stolen": stdin_is_never_stolen,
    "production_is_only_ever_read": production_is_only_ever_read,
    "defaults_come_from_the_rebuild": defaults_come_from_the_rebuild,
}

# Mutations of the REAL script, one per predicate, in the shape a careless edit
# actually takes. Several disable a *branch* while leaving its body intact —
# core#1055's finding is that containment checks cannot see that.
MUTATIONS = {
    "isolated_network": (
        'docker run -d --name "${DB}" --network "${NET}"',
        'docker run -d --name "${DB}"',
    ),
    "preflight_aborts": ("       exit 1 ;;", "       ;;"),
    "refuses_vacuous_verdict": ('[ "${LIVE_ROWS:-0}" -lt 1 ]', "false"),
    "metrics_are_emitted": (
        'echo "datanika_rebuild_parity_slugs_missing $(wc -w <<<"${MISSING_SLUGS}")"',
        ": # metric datanika_rebuild_parity_slugs_missing removed",
    ),
    "verdict_can_fail": ('if [ -n "${FAILURES}" ]; then', "if false; then"),
    # Pin-agnostic on purpose: an anchor carrying the fingerprint stops matching the moment
    # the pin legitimately moves, and a mutation whose anchor matches nothing is a mutation
    # that never applied — a harness reporting its own breakage as "your guard is fine".
    "bootstrap_branch_is_reachable": ('"${EXPECTED_GAP-', '"${EXPECTED_GAP:-'),
    "stdin_is_never_stolen": (
        "psql -U datanika -d datanika -At -F'|' -c \"$1\" </dev/null",
        "psql -U datanika -d datanika -At -F'|' -c \"$1\"",
    ),
    "production_is_only_ever_read": (
        'psql_prod "select count(*) from plans"',
        "psql_prod \"delete from plans where slug = ''\"",
    ),
    "defaults_come_from_the_rebuild": (
        "psql_fresh \"select column_name || '=' || column_default",
        "psql_prod \"select column_name || '=' || column_default",
    ),
}


def test_the_script_exists_and_is_a_bash_script():
    assert SCRIPT.exists(), f"{SCRIPT} is missing"
    assert SCRIPT.read_text(encoding="utf-8").startswith("#!/bin/bash")


def test_every_invariant_holds_on_the_real_script():
    code = _strip_comments(SCRIPT.read_text(encoding="utf-8"))
    failed = [name for name, fn in PREDICATES.items() if not fn(code)]
    assert not failed, f"predicates false on the shipped script: {failed}"


def test_every_predicate_goes_red_on_its_own_mutation():
    """In-suite arming. Each mutation must break exactly its own predicate.

    Not just "some predicate went red": a mutation that trips a *different* check
    would let the intended one stay structurally unable to fail while the suite
    still looked discriminating.
    """
    original = SCRIPT.read_text(encoding="utf-8")
    for name, (search, replace) in MUTATIONS.items():
        assert original.count(search) >= 1, f"mutation anchor for {name} matched nothing"
        mutated = _strip_comments(original.replace(search, replace, 1))
        assert not PREDICATES[name](mutated), f"{name} stayed GREEN against its own mutation"


def test_a_mutation_does_not_trip_every_predicate():
    """Anti-vacuity control: the predicates must be independent.

    Without this, a set of checks that all read the same thing would pass the test
    above while measuring one property seven times.
    """
    original = SCRIPT.read_text(encoding="utf-8")
    for name, (search, replace) in MUTATIONS.items():
        mutated = _strip_comments(original.replace(search, replace, 1))
        still_green = [n for n, fn in PREDICATES.items() if n != name and fn(mutated)]
        assert still_green, f"mutation for {name} broke every other predicate too"


def test_the_ignore_list_is_short_and_stays_short():
    """Every ignored column is an assertion switched off.

    `id`, `created_at`, `updated_at` are per-instance; `paddle_*` are vendor ids that
    are environment-specific by construction. Nothing else has a defence, and a
    growing ignore list is how this drill would quietly stop comparing anything.
    """
    code = "\n".join(_strip_comments(SCRIPT.read_text(encoding="utf-8")))
    m = re.search(r'IGNORE_COLS="([^"]*)"', code)
    assert m, "IGNORE_COLS assignment not found"
    ignored = m.group(1).split()
    assert ignored == ["id", "created_at", "updated_at", "paddle_product_id", "paddle_price_id"], (
        f"IGNORE_COLS changed to {ignored}. Each name here switches off a comparison; "
        "adding one needs a reason in the script and an update here, in the same commit."
    )


def test_the_recorded_defaults_still_match_the_migration_tree():
    """The header's finding must stay true of the chain it describes (core#1071).

    Anchored **outside** the drill: the values come from ``server_defaults()``, which reads
    the migration sources and which this file cannot influence. A control comparing the
    drill's prose against another view of the drill would be blind to both being wrong.

    Positive form on purpose. Banning the old string (*"must not say
    ``default_would_give=true``"*) is satisfied by deleting the line, and by a sentence
    that denies it — which is exactly what a good correction looks like. Requiring the
    right value to be **stated** is not.

    When one of these legitimately moves: change the value here, say so in the header with
    a date and the revision, and hand Infra the re-measurement. The point is that all three
    happen in one commit instead of a fingerprint moving for a reason nobody can name.
    """
    from tests.test_migrations.test_plan_seed_updates_reach_real_rows import server_defaults

    chain = server_defaults()
    assert chain, "the server_default extractor returned nothing; this assertion is vacuous"

    drifted = {
        column: (recorded, chain.get(column))
        for column, recorded in RECORDED_DEFAULT_WOULD_GIVE.items()
        if chain.get(column) != recorded
    }
    assert not drifted, (
        f"the drill's recorded finding no longer matches the migration chain: {drifted} "
        "(column -> (recorded, chain)). Update RECORDED_DEFAULT_WOULD_GIVE *and* the dated "
        "block in this module's docstring, and tell Infra the pinned EXPECTED_GAP needs "
        "re-measuring — a stale narrative naming the safe value is what hid core#1071."
    )


def test_the_pinned_gap_is_a_real_measurement_with_its_reason_beside_it():
    """The expectation must carry the issue it records and the run it came from.

    A pinned baseline with no provenance is the failure mode this whole file exists
    to avoid: the next person re-pins it without reading the gap, and a defect is
    laundered into a constant. So require the issue numbers to sit in the same block
    as the value.
    """
    text = SCRIPT.read_text(encoding="utf-8")
    m = re.search(r'EXPECTED_GAP="\$\{EXPECTED_GAP-([0-9a-f]{16})\}"', text)
    assert m, "EXPECTED_GAP must be a pinned 16-hex-char fingerprint"
    # Anchor on STRUCTURE, not on a character distance: the block is whatever sits between
    # the section header and the assignment. A fixed window silently measured the wrong span
    # the moment the comment grew, which is this project's window-guard trap exactly.
    header = text.rfind("── The pinned expectation", 0, m.start())
    assert header != -1, "the pinned expectation must sit under its own section header"
    block = text[header : m.end()]
    for token in ("core#1060", "core#928", "SHRANK", "GREW"):
        assert token in block, (
            f"the pinned expectation must name {token} in the same block — a baseline "
            "without its reason gets re-pinned rather than read"
        )
