"""A seeding UPDATE must not silently match zero rows (core#780, core#928).

**The class.** A migration writes

    UPDATE plans SET <col> = <v> WHERE slug = '<paid slug>'

but **no migration ever creates the paid slugs.** Over the whole chain the only
slug any migration ``INSERT``s is ``free``; ``pro-monthly``,
``enterprise-monthly``, ``pro-annual`` and ``enterprise-annual`` are created out
of band — annual by ``datanika-cloud/scripts/seed_annual_plans.py``, monthly by
something in neither repo.

So on a from-scratch build alembic runs to head against an empty ``plans``,
every such UPDATE matches zero rows, and the rows are created afterwards taking
each column's ``server_default``. Measured on production 2026-09-02: four of
five rows hold ``max_parallel_runs = 5`` where Enterprise is sold 20, while
``free`` — the one row a migration creates — is correct at 2.

**This is the second instance.** core#713's ``WHERE bytes_included IS NULL``
guard was the first, and ``CLAUDE.md`` records that it "made the seeding half a
no-op". It was written up as a curiosity. It is a class, so it gets a guard.

## What this test can and cannot do

It cannot check production, and it must not try. What it *can* do is read the
migration sources and find the condition under which a fresh build is silently
wrong:

    the slug is never INSERTed by a migration
    AND the column has a server_default
    AND that default differs from the value the UPDATE intends

Every pair meeting all three is a value that will be wrong on a rebuilt
database and will look entirely healthy. Each must be either **corrected** by a
later migration or **listed** in ``KNOWN_UNCORRECTED`` against an issue — so
the exemption is a decision somebody made, not an inheritance.

⚠️ A pair whose column has **no** ``server_default`` is deliberately *not*
flagged. The source cannot say what such a row holds — whatever created it
supplied something — so flagging it would be a guess, and a guard that reports
things it cannot know gets ignored. Those are on core#928, to be settled by one
query, not by inference.
"""

import pathlib
import re

import pytest

import datanika

VERSIONS = pathlib.Path(datanika.__file__).parent / "migrations" / "versions"

# (slug, column) pairs known to land on the wrong value and NOT corrected here.
# Each entry is a decision with a reason, not a silence.
KNOWN_UNCORRECTED: dict[tuple[str, str], str] = {
    ("pro-monthly", "rate_limit_rpm"): (
        "core#928. NOT corrected deliberately. Unlike max_parallel_runs and sso_enabled, this "
        "one has no published value behind it: the burst claim was DELETED rather than "
        "implemented (core#703), so the April migration's 120 is an intent with nothing "
        "corroborating it. Restoring it is a Product decision, not a repair — and an "
        "idempotent UPDATE is only safe when the target value is unambiguous."
    ),
    ("enterprise-monthly", "rate_limit_rpm"): (
        "core#928. Same as pro-monthly above — 300 has no published counterpart."
    ),
}


def _sources() -> list[tuple[str, str]]:
    return [(p.name, p.read_text(encoding="utf-8")) for p in sorted(VERSIONS.glob("*.py"))]


def slugs_created_by_a_migration() -> set[str]:
    """Slugs some migration actually ``INSERT``s into ``plans``.

    ⚠️ Aligns the VALUES list to the column list and reads the ``slug``
    position. The obvious version — every quoted lowercase literal in VALUES —
    also returns ``'monthly'`` (the ``interval`` value) and would have declared
    a slug "created" on a string match. Found by running the extractor rather
    than by reading it.
    """
    created = set()
    for _name, txt in _sources():
        for m in re.finditer(
            r"INSERT INTO plans\s*\((.*?)\)\s*VALUES\s*\((.*?)\)", txt, re.S | re.I
        ):
            columns = [c.strip() for c in m.group(1).split(",")]
            values = [v.strip() for v in m.group(2).split(",")]
            if "slug" not in columns or len(columns) != len(values):
                continue
            raw = values[columns.index("slug")]
            literal = re.fullmatch(r"'([^']*)'", raw)
            if literal:
                created.add(literal.group(1))
    return created


def server_defaults() -> dict[str, str]:
    """column -> server_default literal, for plan columns that declare one."""
    out = {}
    for _name, txt in _sources():
        for m in re.finditer(
            r'sa\.Column\(\s*"(\w+)"\s*,\s*sa\.\w+\([^)]*\)[^)]*?server_default=sa\.text\(\s*"([^"]*)"',
            txt,
            re.S,
        ):
            out[m.group(1)] = m.group(2)
    return out


def plan_update_assignments() -> list[tuple[str, str, str, str]]:
    """(migration, column, intended_value, slug) for every UPDATE plans ... WHERE slug."""
    rows = []
    for name, txt in _sources():
        for m in re.finditer(
            r"UPDATE plans\s+SET\s+(.*?)\s+WHERE\s+slug\s*(?:=|IN)\s*(.*?)(?:\"|'''|\"\"\")",
            txt,
            re.S | re.I,
        ):
            assigns, slugpart = m.group(1), m.group(2)
            slugs = re.findall(r"'([a-z][a-z0-9-]*)'", slugpart)
            for slug in slugs:
                for assignment in assigns.split(","):
                    assignment = assignment.strip()
                    if "=" not in assignment:
                        continue
                    col, val = (x.strip() for x in assignment.split("=", 1))
                    if re.fullmatch(r"\w+", col):
                        rows.append((name, col, val, slug))
    return rows


def exposed_pairs() -> dict[tuple[str, str], tuple[str, str, str]]:
    """(slug, column) -> (migration, intended, default) for silently-wrong pairs."""
    created = slugs_created_by_a_migration()
    defaults = server_defaults()
    out = {}
    for name, col, val, slug in plan_update_assignments():
        if slug in created:
            continue
        default = defaults.get(col)
        if default is None:
            continue  # source cannot say; core#928 settles these by measurement
        if default.strip().lower() == val.strip().lower():
            continue  # default happens to equal intent — harmless
        out[(slug, col)] = (name, val, default)
    return out


def corrected_pairs() -> set[tuple[str, str]]:
    """Pairs a later migration re-applies unconditionally by slug.

    Read from the correction migration itself rather than restated here, so the
    guard cannot drift from what actually ships.
    """
    from datanika.migrations.versions.f6a7b8c9d0e1_correct_paid_plan_concurrency import (
        PUBLISHED_MAX_PARALLEL_RUNS,
        SSO_ENABLED_SLUGS,
    )

    return {(slug, "max_parallel_runs") for slug in PUBLISHED_MAX_PARALLEL_RUNS} | {
        (slug, "sso_enabled") for slug in SSO_ENABLED_SLUGS
    }


def test_the_extractor_is_armed():
    """A regex sweep that stops matching reports a clean chain."""
    assert len(_sources()) >= 30, "migration directory did not load"
    assert slugs_created_by_a_migration() == {"free"}, (
        "the set of slugs a migration creates has changed. If a migration now "
        "creates the paid rows, that is the real fix for core#928 and this whole "
        "guard should be revisited — do not just update the literal."
    )
    assert len(plan_update_assignments()) >= 15, (
        "the UPDATE extractor has stopped matching; every assertion below is now vacuous"
    )
    assert "max_parallel_runs" in server_defaults(), "the server_default extractor is not matching"


def test_every_silently_wrong_pair_is_corrected_or_explicitly_accepted():
    """The class guard. A NEW instance of this defect fails here, at PR time."""
    unhandled = {
        pair: info
        for pair, info in exposed_pairs().items()
        if pair not in corrected_pairs() and pair not in KNOWN_UNCORRECTED
    }
    assert not unhandled, (
        "these migrations configure a plan slug that NO migration creates, on a "
        "column whose server_default differs from the intended value. On a "
        "from-scratch build the UPDATE matches zero rows, the row is created "
        "afterwards, and it silently takes the default (core#780, core#928):\n  "
        + "\n  ".join(
            f"{slug}.{col}: {mig} intends {val!r}, server_default is {dflt!r}"
            for (slug, col), (mig, val, dflt) in sorted(unhandled.items())
        )
        + "\n\nEither add the pair to a correction migration that re-applies it by "
        "slug, or list it in KNOWN_UNCORRECTED with the issue and the reason."
    )


def test_the_guard_still_sees_the_defect_it_was_written_for():
    """The negative control, in-process.

    Without this, the guard above passes when the extractor breaks, when a
    correction migration is deleted, or when somebody widens ``KNOWN_UNCORRECTED``
    to everything. It asserts the sweep still *finds* the four known pairs —
    the three accepted plus the corrected one — so a green above means "handled",
    never "found nothing".
    """
    found = exposed_pairs()
    for pair in [
        *KNOWN_UNCORRECTED,
        ("enterprise-monthly", "max_parallel_runs"),
        ("enterprise-monthly", "sso_enabled"),
    ]:
        assert pair in found, (
            f"{pair} is no longer detected as silently-wrong. If it was genuinely "
            "fixed at the source (a migration now creates the row, or the "
            "server_default now matches), delete it from this list deliberately."
        )


@pytest.mark.parametrize("pair", sorted(KNOWN_UNCORRECTED))
def test_every_accepted_pair_names_an_issue(pair):
    """An exemption without a tracking reference is a silence with extra steps."""
    assert re.search(r"core#\d+", KNOWN_UNCORRECTED[pair]), (
        f"{pair} is exempted without naming an issue"
    )


def test_the_correction_covers_every_slug_the_product_sells():
    """A partial correction is how this defect reproduces itself.

    core#713's byte seeding handled three slugs and left the two annual tiers on
    NULL, because those rows are created by a script rather than a migration —
    the same asymmetry, one column over. Listing all five here means a missing
    row is a no-op rather than an omission.
    """
    from datanika.migrations.versions.f6a7b8c9d0e1_correct_paid_plan_concurrency import (
        PUBLISHED_MAX_PARALLEL_RUNS,
    )

    assert set(PUBLISHED_MAX_PARALLEL_RUNS) == {
        "free",
        "pro-monthly",
        "pro-annual",
        "enterprise-monthly",
        "enterprise-annual",
    }
    # Annual carries the monthly tier's entitlement — `seed_annual_plans.py`'s
    # COPIED_QUOTA_COLUMNS is built on exactly that ("annual and monthly differ
    # in price and cadence, never in quota").
    assert PUBLISHED_MAX_PARALLEL_RUNS["pro-annual"] == PUBLISHED_MAX_PARALLEL_RUNS["pro-monthly"]
    assert (
        PUBLISHED_MAX_PARALLEL_RUNS["enterprise-annual"]
        == PUBLISHED_MAX_PARALLEL_RUNS["enterprise-monthly"]
    )
    # The two the April migration got right, and the one it did not.
    assert PUBLISHED_MAX_PARALLEL_RUNS["free"] == 2
    assert PUBLISHED_MAX_PARALLEL_RUNS["enterprise-monthly"] == 20


def test_the_core_default_is_not_mistaken_for_an_entitlement():
    """core#915's lesson, pinned where it can be checked.

    ``DEFAULT_MAX_PARALLEL`` is the self-hosted answer, not a plan value. If
    somebody ever "simplifies" the correction migration by deleting the slugs
    that happen to equal it, the dict stops being a statement of entitlements
    and becomes a diff against a default — which is how ``pro-monthly`` reads as
    correct today purely by coincidence.
    """
    from datanika.migrations.versions.f6a7b8c9d0e1_correct_paid_plan_concurrency import (
        PUBLISHED_MAX_PARALLEL_RUNS,
    )
    from datanika.services.concurrency_service import DEFAULT_MAX_PARALLEL

    assert PUBLISHED_MAX_PARALLEL_RUNS["pro-monthly"] == DEFAULT_MAX_PARALLEL, (
        "Pro's ceiling equalling the core default is a COINCIDENCE that this "
        "assertion exists to keep visible. If they diverge, the correction "
        "migration is what must change — not this test."
    )
    assert PUBLISHED_MAX_PARALLEL_RUNS["enterprise-monthly"] != DEFAULT_MAX_PARALLEL
