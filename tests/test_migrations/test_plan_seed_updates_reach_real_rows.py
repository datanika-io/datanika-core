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

import ast
import pathlib
import re

import pytest

import datanika

MIGRATIONS = pathlib.Path(datanika.__file__).parent / "migrations"
VERSIONS = MIGRATIONS / "versions"

#: Sentinels for `server_default=` — distinct from a *value* of None, which is
#: alembic's DROP DEFAULT and the whole reason this needs three states.
_DROP = object()
_UNKNOWN = object()

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
    # ── Surfaced by the core#1048 widening. Four columns, one slug, one migration. ──
    #
    # These were invisible to the old extractor for one reason only: their defaults are
    # written as BARE STRINGS in `j9f6g7h8i0c1`'s `create_table`, and it required
    # `sa.text(...)`. They are not new defects — they are core#928's class, and a fresh
    # build gives `enterprise-monthly` Free-tier limits at $399/mo.
    #
    # Listed rather than corrected, and the reason is not effort: a correction migration
    # in `f6a7b8c9d0e1`'s shape **does not fix the from-scratch case**. It runs at the
    # same point in the chain as the original UPDATE and matches zero rows for exactly
    # the same reason, so it repairs deployments where the rows already exist (production
    # — already correct on these four) and inherits the defect on a new one. What would
    # actually fix it is core#928's real answer, and that is a larger decision than this
    # guard's PR. Tracked with the measurement in **core#1060**.
    ("enterprise-monthly", "seats_included"): (
        "core#1060. Fresh build gives 2 where /pricing sells 10. Published value exists, "
        "so this is correctable — but not by a correction migration; see core#1060."
    ),
    ("enterprise-monthly", "max_connections"): (
        "core#1060. Fresh build gives 5 where /pricing sells 50."
    ),
    ("enterprise-monthly", "runs_included"): (
        "core#1060. Fresh build gives 500 where /pricing sells 50,000."
    ),
    ("enterprise-monthly", "extra_seat_price_cents"): (
        "core#1060. Fresh build gives 1200 ($12) where /pricing sells 2500 ($25) — the one "
        "of the four that bills in the wrong direction."
    ),
}


def _sources() -> list[tuple[str, str]]:
    return [(p.name, p.read_text(encoding="utf-8")) for p in sorted(VERSIONS.glob("*.py"))]


def _ordered_sources() -> list[tuple[str, str]]:
    """(filename, source) **oldest first, in migration-chain order**.

    ⚠️ Filename order is not revision order, and the difference is load-bearing now
    that ``server_defaults()`` applies ``alter_column`` changes: a default added in one
    revision and dropped in a later one must end up *dropped*, and alphabetical order
    gets that backwards roughly half the time.

    Asked of **alembic's own** ``ScriptDirectory`` rather than by re-deriving the graph
    from ``down_revision`` here. A hand-rolled walk was tried first and reached 35 of 41
    nodes while naming the wrong head — silently, because a partial walk still returns
    plausible output.
    """
    from alembic.script import ScriptDirectory

    script = ScriptDirectory(str(MIGRATIONS))
    return [
        (pathlib.Path(rev.path).name, pathlib.Path(rev.path).read_text(encoding="utf-8"))
        for rev in reversed(list(script.walk_revisions()))
    ]


def _upgrade_node(source: str):
    """The ``def upgrade()`` node, or ``None``.

    Everything below reads **upgrades only**. A ``downgrade()``'s UPDATE is not a
    statement about what a row should hold — ``o4k1l2m3n5h6``'s downgrade sets
    ``seats_included = 999999`` — and scanning it produced pairs whose "intent" was a
    rollback value. The old extractor scanned whole files and got away with it only
    because the columns involved had bare-string defaults it could not read.
    """
    try:
        tree = ast.parse(source)
    except SyntaxError:  # pragma: no cover - a migration that will not parse
        return None
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == "upgrade":
            return node
    return None


def _default_literal(node):
    """What a ``server_default=`` expression denotes: a string, ``_DROP`` or ``_UNKNOWN``."""
    if node is None:
        return _UNKNOWN
    if isinstance(node, ast.Constant):
        # `server_default="10"` — a BARE STRING. This spelling is why the guard was
        # blind to `max_schedules` and `hard_cap_runs` (core#1048), and it is the
        # spelling `j9f6g7h8i0c1`'s `create_table` uses.
        return _DROP if node.value is None else str(node.value)
    if isinstance(node, ast.Call):
        fn = node.func
        name = fn.attr if isinstance(fn, ast.Attribute) else getattr(fn, "id", "")
        if name == "text" and node.args and isinstance(node.args[0], ast.Constant):
            return str(node.args[0].value)
    return _UNKNOWN


def _kwarg(call: ast.Call, name: str):
    for kw in call.keywords:
        if kw.arg == name:
            return kw.value
    return None


def _is_op_call(node, name: str) -> bool:
    return (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == name
    )


def _str_arg(call: ast.Call, index: int):
    if len(call.args) > index and isinstance(call.args[index], ast.Constant):
        return call.args[index].value
    return None


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


def server_defaults(table: str = "plans", through: str | None = None) -> dict[str, str]:
    """column -> the server_default a ``plans`` row takes **after the whole chain**.

    ``through`` stops after the migration whose filename starts with that revision id, so
    the history can be asked what a row created *at that point* would have taken. That is
    not decoration: it is the only way to pin that an unrelated ``alter_column``
    **preserves** a default, because by the end of the chain the column in question has
    had its default dropped for an unrelated and correct reason. A mutation that treated
    an omitted ``server_default`` as a drop stayed green without it.

    Rewritten in core#1048. The regex it replaces had three defects, and each one
    biased toward reporting a clean chain:

    1. it required ``server_default=sa.text("…")`` and so could not see a **bare
       string** — the spelling ``j9f6g7h8i0c1``'s ``create_table`` uses for
       ``max_schedules`` (``"10"``) and ``k0g7h8i9j1d2`` uses for ``hard_cap_runs``.
       ``exposed_pairs()`` skips a column whose default is ``None`` on the stated
       grounds that *"the source cannot say"* — so a **detectable** pair was routed
       down the branch reserved for undetectable ones;
    2. it read ``sa.Column`` declarations only, so a default **changed or dropped** by
       a later ``alter_column`` was invisible even when correctly spelled. That is not
       hypothetical: ``b4d8f1a2c6e9`` drops ``max_schedules``' default (core#1047), and
       without modelling it this guard would report four pairs that no longer exist;
    3. it matched Columns of **every table**, so a first attempt at the widening
       reported ``saml_sp_entity_id`` and ``email_verified`` as plan defaults.

    AST rather than a wider regex, because (3) is a structural question — *which table
    is this Column inside* — and a regex cannot ask it.
    """
    out: dict[str, str] = {}
    for _name, txt in _ordered_sources():
        upgrade = _upgrade_node(txt)
        if upgrade is None:
            if through is not None and _name.startswith(through):
                break
            continue
        for node in ast.walk(upgrade):
            if (_is_op_call(node, "create_table") or _is_op_call(node, "add_column")) and _str_arg(
                node, 0
            ) == table:
                for arg in node.args[1:]:
                    if not _is_op_call(arg, "Column"):
                        continue
                    column = _str_arg(arg, 0)
                    value = _default_literal(_kwarg(arg, "server_default"))
                    if column and value is not _UNKNOWN and value is not _DROP:
                        out[column] = value
            elif _is_op_call(node, "alter_column") and _str_arg(node, 0) == table:
                column = _str_arg(node, 1)
                kwarg = _kwarg(node, "server_default")
                # An OMITTED server_default means "leave it alone" — not None, which
                # is DROP DEFAULT. Alembic distinguishes them by a sentinel, and so
                # must this, or every unrelated alter_column erases a default here.
                if column is None or kwarg is None:
                    continue
                value = _default_literal(kwarg)
                if value is _DROP:
                    out.pop(column, None)
                elif value is not _UNKNOWN:
                    out[column] = value
        if through is not None and _name.startswith(through):
            break
    return out


def _slug_constants(source: str) -> dict[str, list[str]]:
    """Module-level ``NAME = ("a", "b")`` / ``["a", "b"]`` of string literals."""
    out: dict[str, list[str]] = {}
    try:
        tree = ast.parse(source)
    except SyntaxError:  # pragma: no cover
        return out
    for node in tree.body:
        if isinstance(node, ast.Assign):
            targets, value = node.targets, node.value
        elif isinstance(node, ast.AnnAssign):
            targets, value = [node.target], node.value
        else:
            continue
        if not isinstance(value, ast.Tuple | ast.List):
            continue
        items = [
            e.value for e in value.elts if isinstance(e, ast.Constant) and isinstance(e.value, str)
        ]
        # All-or-nothing: a partially-literal tuple is not a slug list we can trust.
        if items and len(items) == len(value.elts):
            for target in targets:
                if isinstance(target, ast.Name):
                    out[target.id] = items
    return out


def _resolve_interpolated_slugs(source: str, where_clause: str) -> list[str]:
    """Slugs named by a ``{ident}`` or ``{ident()}`` interpolation in a WHERE clause.

    **This is the idiom this codebase uses for every multi-slug change**, and the old
    extractor could not see any of it — it read inline SQL literals only, so

        slugs = ", ".join(f"'{s}'" for s in _NO_MID_CYCLE_BLOCK)
        op.execute(f"UPDATE plans SET hard_cap_runs = false WHERE slug IN ({slugs})")

    yielded **zero** assignments. Both live spellings resolve to a module-level tuple:
    ``c1d2e3f4a5b6`` interpolates a local built from one, ``a9c4e2b7d5f3`` interpolates
    a function that joins one. So: take the identifier, and find the module constant it
    reaches.
    """
    constants = _slug_constants(source)
    if not constants:
        return []
    try:
        tree = ast.parse(source)
    except SyntaxError:  # pragma: no cover
        return []

    found: list[str] = []
    for ident in re.findall(r"\{(\w+)\(?\)?\}", where_clause):
        if ident in constants:
            found += constants[ident]
            continue
        for node in ast.walk(tree):
            defines_ident = (isinstance(node, ast.FunctionDef) and node.name == ident) or (
                isinstance(node, ast.Assign)
                and any(isinstance(t, ast.Name) and t.id == ident for t in node.targets)
            )
            if not defines_ident:
                continue
            for sub in ast.walk(node):
                if isinstance(sub, ast.Name) and sub.id in constants:
                    found += constants[sub.id]
    return found


_UPDATE_RE = re.compile(
    r"UPDATE plans\s+SET\s+(.*?)\s+WHERE\s+slug\s*(?:=|IN)\s*(.*?)(?:\"|'''|\"\"\")",
    re.S | re.I,
)


def plan_update_assignments() -> list[tuple[str, str, str, str]]:
    """(migration, column, intended_value, slug) for every ``UPDATE plans … WHERE slug``.

    Two changes in core#1048, both of which the old version needed:

    * **Upgrades only.** It scanned whole files, so a ``downgrade()``'s UPDATE counted as
      an intent — ``o4k1l2m3n5h6``'s downgrade sets ``seats_included = 999999``. That was
      harmless only because the columns involved had bare-string defaults the old
      ``server_defaults()`` could not read; widening one without the other would have
      produced pairs whose "intended value" was a rollback.
    * **Interpolated slug lists resolve.** ``WHERE slug IN ({slugs})`` is the idiom this
      codebase uses for every multi-slug change and the old extractor found **zero** slugs
      in it — see ``_resolve_interpolated_slugs``.
    """
    rows = []
    for name, txt in _sources():
        upgrade = _upgrade_node(txt)
        if upgrade is None:
            continue
        body = ast.get_source_segment(txt, upgrade) or ""
        for m in _UPDATE_RE.finditer(body):
            assigns, slugpart = m.group(1), m.group(2)
            slugs = re.findall(r"'([a-z][a-z0-9-]*)'", slugpart)
            slugs += _resolve_interpolated_slugs(txt, slugpart)
            for slug in dict.fromkeys(slugs):  # de-duplicate, keep order
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


def test_the_revision_walk_covers_every_migration_on_disk():
    """``server_defaults()`` applies alter_column in chain order, so a partial walk lies.

    A hand-rolled ``down_revision`` walk reached **35 of 41** nodes and named the wrong
    head, silently — a partial walk still returns plausible output, and the failure lands
    on whichever ``alter_column`` it happened to skip.
    """
    ordered = _ordered_sources()
    on_disk = list(VERSIONS.glob("*.py"))
    assert len(ordered) == len(on_disk), (
        f"alembic walked {len(ordered)} revisions but {len(on_disk)} files are on disk. "
        "A migration outside the chain (or a branched graph) makes the default history "
        "below incomplete — run `alembic heads` before reading anything else here."
    )


def test_the_extractor_reads_a_bare_string_default():
    """Blind spot 1 (core#1048), pinned by the spelling rather than by the outcome.

    ``hard_cap_runs`` is declared ``server_default="false"`` — a bare string — and
    ``max_parallel_runs`` as ``sa.text("5")``. Both must be visible, or a detectable pair
    is routed down ``exposed_pairs()``' *"the source cannot say"* branch.
    """
    defaults = server_defaults()
    assert defaults.get("hard_cap_runs") == "false", (
        "the bare-string spelling is not being read — this is the exact blind spot "
        "core#1048 was filed for"
    )
    assert defaults.get("max_parallel_runs") == "5", "the sa.text() spelling regressed"


def test_the_extractor_models_a_dropped_default():
    """``b4d8f1a2c6e9`` drops ``max_schedules``' default (core#1047).

    Declarations alone would still report ``'10'`` from ``j9f6g7h8i0c1``, which would
    expose four ``max_schedules`` pairs that no longer exist. A guard that reports
    resolved defects is how a guard gets ignored.
    """
    assert "max_schedules" not in server_defaults(), (
        "max_schedules still reports a server_default. Either b4d8f1a2c6e9's DROP DEFAULT "
        "is not being modelled, or the migration was reverted — check which before editing "
        "this test."
    )


def test_an_unrelated_alter_column_preserves_the_default():
    """An ``alter_column`` that does not mention ``server_default`` must leave it alone.

    Alembic distinguishes *omitted* (leave it) from ``None`` (DROP DEFAULT) by a sentinel,
    and conflating them silently erases a default every time a column is altered for an
    unrelated reason. ``e3a5c7b9d1f4`` is exactly that: it makes ``max_schedules``
    nullable while the column still carries ``'10'``.

    🔑 **This test exists because a mutation found the gap and the obvious assertion could
    not close it.** Checking the end of the chain says nothing — ``b4d8f1a2c6e9`` drops
    that default anyway, for a correct and unrelated reason, so a broken extractor and a
    correct one agree there. Only asking the history *at that revision* discriminates.
    """
    at_nullable = server_defaults(through="e3a5c7b9d1f4")

    assert at_nullable.get("max_schedules") == "10", (
        "max_schedules lost its default at e3a5c7b9d1f4, which only alters nullability. "
        "An omitted server_default is being read as DROP DEFAULT."
    )


def test_the_extractor_resolves_an_interpolated_slug_list():
    """Blind spot 2 (core#1048) — the idiom this codebase uses for every multi-slug change.

    Both live spellings are covered: ``c1d2e3f4a5b6`` interpolates a local built from a
    module constant, ``a9c4e2b7d5f3`` interpolates a function that joins one. Neither
    yielded a single slug before.
    """
    by_migration: dict[str, set[str]] = {}
    for name, _col, _val, slug in plan_update_assignments():
        by_migration.setdefault(name.split("_")[0], set()).add(slug)

    paid = {"pro-monthly", "pro-annual", "enterprise-monthly", "enterprise-annual"}
    assert by_migration.get("c1d2e3f4a5b6", set()) >= paid, (
        "the local-variable interpolation in c1d2e3f4a5b6 resolved no slugs"
    )
    assert by_migration.get("a9c4e2b7d5f3", set()) >= paid, (
        "the function-call interpolation in a9c4e2b7d5f3 resolved no slugs"
    )


def test_a_downgrades_update_is_not_read_as_an_intent():
    """``o4k1l2m3n5h6``'s downgrade sets ``seats_included = 999999``. That is a rollback
    value, not a statement about what an Enterprise row should hold — and reading it as
    one produces an exposed pair whose "intent" nobody ever meant."""
    intents = {
        val
        for name, col, val, _slug in plan_update_assignments()
        if col == "seats_included" and name.startswith("o4k1l2m3n5h6")
    }
    assert intents == {"10"}, (
        f"seats_included intents from o4k1l2m3n5h6 are {intents}; 999999 appears only in "
        "its downgrade(), so the extractor is scanning past upgrade()"
    )


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


@pytest.mark.parametrize(
    ("pair", "spelling"),
    [
        (
            ("enterprise-monthly", "seats_included"),
            "bare-string server_default in create_table",
        ),
        (
            ("enterprise-monthly", "runs_included"),
            "bare-string server_default in create_table",
        ),
    ],
)
def test_the_corpus_covers_each_newly_readable_spelling(pair, spelling):
    """core#1048's third requirement, and the one it is easiest to skip.

    *"Extend the negative control's corpus with one pair in each newly-covered spelling,
    or the widening is untested in exactly the direction it was written for."* The
    original corpus was four pairs that all happen to use ``sa.text(...)`` and inline
    slug literals — the spellings the old extractor could already read — so it would
    have stayed green against a broken widening.

    The interpolated-slug half is pinned separately by
    ``test_the_extractor_resolves_an_interpolated_slug_list``: it produces no *exposed*
    pair here, because ``c1d2e3f4a5b6`` sets ``hard_cap_runs = false`` where the default
    is already ``false`` (harmless, correctly skipped) and ``a9c4e2b7d5f3``'s
    ``max_schedules`` no longer has a default to disagree with. **An extractor
    improvement with no exposed pair behind it still has to be asserted**, or the only
    evidence it works is that nothing changed.
    """
    assert pair in exposed_pairs(), (
        f"{pair} is not detected, so the widening for '{spelling}' is unproven. This "
        "corpus exists because the original four all used the one spelling the old "
        "extractor could read."
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
