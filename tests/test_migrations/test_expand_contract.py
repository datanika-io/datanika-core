"""The `t1` window: deployed code must survive the new schema (core#449).

Blue/green starts the new container **while the old one still serves**, and
`alembic upgrade head` runs in its startup command:

    t0  old code + old schema     <- serving
    t1  old code + NEW schema     <- serving, new container already migrated  *** here ***
    t2  new code + new schema

At `t1` the previously-deployed version runs against the migrated schema, so a
drop, rename or `SET NOT NULL` breaks production at that instant — not at the
swap, and **not in CI, which only ever runs one version against one schema**
(`plans/infra/SPEC_EXPAND_CONTRACT_MIGRATIONS.md`).

Until now the only control was a checklist item — *a human answering a
question* — which is the same control that let promotion-ref enumeration leak
until it was automated.

**Why this doesn't literally "run the previous release's test suite".** The
spec proposes exactly that, and it would prove very little here: almost every
test builds its schema with `Base.metadata.create_all` on SQLite and never
touches a migrated Postgres. That job would pass while exercising nothing —
the failure mode this repo has spent a week removing.

So it asserts the property the checklist actually asks about — *"would the
currently deployed version still run against this schema?"* — by comparing the
**deployed release's models** against the **new schema**:

1. migrate a real Postgres to the branch's `head`;
2. read the deployed release's models straight out of git (AST, never
   imported — importing two versions of the same package in one process is
   its own bug);
3. assert every table and column those models reference still exists, and that
   nothing tightened to `NOT NULL` under them.

Covered: `DROP COLUMN` · rename · `DROP TABLE` · `SET NOT NULL`.
Not covered, and left explicit rather than implied: type narrowing, and a new
`UNIQUE` on an existing column. Both are on the spec's forbidden list; neither
is reliably derivable from the old models by AST, so the checklist remains the
control for those two.
"""

import ast
import os
import subprocess
import sys

import pytest
import sqlalchemy

from tests.test_migrations.conftest import _no_postgres, _run_alembic

# `roundtrip_db_url` is deliberately NOT imported. It is a session-scoped
# fixture in conftest.py and pytest discovers it; importing it here would
# register a *second* FixtureDef and boot a second Postgres container.

#: What production is running. The `t1` risk is defined against the deployed
#: release, so this is the ref to compare with — not the merge base.
_DEPLOYED_REF = os.environ.get("DEPLOYED_REF", "origin/master")

_MODELS_DIR = "datanika/models"


def _git(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(["git", *args], capture_output=True, text=True, timeout=60, check=False)


def _deployed_model_files() -> list[str]:
    result = _git("ls-tree", "--name-only", _DEPLOYED_REF, f"{_MODELS_DIR}/")
    if result.returncode != 0:
        return []
    return [p for p in result.stdout.split() if p.endswith(".py")]


def _deployed_schema() -> dict[str, dict[str, bool]]:
    """{table: {column: nullable}} as the **deployed** models declare it.

    Parsed with `ast` rather than imported: importing the old and new versions
    of `datanika.models` into one interpreter would collide on module names and
    on SQLAlchemy's declarative registry.
    """
    schema: dict[str, dict[str, bool]] = {}

    for path in _deployed_model_files():
        blob = _git("show", f"{_DEPLOYED_REF}:{path}")
        if blob.returncode != 0:
            continue
        try:
            tree = ast.parse(blob.stdout, filename=path)
        except SyntaxError:  # pragma: no cover - old revision should parse
            continue

        for node in ast.walk(tree):
            if not isinstance(node, ast.ClassDef):
                continue
            table = _tablename(node)
            if not table:
                continue
            columns = schema.setdefault(table, {})
            columns.update(_columns(node))
            # TenantMixin contributes org_id to every tenant table; it is
            # declared on the mixin, not on each model.
            if any(getattr(b, "id", getattr(b, "attr", None)) == "TenantMixin" for b in node.bases):
                columns.setdefault("org_id", False)

    return schema


def _tablename(node: ast.ClassDef) -> str | None:
    for stmt in node.body:
        if (
            isinstance(stmt, ast.Assign)
            and any(getattr(t, "id", None) == "__tablename__" for t in stmt.targets)
            and isinstance(stmt.value, ast.Constant)
        ):
            return str(stmt.value.value)
    return None


def _columns(node: ast.ClassDef) -> dict[str, bool]:
    """Column name -> nullable, for `x: Mapped[...] = mapped_column(...)`."""
    out: dict[str, bool] = {}
    for stmt in node.body:
        if not isinstance(stmt, ast.AnnAssign) or not isinstance(stmt.target, ast.Name):
            continue
        call = stmt.value
        if not isinstance(call, ast.Call):
            continue
        if getattr(call.func, "id", getattr(call.func, "attr", None)) != "mapped_column":
            continue

        name = stmt.target.id
        # An explicit string first arg overrides the attribute name.
        if (
            call.args
            and isinstance(call.args[0], ast.Constant)
            and isinstance(call.args[0].value, str)
        ):
            name = call.args[0].value

        nullable = False
        for kw in call.keywords:
            if kw.arg == "nullable" and isinstance(kw.value, ast.Constant):
                nullable = bool(kw.value.value)
            elif kw.arg == "primary_key" and isinstance(kw.value, ast.Constant):
                nullable = False
        out[name] = nullable
    return out


def _live_schema(engine) -> dict[str, dict[str, bool]]:
    """{table: {column: nullable}} as the migrated database actually is."""
    query = sqlalchemy.text(
        "SELECT table_name, column_name, is_nullable "
        "FROM information_schema.columns WHERE table_schema = 'public'"
    )
    live: dict[str, dict[str, bool]] = {}
    with engine.connect() as conn:
        for table, column, is_nullable in conn.execute(query):
            live.setdefault(table, {})[column] = is_nullable == "YES"
    return live


@pytest.fixture(scope="module")
def migrated_schema(roundtrip_db_url):
    """Postgres migrated to this branch's head, as `information_schema` sees it."""
    result = _run_alembic(["upgrade", "head"], roundtrip_db_url)
    assert result.returncode == 0, f"alembic upgrade head failed:\n{result.stdout}\n{result.stderr}"

    engine = sqlalchemy.create_engine(roundtrip_db_url)
    try:
        yield _live_schema(engine)
    finally:
        engine.dispose()


def _require_deployed_ref() -> None:
    """Skip locally without the ref; fail loudly in CI, which must have it.

    A shallow ``actions/checkout`` has no ``origin/master``, so every check in
    this file would compare an empty dict and pass. Locally that is a missing
    fetch and shouldn't block anyone; in CI it means the workflow lost its
    fetch step and the guard is silently inert.
    """
    if not _deployed_model_files():
        _no_postgres(
            f"Cannot read models at {_DEPLOYED_REF!r} — run `git fetch origin master` "
            "(CI does this explicitly; actions/checkout is shallow). Without it "
            "this file compares nothing."
        )


class TestDeployedCodeSurvivesTheNewSchema:
    def test_the_deployed_ref_is_readable(self):
        """Guard the guard: no ref, no comparison, and silence would look green."""
        _require_deployed_ref()
        assert _deployed_model_files(), f"no model files at {_DEPLOYED_REF}"

    def test_the_deployed_models_actually_parsed(self):
        """The hole this whole file exists to avoid, one level up.

        Every comparison below is `for table in deployed` — so if the AST walk
        silently yielded nothing (a refactor to the model style, a moved
        directory), all three tests pass having compared zero columns. Anchor
        on tables that must exist in any deployed release.
        """
        deployed = _deployed_schema()
        assert len(deployed) >= 10, (
            f"only {len(deployed)} tables parsed from {_DEPLOYED_REF} — the AST walk "
            "is probably not matching the current model style, which would make "
            "every check below vacuously green"
        )
        for table, column in (("users", "email"), ("api_keys", "key_hash")):
            assert column in deployed.get(table, {}), (
                f"expected {table}.{column} in the parsed deployed schema; "
                f"got {sorted(deployed.get(table, {}))}"
            )

    def test_no_table_the_deployed_release_reads_was_dropped(self, migrated_schema):
        deployed = _deployed_schema()
        missing = sorted(t for t in deployed if t not in migrated_schema)
        assert not missing, (
            f"Tables dropped that {_DEPLOYED_REF} still reads: {missing}.\n"
            "At t1 the deployed version runs against this schema and will fail. "
            "Drop belongs in the CONTRACT phase, a release later "
            "(plans/infra/SPEC_EXPAND_CONTRACT_MIGRATIONS.md)."
        )

    def test_no_column_the_deployed_release_reads_was_dropped_or_renamed(self, migrated_schema):
        """A rename is a drop plus an add — it fails here as the drop."""
        deployed = _deployed_schema()
        missing = [
            f"{table}.{column}"
            for table, columns in deployed.items()
            if table in migrated_schema
            for column in columns
            if column not in migrated_schema[table]
        ]
        assert not missing, (
            f"Columns removed that {_DEPLOYED_REF} still selects: {sorted(missing)}.\n"
            "SELECT names every mapped column, so the deployed version breaks on "
            "every query to that table at t1 — not only where the column is used. "
            "Expand now, contract next release."
        )

    def test_nothing_tightened_to_not_null_under_the_deployed_release(self, migrated_schema):
        """The subtler half: the column exists, but old INSERTs now fail."""
        deployed = _deployed_schema()
        tightened = [
            f"{table}.{column}"
            for table, columns in deployed.items()
            if table in migrated_schema
            for column, was_nullable in columns.items()
            if was_nullable
            and column in migrated_schema[table]
            and not migrated_schema[table][column]
        ]
        assert not tightened, (
            f"Columns now NOT NULL that {_DEPLOYED_REF} treats as nullable: "
            f"{sorted(tightened)}.\n"
            "The deployed version omits them on INSERT and will fail at t1. "
            "Backfill and add the constraint in a later release."
        )


class TestTheGuardDiscriminates:
    """A guard that cannot fail is decoration — prove it fails on a real breach."""

    def test_a_dropped_column_is_detected(self, migrated_schema):
        pretend_deployed = {"api_keys": {"key_hash": False, "column_we_removed": False}}
        missing = [
            f"{t}.{c}"
            for t, cols in pretend_deployed.items()
            if t in migrated_schema
            for c in cols
            if c not in migrated_schema[t]
        ]
        assert missing == ["api_keys.column_we_removed"]

    def test_a_tightened_column_is_detected(self, migrated_schema):
        assert "api_keys" in migrated_schema, "fixture did not migrate"
        pretend_deployed = {"api_keys": {"expires_at": True, "key_hash": True}}
        tightened = [
            f"{t}.{c}"
            for t, cols in pretend_deployed.items()
            for c, was_nullable in cols.items()
            if was_nullable and c in migrated_schema[t] and not migrated_schema[t][c]
        ]
        # key_hash is NOT NULL in the live schema; expires_at is nullable.
        assert tightened == ["api_keys.key_hash"], (
            f"expected the NOT NULL column to be flagged, got {tightened}"
        )


if sys.version_info < (3, 12):  # pragma: no cover - repo requires 3.12
    pytest.skip("requires Python 3.12 AST", allow_module_level=True)
