"""Drift guard: every FK dependent of a torn-down table must be torn down first.

``_tear_down_fixture`` hard-deletes fixture rows from a hand-maintained,
order-sensitive list of tables. Its own docstring states the invariant:

    FK-safe ordering: children before parents, with api_keys cleared before
    the users they reference.

**Nothing enforced it.** Add a table with an FK into that list and the seed
keeps passing every unit test, then wedges ``e2e-staging`` permanently on the
first run after the migration lands -- in ``globalSetup``, as an opaque setup
error rather than a test failure, and not self-healing.

This has now happened twice:

* **#415** -- Remote-MCP P2 added ``oauth_grants.api_key_id -> api_keys.id``.
  Fixed, and pinned by ``TestTeardownWithOAuthGrants`` in ``test_e2e_seed.py``.
* **#951** -- PII separation release N (#655) added four tables, of which
  ``user_pii``, ``email_change_requests``, ``invitation_pii`` and
  ``notification_channel_pii`` all point into the teardown set. Six consecutive
  red ``e2e-staging`` runs, zero specs executed.

The response to #415 was a behavioural test for *that FK pair*. It could not
see #951, because a per-pair test only ever covers the pair someone already
debugged. This guard is derived from **SQLAlchemy metadata** instead, so the
next such table fails at PR time rather than in staging global setup.

It is a static guard on purpose: no database, no fixtures, runs in the ``test``
job on every PR. The behavioural counterpart -- an actual DELETE against
enforced foreign keys -- lives in ``test_e2e_seed.py``.
"""

from __future__ import annotations

import ast
import pathlib

import pytest

import datanika.models  # noqa: F401  -- imported for its side effect: populates metadata
from datanika.models.base import Base

SEED_PATH = pathlib.Path(datanika.__file__).parent / "scripts" / "e2e_seed.py"

# FK actions the database resolves for us; a dependent under one of these does
# not need an explicit delete.
DB_HANDLED_ONDELETE = {"CASCADE", "SET NULL", "SET DEFAULT"}


def _table_name_by_class() -> dict[str, str]:
    """Mapped class name -> table name, straight from the mapper registry."""
    return {m.class_.__name__: m.local_table.name for m in Base.registry.mappers}


def _collect_deletes(
    node: ast.AST,
    funcs: dict[str, ast.FunctionDef],
    seen: tuple[str, ...],
    names: dict[str, str],
    out: list[str],
) -> None:
    """Append table names deleted under ``node``, in source order.

    Recognises ``<Model>.__table__.delete()`` and follows calls to module-level
    helpers (``_delete_oauth_chain``) so deletes hidden behind one are counted.
    """
    if isinstance(node, ast.Call):
        f = node.func
        if (
            isinstance(f, ast.Attribute)
            and f.attr == "delete"
            and isinstance(f.value, ast.Attribute)
            and f.value.attr == "__table__"
            and isinstance(f.value.value, ast.Name)
        ):
            cls = f.value.value.id
            # An unmapped name here means the AST shape drifted; surface it as a
            # sentinel rather than silently dropping a delete.
            out.append(names.get(cls, f"<unmapped:{cls}>"))
        if isinstance(f, ast.Name) and f.id in funcs and f.id not in seen:
            _collect_deletes(funcs[f.id], funcs, (*seen, f.id), names, out)
    for child in ast.iter_child_nodes(node):
        _collect_deletes(child, funcs, seen, names, out)


@pytest.fixture(scope="module")
def teardown_delete_order() -> list[str]:
    """Table names ``_tear_down_fixture`` deletes, in source order."""
    tree = ast.parse(SEED_PATH.read_text(encoding="utf-8"))
    funcs = {n.name: n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef)}
    assert "_tear_down_fixture" in funcs, (
        "_tear_down_fixture not found in e2e_seed.py -- this guard is parsing the "
        "wrong thing and every assertion below would hold vacuously."
    )
    out: list[str] = []
    _collect_deletes(
        funcs["_tear_down_fixture"], funcs, ("_tear_down_fixture",), _table_name_by_class(), out
    )
    return out


# --------------------------------------------------------------------------
# Controls. A parser that silently returns nothing makes the real assertion
# below pass no matter how broken the teardown is. These fail loudly instead.
# --------------------------------------------------------------------------


def test_parser_finds_a_plausible_delete_set(teardown_delete_order):
    """If the AST shape drifts, fail here rather than vacuously passing."""
    assert len(teardown_delete_order) >= 20, (
        f"only {len(teardown_delete_order)} deletes parsed out of _tear_down_fixture; "
        "the AST shape has drifted and this guard has stopped seeing the teardown"
    )
    for expected in ("users", "organizations", "api_keys", "memberships"):
        assert expected in teardown_delete_order, f"{expected} not parsed"


def test_parser_follows_helper_calls(teardown_delete_order):
    """oauth_* are deleted only inside _delete_oauth_chain.

    If helper recursion breaks, those deletes vanish from the parsed order and
    the ordering assertion silently weakens.
    """
    for expected in ("oauth_tokens", "oauth_grants"):
        assert expected in teardown_delete_order, (
            f"{expected} missing -- _collect_deletes is no longer following "
            "_delete_oauth_chain, so this guard is weaker than it looks"
        )


def test_no_unmapped_names_in_the_parsed_order(teardown_delete_order):
    unmapped = [t for t in teardown_delete_order if t.startswith("<unmapped:")]
    assert not unmapped, f"could not map to a table: {unmapped}"


# --------------------------------------------------------------------------
# The invariant.
# --------------------------------------------------------------------------


def test_every_fk_dependent_is_deleted_before_its_parent(teardown_delete_order):
    """No table may be deleted while a row could still reference it.

    Derived from metadata, so a newly added table is covered the moment it is
    mapped -- no one has to remember to extend a list.
    """
    first_delete = {}
    for i, tname in enumerate(teardown_delete_order):
        first_delete.setdefault(tname, i)

    problems = []
    for parent_name, parent_pos in first_delete.items():
        for dep in Base.metadata.tables.values():
            if dep.name == parent_name:
                continue
            for fk in dep.foreign_keys:
                if fk.column.table.name != parent_name:
                    continue
                if (fk.constraint.ondelete or "").upper() in DB_HANDLED_ONDELETE:
                    continue
                if dep.name not in first_delete:
                    problems.append(
                        f"{dep.name} -> {parent_name}: never deleted, but {parent_name} is"
                    )
                elif first_delete[dep.name] > parent_pos:
                    problems.append(
                        f"{dep.name} -> {parent_name}: deleted at position "
                        f"{first_delete[dep.name]}, after its parent at {parent_pos}"
                    )

    assert not problems, (
        "e2e seed teardown is not FK-safe. Each line is a table that can hold a row "
        "referencing one the teardown hard-deletes, which wedges e2e-staging in "
        "globalSetup until the rows are removed by hand (#415, #951).\n"
        "Fix by deleting it earlier in _tear_down_fixture, or give the FK an "
        "ON DELETE action.\n  " + "\n  ".join(sorted(set(problems)))
    )
