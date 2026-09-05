"""Nothing in the runtime package writes ``created_at`` or ``updated_at`` (core#1069).

This is the argument release N+1 rests on, and it is the only one that is not a row count.

``f1a4c8e2d6b3`` makes 14 columns ``NOT NULL`` and ``timestamptz``. Both halves are safe
**by construction** if — and only if — no code path sets those columns:

* nothing can write ``NULL``, so ``SET NOT NULL`` cannot be broken by a future row;
* nothing can write a **local-time** value, so ``AT TIME ZONE 'UTC'`` is a relabel rather
  than a shift. Every value comes from ``server_default=func.now()`` / ``onupdate=
  func.now()``, which **Postgres** evaluates, and the production database's ``TimeZone`` is
  ``UTC`` (the *host* is EEST, and that does not reach the column).

⚠️ **Why this matters more than the production gate.** Infra's reading — 14 columns, 0
NULLs — examined **11 rows across 3 tables**; four of the seven hold zero rows and passed
vacuously. A count is not a measurement unless something says the rows were there to count.
This census does not care how many rows exist: it says the writer does not exist.

🚨 **Scope: the RUNTIME package, `datanika/` minus `datanika/migrations/`.** A migration
writing these columns is a deliberate, reviewed act — ``d7f2c8a4b1e6`` backfills
``updated_at`` on purpose — and folding migrations in would make this guard assert
something it does not mean. The claim being pinned is *"the application never writes
them"*, which is what makes a future row's value predictable.

The four constructor kwargs that do exist in `datanika/ui/state/` are on **pydantic display
DTOs** (``InvitationItem``, ``ApiKeyItem``, ``AuditLogItem``, ``NotificationItem``) whose
``created_at`` is a ``str``. They are not mapped classes, and the predicate below asks
SQLAlchemy which names are mapped rather than pattern-matching on the name — because
``InvitationItem`` reads exactly like ``Invitation`` to a grep.

⚠️ Cloud has its own copy of this over ``datanika_cloud/`` and ``scripts/``: three of the
seven tables are cloud's, and a per-repo guard would be satisfied by core and blind to
cloud — the shape that let core#943 happen after cloud had already fixed it.
"""

from __future__ import annotations

import ast
import pathlib

import pytest

RUNTIME = pathlib.Path(__file__).resolve().parents[2] / "datanika"
MIGRATIONS = RUNTIME / "migrations"
COLUMNS = ("created_at", "updated_at")


def mapped_class_names() -> set[str]:
    """Every ORM class name SQLAlchemy knows about, asked of the registry.

    🔑 Imported explicitly rather than relying on whatever pytest happened to import first:
    ``Base.registry`` is populated by import side effects, and a guard derived from a
    partially-populated registry covered 13 of 17 models and reported clean (QA, cloud#171).
    """
    import datanika.models  # noqa: F401  - populates the registry
    from datanika.models.base import Base

    return {mapper.class_.__name__ for mapper in Base.registry.mappers}


def writers(source: str, mapped: set[str]) -> list[str]:
    """Every write of a mixin timestamp in one module: assignments and mapped kwargs.

    Module-level so the suite can arm it against sources it writes itself — a predicate
    proved discriminating once by an external harness is a claim about a past session.
    """
    try:
        tree = ast.parse(source)
    except SyntaxError:  # pragma: no cover - a file that will not parse
        return []

    found: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Attribute) and target.attr in COLUMNS:
                    found.append(f"assignment to .{target.attr} (line {node.lineno})")
        if isinstance(node, ast.Call):
            name = getattr(node.func, "id", None) or getattr(node.func, "attr", None)
            if name not in mapped:
                continue
            for kw in node.keywords:
                if kw.arg in COLUMNS:
                    found.append(f"{name}({kw.arg}=...) (line {node.lineno})")
    return found


def _runtime_sources() -> list[pathlib.Path]:
    return [
        p
        for p in sorted(RUNTIME.rglob("*.py"))
        if "__pycache__" not in p.parts and MIGRATIONS not in p.parents
    ]


# ---------------------------------------------------------------------------
# Arming. A census that stops matching reports a clean tree, which is the
# reassuring answer — so the predicate is shown able to fire, in-suite.
# ---------------------------------------------------------------------------


def test_the_registry_is_populated():
    mapped = mapped_class_names()
    assert len(mapped) >= 15, (
        f"only {len(mapped)} mapped classes were found. A partially-populated registry "
        "makes the kwarg half of this census blind, and blind reads as clean."
    )
    assert "Invitation" in mapped and "NotificationChannel" in mapped


def test_the_scan_covers_the_runtime_package_and_excludes_migrations():
    sources = _runtime_sources()
    assert len(sources) >= 50, f"only {len(sources)} runtime modules found — wrong root?"
    assert not any(MIGRATIONS in p.parents for p in sources), "migrations leaked into the scan"
    assert (MIGRATIONS / "versions").is_dir(), "the exclusion is excluding nothing"


@pytest.mark.parametrize(
    ("label", "source"),
    [
        ("attribute assignment", "def f(row):\n    row.created_at = None\n"),
        ("mapped constructor kwarg", "def f():\n    return Invitation(created_at=None)\n"),
        ("updated_at too", "def f(row):\n    row.updated_at = 1\n"),
    ],
)
def test_the_census_sees_a_write(label, source):
    """Each shape the guard exists to catch, shown detected."""
    assert writers(source, mapped_class_names()), f"the census is blind to a {label}"


@pytest.mark.parametrize(
    ("label", "source"),
    [
        ("a pydantic DTO kwarg", "def f(inv):\n    return InvitationItem(created_at='x')\n"),
        ("a read", "def f(row):\n    return row.created_at.isoformat()\n"),
        ("an ORDER BY", "def f():\n    return q.order_by(Invitation.created_at.desc())\n"),
    ],
)
def test_the_census_does_not_fire_on_a_read_or_a_dto(label, source):
    """Narrowed in both directions. A pattern tightened until it matches nothing also stops
    matching real writes, and that is the worse bug because it is silent."""
    assert not writers(source, mapped_class_names()), f"the census false-positives on {label}"


# ---------------------------------------------------------------------------
# The census itself.
# ---------------------------------------------------------------------------


def test_nothing_in_the_runtime_package_writes_a_mixin_timestamp():
    """🔑 The claim release N+1 rests on.

    If this ever fires, ``f1a4c8e2d6b3``'s safety argument has changed and the new writer
    has to be examined for **both** hazards: can it write ``NULL``, and can it write a
    naive local-time value? Neither is visible in a diff that only adds an assignment.
    """
    mapped = mapped_class_names()
    offenders = {
        str(path.relative_to(RUNTIME)): found
        for path in _runtime_sources()
        if (found := writers(path.read_text(encoding="utf-8", errors="replace"), mapped))
    }
    assert not offenders, (
        "something in the runtime package now writes a TimestampMixin timestamp:\n  "
        + "\n  ".join(f"{k}: {v}" for k, v in sorted(offenders.items()))
        + "\n\ncore#1069's contract migration (f1a4c8e2d6b3) is safe BY CONSTRUCTION "
        "because nothing writes these columns: no NULL is producible and no local-time "
        "value is producible. A new writer breaks that argument, and the production row "
        "count does not replace it — four of the seven tables are empty."
    )
