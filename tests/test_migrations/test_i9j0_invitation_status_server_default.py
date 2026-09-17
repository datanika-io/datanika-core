"""``invitations.status`` column default must be loadable by the ORM (core#1393).

The third instance of core#1391's class. ``m2i9j0k1l3f4`` created the column with
``server_default="pending"``; the mapper is ``Enum(InvitationStatus, native_enum=False)`` with no
``values_callable``, which loads member **names**, so a row that takes the database default raises
``LookupError: 'pending' is not among the defined enum values``. QA measured the default as
``'pending'::character varying`` on production and on staging, with **zero** invitation rows on
either -- armed, not fired.

Why this file rather than the model test
----------------------------------------
``tests/test_models/test_invitation_status_server_default.py`` builds the schema from the model,
so it goes green the moment the model changes whatever the database holds. Only alembic against a
real Postgres can say what the *column* does, and the column is what an out-of-band INSERT meets.
"""

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import create_engine, select, text
from sqlalchemy.orm import Session

from datanika.models.invitation import Invitation, InvitationStatus
from tests.test_migrations.conftest import _run_alembic

PARENT_REVISION = "h8i9j0k1l2m3"
THIS_REVISION = "i9j0k1l2m3n4"


@pytest.fixture
def at_parent(roundtrip_db_url):
    """A database migrated to the revision immediately before this one."""
    engine = create_engine(roundtrip_db_url)
    with engine.begin() as conn:
        conn.execute(text("DROP SCHEMA IF EXISTS public CASCADE"))
        conn.execute(text("CREATE SCHEMA public"))
    result = _run_alembic(["upgrade", PARENT_REVISION], roundtrip_db_url)
    assert result.returncode == 0, f"could not reach {PARENT_REVISION}: {result.stderr}"
    return engine


def _upgrade(db_url: str) -> None:
    result = _run_alembic(["upgrade", THIS_REVISION], db_url)
    assert result.returncode == 0, f"upgrade to {THIS_REVISION} failed: {result.stderr}"


def _column_default(engine) -> str | None:
    with engine.begin() as conn:
        return conn.execute(
            text(
                "SELECT column_default FROM information_schema.columns "
                "WHERE table_schema = 'public' AND table_name = 'invitations' "
                "AND column_name = 'status'"
            )
        ).scalar_one()


def _seed_parents(engine) -> tuple[int, int]:
    with engine.begin() as conn:
        org = conn.execute(
            text(
                "INSERT INTO organizations (name, slug) VALUES ('Probe', 'probe-1393') RETURNING id"
            )
        ).scalar_one()
        user = conn.execute(
            text(
                "INSERT INTO users (email, password_hash, full_name, is_active, email_verified, "
                "created_at, updated_at) VALUES ('inviter-1393@example.com', 'x', 'Inviter', "
                "true, true, now(), now()) RETURNING id"
            )
        ).scalar_one()
    return org, user


_N = iter(range(1_000_000))


def _insert(engine, parents, *, status: str | None = None) -> int:
    """INSERT at the SQL level. ``status=None`` OMITS the column: the database default applies."""
    org, user = parents
    columns = "org_id, role, invited_by_user_id, token_hash, expires_at"
    values = ":org, 'VIEWER', :user, :token_hash, :expires"
    if status is not None:
        columns += ", status"
        values += ", :status"
    with engine.begin() as conn:
        return conn.execute(
            text(f"INSERT INTO invitations ({columns}) VALUES ({values}) RETURNING id"),  # noqa: S608
            {
                "org": org,
                "user": user,
                "token_hash": f"{next(_N):064d}",
                "expires": datetime.now(UTC) + timedelta(days=7),
                "status": status,
            },
        ).scalar_one()


def _stored(engine, row_id: int) -> str:
    with engine.begin() as conn:
        return conn.execute(
            text("SELECT status FROM invitations WHERE id = :id"), {"id": row_id}
        ).scalar_one()


def _load_through_the_mapper(engine, row_id: int):
    with Session(engine) as session:
        return session.execute(
            select(Invitation.status).where(Invitation.id == row_id)
        ).scalar_one()


# ---------------------------------------------------------------------------
# Controls -- they attribute the reds below.
# ---------------------------------------------------------------------------


def test_control_the_default_is_lowercase_at_the_parent_revision(at_parent):
    default = _column_default(at_parent)
    assert default is not None and "'pending'" in default, (
        f"invitations.status already defaulted to {default!r} at {PARENT_REVISION}, so every "
        "assertion below would pass without this migration doing anything"
    )


def test_control_an_omitting_insert_is_unloadable_at_the_parent_revision(at_parent):
    """The defect itself, reproduced against the real column and the real mapper."""
    row_id = _insert(at_parent, _seed_parents(at_parent))

    assert _stored(at_parent, row_id) == "pending"
    with pytest.raises(LookupError, match="pending"):
        _load_through_the_mapper(at_parent, row_id)


# ---------------------------------------------------------------------------
# Regressions -- red before this migration exists.
# ---------------------------------------------------------------------------


def test_an_omitting_insert_loads_through_the_mapper_after_the_migration(
    at_parent, roundtrip_db_url
):
    _upgrade(roundtrip_db_url)
    row_id = _insert(at_parent, _seed_parents(at_parent))

    assert _load_through_the_mapper(at_parent, row_id) is InvitationStatus.PENDING


def test_the_column_default_is_the_models_server_default(at_parent, roundtrip_db_url):
    """Model and migration must agree, derived from the model rather than restated here."""
    _upgrade(roundtrip_db_url)
    declared = Invitation.__table__.c.status.server_default.arg

    default = _column_default(at_parent)
    assert default is not None and f"'{declared}'" in default, (
        f"invitations.status: the migrated column defaults to {default!r} while the model "
        f"declares server_default={declared!r}"
    )


def test_existing_rows_are_left_alone(at_parent, roundtrip_db_url):
    """DDL only. A row the ORM would write, another member, and a row already holding the
    unloadable spelling all keep exactly what they held."""
    parents = _seed_parents(at_parent)
    rows = {
        "PENDING": _insert(at_parent, parents, status="PENDING"),
        "ACCEPTED": _insert(at_parent, parents, status="ACCEPTED"),
        "pending": _insert(at_parent, parents),
    }

    _upgrade(roundtrip_db_url)

    assert {held: _stored(at_parent, rid) for held, rid in rows.items()} == {
        "PENDING": "PENDING",
        "ACCEPTED": "ACCEPTED",
        "pending": "pending",
    }


def test_the_downgrade_restores_the_old_default_without_rewriting_rows(at_parent, roundtrip_db_url):
    _upgrade(roundtrip_db_url)
    parents = _seed_parents(at_parent)
    kept = _insert(at_parent, parents)

    down = _run_alembic(["downgrade", PARENT_REVISION], roundtrip_db_url)
    assert down.returncode == 0, f"downgrade failed: {down.stderr}"

    default = _column_default(at_parent)
    assert default is not None and "'pending'" in default, f"default not restored: {default!r}"
    assert _stored(at_parent, kept) == "PENDING", "the downgrade rewrote an existing row"

    _upgrade(roundtrip_db_url)
    default = _column_default(at_parent)
    assert default is not None and "'PENDING'" in default
