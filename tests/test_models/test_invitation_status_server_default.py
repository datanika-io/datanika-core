"""``invitations.status``: the column default must be a value the ORM can load (core#1393).

``Enum(InvitationStatus, native_enum=False)`` **without** ``values_callable`` persists a member's
**name**. The migrated column defaulted to the member's **value**, ``pending``, which the mapper
cannot load -- core#1391's class, third instance.

These tests build the schema from the **model** (``Base.metadata.create_all``), so they pin the
model's declaration: an INSERT that omits ``status`` must load, and must store the spelling the ORM
itself writes. What the *database* column carries is asserted against real Postgres in
``tests/test_migrations/test_i9j0_invitation_status_server_default.py``.

The control names the column and loads, so the regression cannot pass by every insert failing.
"""

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import text

from datanika.models.invitation import Invitation, InvitationStatus
from datanika.models.user import MemberRole, Organization
from tests.factories import make_user


@pytest.fixture
def parents(db_session):
    org = Organization(name="Acme", slug="acme-1393")
    db_session.add(org)
    db_session.flush()
    inviter = make_user(db_session, email="inviter-1393@test.io", password_hash="x")
    return org.id, inviter.id


def _insert(db_session, parents, *, status: str | None, token_hash: str) -> int:
    org_id, inviter_id = parents
    columns = "org_id, role, invited_by_user_id, token_hash, expires_at"
    values = ":org, 'VIEWER', :inviter, :token_hash, :expires"
    if status is not None:
        columns += ", status"
        values += ", :status"
    db_session.execute(
        text(f"INSERT INTO invitations ({columns}) VALUES ({values})"),  # noqa: S608 - literals
        {
            "org": org_id,
            "inviter": inviter_id,
            "token_hash": token_hash,
            "expires": datetime.now(UTC) + timedelta(days=7),
            "status": status,
        },
    )
    return db_session.execute(text("SELECT max(id) FROM invitations")).scalar_one()


def test_a_row_that_omits_status_loads_through_the_orm(db_session, parents):
    """The regression: before core#1393 the model declared no server default at all."""
    row_id = _insert(db_session, parents, status=None, token_hash="a" * 64)
    db_session.expire_all()

    assert db_session.get(Invitation, row_id).status is InvitationStatus.PENDING


def test_the_default_stores_the_spelling_the_orm_itself_writes(db_session, parents):
    """Derived, not restated: whatever the ORM stores for PENDING is what the default stores."""
    org_id, inviter_id = parents
    omitted_id = _insert(db_session, parents, status=None, token_hash="b" * 64)
    orm_row = Invitation(
        org_id=org_id,
        role=MemberRole.VIEWER,
        invited_by_user_id=inviter_id,
        token_hash="c" * 64,
        expires_at=datetime.now(UTC) + timedelta(days=7),
    )
    db_session.add(orm_row)
    db_session.flush()

    stored = dict(
        db_session.execute(
            text("SELECT id, status FROM invitations WHERE id IN (:a, :b)"),
            {"a": omitted_id, "b": orm_row.id},
        ).all()
    )
    assert stored[omitted_id] == stored[orm_row.id], stored


def test_control_a_row_that_names_status_loads(db_session, parents):
    row_id = _insert(db_session, parents, status="ACCEPTED", token_hash="d" * 64)
    db_session.expire_all()

    assert db_session.get(Invitation, row_id).status is InvitationStatus.ACCEPTED
