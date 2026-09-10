"""Connections enforce their role in the SERVICE (core#681, SPEC_SERVICE_AUTHORIZATION §1).

The first of the eight UI-only subsystems. §1's table for connections:

| threshold | handlers |
|---|---|
| `editor` | `save_connection`, `edit_connection`, `copy_connection` |
| `admin`  | `delete_connection` |

which is §2's one rule, not four decisions: *editor for the ordinary lifecycle; admin for
deletion, and for anything that touches a credential.*

🔑 Why the service and not the handler: a check here is inherited by the Reflex path **and** by
the four mutating REST endpoints. A check in `ConnectionState` is inherited by neither. Every
hour spent hardening the handler widens the gap between the surfaces rather than closing it.

⚠️ **`update_connection` takes `**kwargs`, which is a trap this file exists to keep shut.** A
keyword-only `actor_user_id` added *after* `**kwargs`, or passed by a caller without the parameter
existing, is **silently swallowed**: it matches none of the method's `if "name" in kwargs`
branches, so it is ignored, no check runs, and the call site *looks* like it is passing an actor.
`test_update_does_not_swallow_the_actor` is the assertion that says otherwise.
"""

from __future__ import annotations

import pytest

from datanika.models.connection import ConnectionType
from datanika.models.user import MemberRole, Membership, Organization
from datanika.services.authorization import InsufficientRoleError
from datanika.services.connection_service import ConnectionService
from datanika.services.encryption import EncryptionService
from tests.factories import make_user


@pytest.fixture
def svc():
    from cryptography.fernet import Fernet

    return ConnectionService(EncryptionService(Fernet.generate_key().decode()))


@pytest.fixture
def org(db_session):
    o = Organization(name="Acme", slug="acme-conn-authz")
    db_session.add(o)
    db_session.flush()
    return o


def _actor(db_session, org, role: MemberRole, email: str) -> int:
    user = make_user(db_session, email=email, password_hash="x")
    db_session.add(Membership(user_id=user.id, org_id=org.id, role=role))
    db_session.flush()
    return user.id


@pytest.fixture
def admin(db_session, org):
    return _actor(db_session, org, MemberRole.ADMIN, "admin@conn.io")


@pytest.fixture
def editor(db_session, org):
    return _actor(db_session, org, MemberRole.EDITOR, "editor@conn.io")


@pytest.fixture
def viewer(db_session, org):
    return _actor(db_session, org, MemberRole.VIEWER, "viewer@conn.io")


def _make(svc, db_session, org, actor_user_id, name="C"):
    return svc.create_connection(
        db_session,
        org.id,
        name,
        ConnectionType.POSTGRES,
        {"host": "h", "port": 5432, "database": "d", "user": "u", "password": "p"},
        actor_user_id=actor_user_id,
    )


class TestTheOrdinaryLifecycleNeedsEditor:
    def test_an_editor_can_create(self, svc, db_session, org, editor):
        assert _make(svc, db_session, org, editor) is not None

    def test_a_viewer_cannot_create(self, svc, db_session, org, viewer):
        with pytest.raises(InsufficientRoleError) as exc:
            _make(svc, db_session, org, viewer)
        assert exc.value.required_role == "editor"

    def test_an_editor_can_update(self, svc, db_session, org, editor):
        conn = _make(svc, db_session, org, editor)
        assert svc.update_connection(
            db_session, org.id, conn.id, actor_user_id=editor, name="renamed"
        )

    def test_a_viewer_cannot_update(self, svc, db_session, org, editor, viewer):
        conn = _make(svc, db_session, org, editor)
        with pytest.raises(InsufficientRoleError) as exc:
            svc.update_connection(db_session, org.id, conn.id, actor_user_id=viewer, name="x")
        assert exc.value.required_role == "editor"


class TestDeletionNeedsAdmin:
    """§2: admin for deletion. The threshold is moved, not re-decided (§5 AC1)."""

    def test_an_admin_can_delete(self, svc, db_session, org, admin):
        conn = _make(svc, db_session, org, admin)
        assert svc.delete_connection(db_session, org.id, conn.id, actor_user_id=admin) is True

    def test_an_EDITOR_cannot_delete(self, svc, db_session, org, editor):
        """The discriminating case: editor passes every other threshold in this file."""
        conn = _make(svc, db_session, org, editor)
        with pytest.raises(InsufficientRoleError) as exc:
            svc.delete_connection(db_session, org.id, conn.id, actor_user_id=editor)
        assert exc.value.required_role == "admin"


class TestTheOrderingDoesNotCreateAnOracle:
    """§7.3 — check the role AFTER the org-scoped lookup, BEFORE the mutation.

    Checking first turns a cross-org probe into a refusal that confirms the resource exists
    somewhere. The org-scoped lookup must answer *"not found"* first, and keep doing so.
    """

    def test_a_cross_org_id_is_not_found_rather_than_refused(self, svc, db_session, org, admin):
        other = Organization(name="OtherCo", slug="other-conn-authz")
        db_session.add(other)
        db_session.flush()
        outsider = _actor(db_session, other, MemberRole.ADMIN, "outsider@conn.io")
        conn = _make(svc, db_session, org, admin)

        # The outsider is an ADMIN of their own org, so a role-first check would pass and the
        # refusal would have to come from somewhere else — which is exactly the leak.
        assert svc.delete_connection(db_session, other.id, conn.id, actor_user_id=outsider) is False

    def test_an_unknown_id_in_your_own_org_is_also_not_found(self, svc, db_session, org, admin):
        """Control: `False` above must be the lookup answering, not a blanket false."""
        assert svc.delete_connection(db_session, org.id, 999_999, actor_user_id=admin) is False


class TestTheActorCannotBeOmittedOrSwallowed:
    def test_update_does_not_swallow_the_actor(self, svc, db_session, org, editor, viewer):
        """🚨 The `**kwargs` trap.

        If `actor_user_id` were absorbed by `**kwargs` it would match none of the update
        branches, be silently ignored, and every call site would *look* like it passes an
        actor while nothing is checked. A viewer succeeding here is that bug.
        """
        conn = _make(svc, db_session, org, editor)
        with pytest.raises(InsufficientRoleError):
            svc.update_connection(db_session, org.id, conn.id, actor_user_id=viewer, name="x")

    def test_update_still_applies_its_kwargs(self, svc, db_session, org, editor):
        """The other half: making the actor explicit must not break kwargs handling."""
        conn = _make(svc, db_session, org, editor)
        svc.update_connection(db_session, org.id, conn.id, actor_user_id=editor, name="renamed")
        db_session.refresh(conn)
        assert conn.name == "renamed"

    @pytest.mark.parametrize("method", ["create", "update", "delete"])
    def test_a_caller_that_omits_the_actor_fails_loudly(self, svc, db_session, org, editor, method):
        """Required, not defaulted to `None`.

        A default would make a forgetful caller *refuse in production* — which reads as a
        permissions bug for a legitimate admin. Required makes it a `TypeError` at the call
        site, found by any test that exercises the path.
        """
        conn = _make(svc, db_session, org, editor)
        with pytest.raises(TypeError):
            if method == "create":
                svc.create_connection(db_session, org.id, "N", ConnectionType.POSTGRES, {})
            elif method == "update":
                svc.update_connection(db_session, org.id, conn.id, name="x")
            else:
                svc.delete_connection(db_session, org.id, conn.id)
