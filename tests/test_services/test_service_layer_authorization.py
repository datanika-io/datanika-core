"""The service-layer authorization mechanism (core#681, SPEC_SERVICE_AUTHORIZATION §4).

What is being asserted here is the **property**, not the parameter. §5's "what must not be
asserted" is explicit about the difference:

> Do not test that the service raises when handed a bad role string. That asserts the parameter,
> not the authority, and it passes against the ``role="editor"`` shape §4 forbids. The property
> is: *an actor whose membership does not permit the operation cannot perform it, whatever they
> send.*

So every test below drives a real actor against a real membership row, and the only input is
**who** is acting — never what they claim to be.
"""

from __future__ import annotations

import pytest

from datanika.errors import InternalInvariantError
from datanika.models.user import MemberRole, Membership, Organization
from datanika.services.authorization import InsufficientRoleError, assert_org_role
from tests.factories import make_user


@pytest.fixture
def org(db_session):
    o = Organization(name="Acme", slug="acme-svc-authz")
    db_session.add(o)
    db_session.flush()
    return o


@pytest.fixture
def other_org(db_session):
    o = Organization(name="OtherCo", slug="other-svc-authz")
    db_session.add(o)
    db_session.flush()
    return o


def _member(db_session, org, role: MemberRole, email: str):
    """Through ``tests.factories``, not ``User(...)`` directly.

    SPEC_PII_SEPARATION §8a.2: constructing a guarded model directly produces a row the
    dual-write invariant says cannot exist. CI caught my first draft doing exactly that.
    """
    user = make_user(db_session, email=email, password_hash="x")
    db_session.add(Membership(user_id=user.id, org_id=org.id, role=role))
    db_session.flush()
    return user


class TestTheThresholdIsEnforcedAgainstTheActorsMembership:
    def test_an_actor_at_the_threshold_is_allowed(self, db_session, org):
        user = _member(db_session, org, MemberRole.EDITOR, "editor@x.io")
        membership = assert_org_role(
            db_session, org.id, user.id, required=MemberRole.EDITOR, operation="save_connection"
        )
        assert membership.user_id == user.id

    def test_an_actor_above_the_threshold_is_allowed(self, db_session, org):
        user = _member(db_session, org, MemberRole.ADMIN, "admin@x.io")
        assert assert_org_role(
            db_session, org.id, user.id, required=MemberRole.EDITOR, operation="save_connection"
        )

    def test_an_actor_below_the_threshold_is_refused(self, db_session, org):
        user = _member(db_session, org, MemberRole.VIEWER, "viewer@x.io")
        with pytest.raises(InsufficientRoleError) as exc:
            assert_org_role(
                db_session, org.id, user.id, required=MemberRole.EDITOR, operation="save_connection"
            )
        assert exc.value.required_role == "editor"
        assert exc.value.actor_role == "viewer"

    def test_editor_cannot_reach_an_admin_threshold(self, db_session, org):
        """§2's rule: editor for the ordinary lifecycle, admin for deletion and credentials."""
        user = _member(db_session, org, MemberRole.EDITOR, "editor2@x.io")
        with pytest.raises(InsufficientRoleError) as exc:
            assert_org_role(
                db_session,
                org.id,
                user.id,
                required=MemberRole.ADMIN,
                operation="delete_connection",
            )
        assert exc.value.required_role == "admin"


class TestTheIntersectionIsResolvedNow:
    """🔑 The property the founder decision turns on, and the reason it has a cost.

    A key's authority is intersected with its owner's **current** role at authentication time.
    Nothing about the key changes; nothing is deployed. The same actor, the same call, a
    different answer — because someone edited a membership row in between.
    """

    def test_a_demotion_takes_effect_on_the_next_call(self, db_session, org):
        user = _member(db_session, org, MemberRole.ADMIN, "demoted@x.io")
        assert assert_org_role(
            db_session, org.id, user.id, required=MemberRole.ADMIN, operation="delete_connection"
        )

        membership = (
            db_session.query(Membership)
            .filter(Membership.org_id == org.id, Membership.user_id == user.id)
            .one()
        )
        membership.role = MemberRole.VIEWER
        db_session.flush()

        with pytest.raises(InsufficientRoleError):
            assert_org_role(
                db_session,
                org.id,
                user.id,
                required=MemberRole.ADMIN,
                operation="delete_connection",
            )

    def test_a_promotion_takes_effect_on_the_next_call(self, db_session, org):
        """The other direction. Without it, "always refuse" passes the test above."""
        user = _member(db_session, org, MemberRole.VIEWER, "promoted@x.io")
        with pytest.raises(InsufficientRoleError):
            assert_org_role(
                db_session, org.id, user.id, required=MemberRole.EDITOR, operation="save_connection"
            )

        membership = (
            db_session.query(Membership)
            .filter(Membership.org_id == org.id, Membership.user_id == user.id)
            .one()
        )
        membership.role = MemberRole.EDITOR
        db_session.flush()

        assert assert_org_role(
            db_session, org.id, user.id, required=MemberRole.EDITOR, operation="save_connection"
        )


class TestItFailsClosed:
    def test_a_missing_actor_is_refused(self, db_session, org):
        """Copying `_actor_membership`'s convention and its reason (core#658).

        Allowing `None` leaves the same hole open for the next caller — an API route, the MCP
        surface, a restore flow — while looking fixed.
        """
        with pytest.raises(InsufficientRoleError):
            assert_org_role(
                db_session, org.id, None, required=MemberRole.EDITOR, operation="save_connection"
            )

    def test_a_non_member_is_refused(self, db_session, org):
        user = make_user(db_session, email="stranger@x.io", password_hash="x")
        with pytest.raises(InsufficientRoleError):
            assert_org_role(
                db_session, org.id, user.id, required=MemberRole.EDITOR, operation="save_connection"
            )

    def test_an_admin_of_another_org_is_refused(self, db_session, org, other_org):
        """Authority does not travel between orgs, whatever the actor's role elsewhere."""
        user = _member(db_session, other_org, MemberRole.ADMIN, "elsewhere@x.io")
        with pytest.raises(InsufficientRoleError):
            assert_org_role(
                db_session, org.id, user.id, required=MemberRole.EDITOR, operation="save_connection"
            )

    def test_an_unknown_threshold_is_a_programming_error_not_a_refusal(self, db_session, org):
        """§2: *do not invent a fourth threshold.* A typo must not read as a permissions answer.

        Deliberately **not** an `InsufficientRoleError` — that would render as a `403` telling a
        caller to obtain the role `"editorr"`, which does not exist.
        """
        user = _member(db_session, org, MemberRole.ADMIN, "typo@x.io")
        with pytest.raises(InternalInvariantError) as exc:
            assert_org_role(
                db_session, org.id, user.id, required="editorr", operation="save_connection"
            )
        assert not isinstance(exc.value, InsufficientRoleError)


class TestTheRefusalCanBeActedOn:
    """🚨 §7.1 is this decision's only mitigation, so the carrier is asserted, not assumed."""

    def test_required_role_is_a_field_not_only_prose(self, db_session, org):
        user = _member(db_session, org, MemberRole.VIEWER, "field@x.io")
        with pytest.raises(InsufficientRoleError) as exc:
            assert_org_role(
                db_session,
                org.id,
                user.id,
                required=MemberRole.ADMIN,
                operation="delete_connection",
            )
        assert exc.value.required_role == "admin"
        assert exc.value.operation == "delete_connection"

    def test_the_message_does_not_read_as_expired_or_revoked(self, db_session, org):
        """The whole cost of the decision is that a working key stops working silently.

        A caller told "expired" or "revoked" re-mints a key that was never the problem, and the
        new key fails identically. Naming the *role* is what makes it a one-step support answer.
        """
        user = _member(db_session, org, MemberRole.VIEWER, "wording@x.io")
        with pytest.raises(InsufficientRoleError) as exc:
            assert_org_role(
                db_session,
                org.id,
                user.id,
                required=MemberRole.ADMIN,
                operation="delete_connection",
            )
        text = str(exc.value).lower()
        for forbidden in ("expired", "revoked", "invalid", "not found", "unauthorized"):
            assert forbidden not in text, f"refusal reads as {forbidden!r}: {exc.value}"
        assert "admin" in text
