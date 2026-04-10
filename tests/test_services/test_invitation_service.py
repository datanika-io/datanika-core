"""TDD tests for InvitationService — email-based org invitations."""

import uuid

import pytest

from datanika.models.invitation import InvitationStatus
from datanika.models.user import MemberRole, Membership, Organization, User
from datanika.services.auth import AuthService
from datanika.services.invitation_service import InvitationService
from datanika.services.user_service import UserService


@pytest.fixture
def auth():
    return AuthService("test-secret-key-for-invitations")


@pytest.fixture
def user_svc(auth):
    return UserService(auth)


@pytest.fixture
def inv_svc(auth):
    return InvitationService(auth)


@pytest.fixture
def org(db_session):
    o = Organization(name="InviteOrg", slug=f"invite-{uuid.uuid4().hex[:8]}")
    db_session.add(o)
    db_session.flush()
    return o


@pytest.fixture
def owner(db_session, org):
    u = User(
        email=f"owner-{uuid.uuid4().hex[:6]}@test.com",
        password_hash="hashed",
        full_name="Owner",
        email_verified=True,
    )
    db_session.add(u)
    db_session.flush()
    m = Membership(user_id=u.id, org_id=org.id, role=MemberRole.OWNER)
    db_session.add(m)
    db_session.flush()
    return u


class TestCreateInvitation:
    def test_creates_invitation(self, db_session, inv_svc, org, owner):
        inv = inv_svc.create_invitation(
            db_session, org.id, "newuser@test.com", MemberRole.EDITOR, owner.id
        )
        assert inv.id is not None
        assert inv.email == "newuser@test.com"
        assert inv.role == MemberRole.EDITOR
        assert inv.status == InvitationStatus.PENDING
        assert inv.token != ""
        assert inv.org_id == org.id
        assert inv.invited_by_user_id == owner.id

    def test_duplicate_pending_invitation_rejected(self, db_session, inv_svc, org, owner):
        inv_svc.create_invitation(db_session, org.id, "dup@test.com", MemberRole.VIEWER, owner.id)
        with pytest.raises(ValueError, match="pending invitation"):
            inv_svc.create_invitation(
                db_session, org.id, "dup@test.com", MemberRole.VIEWER, owner.id
            )

    def test_already_member_rejected(self, db_session, inv_svc, org, owner):
        with pytest.raises(ValueError, match="already a member"):
            inv_svc.create_invitation(db_session, org.id, owner.email, MemberRole.EDITOR, owner.id)


class TestAcceptInvitation:
    def test_accept_creates_membership(self, db_session, inv_svc, user_svc, org, owner):
        inv = inv_svc.create_invitation(
            db_session, org.id, "accept@test.com", MemberRole.EDITOR, owner.id
        )
        db_session.flush()

        # Create the user who will accept
        user = user_svc.register_user(db_session, "accept@test.com", "pass123", "Accepter")
        db_session.flush()

        membership = inv_svc.accept_invitation(db_session, inv.token)
        assert membership is not None
        assert membership.user_id == user.id
        assert membership.org_id == org.id
        assert membership.role == MemberRole.EDITOR

        # Invitation marked as accepted
        db_session.refresh(inv)
        assert inv.status == InvitationStatus.ACCEPTED

    def test_accept_invalid_token_returns_none(self, db_session, inv_svc):
        result = inv_svc.accept_invitation(db_session, "bogus-token")
        assert result is None

    def test_accept_already_accepted_returns_none(self, db_session, inv_svc, user_svc, org, owner):
        inv = inv_svc.create_invitation(
            db_session, org.id, "twice@test.com", MemberRole.VIEWER, owner.id
        )
        user_svc.register_user(db_session, "twice@test.com", "pass123", "Twice")
        db_session.flush()

        inv_svc.accept_invitation(db_session, inv.token)
        result = inv_svc.accept_invitation(db_session, inv.token)
        assert result is None


class TestCancelInvitation:
    def test_cancel_marks_cancelled(self, db_session, inv_svc, org, owner):
        inv = inv_svc.create_invitation(
            db_session, org.id, "cancel@test.com", MemberRole.VIEWER, owner.id
        )
        result = inv_svc.cancel_invitation(db_session, org.id, inv.id)
        assert result is True
        db_session.refresh(inv)
        assert inv.status == InvitationStatus.CANCELLED


class TestListPendingInvitations:
    def test_lists_pending_only(self, db_session, inv_svc, org, owner):
        inv_svc.create_invitation(
            db_session, org.id, "pending1@test.com", MemberRole.VIEWER, owner.id
        )
        inv_svc.create_invitation(
            db_session, org.id, "pending2@test.com", MemberRole.EDITOR, owner.id
        )
        inv3 = inv_svc.create_invitation(
            db_session, org.id, "cancelled@test.com", MemberRole.VIEWER, owner.id
        )
        inv_svc.cancel_invitation(db_session, org.id, inv3.id)
        db_session.flush()

        pending = inv_svc.list_pending_invitations(db_session, org.id)
        assert len(pending) == 2
        emails = {i.email for i in pending}
        assert emails == {"pending1@test.com", "pending2@test.com"}
