"""TDD tests for InvitationService — email-based org invitations."""

import uuid
from datetime import UTC, datetime

import pytest

from datanika.models.invitation import InvitationStatus
from datanika.models.pii import UserPII
from datanika.models.user import MemberRole, Membership, Organization, User
from datanika.services.auth import AuthService
from datanika.services.invitation_service import InvitationService
from datanika.services.user_service import UserService
from tests.factories import make_user


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
    u = make_user(
        db_session,
        email=f"owner-{uuid.uuid4().hex[:6]}@test.com",
        password_hash="hashed",
        full_name="Owner",
        email_verified=True,
    )
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


class TestAlreadyMemberGuardDoesNotRestOnTheIdentityLookup:
    """core#1010 — the guard must be answered by the MEMBERSHIP, not by whether
    the address happens to resolve to a live ``User`` row.

    The shipped code asked ``get_user_by_email`` first and only checked
    ``Membership`` *inside* ``if existing_user:``. When that resolution failed
    the guard was not answered ``no`` — it was **skipped**, and the invitation
    was created for somebody who is already a member.

    ⚠️ **Not reachable in production today.** Release N dual-writes, so every
    real user has ``users.email`` set and ``get_user_by_email``'s legacy ``or_``
    half finds them even with no sidecar row. It becomes reachable at **N+1**,
    which deletes that half — ``SPEC_PII_SEPARATION.md`` §8a.4 Kind 3. These
    tests model N+1 rather than waiting for it, because §8a.6 requires N+1's
    diff to be **only deletions** and a security fix arriving inside that diff
    is exactly the pair somebody later has to tell apart.

    🔑 ``test_already_member_rejected`` above used to expose this **by
    accident** — its fixture owner had no sidecar row. PR #1013 gave it one, so
    it now passes while ``create_invitation`` stays as structurally fail-open as
    it ever was. *A fix that repairs the precondition a defect needed can retire
    the defect's only witness while leaving the defect.* These tests build the
    state on purpose so that cannot happen again.
    """

    @staticmethod
    def _sidecar_less_member(session, org, email: str) -> User:
        """A ``User`` with an active ``Membership`` and **no** ``user_pii`` row.

        ⚠️ Deliberately **not** ``tests.factories.make_user``: that factory
        exists to create the sidecar, and routing this through it is precisely
        what would silently retire the assertion. This is the second site in the
        repo that must build a sidecar-less user on purpose — the first is
        ``test_pii_separation.py``'s §8a.4 Kind 1 fixture, which §8a.4 likewise
        says never to "fix". **Do not helpfully convert either.**
        """
        user = User(email=email, full_name="Sidecarless", password_hash="hashed")
        session.add(user)
        session.flush()
        if session.get(UserPII, user.id) is not None:
            raise RuntimeError(
                "precondition failed: this user has a user_pii row, so the state "
                "core#1010 is about does not exist and nothing below is a measurement"
            )
        session.add(Membership(user_id=user.id, org_id=org.id, role=MemberRole.VIEWER))
        session.flush()
        return user

    def test_refuses_when_the_identity_lookup_returns_none(
        self, db_session, inv_svc, org, owner, monkeypatch
    ):
        """The criterion. An unresolvable address must not skip the guard.

        ``get_user_by_email`` is patched to ``None`` because that is the issue's
        condition stated exactly, and because it is what N+1 does to a
        sidecar-less user once the join becomes the only half. Patching the
        **collaborator** rather than editing its query also pins the invariant
        that matters: ``create_invitation`` may not depend on that lookup
        succeeding. A fix routed back through it fails here, correctly.

        ⚠️ This test also fails if N+1 deletes ``InvitationService``'s *own*
        legacy ``users.email`` clause. That is the intended coupling — it is a
        behavioural reminder that the two sites must be retired together, which
        is worth more than a comment asking nicely.
        """
        email = f"ghost-{uuid.uuid4().hex[:6]}@test.com"
        self._sidecar_less_member(db_session, org, email)
        monkeypatch.setattr(UserService, "get_user_by_email", lambda self, session, email: None)

        with pytest.raises(ValueError, match="already a member"):
            inv_svc.create_invitation(db_session, org.id, email, MemberRole.EDITOR, owner.id)

    def test_refuses_when_the_user_row_is_soft_deleted(self, db_session, inv_svc, org, owner):
        """``users.deleted_at IS NULL`` is a *login* filter, not a membership one.

        ``get_user_by_email`` carries it for a good reason — without it a
        soft-deleted user still authenticates. Borrowing it to answer *"is this
        address a member?"* is a different question, and the borrowing is the
        second way the resolution fails while the ``Membership`` row is intact.

        No monkeypatching here: the real lookup returns ``None`` on its own, so
        this one is red against the shipped code with nothing arming it.
        """
        email = f"gone-{uuid.uuid4().hex[:6]}@test.com"
        user = make_user(db_session, email=email, password_hash="hashed", full_name="Gone")
        db_session.add(Membership(user_id=user.id, org_id=org.id, role=MemberRole.VIEWER))
        user.deleted_at = datetime.now(UTC)
        db_session.flush()

        with pytest.raises(ValueError, match="already a member"):
            inv_svc.create_invitation(db_session, org.id, email, MemberRole.EDITOR, owner.id)

    def test_a_genuinely_new_address_is_still_invitable(
        self, db_session, inv_svc, org, owner, monkeypatch
    ):
        """The negative control, and the reason the fix must not fail closed.

        Refusing every address we cannot resolve would refuse **every invitation
        to a new user**, which is the primary use of the feature. The failure
        *direction* was never the defect; the conflation was. Run with the same
        patch as the criterion above, so a fix cannot pass that one by treating
        an unresolvable address as a member.
        """
        monkeypatch.setattr(UserService, "get_user_by_email", lambda self, session, email: None)

        inv = inv_svc.create_invitation(
            db_session,
            org.id,
            f"brand-new-{uuid.uuid4().hex[:6]}@test.com",
            MemberRole.EDITOR,
            owner.id,
        )
        assert inv.status == InvitationStatus.PENDING

    def test_a_membership_in_another_org_does_not_block(self, db_session, inv_svc, org, owner):
        """The second negative control: the guard is per-org, and must stay so.

        A query written from ``Membership`` is one missing ``org_id`` clause away
        from refusing every address that is a member of *anything*, and that
        failure is invisible in a single-org test.
        """
        other = Organization(name="OtherOrg", slug=f"other-{uuid.uuid4().hex[:8]}")
        db_session.add(other)
        db_session.flush()
        email = f"elsewhere-{uuid.uuid4().hex[:6]}@test.com"
        user = make_user(db_session, email=email, password_hash="hashed", full_name="Elsewhere")
        db_session.add(Membership(user_id=user.id, org_id=other.id, role=MemberRole.VIEWER))
        db_session.flush()

        inv = inv_svc.create_invitation(db_session, org.id, email, MemberRole.EDITOR, owner.id)
        assert inv.status == InvitationStatus.PENDING

    def test_a_soft_deleted_membership_does_not_block(self, db_session, inv_svc, org, owner):
        """The third negative control: a removed member can be re-invited.

        ``Membership.deleted_at`` is how erasure and ``remove_member`` retire a
        membership, so a guard that ignores it locks a removed colleague out
        permanently.
        """
        email = f"removed-{uuid.uuid4().hex[:6]}@test.com"
        user = make_user(db_session, email=email, password_hash="hashed", full_name="Removed")
        db_session.add(
            Membership(
                user_id=user.id,
                org_id=org.id,
                role=MemberRole.VIEWER,
                deleted_at=datetime.now(UTC),
            )
        )
        db_session.flush()

        inv = inv_svc.create_invitation(db_session, org.id, email, MemberRole.EDITOR, owner.id)
        assert inv.status == InvitationStatus.PENDING


class TestAcceptInvitation:
    def test_accept_creates_membership(self, db_session, inv_svc, user_svc, org, owner):
        inv = inv_svc.create_invitation(
            db_session, org.id, "accept@test.com", MemberRole.EDITOR, owner.id
        )
        db_session.flush()

        # Create the user who will accept
        user = user_svc.register_user(
            db_session, "accept@test.com", "accepter password", "Accepter"
        )
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
        user_svc.register_user(db_session, "twice@test.com", "twice password", "Twice")
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
