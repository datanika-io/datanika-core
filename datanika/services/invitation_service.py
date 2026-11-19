"""InvitationService — create, accept, cancel, and list org invitations."""

import hashlib
import logging
from datetime import UTC, datetime, timedelta

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from datanika.errors import UserFacingError
from datanika.models.invitation import Invitation, InvitationStatus
from datanika.models.pii import InvitationPII, UserPII
from datanika.models.user import MemberRole, Membership, User
from datanika.services.auth import AuthService

logger = logging.getLogger(__name__)


def hash_invitation_token(raw: str) -> str:
    """SHA-256 hex of the value in the emailed link (D3).

    Same construction as ``PasswordResetService._hash`` and ``OAuthGrant.code_hash``, and
    it must stay byte-identical to the migration's
    ``encode(sha256(convert_to(token, 'UTF8')), 'hex')`` or the backfilled rows become
    unfindable. SHA-256 rather than bcrypt because the token already carries plenty of
    entropy — there is nothing to stretch — and the lookup has to be an indexed equality.

    ``invitations`` previously stored its JWT **verbatim**, in a database whose nightly
    ``pg_dump`` ships off-box and is kept 30 days, and that JWT's payload contains
    ``{"email": <invitee>}``. ``models/password_reset.py`` documents why not to do that,
    in this same repo, and names this table as the counter-example.
    """
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


class InvitationService:
    def __init__(self, auth: AuthService):
        self._auth = auth

    @staticmethod
    def _has_active_membership(session: Session, org_id: int, email: str) -> bool:
        """Whether this address already holds an active membership in this org.

        🚨 **Asked from ``Membership``, on purpose (core#1010).** This used to be
        ``get_user_by_email`` followed by a membership query *inside*
        ``if existing_user:``, which answers a different question — *"can I
        resolve this address to a live ``User`` row, and if so, is it a
        member?"* — and when the resolution failed the guard was not answered
        ``no``, it was **skipped**. Starting from the membership table means an
        unresolvable address and a non-member produce the same correct answer,
        and no third, ambiguous case can silently produce the wrong one.

        Two ways that resolution fails while the ``Membership`` row is intact:

        * ``get_user_by_email`` filters ``users.deleted_at IS NULL``. That is
          right for **login** and is not this question, so it is not borrowed
          here.
        The address is read from ``user_pii`` only. ``users.email`` stopped being
        written in N+1, so a legacy-column match would answer a column the product
        no longer treats as the account address.

        ⚠️ **Do not make this fail closed.** Refusing addresses we cannot resolve
        refuses every invitation to a new user, which is the feature. The failure
        direction was never the defect.
        ``test_refuses_a_member_whose_address_lives_only_in_the_sidecar`` is the
        witness that this join, and not ``users.email``, answers the guard.
        """
        return (
            session.execute(
                select(Membership.id)
                .join(User, User.id == Membership.user_id)
                .join(UserPII, UserPII.user_id == User.id)
                .where(
                    Membership.org_id == org_id,
                    Membership.deleted_at.is_(None),
                    func.lower(UserPII.email) == email,
                )
                .limit(1)
            ).first()
            is not None
        )

    def create_invitation(
        self,
        session: Session,
        org_id: int,
        email: str,
        role: MemberRole,
        invited_by_user_id: int,
        expires_days: int = 7,
    ) -> Invitation:
        """Create a pending invitation.  Raises ValueError on duplicates.

        ⚠️ Authorized through the **same** seam as `UserService.add_member`
        (`SPEC_ORG_ROLES.md` §2), not a second copy of the rules. This path
        used to compare the granted role to nothing at all, and the invite-role
        select offered `owner` — which is core#658's shorter escalation, the
        one where the attacker's own role never changes.
        """
        from datanika.services.user_service import UserService

        user_svc = UserService(self._auth)
        user_svc._assert_may_manage(session, org_id, invited_by_user_id, granted_role=role)

        email = email.strip().lower()

        # Check if already a member (core#1010).
        if self._has_active_membership(session, org_id, email):
            raise UserFacingError(f"{email} is already a member of this organization")

        # Check for existing pending invitation
        existing_inv = session.execute(
            select(Invitation).where(
                Invitation.org_id == org_id,
                Invitation.email == email,
                Invitation.status == InvitationStatus.PENDING,
            )
        ).scalar_one_or_none()
        if existing_inv:
            raise UserFacingError(f"There is already a pending invitation for {email}")

        token = self._auth.create_email_verification_token(
            user_id=invited_by_user_id, email=email, expires_hours=expires_days * 24
        )

        invitation = Invitation(
            org_id=org_id,
            email=email,
            role=role,
            invited_by_user_id=invited_by_user_id,
            # `email` stays until N+2 drops the column. The plaintext token does not:
            # the mailed value is an unmapped attribute so assigning it cannot flush
            # back into `invitations.token`.
            token_hash=hash_invitation_token(token),
            status=InvitationStatus.PENDING,
            expires_at=datetime.now(UTC) + timedelta(days=expires_days),
        )
        invitation.plaintext_token = token
        session.add(invitation)
        session.flush()
        session.add(InvitationPII(invitation_id=invitation.id, email=email))
        session.flush()
        return invitation

    def accept_invitation(
        self, session: Session, token: str, *, user_id: int | None = None
    ) -> Membership | None:
        """Accept an invitation by token.  Returns the new Membership or None.

        ``user_id`` is for a caller acting for ONE particular account — today, a signup, by email
        or by social login (core#624). Without it the account is resolved from the
        **invitation's** address, so a token issued to an address that already has an account
        would join *that* account: its invitation consumed by someone else's signup, and the
        account actually signing up left with no organization at all. A mismatch returns ``None``
        before anything is written, so the invitation stays with the person it was sent to.
        """
        invitation = self.get_invitation_by_token(session, token)

        if invitation is None:
            return None
        if invitation.status != InvitationStatus.PENDING:
            return None
        if invitation.expires_at < datetime.now(UTC):
            invitation.status = InvitationStatus.EXPIRED
            session.flush()
            return None

        # An invitation is a role grant that outlives the check that made it.
        # SPEC_ORG_ROLES R1 puts `owner` off every grant path, so a stored
        # `owner` invitation — created before this landed, or by any future
        # writer of the Invitation table — must not become an owner membership
        # on accept. Refused rather than silently downgraded: an owner can
        # re-issue it, and quietly handing someone a different role than the
        # one they were offered is its own surprise.
        if invitation.role is MemberRole.OWNER:
            logger.warning(
                "Refusing invitation %s: ownership is not grantable by invitation",
                invitation.id,
            )
            return None

        # The invitee's address is the sidecar. `invitations.email` stays populated
        # until N+2 and is not a fallback here.
        from datanika.services.user_service import UserService as _UserService

        pii = session.get(InvitationPII, invitation.id)
        invited_email = pii.email if pii is not None else None
        if not invited_email:
            return None
        user = _UserService(self._auth).get_user_by_email(session, invited_email)
        if user is None:
            return None  # User must register first
        if user_id is not None and user.id != user_id:
            return None

        # Create membership
        membership = Membership(
            user_id=user.id,
            org_id=invitation.org_id,
            role=invitation.role,
        )
        session.add(membership)
        invitation.status = InvitationStatus.ACCEPTED
        session.flush()
        return membership

    def cancel_invitation(self, session: Session, org_id: int, invitation_id: int) -> bool:
        """Cancel a pending invitation.  Returns True if cancelled."""
        invitation = session.execute(
            select(Invitation).where(
                Invitation.id == invitation_id,
                Invitation.org_id == org_id,
                Invitation.status == InvitationStatus.PENDING,
            )
        ).scalar_one_or_none()
        if invitation is None:
            return False
        invitation.status = InvitationStatus.CANCELLED
        session.flush()
        return True

    def list_pending_invitations(self, session: Session, org_id: int) -> list[Invitation]:
        """List all pending invitations for an org."""
        return list(
            session.execute(
                select(Invitation).where(
                    Invitation.org_id == org_id,
                    Invitation.status == InvitationStatus.PENDING,
                )
            )
            .scalars()
            .all()
        )

    def get_invitation_by_token(self, session: Session, token: str) -> Invitation | None:
        """Look up an invitation by the value in the emailed link.

        Matched on the **hash** (D3). The plaintext column is not a lookup key.
        """
        return session.execute(
            select(Invitation).where(Invitation.token_hash == hash_invitation_token(token))
        ).scalar_one_or_none()
