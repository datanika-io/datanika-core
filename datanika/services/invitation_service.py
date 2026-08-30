"""InvitationService — create, accept, cancel, and list org invitations."""

import logging
from datetime import UTC, datetime, timedelta

from sqlalchemy import select
from sqlalchemy.orm import Session

from datanika.models.invitation import Invitation, InvitationStatus
from datanika.models.user import MemberRole, Membership, User
from datanika.services.auth import AuthService

logger = logging.getLogger(__name__)


class InvitationService:
    def __init__(self, auth: AuthService):
        self._auth = auth

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

        UserService(self._auth)._assert_may_manage(
            session, org_id, invited_by_user_id, granted_role=role
        )

        email = email.strip().lower()

        # Check if already a member
        existing_user = session.execute(
            select(User).where(User.email == email)
        ).scalar_one_or_none()
        if existing_user:
            existing_membership = session.execute(
                select(Membership).where(
                    Membership.user_id == existing_user.id,
                    Membership.org_id == org_id,
                    Membership.deleted_at.is_(None),
                )
            ).scalar_one_or_none()
            if existing_membership:
                raise ValueError(f"{email} is already a member of this organization")

        # Check for existing pending invitation
        existing_inv = session.execute(
            select(Invitation).where(
                Invitation.org_id == org_id,
                Invitation.email == email,
                Invitation.status == InvitationStatus.PENDING,
            )
        ).scalar_one_or_none()
        if existing_inv:
            raise ValueError(f"There is already a pending invitation for {email}")

        token = self._auth.create_email_verification_token(
            user_id=invited_by_user_id, email=email, expires_hours=expires_days * 24
        )

        invitation = Invitation(
            org_id=org_id,
            email=email,
            role=role,
            invited_by_user_id=invited_by_user_id,
            token=token,
            status=InvitationStatus.PENDING,
            expires_at=datetime.now(UTC) + timedelta(days=expires_days),
        )
        session.add(invitation)
        session.flush()
        return invitation

    def accept_invitation(self, session: Session, token: str) -> Membership | None:
        """Accept an invitation by token.  Returns the new Membership or None."""
        invitation = session.execute(
            select(Invitation).where(Invitation.token == token)
        ).scalar_one_or_none()

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

        # Find the user by email
        user = session.execute(
            select(User).where(User.email == invitation.email)
        ).scalar_one_or_none()
        if user is None:
            return None  # User must register first

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
        """Look up an invitation by its token."""
        return session.execute(
            select(Invitation).where(Invitation.token == token)
        ).scalar_one_or_none()
