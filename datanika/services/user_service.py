"""User, Organization, and Membership management service."""

import re
import secrets
from datetime import UTC, datetime

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from datanika.models.user import MemberRole, Membership, Organization, User
from datanika.services.auth import AuthService


class UserServiceError(ValueError):
    pass


class UserService:
    def __init__(self, auth_service: AuthService):
        self._auth = auth_service

    # -- User registration & auth --

    def register_user(self, session: Session, email: str, password: str, full_name: str) -> User:
        if not email or not email.strip():
            raise UserServiceError("Email is required")
        if not password:
            raise UserServiceError("Password is required")

        email = email.strip().lower()

        existing = self.get_user_by_email(session, email)
        if existing is not None:
            raise UserServiceError("Email already exists")

        user = User(
            email=email,
            password_hash=self._auth.hash_password(password),
            full_name=full_name,
        )
        session.add(user)
        session.flush()
        return user

    def authenticate(self, session: Session, email: str, password: str) -> dict | None:
        user = self.get_user_by_email(session, email)
        if user is None or not user.is_active:
            return None
        if not self._auth.verify_password(password, user.password_hash):
            return None

        # Find the user's first org membership
        stmt = (
            select(Membership)
            .where(
                Membership.user_id == user.id,
                Membership.deleted_at.is_(None),
            )
            .order_by(Membership.id)
            .limit(1)
        )
        membership = session.execute(stmt).scalar_one_or_none()
        if membership is None:
            return None

        return {
            "user": user,
            "access_token": self._auth.create_access_token(user.id, membership.org_id),
            "refresh_token": self._auth.create_refresh_token(user.id),
        }

    def authenticate_for_org(
        self, session: Session, email: str, password: str, org_id: int
    ) -> dict | None:
        user = self.get_user_by_email(session, email)
        if user is None or not user.is_active:
            return None
        if not self._auth.verify_password(password, user.password_hash):
            return None

        membership = self.get_membership(session, org_id, user.id)
        if membership is None:
            return None

        return {
            "user": user,
            "access_token": self._auth.create_access_token(user.id, org_id),
            "refresh_token": self._auth.create_refresh_token(user.id),
        }

    def get_user_by_email(self, session: Session, email: str) -> User | None:
        email = email.strip().lower()
        stmt = select(User).where(func.lower(User.email) == email)
        return session.execute(stmt).scalar_one_or_none()

    def get_user(self, session: Session, user_id: int) -> User | None:
        stmt = select(User).where(User.id == user_id)
        return session.execute(stmt).scalar_one_or_none()

    # -- Org management --

    def create_org(
        self, session: Session, name: str, slug: str, owner_user_id: int
    ) -> Organization:
        if not name or not name.strip():
            raise UserServiceError("Name is required")
        if not slug or not slug.strip():
            raise UserServiceError("Slug is required")

        # Verify user exists
        user = self.get_user(session, owner_user_id)
        if user is None:
            raise UserServiceError("User not found")

        # Check slug uniqueness
        existing = session.execute(
            select(Organization).where(Organization.slug == slug)
        ).scalar_one_or_none()
        if existing is not None:
            raise UserServiceError("Slug already exists")

        org = Organization(name=name, slug=slug)
        session.add(org)
        session.flush()

        # Create owner membership
        membership = Membership(
            user_id=owner_user_id,
            org_id=org.id,
            role=MemberRole.OWNER,
        )
        session.add(membership)
        session.flush()

        return org

    def get_user_orgs(self, session: Session, user_id: int) -> list[Organization]:
        stmt = (
            select(Organization)
            .join(Membership, Membership.org_id == Organization.id)
            .where(
                Membership.user_id == user_id,
                Membership.deleted_at.is_(None),
            )
            .order_by(Organization.id)
        )
        return list(session.execute(stmt).scalars().all())

    def update_org(
        self, session: Session, org_id: int, user_id: int | None = None, **kwargs
    ) -> Organization | None:
        stmt = select(Organization).where(Organization.id == org_id)
        org = session.execute(stmt).scalar_one_or_none()
        if org is None:
            return None

        # If modifying fields, verify user has admin/owner role
        if kwargs and user_id is not None:
            membership = self.get_membership(session, org_id, user_id)
            if membership is None or membership.role not in (
                MemberRole.OWNER,
                MemberRole.ADMIN,
            ):
                raise UserServiceError("Only admins and owners can update the organization")

        if "name" in kwargs:
            org.name = kwargs["name"]
        if "slug" in kwargs:
            org.slug = kwargs["slug"]

        session.flush()
        return org

    # -- Membership management --

    def add_member(
        self, session: Session, org_id: int, user_id: int, role: MemberRole
    ) -> Membership:
        # Verify org exists
        org = session.execute(
            select(Organization).where(Organization.id == org_id)
        ).scalar_one_or_none()
        if org is None:
            raise UserServiceError("Organization not found")

        # Verify user exists
        user = self.get_user(session, user_id)
        if user is None:
            raise UserServiceError("User not found")

        # Check for duplicate active membership
        existing = self.get_membership(session, org_id, user_id)
        if existing is not None:
            raise UserServiceError("Already a member of this organization")

        membership = Membership(user_id=user_id, org_id=org_id, role=role)
        session.add(membership)
        session.flush()
        return membership

    def remove_member(self, session: Session, org_id: int, membership_id: int) -> bool:
        stmt = select(Membership).where(
            Membership.id == membership_id,
            Membership.org_id == org_id,
            Membership.deleted_at.is_(None),
        )
        membership = session.execute(stmt).scalar_one_or_none()
        if membership is None:
            return False

        # Prevent removing last owner
        if membership.role == MemberRole.OWNER:
            self._check_last_owner(session, org_id)

        membership.deleted_at = datetime.now(UTC)
        session.flush()
        return True

    def list_members(self, session: Session, org_id: int) -> list[Membership]:
        stmt = (
            select(Membership)
            .where(
                Membership.org_id == org_id,
                Membership.deleted_at.is_(None),
            )
            .order_by(Membership.id)
        )
        return list(session.execute(stmt).scalars().all())

    def change_role(
        self, session: Session, org_id: int, membership_id: int, new_role: MemberRole
    ) -> Membership | None:
        stmt = select(Membership).where(
            Membership.id == membership_id,
            Membership.org_id == org_id,
            Membership.deleted_at.is_(None),
        )
        membership = session.execute(stmt).scalar_one_or_none()
        if membership is None:
            return None

        # Prevent demoting last owner
        if membership.role == MemberRole.OWNER and new_role != MemberRole.OWNER:
            self._check_last_owner(session, org_id)

        membership.role = new_role
        session.flush()
        return membership

    def get_membership(self, session: Session, org_id: int, user_id: int) -> Membership | None:
        stmt = select(Membership).where(
            Membership.org_id == org_id,
            Membership.user_id == user_id,
            Membership.deleted_at.is_(None),
        )
        return session.execute(stmt).scalar_one_or_none()

    # -- OAuth --

    def find_or_create_oauth_user(
        self,
        session: Session,
        email: str,
        full_name: str,
        oauth_provider: str,
        oauth_provider_id: str,
    ) -> tuple[User, bool]:
        """Find existing user by email or create for OAuth. Returns (user, is_new)."""
        email = email.strip().lower()
        user = self.get_user_by_email(session, email)
        if user is not None:
            # Update OAuth fields if not set
            if not user.oauth_provider:
                user.oauth_provider = oauth_provider
                user.oauth_provider_id = oauth_provider_id
                session.flush()
            return user, False

        # Create new user with random password (OAuth users don't need one)
        random_hash = self._auth.hash_password(secrets.token_urlsafe(32))
        user = User(
            email=email,
            password_hash=random_hash,
            full_name=full_name or email.split("@")[0],
            oauth_provider=oauth_provider,
            oauth_provider_id=oauth_provider_id,
        )
        session.add(user)
        session.flush()

        # Create default org
        slug = re.sub(r"[^a-z0-9]+", "-", full_name.lower()).strip("-") or "org"
        org_name = f"{full_name}'s Org"
        org = Organization(name=org_name, slug=f"{slug}-{user.id}")
        session.add(org)
        session.flush()

        membership = Membership(user_id=user.id, org_id=org.id, role=MemberRole.OWNER)
        session.add(membership)
        session.flush()

        return user, True

    # -- Helpers --

    def _check_last_owner(self, session: Session, org_id: int) -> None:
        """Raise if there is only one active owner in the org."""
        count = session.execute(
            select(func.count())
            .select_from(Membership)
            .where(
                Membership.org_id == org_id,
                Membership.role == MemberRole.OWNER,
                Membership.deleted_at.is_(None),
            )
        ).scalar()
        if count <= 1:
            raise UserServiceError("Cannot remove or demote the last owner")
