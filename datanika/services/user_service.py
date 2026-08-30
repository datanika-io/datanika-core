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
        self._validate_password(password)

        email = email.strip().lower()

        existing = self.get_user_by_email(session, email)
        if existing is not None:
            raise UserServiceError("Email already exists")

        user = User(
            email=email,
            password_hash=self._auth.hash_password(password),
            full_name=full_name,
            # A human chose this password, so the account is a password account
            # from now on. ``find_or_create_oauth_user`` deliberately leaves the
            # column NULL, which is what makes it a reliable discriminator.
            password_changed_at=datetime.now(UTC),
        )
        session.add(user)
        session.flush()
        return user

    def authenticate(
        self,
        session: Session,
        email: str,
        password: str,
        require_email_verified: bool = False,
    ) -> dict | None:
        user = self.get_user_by_email(session, email)
        if user is None or not user.is_active:
            return None
        if require_email_verified and not user.email_verified:
            return None
        if not self._auth.verify_password(password, user.password_hash):
            return None

        # Find the user's most recent org membership (last invited org takes priority)
        stmt = (
            select(Membership)
            .where(
                Membership.user_id == user.id,
                Membership.deleted_at.is_(None),
            )
            .order_by(Membership.id.desc())
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

    # -- Password management (core#623) --

    def _validate_password(self, password: str) -> None:
        """Adapt the one validator to this service's error type."""
        try:
            self._auth.validate_password_strength(password)
        except ValueError as exc:
            raise UserServiceError(str(exc)) from exc

    @staticmethod
    def has_usable_password(user: User) -> bool:
        """Whether a human ever chose this account's password.

        ⚠️ Deliberately **not** ``user.oauth_provider is None``.
        ``find_or_create_oauth_user`` backfills ``oauth_provider`` onto a
        pre-existing password account the first time its owner signs in with a
        provider, so that inference reports False for people who *do* have a
        password — and the caller then skips current-password re-verification,
        letting anyone with a hijacked live session change it without knowing
        the old one.

        ``password_changed_at`` is a stored fact instead of an inference:
        written by ``register_user`` and by every password change, never
        written by OAuth account creation, and never cleared by provider
        linking.
        """
        return user.password_changed_at is not None

    def change_password(
        self,
        session: Session,
        user_id: int,
        new_password: str,
        *,
        current_password: str = "",
    ) -> User:
        """Set a new password. Raises ``UserServiceError`` on any refusal.

        One method covers both the "change" and the "set" cases, because the
        difference between them is a fact about the *account*, not a choice the
        caller gets to make. An account that has a password always has to prove
        knowledge of it; passing nothing is refused rather than treated as the
        set-a-password case. A UI that forgets the field therefore fails closed.
        """
        user = self.get_user(session, user_id)
        if user is None or not user.is_active:
            raise UserServiceError("User not found")

        if self.has_usable_password(user):
            if not current_password:
                raise UserServiceError("Current password is required")
            if not self._auth.verify_password(current_password, user.password_hash):
                raise UserServiceError("Current password is incorrect")

        self._validate_password(new_password)

        if self._auth.verify_password(new_password, user.password_hash):
            raise UserServiceError("Choose a password different from your current one")

        user.password_hash = self._auth.hash_password(new_password)
        user.password_changed_at = datetime.now(UTC)
        session.flush()
        return user

    def redeem_refresh_token(self, session: Session, refresh_token: str) -> dict | None:
        """Exchange a refresh token for a fresh access token, or ``None``.

        This is where the *enforceable* half of "sign out other sessions" lives
        (core#623, D4). Datanika has no durable session to invalidate — Reflex
        state is in-memory and per-process — but the refresh token is a real
        7-day bearer credential, and it already carries ``iat``. A token minted
        before the password changed is refused here, with no new claim, no new
        table, and no database read on the 15-minute access path.

        ``iat`` is whole seconds while ``password_changed_at`` has microseconds,
        so the boundary is floored to the second. Without that, the session a
        user establishes *immediately* after changing their password is revoked
        by the change that just happened.
        """
        payload = self._auth.decode_token(refresh_token, expected_type="refresh")
        if payload is None:
            return None

        user = self.get_user(session, payload.get("user_id", 0))
        if user is None or not user.is_active:
            return None

        changed_at = user.password_changed_at
        if changed_at is not None:
            if changed_at.tzinfo is None:
                changed_at = changed_at.replace(tzinfo=UTC)
            issued_at = payload.get("iat")
            if issued_at is None:
                return None
            if int(issued_at) < int(changed_at.replace(microsecond=0).timestamp()):
                return None

        stmt = (
            select(Membership)
            .where(Membership.user_id == user.id, Membership.deleted_at.is_(None))
            .order_by(Membership.id.desc())
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
        if "default_dbt_schema" in kwargs:
            org.default_dbt_schema = kwargs["default_dbt_schema"]

        session.flush()
        return org

    # -- Membership management --

    def add_member(
        self, session: Session, org_id: int, user_id: int, role: MemberRole
    ) -> Membership:
        from datanika.hooks import emit

        emit("membership.before_create", session=session, org_id=org_id)
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
        *,
        email_verified: bool = False,
    ) -> tuple[User, bool]:
        """Find existing user by OAuth identity or email, else create. -> (user, is_new).

        SECURITY (auth boundary). Two rules decide who this returns:

        1. ``(oauth_provider, oauth_provider_id)`` is the identity. It is looked
           up first and compared on *every* login, not written once at link
           time — a provider presenting a different subject for the same address
           is telling us the address changed hands.
        2. The email is only a claim. It may reach an existing account, or
           create one, **only** when the caller states the provider verified it.
           ``email_verified`` is keyword-only and defaults to ``False`` so that a
           caller which forgets it fails closed instead of trusting silently.

        Auto-linking a verified provider email onto an existing password account
        is a deliberate product decision (SPEC_SIGNUP_SOCIAL_AUTH.md), not an
        oversight — but it is only sound because of rule 2.
        """
        email = email.strip().lower()
        oauth_provider_id = (oauth_provider_id or "").strip()

        # 1. Identity first. An empty subject is not an identity and must never
        #    be used as a lookup key, or it would match every unbound row.
        if oauth_provider_id:
            stmt = select(User).where(
                User.oauth_provider == oauth_provider,
                User.oauth_provider_id == oauth_provider_id,
            )
            user = session.execute(stmt).scalars().first()
            if user is not None:
                return user, False

        # 2. Past this point the email is the only thing linking the caller to
        #    an account, so it has to be worth something.
        if not email_verified:
            raise UserServiceError(
                "Cannot sign in: the identity provider did not confirm this email address."
            )

        user = self.get_user_by_email(session, email)
        if user is not None:
            if user.oauth_provider == oauth_provider:
                stored_id = (user.oauth_provider_id or "").strip()
                if not stored_id:
                    # Linked before the subject was recorded — bind it now
                    # rather than locking a legitimate user out.
                    user.oauth_provider_id = oauth_provider_id
                    session.flush()
                elif stored_id != oauth_provider_id:
                    raise UserServiceError(
                        "Cannot sign in: this account is linked to a different "
                        "identity at this provider."
                    )
            elif not user.oauth_provider:
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
            # Safe because the gate above refused anything unverified. This
            # comment used to assert it; now the code enforces it.
            email_verified=True,
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
