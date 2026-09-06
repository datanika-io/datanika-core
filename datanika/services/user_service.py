"""User, Organization, and Membership management service."""

import hashlib
import logging
import re
import secrets
import shutil
from datetime import UTC, datetime
from pathlib import Path

from sqlalchemy import delete, false, func, or_, select, update
from sqlalchemy.orm import Session

from datanika.config import settings
from datanika.errors import UserFacingError
from datanika.models.audit_log import AuditAction, AuditLog
from datanika.models.base import Base
from datanika.models.invitation import Invitation, InvitationStatus
from datanika.models.notification_channel import NotificationChannel
from datanika.models.pii import InvitationPII, NotificationChannelPII, UserPII
from datanika.models.user import MemberRole, Membership, Organization, User
from datanika.services.audit_service import redact_pii_payload
from datanika.services.auth import AuthService

logger = logging.getLogger(__name__)


class UserServiceError(UserFacingError):
    pass


class UserService:
    def __init__(self, auth_service: AuthService):
        self._auth = auth_service

    # -- Rate-limit bucket naming (core#639) ---------------------------

    @staticmethod
    def signup_ip_bucket(client_ip: str) -> str:
        """Bucket for one client address attempting to register.

        Callers must pass an address they can stand behind — see
        ``services/client_ip.py``. An empty address means *skip this bucket*,
        never *use a placeholder*: in production every socket peer is
        127.0.0.1, so a placeholder collapses the internet into one bucket and
        the eleventh signup from anyone locks out everyone.
        """
        return f"signup:ip:{client_ip}"

    @staticmethod
    def signup_email_bucket(email: str) -> str:
        """Bucket for one address being registered, with the address hashed.

        Same reasoning as ``PasswordResetService.email_bucket``: keying on the
        plaintext would turn the Redis keyspace into a readable list of the
        addresses people have tried to register — an enumeration oracle
        reachable by anyone who can read Redis, which is a much lower bar than
        reading the database. Normalised first, or ``  Alice@Example.com  `` is
        a fresh budget for the same account.
        """
        normalised = (email or "").strip().lower()
        return f"signup:email:{hashlib.sha256(normalised.encode()).hexdigest()}"

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
        # Dual-write, release N. The legacy columns above are still written because the
        # previously deployed code reads them, and they are dropped in N+2.
        session.add(UserPII(user_id=user.id, email=email, full_name=full_name))
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
        """The one chokepoint for looking a person up by address.

        Two things about this query are load-bearing (SPEC_PII_SEPARATION D2).

        🚨 **``users.deleted_at IS NULL``.** Without it a soft-deleted user still
        authenticates, which turns the whole erasure feature into a security regression.
        There is a compensating property worth knowing: an *erased* user has no
        ``user_pii`` row at all, so the join half returns nothing and login becomes
        **structurally** impossible rather than policy-impossible. Belt and braces —
        erasure also sets ``is_active = False``.

        ⚠️ **The legacy ``users.email`` clause is not a fallback for old data, it is the
        t1 window.** Release N dual-writes; under blue/green the *previously deployed*
        container is still serving and still registering people while this code runs, and
        it writes ``users.email`` with no ``user_pii`` row. A join-only read would leave
        every account created during the swap unable to sign in. The clause is deleted in
        **N+1**, when nothing writes the legacy column any more.

        ``scalar_one_or_none`` is kept deliberately over ``.first()``: two rows here would
        mean the two columns had diverged for different people, and an arbitrary pick on a
        login lookup is a worse outcome than a loud failure.
        """
        email = email.strip().lower()
        stmt = (
            select(User)
            .outerjoin(UserPII, UserPII.user_id == User.id)
            .where(
                or_(
                    func.lower(UserPII.email) == email,
                    func.lower(User.email) == email,  # legacy half — removed in N+1
                ),
                User.deleted_at.is_(None),
            )
        )
        return session.execute(stmt).scalar_one_or_none()

    @staticmethod
    def get_user_pii(session: Session, user_id: int) -> UserPII | None:
        """The personal data of a user, or ``None`` once it has been erased."""
        return session.get(UserPII, user_id)

    @staticmethod
    def _sync_user_pii(session: Session, user: User) -> UserPII:
        """Mirror the legacy columns onto the sidecar during the dual-write window.

        Creates the row when it is missing rather than assuming it exists: at t1 the
        previously deployed code is still registering people, and those accounts arrive
        with legacy columns and no sidecar. Deleted in **N+1**, when the legacy columns
        stop being written and this becomes a one-way copy of nothing.
        """
        pii = session.get(UserPII, user.id)
        if pii is None:
            pii = UserPII(user_id=user.id, email=user.email, full_name=user.full_name)
            session.add(pii)
        if user.email is not None:
            pii.email = user.email
        if user.full_name is not None:
            pii.full_name = user.full_name
        pii.oauth_provider_id = user.oauth_provider_id
        session.flush()
        return pii

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

    def redeem_refresh_token(
        self, session: Session, refresh_token: str, org_id: int | None = None
    ) -> dict | None:
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

        # ``org_id`` is the org the caller is *currently in*. Without it this
        # falls back to the newest membership, which for a user in more than one
        # org silently moves the session somewhere else on renewal — a bug you
        # only see if your newest membership is not the org you work in (#671).
        # It is a filter, never a grant: a membership that does not exist, or is
        # soft-deleted, falls through to the same fallback rather than being
        # honoured.
        base = select(Membership).where(
            Membership.user_id == user.id, Membership.deleted_at.is_(None)
        )
        membership = None
        if org_id:
            membership = session.execute(
                base.where(Membership.org_id == org_id)
            ).scalar_one_or_none()
        if membership is None:
            membership = session.execute(
                base.order_by(Membership.id.desc()).limit(1)
            ).scalar_one_or_none()
        if membership is None:
            return None

        return {
            "user": user,
            "org_id": membership.org_id,
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

        # No kwargs is a read — `SettingsState.load_settings` calls it that way
        # purely to fetch the org — so it needs no actor.
        #
        # Mutation is **owner-only** (SPEC_ORG_ROLES §4, last row), and it
        # requires an actor. Both halves changed:
        #
        # * the UI gated this at `owner` while the service accepted owner *or*
        #   admin, so "the only owner-exclusive power is renaming the org" was
        #   true through the UI and was not a property of the system;
        # * `user_id=None` skipped the check **entirely**, which is core#658's
        #   unauthenticated shape on a different method.
        #
        # An org's identity is not an admin's to change: `organizations.slug`
        # is unique and appears in SSO URLs.
        if kwargs:
            if user_id is None:
                raise UserServiceError(
                    "Updating the organization requires the acting user's identity"
                )
            membership = self.get_membership(session, org_id, user_id)
            if membership is None or membership.role is not MemberRole.OWNER:
                raise UserServiceError("Only the owner can update the organization")

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
        self,
        session: Session,
        org_id: int,
        user_id: int,
        role: MemberRole,
        *,
        actor_user_id: int | None = None,
    ) -> Membership:
        """Add an existing user to an org, as `actor_user_id`.

        ⚠️ `actor_user_id` is keyword-only and fails closed when omitted. This
        is the **shorter** of core#658's two escalation paths and the one its
        body does not describe: an admin never needs to touch their own row,
        they invite an address they already control **directly as owner**, and
        a second owner exists with no interaction from the real owner — which
        also arms the removal step, because the last-owner guard permits 2 → 1.
        A fix that only watched for self-promotion would leave this open.
        """
        from datanika.hooks import emit

        self._assert_may_manage(session, org_id, actor_user_id, granted_role=role)

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

    def remove_member(
        self,
        session: Session,
        org_id: int,
        membership_id: int,
        *,
        actor_user_id: int | None = None,
    ) -> bool:
        """Remove a member, as `actor_user_id`.

        core#658 notes that this shares the UI gate with `change_role`, so
        closing only the promotion still lets an admin evict an owner whenever
        a second owner exists — the last-owner guard permits 2 → 1 and was
        never an authorization check.

        Removing **yourself** is always permitted (SPEC_ORG_ROLES R6) and is
        what :meth:`leave_org` calls; it is still subject to the last-owner
        guard, so a sole owner must transfer or delete the org first.
        """
        stmt = select(Membership).where(
            Membership.id == membership_id,
            Membership.org_id == org_id,
            Membership.deleted_at.is_(None),
        )
        membership = session.execute(stmt).scalar_one_or_none()
        if membership is None:
            # Authorize before reporting absence, so a caller with no business
            # here cannot use this as a membership-id oracle.
            self._assert_may_manage(session, org_id, actor_user_id)
            return False

        if membership.user_id != actor_user_id:
            self._assert_may_manage(session, org_id, actor_user_id, target_role=membership.role)
        else:
            self._actor_membership(session, org_id, actor_user_id)

        # Prevent removing last owner
        if membership.role == MemberRole.OWNER:
            self._check_last_owner(session, org_id)

        membership.deleted_at = datetime.now(UTC)
        session.flush()
        return True

    def leave_org(self, session: Session, org_id: int, *, actor_user_id: int) -> bool:
        """Remove your own membership (SPEC_ORG_ROLES R6, audit P8).

        Any member may leave. The last owner cannot, and that is the point —
        their exits are transfer-then-leave, or delete the org. Before this
        existed a viewer or editor was in an org until somebody else removed
        them; there was no `leave_*` handler anywhere in the codebase.
        """
        membership = self.get_membership(session, org_id, actor_user_id)
        if membership is None:
            raise UserServiceError("You are not a member of this organization")
        return self.remove_member(session, org_id, membership.id, actor_user_id=actor_user_id)

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
        self,
        session: Session,
        org_id: int,
        membership_id: int,
        new_role: MemberRole,
        *,
        actor_user_id: int | None = None,
    ) -> Membership | None:
        """Change a member's role, as `actor_user_id`.

        ⚠️ **`new_role=OWNER` is refused from every caller, including an
        owner** (SPEC_ORG_ROLES R1). Use :meth:`transfer_ownership`.

        The only guard here used to fire on *demotion*
        (`role == OWNER and new_role != OWNER`), and a promotion is not a
        demotion — so an admin setting themselves to owner passed through
        untouched. Raising the UI gate alone would have closed the one
        reachable caller and left a privilege-granting method with no
        authorization of its own.
        """
        stmt = select(Membership).where(
            Membership.id == membership_id,
            Membership.org_id == org_id,
            Membership.deleted_at.is_(None),
        )
        membership = session.execute(stmt).scalar_one_or_none()
        if membership is None:
            self._assert_may_manage(session, org_id, actor_user_id, granted_role=new_role)
            return None

        self._assert_may_manage(
            session,
            org_id,
            actor_user_id,
            target_role=membership.role,
            granted_role=new_role,
        )

        # Prevent demoting last owner
        if membership.role == MemberRole.OWNER and new_role != MemberRole.OWNER:
            self._check_last_owner(session, org_id)

        membership.role = new_role
        session.flush()
        return membership

    def transfer_ownership(
        self,
        session: Session,
        org_id: int,
        successor_user_id: int,
        *,
        actor_user_id: int,
    ) -> Membership:
        """Hand ownership to an existing member. Owner-only.

        `SPEC_ORG_ROLES.md` §3. Successor becomes `owner`, the actor becomes
        `admin`, atomically — **the owner count is preserved throughout**, so
        the last-owner invariant is never transiently violated and this needs
        no exemption from it.

        This is the *only* way to reach `MemberRole.OWNER` outside the two
        account-creation paths, which is what makes R1 safe to state absolutely:
        a future bug in the role-change predicate cannot reach ownership,
        because ownership is not on that control any more.

        The successor must be an **existing active member** — inviting a
        stranger straight into ownership is exactly the escalation path this
        closes. No acceptance handshake, decided in §3 with a reason that will
        expire: it needs a reliable email round trip, and [core#652] establishes
        that notification channels have never dispatched.
        """
        actor = self._actor_membership(session, org_id, actor_user_id)
        if actor.role is not MemberRole.OWNER:
            raise UserServiceError("Only an owner can transfer ownership")

        if successor_user_id == actor_user_id:
            raise UserServiceError("You are already the owner")

        successor = self.get_membership(session, org_id, successor_user_id)
        if successor is None:
            raise UserServiceError("The new owner must already be a member of this organization")

        successor.role = MemberRole.OWNER
        actor.role = MemberRole.ADMIN
        session.flush()
        return successor

    def add_owner(
        self, session: Session, org_id: int, user_id: int, *, actor_user_id: int
    ) -> Membership:
        """Promote an existing member to a second owner. Owner-only.

        `SPEC_ORG_ROLES.md` §3: multi-owner stays possible — bus-factor is a
        real need — it is simply not reachable from the role dropdown. The
        defect was never that two owners can exist, it was that an *admin*
        could become the second one.
        """
        actor = self._actor_membership(session, org_id, actor_user_id)
        if actor.role is not MemberRole.OWNER:
            raise UserServiceError("Only an owner can add another owner")

        target = self.get_membership(session, org_id, user_id)
        if target is None:
            raise UserServiceError("The new owner must already be a member of this organization")
        target.role = MemberRole.OWNER
        session.flush()
        return target

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

        3. Rule 2 is only half of the decision. It establishes that *the
           provider* proved the address; it says nothing about whether **the
           account being linked to** ever proved it. Both sides have to hold, or
           an address typed at signup by someone who never owned it becomes a
           trap for the person who does. See
           ``_assert_local_account_proved_its_email``.

        Auto-linking a verified provider email onto an existing password account
        is a deliberate product decision (SPEC_SIGNUP_SOCIAL_AUTH.md), not an
        oversight — but it is only sound because of rules 2 **and** 3.
        """
        email = email.strip().lower()
        oauth_provider_id = (oauth_provider_id or "").strip()

        # 1. Identity first. An empty subject is not an identity and must never
        #    be used as a lookup key, or it would match every unbound row.
        if oauth_provider_id:
            # `oauth_provider` stays on `users` — "google" is not personal data. Only the
            # subject moves, and for SAML/OIDC SSO that subject IS the address verbatim
            # (`sso_routes.py` passes `oauth_provider_id=email`), which is why. Dual-read
            # for the t1 window, exactly as in `get_user_by_email`; the legacy half goes
            # in N+1.
            stmt = (
                select(User)
                .outerjoin(UserPII, UserPII.user_id == User.id)
                .where(
                    User.oauth_provider == oauth_provider,
                    or_(
                        UserPII.oauth_provider_id == oauth_provider_id,
                        User.oauth_provider_id == oauth_provider_id,  # legacy — gone in N+1
                    ),
                    User.deleted_at.is_(None),
                )
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
                    self._sync_user_pii(session, user)
                    session.flush()
                elif stored_id != oauth_provider_id:
                    raise UserServiceError(
                        "Cannot sign in: this account is linked to a different "
                        "identity at this provider."
                    )
            elif not user.oauth_provider:
                self._assert_local_account_proved_its_email(user)
                user.oauth_provider = oauth_provider
                user.oauth_provider_id = oauth_provider_id
                self._sync_user_pii(session, user)
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
        session.add(
            UserPII(
                user_id=user.id,
                email=email,
                full_name=user.full_name,
                oauth_provider_id=oauth_provider_id or None,
            )
        )
        session.flush()

        # Create default org.
        #
        # 🚨 The slug is no longer derived from the person's name (D4). A slug is an
        # *identifier*: unique-constrained, in URLs, and matched by the SSO callback
        # (`sso_routes.py` compares `Organization.slug == org_slug`), so a name-derived
        # slug publishes a person's name in a durable key. §2c measured this in
        # production — `organizations.slug` contained a live `users.full_name` in **5 of
        # 5** rows. The display `name` may stay: it is text inside the tenant, and the
        # erasure sweep rewrites it (D5 step 7).
        org_name = f"{full_name}'s Org"
        org = Organization(name=org_name, slug=f"org-{user.id}")
        session.add(org)
        session.flush()

        membership = Membership(user_id=user.id, org_id=org.id, role=MemberRole.OWNER)
        session.add(membership)
        session.flush()

        return user, True

    @staticmethod
    def _assert_local_account_proved_its_email(user: User) -> None:
        """SECURITY (auth boundary). The other side of rule 2.

        Binding a provider identity to a pre-existing row hands whoever holds
        that row's password everything the arriving user does from then on, and
        hands the arriving user an account they did not create. That is only
        acceptable when the row is entitled to the address.

        Two ways it can be:

        * ``email_verified`` — someone followed the link we mailed to that
          address, so the row has proved it. Set by ``/api/verify-email``, by
          the create branch above, and by completing a password reset (which is
          the same proof by a different route, and is the recovery path for
          every account that predates verification being wired up).
        * ``password_changed_at IS NULL`` — no human ever chose a password for
          this row, so there is no password login to hand over. Linking can only
          grant access to someone who has already proved the address.

        Anything else is an unproven claim on both sides of the join, and the
        refusal has to name a way back or it is a lockout: the password still
        works, and the reset flow both proves the address and clears this.
        """
        if user.email_verified or user.password_changed_at is None:
            return
        raise UserServiceError(
            "An account already exists for this email address and has not been "
            "confirmed, so it cannot be linked to a social login yet. Sign in "
            "with your password, or use 'Forgot password' to confirm the "
            "address first."
        )

    # -- Member-management authorization (SPEC_ORG_ROLES.md §2, §4) --

    def _actor_membership(self, session: Session, org_id: int, actor_user_id: int | None):
        """Resolve the caller's own membership, or refuse.

        ⚠️ **Fails closed on a missing actor, deliberately.** Before [core#658]
        these mutators took no caller identity at all, so "an admin may not
        grant owner" was not even expressible — there was nothing to compare
        against. The single gate was `_check_role("admin")` in the Reflex state
        layer, which the attacker passes by construction.

        Defaulting `actor_user_id` to `None` and *allowing* it would leave the
        same hole open for the next caller (an API route, the MCP surface, an
        invitation flow) while looking fixed. This mirrors the convention
        `find_or_create_oauth_user` already sets for `email_verified`: a caller
        that forgets it is refused rather than trusted.
        """
        if actor_user_id is None:
            raise UserServiceError("Member management requires the acting user's identity")
        actor = self.get_membership(session, org_id, actor_user_id)
        if actor is None:
            raise UserServiceError("You are not a member of this organization")
        return actor

    def _assert_may_manage(
        self,
        session: Session,
        org_id: int,
        actor_user_id: int | None,
        *,
        target_role: MemberRole | None = None,
        granted_role: MemberRole | None = None,
    ):
        """Gate one member-management operation. Returns the actor's membership.

        Implements SPEC_ORG_ROLES R1-R4, derived from `ROLE_PERMISSIONS` rather
        than restating it:

        * **R1** — `owner` is never a grantable role, from any caller including
          an owner. Ownership moves only through :meth:`transfer_ownership`.
          This is the root-cause fix: promoting to owner and re-roling a viewer
          used to be the same call, so any guard on it was one predicate away
          from being wrong.
        * **R2** — you may not grant a role at or above your own. An admin
          cannot mint another admin.
        * **R3** — you may only act on members strictly below you. An admin
          cannot touch another admin, and cannot touch an owner.
        * **R4** — one exception: an owner may act on another owner, subject to
          the last-owner guard. Two co-founders separating must not need us.
        """
        from datanika.services.auth import may_grant_role, may_manage_member

        actor = self._actor_membership(session, org_id, actor_user_id)
        actor_role = actor.role.value

        # The coarse gate goes through `AuthService.has_permission`, not
        # through `ROLE_PERMISSIONS` directly. That is deliberate: until now
        # `has_permission` had zero production callers, so
        # `test_auth_security.py`'s green assertions described a model nothing
        # consulted. This is the call that makes them true.
        if not self._auth.has_permission(actor_role, "manage_members_below"):
            raise UserServiceError("Only owners and admins can manage members")

        if granted_role is not None and not may_grant_role(actor_role, granted_role.value):
            if granted_role is MemberRole.OWNER:
                raise UserServiceError("Ownership cannot be granted here. Use Transfer ownership.")
            raise UserServiceError(f"An {actor_role} cannot grant the {granted_role.value} role")

        if target_role is not None and not may_manage_member(actor_role, target_role.value):
            raise UserServiceError(
                f"An {actor_role} cannot manage a member with the {target_role.value} role"
            )

        return actor

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

    # ------------------------------------------------------------------
    # Erasure and org deletion — SPEC_PII_SEPARATION D5/D6, core#655
    # ------------------------------------------------------------------

    @staticmethod
    def org_scoped_core_tables() -> list[str]:
        """Tables an org deletion must soft-delete. Derived, not listed.

        ⚠️ **Soft-deleting only the `organizations` row hides nothing.**
        `Organization.deleted_at` is read in exactly one place in the codebase
        (`sso_service.py:81`) and written nowhere, so org-scoped queries do not filter on
        it. Every row has to be marked individually.

        🚨 **Filtered to `datanika.models.*`, and that line is load-bearing.**
        `subscriptions`, `usage_ledger` and `charges` are org-scoped and carry a
        `deleted_at`, so a naive walk of `Base.metadata` would soft-delete them whenever
        the cloud plugin happens to be installed. `datanika.io/privacy` §6 promises
        billing records are kept **7 years, as tax law requires**, and the spec's scope
        table puts them explicitly out of erasure's reach. Selecting by the *defining
        module* excludes them without core naming, importing or knowing about cloud.

        Pinned by `test_pii_separation.py`: a derivation that silently returned fewer
        tables would leave rows visible while every count read zero.
        """
        names: list[str] = []
        for mapper in Base.registry.mappers:
            if not mapper.class_.__module__.startswith("datanika.models"):
                continue
            table = mapper.local_table
            if table is not None and "org_id" in table.c and "deleted_at" in table.c:
                names.append(table.name)
        return sorted(set(names))

    def delete_org(
        self, session: Session, org_id: int, *, projects_dir: str | None = None
    ) -> dict[str, int]:
        """Soft-delete an organization and every row that belongs to it (D6).

        Three things live outside this transaction and are handled explicitly, because
        without them an org deletion is a deletion in name only:

        1. **The Paddle subscription.** Emitted as `org.before_delete`, and `emit` rather
           than `announce` precisely because a subscriber must be able to **veto**: cloud
           cancels the subscription and raises if the cancellation call fails, and this
           method then never runs. Order matters — cancel, then delete; the reverse leaves
           a subscription with no org to attribute it to. Core cannot call
           `BillingService` directly and must not try.
        2. **`dbt_projects/tenant_{org_id}/` on disk.** Nothing soft-deletes a directory.
        3. **Warehouse schemas the org's pipelines created.** ⚠️ **We do not delete these,
           deliberately** — they are the customer's data in the customer's own account,
           under their own credentials. Silently dropping schemas in someone else's
           warehouse is a far worse failure than leaving them, and the confirmation copy
           says so in one line.
        """
        from datanika.hooks import emit

        org = session.get(Organization, org_id)
        if org is None:
            raise UserServiceError("Organization not found")

        # The veto point. Anything raised here aborts before a single row is touched.
        emit("org.before_delete", session=session, org_id=org_id)

        now = datetime.now(UTC)
        counts: dict[str, int] = {}
        for table_name in self.org_scoped_core_tables():
            table = Base.metadata.tables[table_name]
            result = session.execute(
                update(table)
                .where(table.c.org_id == org_id, table.c.deleted_at.is_(None))
                .values(deleted_at=now)
            )
            counts[table_name] = result.rowcount or 0

        counts["organizations"] = 0
        if org.deleted_at is None:
            org.deleted_at = now
            counts["organizations"] = 1
        session.flush()

        counts["dbt_project_dirs"] = _remove_tenant_dbt_project(org_id, projects_dir)
        return counts

    def _classify_memberships(
        self, session: Session, user_id: int
    ) -> tuple[list[Membership], list[int], str | None]:
        """``(active memberships, orgs this person is alone in, org that blocks erasure)``.

        One classifier, two callers — `erase_user` and `erasure_preconditions`. The UI
        needs the same answer *before* anything is typed, and a second copy of the rule in
        the state layer is how the two drift into disagreeing about whether a deletion is
        even possible.
        """
        memberships = list(
            session.execute(
                select(Membership).where(
                    Membership.user_id == user_id, Membership.deleted_at.is_(None)
                )
            )
            .scalars()
            .all()
        )
        sole_member_org_ids: list[int] = []
        blocking_org: str | None = None
        for m in memberships:
            others = session.execute(
                select(func.count())
                .select_from(Membership)
                .where(
                    Membership.org_id == m.org_id,
                    Membership.user_id != user_id,
                    Membership.deleted_at.is_(None),
                )
            ).scalar()
            if not others:
                sole_member_org_ids.append(m.org_id)
                continue
            if m.role is MemberRole.OWNER and blocking_org is None:
                owners = session.execute(
                    select(func.count())
                    .select_from(Membership)
                    .where(
                        Membership.org_id == m.org_id,
                        Membership.role == MemberRole.OWNER,
                        Membership.deleted_at.is_(None),
                    )
                ).scalar()
                if owners <= 1:
                    org = session.get(Organization, m.org_id)
                    blocking_org = org.name if org else str(m.org_id)
        return memberships, sole_member_org_ids, blocking_org

    @staticmethod
    def get_org_by_id(session: Session, org_id: int) -> Organization | None:
        """An organization by primary key. Used by the delete-confirmation check, which
        compares what the user typed against the org's real name."""
        return session.get(Organization, org_id)

    def erasure_preconditions(self, session: Session, user_id: int) -> dict[str, str]:
        """What erasing this account would do, answerable **before** they confirm.

        `{"sole_member_org": <name or "">, "blocking_org": <name or "">}`.

        D9 requires the dialog to state the org consequence before the confirm button is
        enabled, and §9a(1) requires the sole-owner refusal to reach the person at the
        moment they click. Both need this answer up front, and both must get it from the
        same code path `erase_user` will take — otherwise the dialog can promise a
        deletion the service then refuses.
        """
        _, sole_ids, blocking = self._classify_memberships(session, user_id)
        sole_name = ""
        if sole_ids:
            org = session.get(Organization, sole_ids[0])
            sole_name = org.name if org else ""
        return {"sole_member_org": sole_name, "blocking_org": blocking or ""}

    def erase_user(
        self, session: Session, user_id: int, *, projects_dir: str | None = None
    ) -> dict[str, int]:
        """GDPR Art. 17 erasure: hard-delete the person, soft-delete the record (D5).

        §0's rule, stated once so it is findable: *a row that identifies a **person** is
        hard-deleted; a row that identifies a **record** is soft-deleted. Nothing personal
        is ever soft-deleted, and nothing structural is ever hard-deleted.* A soft-deleted
        row is still a row in Postgres — `deleted_at` hides it from the application and
        from nobody else: not from `pg_dump`, not from a backup, not from a regulator.

        **Synchronous, inside the caller's transaction** — not queued (D14.1). The
        published promise on `datanika.io/privacy` asserts a *mechanism*, not merely a
        number: *"The 30 days is the off-site backup retention window."* That arithmetic
        holds only if the live purge is prompt. A weekly batch makes it T+36 and a daily
        one T+31, and both break a test-locked sentence on the marketing site.

        Refuses if the user is the sole owner of a **shared** org (§9a(1)) — synchronously,
        with both exits named, because a refusal discovered a week later through no
        notification at all is worse than not offering the control.

        Returns a count per class of work. Never a value: an erasure record that names
        what it erased defeats itself.
        """
        from datanika.services.audit_service import AuditService

        user = session.get(User, user_id)
        if user is None:
            raise UserServiceError("User not found")

        memberships, sole_member_org_ids, blocking_org = self._classify_memberships(
            session, user_id
        )

        # ── §9a(1): refuse BEFORE touching anything ────────────────────────────
        if blocking_org is not None:
            raise UserServiceError(
                f"You are the only owner of {blocking_org}. "
                "Transfer ownership or delete the organization first."
            )

        pii = self.get_user_pii(session, user_id)
        erased_email = (pii.email if pii else None) or user.email
        erased_name = (pii.full_name if pii else None) or user.full_name
        counts: dict[str, int] = {}

        # ── step 7, rewritten (§0.1) ───────────────────────────────────────────
        # Rename EVERY org this person belonged to, including the ones step 6 is about to
        # delete, and rename BEFORE the soft delete. The org that existed only for the
        # erased person is precisely the one a "surviving orgs" sweep never reaches — and
        # after a soft delete an `UPDATE ... WHERE deleted_at IS NULL`, the shape every
        # org-scoped query here uses, no longer matches the row at all.
        renamed = 0
        for m in memberships:
            org = session.get(Organization, m.org_id)
            if org is None:
                continue
            if _contains_name(org.name, erased_name) or _contains_name(org.slug, erased_name):
                org.name = f"Organization {org.id}"
                org.slug = f"org-{org.id}"
                renamed += 1
        session.flush()
        counts["organizations_renamed"] = renamed

        # ── step 1: the erasure itself ─────────────────────────────────────────
        counts["user_pii"] = 0
        if pii is not None:
            session.delete(pii)
            counts["user_pii"] = 1

        # ── step 1a (§0.2) ─────────────────────────────────────────────────────
        # NULL the legacy columns this release still dual-writes. Without this, every
        # erasure until N+2 deletes the copy and leaves the original — while criterion 8,
        # `get_user_by_email`, login and every UI surface still read as erased, so nothing
        # short of a raw query over the legacy columns can see the failure. Possible only
        # because release N drops these NOT NULLs.
        # 🚨 DELETE THIS BLOCK IN N+2, when the columns go.
        user.email = None
        user.full_name = None
        user.oauth_provider_id = None

        # ── step 2: credentials, hard-deleted ──────────────────────────────────
        #
        # Not soft: an API key that still authenticates for a person who no longer exists
        # is a backdoor, and `deleted_at` is not read by the auth path.
        #
        # ⚠️ Order and shape are both constrained by foreign keys, and getting either
        # wrong fails loudly only at the second table:
        #   * `oauth_tokens` has **no `user_id`** — it reaches the person through
        #     `grant_id -> oauth_grants.user_id`, so it cannot be deleted by the same
        #     predicate as the others;
        #   * `oauth_grants.api_key_id` references `api_keys`, so the grants must go
        #     before the keys.
        counts["oauth_tokens"] = _delete_oauth_tokens_for_user(session, user_id)
        for table_name in ("oauth_grants", "api_keys", "password_reset_tokens"):
            counts[table_name] = _delete_by_user(session, table_name, user_id)
        counts["email_change_requests"] = _delete_by_user(session, "email_change_requests", user_id)

        # Every org this person has EVER belonged to, soft-deleted memberships included.
        # Scoping the two tenant-owned sweeps below to this set rather than exempting them
        # in `CROSS_ORG_ALLOWLIST` — flagged by `test_tenant_fk_boundary.py`, correctly.
        # An unbounded scan of `notification_channels` or `invitations` is not made safe by
        # the fact that erasure is person-scoped, and the exemption is the wrong tool: this
        # is neither a credential lookup nor a platform-wide sweep, it is one person's rows
        # in one person's orgs. Soft-deleted memberships are included deliberately (D12.4)
        # — an invitation sent from an org they have since left is still their data.
        erased_org_ids = _org_ids_ever(session, user_id)

        # ── step 3: pending invitations this person sent ───────────────────────
        # The invitee's address is *their* personal data, sitting on a row this user
        # authored, and the invitation cannot complete anyway.
        pending = (
            list(
                session.execute(
                    select(Invitation).where(
                        Invitation.org_id.in_(erased_org_ids),
                        Invitation.invited_by_user_id == user_id,
                        Invitation.status == InvitationStatus.PENDING,
                    )
                )
                .scalars()
                .all()
            )
            if erased_org_ids
            else []
        )
        for inv in pending:
            inv_pii = session.get(InvitationPII, inv.id)
            if inv_pii is not None:
                session.delete(inv_pii)
            # D5 step 3 says mark them REVOKED. `InvitationStatus` has no such member, and
            # adding one is an enum expand/contract of its own that does not belong inside
            # a privacy release. CANCELLED is what `cancel_invitation` already writes and
            # what the UI already renders. Raised on core#655.
            inv.status = InvitationStatus.CANCELLED
            inv.email = None
            inv.token = None
        counts["invitations_revoked"] = len(pending)

        # ── notification channels delivering to this address (criterion 10) ────
        counts["notification_channels_cleared"] = (
            _clear_channels_for(session, erased_email, erased_org_ids)
            if erased_email and erased_org_ids
            else 0
        )

        # ── steps 4 and 5 ──────────────────────────────────────────────────────
        now = datetime.now(UTC)
        for m in memberships:
            m.deleted_at = now
        counts["memberships"] = len(memberships)
        user.deleted_at = now
        user.is_active = False
        session.flush()

        # ── step 6: orgs that existed only for this person ─────────────────────
        for org_id in sole_member_org_ids:
            self.delete_org(session, org_id, projects_dir=projects_dir)
        counts["organizations_deleted"] = len(sole_member_org_ids)

        # ── D12.4 item 2: the residual audit sweep (a canary) ──────────────────
        counts["audit_payloads_redacted"] = _sweep_audit_payloads(session, user_id)

        # ── step 8: one audit row, naming nobody ───────────────────────────────
        #
        # D5 says `action="user.erased"`. `AuditAction` has no such member, and adding one
        # is a t1 hazard rather than a typo: the column is `Enum(AuditAction,
        # native_enum=False)`, so the previously deployed code raises `LookupError` when
        # it *reads* a value it does not know — and the audit page lists rows for a whole
        # org, so a single erasure row would break that page for every reader on the old
        # container mid-swap. DELETE + `resource_type="user"` records the same fact inside
        # the existing enum. Raised on core#655.
        if memberships:
            AuditService().log_action(
                session,
                memberships[0].org_id,
                user_id,
                AuditAction.DELETE,
                "user",
                resource_id=user_id,
                new_values={},
            )
        session.flush()

        # Counts only, never a value — the same trap D5 step 8 guards on the audit row.
        logger.info(
            "Erased user id=%s: %s",
            user_id,
            ", ".join(f"{k}={v}" for k, v in sorted(counts.items())),
        )
        return counts


# ---------------------------------------------------------------------------
# Erasure and org deletion — SPEC_PII_SEPARATION D5/D6, core#655
#
# Everything below is deliberately dialect-portable. The service suite runs on
# SQLite (`Base.metadata.create_all`) while production is Postgres, so a jsonb
# operator here would not be "a Postgres optimisation" — it would be code that
# no test in this repo can execute.
# ---------------------------------------------------------------------------


def _contains_name(haystack: str | None, name: str | None) -> bool:
    """Does an org name or slug still carry a person's name?

    Matched case-insensitively, and also against the slugified form, because
    ``organizations.slug`` held ``lower(replace(full_name, ' ', '-'))`` in 5 of 5
    production rows. Checking only the literal name would leave the slug — which is the
    unique, URL-bearing half — untouched.
    """
    if not haystack or not name:
        return False
    hay = haystack.lower()
    needle = name.strip().lower()
    if not needle:
        return False
    slugged = re.sub(r"[^a-z0-9]+", "-", needle).strip("-")
    return needle in hay or bool(slugged) and slugged in hay


def _remove_tenant_dbt_project(org_id: int, projects_dir: str | None) -> int:
    """Delete ``dbt_projects/tenant_{org_id}/``. Returns how many directories went.

    D6 item 1: this lives outside the database and **nothing soft-deletes a directory**.
    Failure is logged and not raised — a leftover directory is a tidiness problem, while
    aborting here would leave the database half-deleted, which is worse. The count is
    returned so a caller can tell "removed" from "was not there".
    """
    root = Path(projects_dir or settings.dbt_projects_dir)
    path = root / f"tenant_{org_id}"
    if not path.is_dir():
        return 0
    try:
        shutil.rmtree(path)
    except OSError:
        logger.exception("Could not remove dbt project directory for org %s", org_id)
        return 0
    return 1


def _delete_by_user(session: Session, table_name: str, user_id: int) -> int:
    """Hard-delete every row of ``table_name`` belonging to a user.

    Addressed through ``Base.metadata`` rather than a formatted string: five unrelated
    models want the same statement, and building it from the metadata means there is no
    SQL text to get wrong and nothing for a table name to be interpolated into.
    """
    table = Base.metadata.tables[table_name]
    result = session.execute(delete(table).where(table.c.user_id == user_id))
    return result.rowcount or 0


def _delete_oauth_tokens_for_user(session: Session, user_id: int) -> int:
    """Hard-delete MCP access/refresh tokens belonging to a person.

    ``oauth_tokens`` carries ``org_id`` and ``grant_id`` and **no ``user_id``** — the only
    route to the person is through the grant. A `_delete_by_user` call for this table
    raises ``AttributeError: user_id`` rather than deleting nothing, which is the good
    outcome; the bad one would have been a table that happened to have a `user_id` column
    meaning something else.
    """
    tokens = Base.metadata.tables["oauth_tokens"]
    grants = Base.metadata.tables["oauth_grants"]
    result = session.execute(
        delete(tokens).where(
            tokens.c.grant_id.in_(select(grants.c.id).where(grants.c.user_id == user_id))
        )
    )
    return result.rowcount or 0


def _org_ids_ever(session: Session, user_id: int) -> list[int]:
    """Every org this person has ever belonged to, **including soft-deleted memberships**.

    The scope for the two tenant-owned sweeps in `erase_user`. Soft-deleted memberships
    count: an invitation sent from an org someone has since left is still their data, and
    D12.4 says the same about the audit sweep for the same reason.
    """
    return list(
        session.execute(select(Membership.org_id).where(Membership.user_id == user_id))
        .scalars()
        .all()
    )


def _clear_channels_for(session: Session, address: str, org_ids: list[int]) -> int:
    """Stop a notification channel delivering to an erased address.

    Not one of D5's steps — the spec extracts ``notification_channels.config`` into a
    sidecar (§2 row 6) and its §4 contract release drops five *columns*, saying nothing
    about the JSON. So without this the address survives the entire four-release chain
    and only criterion 10 could ever see it, on production, two releases late. Raised on
    core#655.

    The channel is **deactivated**, not silently left with no recipient: a channel that
    has lost its delivery address should fail loudly rather than quietly stop alerting.
    Secrets in the same JSON column (the Slack webhook URL, the Telegram bot token) are
    left alone — they are an org property, not personal data.
    """
    cleared = 0
    # Scoped to the person's own orgs. `test_tenant_fk_boundary.py` flags an unscoped read
    # of a tenant-owned model, and it is right to: erasure being person-scoped does not
    # make a full-table scan of every tenant's notification channels acceptable.
    channels = (
        session.execute(select(NotificationChannel).where(NotificationChannel.org_id.in_(org_ids)))
        .scalars()
        .all()
    )
    for ch in channels:
        config = ch.config if isinstance(ch.config, dict) else {}
        keys = [
            k for k in ("email", "chat_id") if str(config.get(k, "")).lower() == address.lower()
        ]
        pii = session.get(NotificationChannelPII, ch.id)
        matches_pii = pii is not None and (pii.recipient or "").lower() == address.lower()
        if not keys and not matches_pii:
            continue
        if pii is not None:
            session.delete(pii)
        if keys:
            ch.config = {k: v for k, v in config.items() if k not in keys}
        ch.is_active = False
        cleared += 1
    if cleared:
        session.flush()
    return cleared


def _sweep_audit_payloads(session: Session, user_id: int) -> int:
    """D12.4 item 2 — the residual sweep. A **canary**, not a cleanup.

    After D11 (call sites store internal ids) and D12 (redaction at the chokepoint) this
    must find **zero**. If it ever redacts something, one of those two failed and the
    count is the only thing that will say so — which is why it returns a count rather
    than quietly repairing.

    🚨 **A clean run is not evidence, and that is why the count is reported rather than
    inferred from silence.** "Finds zero" is also what this returns before the feature
    exists at all. Its acceptance evidence is a run against a deliberately planted
    PII-bearing row (§2c criterion 2), which is what
    `test_pii_separation.py::test_the_residual_sweep_finds_a_planted_row` does.

    Scope is every row in every org this person has ever belonged to — **including
    soft-deleted memberships** — plus rows they authored. `WHERE user_id = <erased>`
    alone would miss the majority case: three of the PII-writing call sites store the
    *subject's* address on a row whose `user_id` is the *actor*.
    """
    org_ids = _org_ids_ever(session, user_id)
    stmt = select(AuditLog).where(
        or_(AuditLog.user_id == user_id, AuditLog.org_id.in_(org_ids) if org_ids else false())
    )
    redacted = 0
    for row in session.execute(stmt).scalars():
        for field in ("old_values", "new_values"):
            payload = getattr(row, field)
            if not isinstance(payload, dict):
                continue
            cleaned = redact_pii_payload(payload)
            if cleaned != payload:
                setattr(row, field, cleaned)
                redacted += 1
    if redacted:
        session.flush()
        logger.warning(
            "Residual audit sweep redacted %s payload(s) during erasure of user id=%s. "
            "This should be zero: D11 and the log_action redactor are supposed to make "
            "it impossible for a payload to carry personal data at all.",
            redacted,
            user_id,
        )
    return redacted
