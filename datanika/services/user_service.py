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
                self._assert_local_account_proved_its_email(user)
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
