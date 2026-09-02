"""Authentication state — login, signup, logout, org switching."""

import logging
import re

import reflex as rx
from pydantic import BaseModel

from datanika.config import settings
from datanika.hooks import collect_events
from datanika.services.auth import AuthService
from datanika.services.auth_redirects import AUTH_ERROR_KEYS
from datanika.services.captcha_service import CaptchaService
from datanika.services.email_verification import request_email_verification
from datanika.services.user_service import UserService, UserServiceError
from datanika.ui.state.base_state import check_role_hierarchy, get_sync_session

logger = logging.getLogger(__name__)

# Option C auth bridge: valid template slug pattern + max length. Cold-traffic
# visitors who click "Try this template" on a public /templates/<slug> landing
# page must have the slug preserved across the signup wall so the post-auth
# redirect can land on /connections?template=<slug>. The slug is compared
# against this pattern (not against the in-app template registry) to keep the
# auth layer decoupled from ConnectionState; unknown-but-well-formed slugs are
# silently ignored downstream by ConnectionState.load_template_from_query.
# Rejecting malformed or over-long slugs avoids an open-redirect vector.
_TEMPLATE_SLUG_RE = re.compile(r"^[a-z0-9][a-z0-9-]{0,63}$")

# Longest ?next= we will honour. An OAuth consent URL carries a PKCE challenge
# and an opaque client state, so it is comfortably the biggest legitimate one.
_MAX_NEXT_LEN = 2048


def _safe_next_path(raw: str) -> str:
    """Return ``raw`` if it is a same-site absolute path, else ``""``.

    The ``?next=`` bridge exists so an interrupted flow can resume after the
    login wall — ``/oauth/consent`` (#394) is the first caller, where dropping
    the user on the dashboard would strand an MCP client mid-handshake.

    It is also the classic open-redirect vector, so the allowed shape is
    deliberately narrow: an absolute path on this site and nothing else.
    ``//evil.com`` is protocol-relative, and a browser normalises the
    backslashes in ``/\\evil.com`` to the same thing — both would send a
    freshly-authenticated user straight off-site, which is precisely when they
    are most likely to type a password into whatever they land on.
    """
    if not raw or len(raw) > _MAX_NEXT_LEN:
        return ""
    if not raw.startswith("/"):
        return ""
    if raw.startswith("//") or "\\" in raw:
        return ""
    if any(ch.isspace() or ord(ch) < 0x20 for ch in raw):
        return ""
    return raw


class UserInfo(BaseModel):
    id: int = 0
    email: str = ""
    full_name: str = ""


class OrgInfo(BaseModel):
    id: int = 0
    name: str = ""
    slug: str = ""


def _slugify(text: str) -> str:
    """Simple slug from text: lowercase, replace non-alnum with hyphens.

    🚨 **Do not use this for an organization slug.** A slug is an identifier —
    unique-constrained, in URLs, and matched by the SSO callback — so deriving one from a
    person's name publishes that name in a durable key that erasing ``users.full_name``
    does not reach (core#655 D4). Org slugs are ``org-{user.id}``.

    Kept because it is a general helper and the template-slug plumbing above is unrelated
    to people's names; ``test_auth_state.py`` asserts signup no longer calls it.
    """
    slug = re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")
    return slug or "org"


class AuthState(rx.State):
    access_token: str = ""
    refresh_token: str = ""
    current_user: UserInfo = UserInfo()
    current_org: OrgInfo = OrgInfo()
    user_orgs: list[OrgInfo] = []
    current_role: str = ""

    auth_error: str = ""
    # Set by ``BaseState._check_role`` when a mutating handler finds the session
    # has ended (#673). It lives here, not on ``BaseState``, on purpose: every
    # substate gets its *own* copy of an inherited var, so a flag set on
    # ``ApiKeyState`` is invisible to ``page_layout``. ``AuthState`` is one
    # object, and the layout already reads it.
    #
    # ⚠️ Distinct from ``show_session_expired``, which reads ``?expired=1`` on
    # ``/login`` — that is the *page-load* path (#671), where the tab navigates
    # and can carry a query parameter. A handler cannot navigate, so it has to
    # leave a mark on the state the layout can see.
    session_expired: bool = False
    # Why the most recent mutating action was refused (#744). Here for the same
    # reason as ``session_expired`` — an inherited var is copied per substate,
    # so a value written on ``UploadState`` is invisible to ``page_layout``.
    #
    # Unlike ``session_expired``, that copying **is** the defect rather than a
    # constraint working around it: ``_check_role`` recorded the refusal in
    # ``self.error_message``, and 10 of the 15 state classes that assign
    # ``error_message`` are rendered by no page or component at all (#887).
    # ``uploads.py`` is one of them, so pressing Run without the editor role
    # created no row, raised no error and displayed nothing.
    #
    # ⚠️ Deliberately NOT set when the session has ended. That path already has
    # ``signed_out_panel``, and "Permission denied. Requires editor role or
    # higher." sends somebody who needs to sign in to go and ask an admin for
    # access they already have (#673 AC3).
    action_error: str = ""
    invite_email: str = ""
    # What happened to the confirmation mail on the most recent signup (core#700).
    # One of VerificationMailResult's values, or "" when nothing has been attempted.
    # Until this existed a successful send and a failed one were the same event to
    # every surface a person can see, and the only proof either way was opening an
    # inbox by hand.
    verification_mail_state: str = ""

    def clear_auth_error(self):
        self.auth_error = ""

    def prefill_invite_email(self):
        """Pre-fill email from invite link query param."""
        email = self.router.page.params.get("email", "")
        if email:
            self.invite_email = email

    @rx.var
    def is_authenticated(self) -> bool:
        return self.access_token != ""

    @rx.var
    def show_reset_done(self) -> bool:
        """Whether /login arrived from a completed password reset (core#623).

        The reset flow lands here signed out rather than signing the user in —
        an emailed link that produces a live session makes the email itself a
        bearer credential. This is the callout that tells them it worked, and it
        matches how ``verify_email`` and ``accept_invite`` already flag success.
        """
        return self.router.page.params.get("reset", "") == "1"

    @rx.var
    def show_session_expired(self) -> bool:
        """Whether /login arrived from a session that timed out (#671).

        Set only when a session actually ended — ``check_auth`` distinguishes a
        timed-out session from a visitor who was never signed in, because
        telling the second group their session expired is confusing.
        """
        return self.router.page.params.get("expired", "") == "1"

    @rx.var
    def show_link_blocked(self) -> bool:
        """Whether a social login was refused because the local account is unproven.

        A **bounded flag**, not the message: the backend route that sets it is a
        Starlette redirect, so it cannot write ``auth_error`` (server-side state
        the redirect never touches), and rendering free text out of the query
        string would put a phishing surface on the sign-in page. Same shape as
        ``?reset=1`` and ``?expired=1``, for the same reason.
        """
        return self.router.page.params.get("link_blocked", "") == "1"

    @rx.var
    def auth_error_reason(self) -> str:
        """The ``?auth_error=`` slug, or "" when it is not one we publish (#686).

        A **whitelist**, not a passthrough. The page renders a translated sentence chosen
        by this slug; it never renders the query string itself. Under the previous
        free-text parameter anyone could send a link that put their own text inside our
        sign-in card, under our logo, in our styling.
        """
        reason = self.router.page.params.get("auth_error", "")
        return reason if reason in AUTH_ERROR_KEYS else ""

    @rx.var
    def show_email_verified(self) -> bool:
        """Whether /login arrived from a completed email confirmation (#700, #659)."""
        return self.router.page.params.get("verified", "") == "1"

    @rx.var
    def show_verify_error(self) -> bool:
        """Whether the confirmation link was invalid, expired, or failed to apply (#700).

        Sent by three separate branches of ``verify_email``; one message covers all of
        them because the remedy is identical - sign in and ask for a new link.
        """
        return self.router.page.params.get("verify_error", "") == "1"

    @rx.var
    def show_invite_accepted(self) -> bool:
        """Whether an invitation was accepted and the user should now sign in (#659).

        The happy path already worked *silently*: ``org_id`` on the same redirect is read
        below and switches the session into the invited org. Only the acknowledgement was
        missing.
        """
        return self.router.page.params.get("invite_accepted", "") == "1"

    @rx.var
    def show_invite_error(self) -> bool:
        """Whether an invitation link failed (#659).

        The costly one. An invitee has no account, no error, and no reason not to click
        the same dead link again - and the person who invited them gets no signal either.
        """
        return self.router.page.params.get("invite_error", "") == "1"

    @rx.var
    def org_id(self) -> int:
        return self.current_org.id if self.current_org.id else 0

    @rx.var
    def can_edit(self) -> bool:
        """Whether the current member may create/edit/run resources (editor+).

        Mirrors the ``_check_role("editor")`` gate on the create/edit/run/toggle
        state handlers so the UI hides controls the member cannot use. Enforcement
        still lives in the handlers — this only governs visibility (see #313).
        """
        return check_role_hierarchy(self.current_role, "editor")

    @rx.var
    def can_delete(self) -> bool:
        """Whether the current member may delete resources (admin+).

        Mirrors the ``_check_role("admin")`` gate on the delete handlers.
        """
        return check_role_hierarchy(self.current_role, "admin")

    @rx.var
    def can_administer(self) -> bool:
        """Whether the current member may manage org-level facilities (admin+).

        API keys, notification channels and backup export are **not** tenant
        resources — they are levers on the organisation itself, and every one of
        their handlers gates on ``_check_role("admin")``, including the *create*
        ones. ``can_edit`` (editor) is therefore too weak to gate them: an editor
        given the API-keys create form gets a Create button that always fails,
        which is core#658's defect exactly.

        ⚠️ **Same threshold as** :attr:`can_delete`, deliberately, and this is a
        duplicate predicate rather than an alias. They answer two different
        questions — *may I destroy a row?* and *may I operate the organisation?*
        — which happen to have one answer under today's role table. Aliasing
        them would mean the next change to either has to be made by someone who
        first notices they were ever distinct. Both are checked against
        ``check_role_hierarchy(..., "admin")`` by
        ``tests/test_ui/test_rbac_ui_visibility.py::TestVisibilityVars``, so a
        drift in either direction is a test failure rather than a reading.
        """
        return check_role_hierarchy(self.current_role, "admin")

    def _get_user_service(self) -> UserService:
        auth = AuthService(settings.secret_key)
        return UserService(auth)

    def _post_auth_redirect_target(self) -> str:
        """Return the path to redirect to after a successful login/signup.

        ``?next=<path>`` wins when present and same-site: it means the user was
        pulled out of a flow to authenticate and has somewhere specific to go
        back to. See ``_safe_next_path`` for why the shape is checked.

        Otherwise, a well-formed ``?template=<slug>`` query parameter (from a
        public ``datanika.io/templates/<slug>`` landing page CTA) redirects to
        ``/connections?template=<slug>`` so
        ``ConnectionState.load_template_from_query`` prefills the form.
        Failing both, the dashboard root.

        Slugs are validated against ``_TEMPLATE_SLUG_RE`` to avoid an
        open-redirect vector. Unknown-but-well-formed slugs pass through;
        ConnectionState silently ignores them downstream.
        """
        nxt = _safe_next_path(self.router.page.params.get("next", ""))
        if nxt:
            return nxt

        slug = self.router.page.params.get("template", "")
        if not slug or not _TEMPLATE_SLUG_RE.match(slug):
            return "/"
        return f"/connections?template={slug}"

    def _load_current_role(self, user_id: int, org_id: int):
        """Load the user's role for the given org from the membership table."""
        from sqlalchemy import select

        from datanika.models.user import Membership
        from datanika.ui.state.base_state import get_sync_session

        with get_sync_session() as session:
            membership = session.execute(
                select(Membership).where(
                    Membership.user_id == user_id,
                    Membership.org_id == org_id,
                    Membership.deleted_at.is_(None),
                )
            ).scalar_one_or_none()
            self.current_role = membership.role.value if membership else ""

    def login(self, form_data: dict):
        self.auth_error = ""
        email = (form_data.get("email") or "").strip()
        password = form_data.get("password") or ""
        if not email or not password:
            self.auth_error = "Email and password are required"
            return

        captcha_token = form_data.get("captcha_token", "")
        if not CaptchaService().verify(captcha_token, "login"):
            self.auth_error = "CAPTCHA verification failed. Please try again."
            return

        # Authenticate against DB
        svc = self._get_user_service()
        try:
            with get_sync_session() as session:
                result = svc.authenticate(session, email, password)
                if result is not None:
                    user = result["user"]
                    user_id = user.id
                    user_email = user.email
                    user_name = user.full_name
                    access_token = result["access_token"]
                    refresh_token = result["refresh_token"]
                    orgs = [
                        OrgInfo(id=o.id, name=o.name, slug=o.slug)
                        for o in svc.get_user_orgs(session, user.id)
                    ]
        except Exception:
            self.auth_error = "Login failed. Please try again."
            return

        if result is None:
            self.auth_error = "Invalid email or password"
            return

        # Audit login
        try:
            from datanika.models.audit_log import AuditAction
            from datanika.services.audit_service import AuditService

            with get_sync_session() as audit_session:
                org_id_for_audit = orgs[0].id if orgs else 0
                AuditService().log_action(
                    audit_session,
                    org_id_for_audit,
                    user_id,
                    AuditAction.LOGIN,
                    "session",
                )
                audit_session.commit()
        except Exception:
            # Deliberate swallow, loud failure (core#723): a broken audit write
            # must not stop a login, but it must not vanish either.
            logger.exception("Audit write failed and was dropped: action=LOGIN user=%s", user_id)

        # Apply auth state
        self.access_token = access_token
        self.refresh_token = refresh_token
        self.current_user = UserInfo(id=user_id, email=user_email, full_name=user_name)
        self.user_orgs = orgs

        auth = AuthService(settings.secret_key)

        # If redirected from invite acceptance, switch to the invited org
        invite_org_id = self.router.page.params.get("org_id", "")
        if invite_org_id:
            try:
                target_org_id = int(invite_org_id)
                for o in self.user_orgs:
                    if o.id == target_org_id:
                        self.current_org = o
                        self.access_token = auth.create_access_token(user_id, target_org_id)
                        self._load_current_role(user_id, target_org_id)
                        return rx.redirect("/")
            except (ValueError, TypeError):
                pass

        payload = auth.decode_token(access_token, expected_type="access")
        if payload is None:
            self.auth_error = "Invalid access token"
            return
        org_id = payload["org_id"]
        for o in self.user_orgs:
            if o.id == org_id:
                self.current_org = o
                break
        self._load_current_role(user_id, org_id)
        return rx.redirect(self._post_auth_redirect_target())

    def signup(self, form_data: dict):
        self.auth_error = ""

        captcha_token = form_data.get("captcha_token", "")
        if not CaptchaService().verify(captcha_token, "signup"):
            self.auth_error = "CAPTCHA verification failed. Please try again."
            return

        email = form_data.get("email", "")
        password = form_data.get("password", "")
        full_name = form_data.get("full_name", "")
        svc = self._get_user_service()
        try:
            with get_sync_session() as session:
                user = svc.register_user(session, email, password, full_name)
                org_name = f"{full_name}'s Org"
                # 🚨 The slug is no longer derived from the person's name (core#655, D4).
                # A slug is an *identifier*: unique-constrained, in URLs, and matched by
                # the SSO callback (`sso_routes.py` compares `Organization.slug ==
                # org_slug`), so a name-derived slug publishes a person's name in a
                # durable key — measured in 5 of 5 production rows. The display `name`
                # may stay: it is text inside the tenant, and D5 step 7's erasure sweep
                # rewrites it. The `user.id` suffix that #127 added for uniqueness is now
                # the whole slug, so uniqueness is if anything stronger.
                org_slug = f"org-{user.id}"
                org = svc.create_org(session, org_name, org_slug, user.id)
                # Capture ids before commit expires ORM attributes
                org_id = org.id
                user_id = user.id
                user_email = user.email
                session.commit()

            # Confirm the address. Until this call existed, ``email_verified``
            # was False on every password account ever created, which is what
            # made an unproven account eligible for a social-login auto-link.
            # Best-effort by construction: the account is already committed, so
            # a missing relay or an unreachable broker must not surface as a
            # failed signup. See services/email_verification.py.
            mail_result = request_email_verification(
                user_id, user_email, AuthService(settings.secret_key)
            )
            self.verification_mail_state = mail_result.value

            # Now authenticate to get tokens
            with get_sync_session() as session:
                result = svc.authenticate(session, email, password)
                if result is None:
                    self.auth_error = "Signup succeeded but login failed"
                    return
                self.access_token = result["access_token"]
                self.refresh_token = result["refresh_token"]
                self.current_user = UserInfo(
                    id=result["user"].id,
                    email=result["user"].email,
                    full_name=result["user"].full_name,
                )
                self.current_org = OrgInfo(id=org_id, name=org_name, slug=org_slug)
                self.user_orgs = [self.current_org]
                self.current_role = "owner"

                # Accept pending invitation if signup came from invite link
                invite_token = self.router.page.params.get("invite_token", "")
                if invite_token:
                    try:
                        from datanika.services.invitation_service import InvitationService

                        inv_svc = InvitationService(AuthService(settings.secret_key))
                        membership = inv_svc.accept_invitation(session, invite_token)
                        if membership:
                            invited_org = session.get(type(org), membership.org_id)
                            if invited_org:
                                invited = OrgInfo(
                                    id=invited_org.id,
                                    name=invited_org.name,
                                    slug=invited_org.slug,
                                )
                                self.user_orgs.append(invited)
                                # Switch to the invited org
                                self.current_org = invited
                                self.current_role = membership.role.value if membership.role else ""
                                auth = AuthService(settings.secret_key)
                                self.access_token = auth.create_access_token(
                                    result["user"].id, invited_org.id
                                )
                            session.commit()
                    except Exception:
                        # Best-effort by design -- signup must succeed even if
                        # the invitation cannot be applied. But this one is
                        # user-visible when it fails (they sign up and are not
                        # in the org they were invited to), so it is exactly the
                        # thing support needs a log line for (core#723).
                        logger.exception(
                            "Invitation acceptance failed during signup and was dropped: email=%s",
                            self.email,
                        )
        except UserServiceError as exc:
            # Typed validation errors from user_service carry curated,
            # user-facing messages ("Email already exists", "Name is
            # required", etc.). Surface them verbatim so users can
            # recover instead of bouncing off a generic toast. #128.
            self.auth_error = str(exc)
            return
        except Exception:
            # Real failure of an unexpected kind — DB connection loss,
            # CAPTCHA-service timeout, OAuth upstream exception, etc.
            # Log the full traceback so ops can see root causes in
            # container logs; show a generic message to the user. #128.
            logger.exception("Unexpected signup failure")
            self.auth_error = "Signup failed. Please try again."
            return
        # Let plugins contribute Reflex events on signup success (issue
        # #99 open-core split). The cloud plugin subscribes to
        # user.signup_completed and returns an rx.call_script firing the
        # Google Ads conversion event; on open-source core with no plugin
        # loaded, collect_events returns an empty list and we just redirect.
        #
        # The redirect target is computed by _post_auth_redirect_target()
        # from #101 — honours ?template=<slug> query-string propagation
        # so Option C template-landing signups go to
        # /connections?template=<slug> instead of the default /.
        extra_events = collect_events("user.signup_completed", user_id=self.current_user.id)
        return [*extra_events, rx.redirect(self._post_auth_redirect_target())]

    def dismiss_verification_notice(self):
        """Clear the post-signup confirmation banner (core#700)."""
        self.verification_mail_state = ""

    def dismiss_action_error(self):
        """Clear the refusal callout (core#744)."""
        self.action_error = ""

    def logout(self):
        # Audit logout before clearing state
        try:
            from datanika.models.audit_log import AuditAction
            from datanika.services.audit_service import AuditService

            if self.current_user.id and self.current_org.id:
                with get_sync_session() as audit_session:
                    AuditService().log_action(
                        audit_session,
                        self.current_org.id,
                        self.current_user.id,
                        AuditAction.LOGOUT,
                        "session",
                    )
                    audit_session.commit()
        except Exception:
            # As with LOGIN above: swallow, but leave evidence (core#723).
            logger.exception("Audit write failed and was dropped: action=LOGOUT")

        self._clear_session()
        self.auth_error = ""
        return rx.redirect("/login")

    @rx.var
    def org_name_options(self) -> list[str]:
        return [o.name for o in self.user_orgs]

    def switch_org_by_name(self, name: str):
        for o in self.user_orgs:
            if o.name == name:
                return self.switch_org(o.id)

    def switch_org_by_name_in_place(self, name: str):
        """Switch orgs without navigating away.

        ``switch_org`` sends the user to the dashboard afterwards, which is
        right for the sidebar switcher and wrong for ``/oauth/consent`` (#394):
        the OAuth request lives entirely in that page's query string, and an
        MCP client cannot re-send it, so leaving the page abandons the flow.
        """
        for o in self.user_orgs:
            if o.name == name:
                self._apply_org_switch(o.id)
                return

    def _apply_org_switch(self, org_id: int) -> bool:
        """Move the session to ``org_id``. False (with ``auth_error``) if not a member."""
        self.auth_error = ""
        # This mints a fresh access *and* refresh token. Without this guard it
        # is a way to extend a session indefinitely without ever passing the
        # check ``check_auth`` performs — click "switch org" every ten minutes
        # and the expiry never applies to you (#671). Fails closed for all
        # three callers rather than at each one.
        if not self._revalidate_session():
            self._clear_session()
            self.auth_error = "Your session has expired. Please sign in again."
            return False
        # Verify the user is a member of the target org
        svc = self._get_user_service()
        with get_sync_session() as session:
            membership = svc.get_membership(session, org_id, self.current_user.id)
            if membership is None:
                self.auth_error = "You are not a member of that organization"
                return False
        auth = AuthService(settings.secret_key)
        self.access_token = auth.create_access_token(self.current_user.id, org_id)
        self.refresh_token = auth.create_refresh_token(self.current_user.id)
        for o in self.user_orgs:
            if o.id == org_id:
                self.current_org = o
                break
        self._load_current_role(self.current_user.id, org_id)
        return True

    def switch_org(self, org_id: int):
        if self._apply_org_switch(org_id):
            return rx.redirect("/")

    def handle_oauth_complete(self):
        """Extract tokens from URL query params after OAuth callback redirect."""
        params = self.router.page.params
        token = params.get("token", "")
        refresh = params.get("refresh", "")

        if not token:
            self.auth_error = "OAuth authentication failed"
            return rx.redirect("/login")

        self.access_token = token
        self.refresh_token = refresh

        # Decode token to get user_id and org_id
        auth = AuthService(settings.secret_key)
        payload = auth.decode_token(token, expected_type="access")
        if payload is None:
            self.auth_error = "Invalid authentication token"
            return rx.redirect("/login")

        user_id = payload["user_id"]
        org_id = payload["org_id"]

        svc = self._get_user_service()
        with get_sync_session() as session:
            user = svc.get_user(session, user_id)
            if user is None:
                self.auth_error = "User not found"
                return rx.redirect("/login")

            self.current_user = UserInfo(id=user.id, email=user.email, full_name=user.full_name)
            orgs = svc.get_user_orgs(session, user_id)
            self.user_orgs = [OrgInfo(id=o.id, name=o.name, slug=o.slug) for o in orgs]
            for o in self.user_orgs:
                if o.id == org_id:
                    self.current_org = o
                    break
        self._load_current_role(user_id, org_id)
        return rx.redirect(self._post_auth_redirect_target())

    def _clear_session(self) -> None:
        """Drop every trace of the signed-in user from this state object.

        Shared by ``logout`` and by the revalidation failure path, because
        "clear the session" that clears five of six vars is the shape that
        leaves a half-signed-in shell: ``is_authenticated`` reads
        ``access_token``, but the sidebar renders ``current_org`` and
        ``current_role``.
        """
        self.access_token = ""
        self.refresh_token = ""
        self.current_user = UserInfo()
        self.current_org = OrgInfo()
        self.user_orgs = []
        self.current_role = ""
        # Cleared here so a deliberate ``logout`` never lands on the "you were
        # signed out" panel. ``_check_role`` sets it True *after* calling this
        # (#673), which is the one path that means it.
        self.session_expired = False

    def _revalidate_session(self) -> bool:
        """Whether this session may continue — renewing the access token if needed.

        Called from every protected page load, so the common case has to be
        cheap: a valid access token returns on a signature check with **no
        database read**. Only an aged-out token pays for a query.

        The renewal goes through ``UserService.redeem_refresh_token``, which is
        where the ``password_changed_at`` comparison lives. That is what makes a
        password change end other sessions: the access token those sessions hold
        expires within ``ACCESS_TOKEN_TTL_MINUTES``, and the refresh token they
        would renew with was minted before the change, so it is refused.

        Revocation latency is therefore the access-token TTL, not zero. Making
        it zero means reading ``password_changed_at`` on every protected page
        load; 10 minutes was the founder's call on that trade (#671).
        """
        if not self.access_token:
            return False

        auth = AuthService(settings.secret_key)
        if auth.decode_token(self.access_token, expected_type="access") is not None:
            return True

        if not self.refresh_token:
            return False

        svc = self._get_user_service()
        with get_sync_session() as session:
            renewed = svc.redeem_refresh_token(
                session, self.refresh_token, self.current_org.id or None
            )
            if renewed is None:
                return False
            self.access_token = renewed["access_token"]
            self.refresh_token = renewed["refresh_token"]
            org_id = renewed["org_id"]

        if org_id != self.current_org.id:
            for o in self.user_orgs:
                if o.id == org_id:
                    self.current_org = o
                    break

        return True

    async def check_auth(self):
        # ⚠️ This used to be `if not self.access_token`, which tests that a
        # string is non-empty. It never decoded the token, so the expiry was
        # unenforced and nothing ever re-checked a session after login (#671).
        had_session = bool(self.access_token)
        # A refusal belongs to the action that caused it, not to the next page
        # (#744). Navigating is the user moving on, so the callout goes with them.
        self.action_error = ""
        if not self._revalidate_session():
            self._clear_session()
            # A visitor who was never signed in is not a session that timed
            # out, and telling them their session expired is confusing.
            return rx.redirect("/login?expired=1" if had_session else "/login")
        # Ensure current_role is loaded (e.g. after page refresh)
        if not self.current_role and self.current_user.id and self.current_org.id:
            self._load_current_role(self.current_user.id, self.current_org.id)
        # Refresh translations so plugin keys (e.g. billing.*) are available
        from datanika.ui.state.i18n_state import I18nState

        i18n = await self.get_state(I18nState)
        i18n.ensure_loaded()
