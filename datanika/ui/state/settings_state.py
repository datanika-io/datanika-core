"""Settings state — org profile and member management."""

import reflex as rx
from pydantic import BaseModel

from datanika.config import settings as app_settings
from datanika.plugin_registry import BILLING_ROUTE
from datanika.services.auth import AuthService
from datanika.services.user_service import UserService
from datanika.ui.state.auth_state import AuthState
from datanika.ui.state.base_state import BaseState, get_sync_session


class MemberItem(BaseModel):
    id: int = 0
    user_id: int = 0
    email: str = ""
    full_name: str = ""
    role: str = ""
    # Per-row capability, computed server-side from the same rules the service
    # enforces (SPEC_ORG_ROLES R2/R3). Carried on the row because a Reflex
    # `rx.cond` in the table body cannot run the relational check itself, and
    # duplicating the rank comparison in the component would be a second
    # answer to a question that already has one.
    can_manage: bool = False
    is_self: bool = False
    assignable_roles: list[str] = []


class InvitationItem(BaseModel):
    id: int = 0
    email: str = ""
    role: str = ""
    created_at: str = ""


class SettingsState(BaseState):
    org_name: str = ""
    org_slug: str = ""
    members: list[MemberItem] = []
    pending_invitations: list[InvitationItem] = []
    invite_email: str = ""
    invite_role: str = "viewer"
    edit_org_name: str = ""
    edit_org_slug: str = ""
    edit_default_dbt_schema: str = ""
    # Viewer context for UI honesty (SPEC_ORG_ROLES §4). core#658 AC4: the
    # member table used to render a role select and a Remove button for every
    # member regardless of who was looking. The server checks were real, so a
    # viewer simply saw controls that always failed.
    current_role: str = ""
    current_user_id: int = 0
    can_manage_members: bool = False
    is_owner: bool = False
    invite_roles: list[str] = []
    # Successor picker for Transfer ownership. Emails, not ids: the value
    # is what the owner reads in the dialog, and it is resolved back to a
    # membership server-side.
    transfer_candidates: list[str] = []
    transfer_to_email: str = ""

    def redirect_legacy_billing_tab(self):
        """Send `/settings?tab=billing` to the billing page (#654).

        Core has never read a `tab` parameter, so the link was wrong from the
        day it was written — but one of the four call sites was the
        quota-warning email, which cloud queues per org owner
        (`datanika_cloud/billing/meter.py`). A URL in somebody's inbox cannot be
        edited, so fixing the source is not the whole fix.

        In the **core** edition there is no billing page to send them to: the
        route is registered by the plugin. The parameter stays ignored there,
        which is exactly what it has always been, rather than 404ing on a route
        that does not exist.
        """
        if self.router.page.params.get("tab") != "billing":
            return None
        if app_settings.datanika_edition != "cloud":
            return None
        return rx.redirect(BILLING_ROUTE)

    def set_edit_org_name(self, value: str):
        self.edit_org_name = value

    def set_edit_org_slug(self, value: str):
        self.edit_org_slug = value

    def set_edit_default_dbt_schema(self, value: str):
        self.edit_default_dbt_schema = value

    def set_invite_email(self, value: str):
        self.invite_email = value

    def set_invite_role(self, value: str):
        self.invite_role = value

    def _get_user_service(self) -> UserService:
        auth = AuthService(app_settings.secret_key)
        return UserService(auth)

    async def load_settings(self):
        auth_state = await self.get_state(AuthState)
        if not auth_state.current_org.id:
            return
        svc = self._get_user_service()
        with get_sync_session() as session:
            org = svc.update_org(session, auth_state.current_org.id)
            if org:
                self.org_name = org.name
                self.org_slug = org.slug
                self.edit_org_name = org.name
                self.edit_org_slug = org.slug
                self.edit_default_dbt_schema = org.default_dbt_schema
            from datanika.services.auth import (
                assignable_roles,
                may_manage_member,
            )

            self.current_role = auth_state.current_role
            self.current_user_id = auth_state.current_user.id
            self.invite_roles = assignable_roles(self.current_role)
            self.can_manage_members = bool(self.invite_roles)
            self.is_owner = self.current_role == "owner"
            if self.invite_role not in self.invite_roles:
                self.invite_role = self.invite_roles[0] if self.invite_roles else ""

            members = svc.list_members(session, auth_state.current_org.id)
            self.members = []
            for m in members:
                user = svc.get_user(session, m.user_id)
                is_self = m.user_id == self.current_user_id
                self.members.append(
                    MemberItem(
                        id=m.id,
                        user_id=m.user_id,
                        email=user.email if user else "",
                        full_name=user.full_name if user else "",
                        role=m.role.value,
                        is_self=is_self,
                        can_manage=(
                            not is_self and may_manage_member(self.current_role, m.role.value)
                        ),
                        assignable_roles=self.invite_roles,
                    )
                )
            self.transfer_candidates = [m.email for m in self.members if not m.is_self]
            if self.transfer_to_email not in self.transfer_candidates:
                self.transfer_to_email = ""

            # Load pending invitations
            from datanika.services.invitation_service import InvitationService

            inv_svc = InvitationService(AuthService(app_settings.secret_key))
            invitations = inv_svc.list_pending_invitations(session, auth_state.current_org.id)
            self.pending_invitations = [
                InvitationItem(
                    id=inv.id,
                    email=inv.email,
                    role=inv.role.value,
                    created_at=(
                        inv.created_at.strftime("%Y-%m-%d %H:%M") if inv.created_at else ""
                    ),
                )
                for inv in invitations
            ]
        self.error_message = ""

    async def update_org(self):
        if not await self._check_role("owner"):
            return
        auth_state = await self.get_state(AuthState)
        svc = self._get_user_service()
        try:
            with get_sync_session() as session:
                svc.update_org(
                    session,
                    auth_state.current_org.id,
                    user_id=auth_state.current_user.id,
                    name=self.edit_org_name,
                    slug=self.edit_org_slug,
                    default_dbt_schema=self.edit_default_dbt_schema,
                )
                self._audit(
                    session,
                    auth_state.current_org.id,
                    auth_state.current_user.id,
                    "update",
                    "org",
                    resource_id=auth_state.current_org.id,
                    # D11. This is the site a key-name rule cannot reach (D12.1): the key
                    # is `name`, and the redactor cannot tell `{"name": "My Postgres"}` on
                    # 25 label-carrying call sites from `{"name": "Anna's Org"}` here. The
                    # discriminator is `(resource_type, key)`, so it is fixed at the call
                    # site or not at all — which is why D12 needs BOTH mechanisms.
                    # §2c measured `organizations.name` carrying a live `users.full_name`
                    # in 5 of 5 production rows.
                    old_values={"org_id": auth_state.current_org.id},
                    new_values={"org_id": auth_state.current_org.id},
                )
                session.commit()
        except Exception as e:
            self.error_message = self._safe_error(e, "Failed to update organization")
            return
        self.org_name = self.edit_org_name
        self.org_slug = self.edit_org_slug
        # Update AuthState's current_org
        from datanika.ui.state.auth_state import OrgInfo

        auth_state.current_org = OrgInfo(
            id=auth_state.current_org.id,
            name=self.edit_org_name,
            slug=self.edit_org_slug,
        )
        self.error_message = ""

    async def add_member_by_email(self):
        if not await self._check_role("admin"):
            return
        auth_state = await self.get_state(AuthState)
        from datanika.config import settings

        if settings.smtp_host:
            # SMTP configured — use email invitation flow
            await self._send_invitation(auth_state)
        else:
            # No SMTP — fall back to direct add (user must already exist)
            await self._add_existing_user(auth_state)

    async def _send_invitation(self, auth_state):
        try:
            from datanika.config import settings
            from datanika.models.user import MemberRole
            from datanika.services.auth import AuthService
            from datanika.services.invitation_service import InvitationService
            from datanika.tasks.email_tasks import send_invitation_email_task

            inv_svc = InvitationService(AuthService(settings.secret_key))
            with get_sync_session() as session:
                invitation = inv_svc.create_invitation(
                    session,
                    auth_state.current_org.id,
                    self.invite_email,
                    MemberRole(self.invite_role),
                    auth_state.current_user.id,
                )
                # D11: the audit payload carries the internal id, not the address. The
                # address stays resolvable through `invitation_pii` while that row
                # exists and stops resolving once it is erased — so erasure sweeps
                # nothing here, and the security trail stays complete.
                self._audit(
                    session,
                    auth_state.current_org.id,
                    auth_state.current_user.id,
                    "create",
                    "member",
                    resource_id=invitation.id,
                    new_values={"invitation_id": invitation.id, "role": self.invite_role},
                )
                session.commit()

                send_invitation_email_task.delay(
                    self.invite_email,
                    auth_state.current_org.name,
                    auth_state.current_user.full_name or auth_state.current_user.email,
                    invitation.token,
                )
        except Exception as e:
            self.error_message = self._safe_error(e, "Failed to send invitation")
            return
        self.invite_email = ""
        self.error_message = ""
        await self.load_settings()

    async def _add_existing_user(self, auth_state):
        svc = self._get_user_service()
        try:
            with get_sync_session() as session:
                user = svc.get_user_by_email(session, self.invite_email)
                if user is None:
                    self.error_message = "User not found. Configure SMTP to send email invitations."
                    return
                from datanika.models.user import MemberRole

                membership = svc.add_member(
                    session,
                    auth_state.current_org.id,
                    user.id,
                    MemberRole(self.invite_role),
                    actor_user_id=auth_state.current_user.id,
                )
                # D11. This is one of the two sites §2a flags as having no `resource_id`
                # to substitute — `add_member` already returns the Membership, so it does.
                self._audit(
                    session,
                    auth_state.current_org.id,
                    auth_state.current_user.id,
                    "create",
                    "member",
                    resource_id=membership.id,
                    new_values={"membership_id": membership.id, "role": self.invite_role},
                )
                session.commit()
        except Exception as e:
            self._set_error(e, "Failed to add member")
            return
        self.invite_email = ""
        self.error_message = ""
        await self.load_settings()

    async def change_member_role(self, membership_id: int, new_role: str):
        if not await self._check_role("admin"):
            return
        auth_state = await self.get_state(AuthState)
        svc = self._get_user_service()
        try:
            with get_sync_session() as session:
                from datanika.models.user import MemberRole

                member_info = next((m for m in self.members if m.id == membership_id), None)
                old_role = member_info.role if member_info else ""
                svc.change_role(
                    session,
                    auth_state.current_org.id,
                    membership_id,
                    MemberRole(new_role),
                    actor_user_id=auth_state.current_user.id,
                )
                self._audit(
                    session,
                    auth_state.current_org.id,
                    auth_state.current_user.id,
                    "update",
                    "member",
                    resource_id=membership_id,
                    old_values={"role": old_role},
                    new_values={"role": new_role},
                )
                session.commit()
        except Exception as e:
            self.error_message = self._safe_error(e, "Failed to change role")
            return
        self.error_message = ""
        await self.load_settings()

    async def remove_member(self, membership_id: int):
        if not await self._check_role("admin"):
            return
        auth_state = await self.get_state(AuthState)
        svc = self._get_user_service()
        try:
            with get_sync_session() as session:
                # Capture member info for audit before removal. D11: the id, not the
                # address — this is one of the three sites that stored a *third party's*
                # address on a row whose `user_id` is the actor, which is exactly why a
                # `WHERE user_id = <erased>` scrub could never have been the primary
                # erasure mechanism (D12.4).
                member_info = next((m for m in self.members if m.id == membership_id), None)
                old_values = (
                    {"membership_id": membership_id, "role": member_info.role}
                    if member_info
                    else {"membership_id": membership_id}
                )
                svc.remove_member(
                    session,
                    auth_state.current_org.id,
                    membership_id,
                    actor_user_id=auth_state.current_user.id,
                )
                self._audit(
                    session,
                    auth_state.current_org.id,
                    auth_state.current_user.id,
                    "delete",
                    "member",
                    resource_id=membership_id,
                    old_values=old_values,
                )
                session.commit()
        except Exception as e:
            self.error_message = self._safe_error(e, "Failed to remove member")
            return
        self.error_message = ""
        await self.load_settings()
        yield await self._deleted_toast("settings.member_removed_toast", "Member removed")

    def set_transfer_to_email(self, value: str):
        self.transfer_to_email = value

    async def transfer_ownership(self):
        """Hand ownership to another member (SPEC_ORG_ROLES §3).

        Owner-only, and the **only** route to `MemberRole.OWNER` outside
        account creation. Audited as its own action rather than as two
        `change_role` events, so the org history says what happened.
        """
        if not await self._check_role("owner"):
            return
        auth_state = await self.get_state(AuthState)
        svc = self._get_user_service()
        successor = next(
            (m for m in self.members if m.email == self.transfer_to_email and not m.is_self),
            None,
        )
        if successor is None:
            self.error_message = "Choose the member who will become the owner"
            return
        successor_id = successor.user_id
        try:
            with get_sync_session() as session:
                svc.transfer_ownership(
                    session,
                    auth_state.current_org.id,
                    successor_id,
                    actor_user_id=auth_state.current_user.id,
                )
                self._audit(
                    session,
                    auth_state.current_org.id,
                    auth_state.current_user.id,
                    "transfer_ownership",
                    "member",
                    resource_id=successor_id,
                    old_values={"owner_user_id": auth_state.current_user.id},
                    new_values={"owner_user_id": successor_id},
                )
                session.commit()
        except Exception as e:
            self.error_message = self._safe_error(e, "Failed to transfer ownership")
            return
        self.transfer_to_email = ""
        self.error_message = ""
        # The actor is an admin from this point on. Without this the session
        # keeps its stale `owner` role and every owner-only control stays
        # rendered until the next full page load.
        auth_state._load_current_role(auth_state.current_user.id, auth_state.current_org.id)
        await self.load_settings()

    async def leave_org(self):
        """Remove your own membership (SPEC_ORG_ROLES R6, audit P8).

        Deliberately **not** gated on a minimum role: leaving is the one
        member-management action every member has. The last owner is refused
        by the service's owner-count invariant, not by a role check.

        It does need the **session** checked, though (#673), and that is the
        distinction ``_require_live_session`` exists for — every other
        member-management handler in this class calls ``_check_role("admin")``,
        and doing that here would contradict the paragraph above.
        """
        if not await self._require_live_session():
            return

        auth_state = await self.get_state(AuthState)
        svc = self._get_user_service()
        try:
            with get_sync_session() as session:
                svc.leave_org(
                    session,
                    auth_state.current_org.id,
                    actor_user_id=auth_state.current_user.id,
                )
                self._audit(
                    session,
                    auth_state.current_org.id,
                    auth_state.current_user.id,
                    "delete",
                    "member",
                    # 🚨 D11 — and this call site is in NEITHER §2a's table NOR D11's list
                    # of five. It is the purest instance of the defect (the payload was
                    # nothing but an address) on the one handler a departing member
                    # reaches by themselves. Found by the derived guard in
                    # tests/test_services/test_audit_payload_call_sites.py, which is the
                    # argument for deriving the check rather than enumerating the sites.
                    old_values={"user_id": auth_state.current_user.id},
                )
                session.commit()
        except Exception as e:
            self.error_message = self._safe_error(e, "Failed to leave organization")
            return
        self.error_message = ""

        # ⚠️ Redirecting to "/" is not enough, and this is the part that is easy
        # to get wrong. The session is still pointed at the org just left: the
        # access token carries its `org_id` claim, `current_org` still names it,
        # and `BaseState._get_org_id` reads `current_org` — so the next page
        # would go on operating inside an org this user is no longer a member
        # of, until the 10-minute token expiry happened to bite.
        #
        # Re-derive membership from the database, then either move the session
        # to a remaining org (`switch_org` mints fresh tokens and re-reads the
        # role) or end it. A member who arrived by invitation may have had only
        # this one org, so "log out" is a real branch, not a defensive stub.
        auth_state.user_orgs = [
            o for o in auth_state.user_orgs if o.id != auth_state.current_org.id
        ]
        if auth_state.user_orgs:
            return auth_state.switch_org(auth_state.user_orgs[0].id)
        return auth_state.logout()

    async def cancel_invitation(self, invitation_id: int):
        if not await self._check_role("admin"):
            return
        auth_state = await self.get_state(AuthState)
        try:
            from datanika.services.invitation_service import InvitationService

            inv_svc = InvitationService(AuthService(app_settings.secret_key))
            with get_sync_session() as session:
                inv_info = next(
                    (i for i in self.pending_invitations if i.id == invitation_id), None
                )
                # D11: the invitation id, not the invitee's address.
                old_values = (
                    {"invitation_id": invitation_id, "role": inv_info.role}
                    if inv_info
                    else {"invitation_id": invitation_id}
                )
                inv_svc.cancel_invitation(session, auth_state.current_org.id, invitation_id)
                self._audit(
                    session,
                    auth_state.current_org.id,
                    auth_state.current_user.id,
                    "delete",
                    "member",
                    resource_id=invitation_id,
                    old_values=old_values,
                )
                session.commit()
        except Exception as e:
            self.error_message = self._safe_error(e, "Failed to cancel invitation")
            return
        self.error_message = ""
        await self.load_settings()
