"""Settings page — org profile, members, API keys, notifications, and backup/import."""

import reflex as rx

from datanika.config import settings
from datanika.ui.components.api_key_row import api_key_row
from datanika.ui.components.billing_preview_modal import billing_preview_modal
from datanika.ui.components.layout import page_layout
from datanika.ui.components.quota_callout import error_or_quota_callout
from datanika.ui.state.account_state import AccountState
from datanika.ui.state.api_key_state import ApiKeyItem, ApiKeyState
from datanika.ui.state.backup_state import BackupState
from datanika.ui.state.i18n_state import I18nState
from datanika.ui.state.notification_state import ChannelItem, NotificationState
from datanika.ui.state.settings_state import InvitationItem, MemberItem, SettingsState

_t = I18nState.translations


def account_card() -> rx.Component:
    """Change (or first set) your own password — core#623, Part A.

    Rendered **first** on /settings. Every other card on the page is org-scoped
    (Organization Profile, Members, Invite, Notifications, API Keys, Backup);
    this is the first user-scoped control, so it says so in a subtitle rather
    than being buried between two org cards.

    ⚠️ ``rx.form`` + ``on_submit``, deliberately unlike every other card here.
    The rest of this page binds inputs to state vars, which for a password field
    means the plaintext is shipped to the server on **every keystroke** and then
    sits in server-side Reflex state for the life of the session. Submitting the
    form sends it once — which is what /login and /signup already do.
    """
    return rx.card(
        rx.vstack(
            rx.heading(_t["account.title"], size="4"),
            rx.text(_t["account.subtitle"], size="2", color="gray"),
            rx.cond(
                AccountState.error != "",
                rx.callout(
                    AccountState.error,
                    icon="triangle_alert",
                    color_scheme="red",
                    width="100%",
                ),
            ),
            rx.cond(
                AccountState.success,
                rx.vstack(
                    rx.callout(
                        _t["account.password_updated"],
                        icon="circle_check",
                        color_scheme="green",
                        width="100%",
                    ),
                    rx.text(
                        _t["account.review_api_keys"],
                        size="1",
                        color="gray",
                    ),
                    spacing="2",
                    width="100%",
                ),
            ),
            rx.form(
                rx.vstack(
                    rx.text(
                        rx.cond(
                            AccountState.has_password,
                            _t["account.change_password"],
                            _t["auth.set_password"],
                        ),
                        size="2",
                        weight="bold",
                    ),
                    # D6: an account that has never had a password gets no
                    # current-password field, because it could never fill one.
                    # An account that has one always faces it — including one
                    # that later linked Google, which is why this branches on a
                    # stored fact and not on ``oauth_provider``.
                    rx.cond(
                        AccountState.has_password,
                        rx.vstack(
                            rx.text(_t["account.current_password"], size="2", weight="medium"),
                            rx.input(
                                name="current_password",
                                type="password",
                                custom_attrs={"autoComplete": "current-password"},
                                width="100%",
                            ),
                            spacing="1",
                            width="100%",
                        ),
                        rx.text(_t["account.set_password_hint"], size="2", color="gray"),
                    ),
                    rx.text(_t["auth.new_password"], size="2", weight="medium"),
                    rx.input(
                        name="password",
                        type="password",
                        custom_attrs={"autoComplete": "new-password"},
                        width="100%",
                    ),
                    rx.text(_t["auth.confirm_password"], size="2", weight="medium"),
                    rx.input(
                        name="confirm",
                        type="password",
                        custom_attrs={"autoComplete": "new-password"},
                        width="100%",
                    ),
                    rx.text(_t["account.password_rules"], size="1", color="gray"),
                    rx.button(
                        rx.cond(
                            AccountState.has_password,
                            _t["account.update_password"],
                            _t["auth.set_password"],
                        ),
                        type="submit",
                        size="2",
                    ),
                    spacing="3",
                    width="100%",
                ),
                on_submit=AccountState.change_password,
                reset_on_submit=True,
                width="100%",
            ),
            # Cross-link to the published docs. Safe here and NOT on
            # /reset-password: that page still has the token in the address bar
            # when it loads, and any off-site link would carry it out on the
            # Referer.
            rx.link(
                rx.hstack(
                    rx.icon("external-link", size=14),
                    rx.text(_t["guide.docs_link"], size="1"),
                    align="center",
                    spacing="1",
                ),
                href="https://datanika.io/docs/organizations",
                is_external=True,
                color_scheme="violet",
            ),
            spacing="4",
            width="100%",
        ),
        width="100%",
    )


def org_profile_card() -> rx.Component:
    return rx.card(
        rx.vstack(
            rx.heading(_t["settings.org_profile"], size="4"),
            rx.vstack(
                rx.text(_t["common.name"], size="2", weight="medium"),
                rx.input(
                    value=SettingsState.edit_org_name,
                    on_change=SettingsState.set_edit_org_name,
                    width="100%",
                ),
                rx.text(_t["settings.slug"], size="2", weight="medium"),
                rx.input(
                    value=SettingsState.edit_org_slug,
                    on_change=SettingsState.set_edit_org_slug,
                    width="100%",
                ),
                rx.text(_t["settings.default_dbt_schema"], size="2", weight="medium"),
                rx.input(
                    value=SettingsState.edit_default_dbt_schema,
                    on_change=SettingsState.set_edit_default_dbt_schema,
                    width="100%",
                ),
                rx.button(_t["common.save"], on_click=SettingsState.update_org, size="2"),
                spacing="3",
                width="100%",
            ),
            spacing="4",
            width="100%",
        ),
        width="100%",
    )


def _remove_member_dialog(member: MemberItem) -> rx.Component:
    """Ask before evicting somebody (core#851).

    This is the only destructive control in the product that takes something
    away from a **different person**, immediately, and it fired on the first
    click. The dialog names the member by id *and* email, because the members
    table is the one place where two rows can look alike at a glance — same
    role, similar name — and the id is what an automated caller aims by.

    ⚠️ The gate stays at the call site (``rx.cond(member.can_manage, …)``)
    rather than moving in here, unlike the resource pages. ``can_manage`` is a
    **per-row** value computed in ``load_settings`` from the same predicates the
    service enforces, so it is not a page-wide ``AuthState`` var that a helper
    could re-read; folding it in would mean passing the row's own permission
    into a component that then re-tests it. ``settings`` is not one of the
    ``RESOURCE_PAGES`` the lexical gate-scan in ``test_rbac_ui_visibility.py``
    walks, so nothing is lost by leaving it outside.
    """
    return rx.alert_dialog.root(
        rx.alert_dialog.trigger(
            rx.button(
                _t["settings.remove"],
                size="1",
                color_scheme="red",
                variant="ghost",
            ),
        ),
        rx.alert_dialog.content(
            rx.alert_dialog.title(_t["settings.remove_member_title"]),
            rx.alert_dialog.description(_t["settings.remove_member_body"]),
            rx.vstack(
                rx.card(
                    rx.text("#", member.id, "  ", member.email, size="2", weight="bold"),
                    rx.text(member.role, size="1", color="var(--gray-9)"),
                ),
                rx.text(_t["settings.remove_member_reversible"], size="1", color="var(--gray-9)"),
                spacing="3",
                width="100%",
                margin_top="12px",
            ),
            rx.flex(
                rx.alert_dialog.cancel(
                    rx.button(_t["common.cancel"], variant="soft", color_scheme="gray"),
                ),
                rx.alert_dialog.action(
                    rx.button(
                        _t["settings.remove_member_confirm"],
                        color_scheme="red",
                        on_click=SettingsState.remove_member(member.id),
                    ),
                ),
                spacing="3",
                justify="end",
                margin_top="16px",
            ),
            max_width="480px",
        ),
    )


def member_row(member: MemberItem) -> rx.Component:
    """One row of the members table, rendered for what the viewer may do.

    core#658 AC4 / SPEC_ORG_ROLES §4. This used to render the role select and
    the Remove button for **every** member regardless of who was looking. The
    server-side checks are real, so nothing was exploitable — a viewer simply
    saw two controls that always failed, which is a bug report waiting to be
    filed.

    `can_manage` and `assignable_roles` are computed in `load_settings` from
    the same predicates the service enforces, so this component does not carry
    a second copy of the rules. `owner` is absent from the options entirely:
    ownership moves through Transfer ownership (R1).
    """
    return rx.table.row(
        rx.table.cell(member.email),
        rx.table.cell(member.full_name),
        rx.table.cell(
            rx.cond(
                member.can_manage,
                rx.select(
                    member.assignable_roles,
                    value=member.role,
                    on_change=lambda val: SettingsState.change_member_role(member.id, val),
                    size="1",
                    width="100%",
                ),
                rx.text(member.role, size="2"),
            ),
        ),
        rx.table.cell(
            rx.cond(
                member.is_self,
                rx.button(
                    _t["settings.leave_org"],
                    on_click=SettingsState.leave_org,
                    size="1",
                    color_scheme="red",
                    variant="ghost",
                ),
                rx.cond(
                    member.can_manage,
                    _remove_member_dialog(member),
                    rx.fragment(),
                ),
            ),
        ),
    )


def transfer_ownership_card() -> rx.Component:
    """Owner-only. The single route to `MemberRole.OWNER` (SPEC_ORG_ROLES §3).

    The successor is chosen from existing members — never an email field.
    Inviting a stranger straight into ownership is the escalation path this
    whole change closes.
    """
    return rx.cond(
        SettingsState.is_owner,
        rx.vstack(
            rx.separator(),
            rx.heading(_t["settings.transfer_ownership"], size="3"),
            rx.text(_t["settings.transfer_ownership_help"], size="2", color="gray"),
            rx.hstack(
                rx.select(
                    SettingsState.transfer_candidates,
                    value=SettingsState.transfer_to_email,
                    on_change=SettingsState.set_transfer_to_email,
                    size="2",
                ),
                rx.button(
                    _t["settings.transfer_ownership"],
                    on_click=SettingsState.transfer_ownership,
                    size="2",
                    color_scheme="amber",
                ),
                spacing="2",
            ),
            spacing="3",
            width="100%",
            align="start",
        ),
        rx.fragment(),
    )


def _invitation_row(inv: InvitationItem) -> rx.Component:
    return rx.table.row(
        rx.table.cell(inv.email),
        rx.table.cell(inv.role),
        rx.table.cell(inv.created_at),
        rx.table.cell(
            rx.button(
                _t["common.cancel"],
                on_click=SettingsState.cancel_invitation(inv.id),
                size="1",
                color_scheme="red",
                variant="ghost",
            ),
        ),
    )


def members_card() -> rx.Component:
    return rx.card(
        rx.vstack(
            rx.heading(_t["settings.members"], size="4"),
            rx.table.root(
                rx.table.header(
                    rx.table.row(
                        rx.table.column_header_cell(_t["settings.email"]),
                        rx.table.column_header_cell(_t["common.name"]),
                        rx.table.column_header_cell(_t["settings.role"]),
                        rx.table.column_header_cell(_t["common.actions"]),
                    ),
                ),
                rx.table.body(
                    rx.foreach(SettingsState.members, member_row),
                ),
                width="100%",
            ),
            rx.cond(
                SettingsState.pending_invitations.length() > 0,
                rx.vstack(
                    rx.separator(),
                    rx.heading(_t["settings.pending_invitations"], size="3"),
                    rx.table.root(
                        rx.table.header(
                            rx.table.row(
                                rx.table.column_header_cell(_t["settings.email"]),
                                rx.table.column_header_cell(_t["settings.role"]),
                                rx.table.column_header_cell(_t["settings.sent_at"]),
                                rx.table.column_header_cell(_t["common.actions"]),
                            ),
                        ),
                        rx.table.body(
                            rx.foreach(SettingsState.pending_invitations, _invitation_row),
                        ),
                        width="100%",
                    ),
                    spacing="3",
                    width="100%",
                ),
            ),
            transfer_ownership_card(),
            # The invite form is hidden from members who may not invite, for
            # the same reason the member row is gated (core#658 AC4): a
            # control that exists only to be refused is a defect.
            rx.cond(
                SettingsState.can_manage_members,
                rx.vstack(
                    rx.separator(),
                    rx.heading(_t["settings.invite_member"], size="3"),
                    rx.vstack(
                        rx.text(_t["settings.email"], size="2", weight="medium"),
                        rx.input(
                            placeholder="user@example.com",
                            value=SettingsState.invite_email,
                            on_change=SettingsState.set_invite_email,
                            width="100%",
                        ),
                        rx.text(_t["settings.role"], size="2", weight="medium"),
                        rx.hstack(
                            rx.select(
                                SettingsState.invite_roles,
                                value=SettingsState.invite_role,
                                on_change=SettingsState.set_invite_role,
                                size="2",
                            ),
                            rx.button(
                                _t["common.add"],
                                on_click=SettingsState.add_member_by_email,
                                size="2",
                            ),
                            spacing="2",
                        ),
                        spacing="3",
                        width="100%",
                    ),
                    spacing="3",
                    width="100%",
                    align="start",
                ),
                rx.fragment(),
            ),
            spacing="4",
            width="100%",
        ),
        width="100%",
    )


def _conflict_row(conflict: dict) -> rx.Component:
    return rx.hstack(
        rx.badge(conflict["type"], variant="outline"),
        rx.text(conflict["name"], weight="medium"),
        rx.select(
            [
                _t["settings.conflict_skip"],
                _t["settings.conflict_overwrite"],
                _t["settings.conflict_rename"],
            ],
            value=conflict["resolution"],
            on_change=lambda val: BackupState.set_conflict_resolution(conflict["key"], val),
            size="1",
        ),
        spacing="3",
        align="center",
        width="100%",
    )


def backup_restore_card() -> rx.Component:
    return rx.card(
        rx.vstack(
            rx.heading(_t["settings.backup_restore"], size="4"),
            # Export and restore both write their failure to
            # `BackupState.error_message`, and until core#887 nothing on this
            # page read it — so a failed export was a button click with no
            # visible result, on the same page where `SettingsState`'s errors
            # do show. The success side (`restore_result`) was already
            # rendered below, which is what made the asymmetry invisible.
            error_or_quota_callout(BackupState),
            rx.hstack(
                rx.button(
                    _t["settings.export_backup"],
                    on_click=BackupState.export_backup,
                    size="2",
                ),
                spacing="3",
            ),
            rx.text(_t["settings.export_backup_hint"], size="1", color_scheme="gray"),
            rx.separator(),
            rx.text(_t["settings.restore_backup"], size="2", weight="medium"),
            rx.upload(
                rx.button(_t["settings.restore_backup"], size="2", variant="outline"),
                accept={".json": ["application/json"]},
                max_files=1,
                on_drop=BackupState.handle_restore_upload(rx.upload_files()),  # type: ignore
                id="backup_upload",
            ),
            rx.cond(
                BackupState.restore_pending,
                rx.vstack(
                    rx.cond(
                        BackupState.restore_foreign_org != "",
                        rx.callout(
                            rx.vstack(
                                rx.text(_t["settings.restore_foreign_org"], weight="bold"),
                                rx.text(BackupState.restore_foreign_org),
                                rx.text(_t["settings.restore_foreign_org_hint"], size="1"),
                                spacing="1",
                            ),
                            icon="shield_alert",
                            color_scheme="red",
                            width="100%",
                        ),
                    ),
                    rx.cond(
                        BackupState.restore_conflicts.length() > 0,
                        rx.callout(
                            _t["settings.restore_conflicts"],
                            icon="triangle_alert",
                            color_scheme="orange",
                            width="100%",
                        ),
                    ),
                    rx.foreach(BackupState.restore_conflicts, _conflict_row),
                    rx.hstack(
                        rx.button(
                            _t["settings.confirm_restore"],
                            on_click=BackupState.confirm_restore,
                            size="2",
                        ),
                        rx.button(
                            _t["common.cancel"],
                            on_click=BackupState.cancel_restore,
                            size="2",
                            variant="outline",
                        ),
                        spacing="2",
                    ),
                    spacing="3",
                    width="100%",
                ),
            ),
            rx.cond(
                BackupState.restore_result != "",
                rx.callout(
                    rx.vstack(
                        rx.text(_t["settings.restore_success"], weight="bold"),
                        rx.text(BackupState.restore_result),
                        spacing="1",
                    ),
                    icon="check",
                    color_scheme="green",
                    width="100%",
                ),
            ),
            spacing="4",
            width="100%",
        ),
        width="100%",
    )


def _api_key_row(key: ApiKeyItem) -> rx.Component:
    """Delegated to the shared component so /api-keys and this card cannot
    describe the same Revoke button differently (core#851)."""
    return api_key_row(key)


def api_keys_card() -> rx.Component:
    """API keys management card for Settings page."""
    return rx.card(
        rx.vstack(
            rx.hstack(
                rx.heading(_t["api_keys.title"], size="4"),
                rx.spacer(),
                rx.button(
                    _t["api_keys.new"],
                    on_click=ApiKeyState.toggle_create,
                    size="2",
                ),
                width="100%",
                align="center",
            ),
            rx.cond(
                ApiKeyState.show_create,
                rx.vstack(
                    rx.separator(),
                    rx.text(_t["common.name"], size="2", weight="medium"),
                    rx.input(
                        placeholder="e.g. CI/CD deploy key",
                        value=ApiKeyState.new_key_name,
                        on_change=ApiKeyState.set_new_key_name,
                        width="100%",
                    ),
                    rx.button(
                        _t["api_keys.create"],
                        on_click=ApiKeyState.create_api_key,
                        size="2",
                    ),
                    spacing="3",
                    width="100%",
                ),
            ),
            rx.cond(
                ApiKeyState.new_key_raw != "",
                rx.callout(
                    rx.vstack(
                        rx.text(_t["api_keys.copy_warning"], weight="bold"),
                        rx.code(ApiKeyState.new_key_raw, size="2"),
                        spacing="2",
                    ),
                    icon="key",
                    color_scheme="green",
                    width="100%",
                ),
            ),
            rx.cond(
                ApiKeyState.keys.length() > 0,
                rx.table.root(
                    rx.table.header(
                        rx.table.row(
                            rx.table.column_header_cell(_t["common.name"]),
                            rx.table.column_header_cell(_t["api_keys.scopes"]),
                            rx.table.column_header_cell(_t["api_keys.created"]),
                            rx.table.column_header_cell(_t["api_keys.last_used"]),
                            rx.table.column_header_cell(_t["api_keys.expires"]),
                            rx.table.column_header_cell(_t["common.actions"]),
                        ),
                    ),
                    rx.table.body(
                        rx.foreach(ApiKeyState.keys, _api_key_row),
                    ),
                    width="100%",
                ),
                rx.text(_t["api_keys.no_keys"], color="gray"),
            ),
            spacing="4",
            width="100%",
        ),
        width="100%",
    )


def _delete_channel_dialog(ch: ChannelItem) -> rx.Component:
    """Ask before deleting an alerting channel (core#851).

    Two consequences the bare trash icon stated neither of. Deleting the
    channel stops the org's alerts for the events it carried — the failure this
    hides is the *next* failed run, unnoticed. And the row holds the Slack
    webhook URL or the Telegram bot token; those are write-only in the form and
    are not shown again, so re-creating the channel means going back to the
    third-party console for a value the product cannot give back.
    """
    return rx.alert_dialog.root(
        rx.alert_dialog.trigger(
            rx.button(
                rx.icon("trash_2", size=14),
                variant="ghost",
                size="1",
                color_scheme="red",
            ),
        ),
        rx.alert_dialog.content(
            rx.alert_dialog.title(_t["notifications.delete_title"]),
            rx.alert_dialog.description(_t["notifications.delete_body"]),
            rx.vstack(
                rx.card(
                    rx.text("#", ch.id, "  ", ch.name, size="2", weight="bold"),
                    rx.text(ch.channel_type, size="1", color="var(--gray-9)"),
                ),
                rx.callout(
                    _t["notifications.delete_secret"],
                    icon="triangle_alert",
                    color_scheme="amber",
                    width="100%",
                ),
                spacing="3",
                width="100%",
                margin_top="12px",
            ),
            rx.flex(
                rx.alert_dialog.cancel(
                    rx.button(_t["common.cancel"], variant="soft", color_scheme="gray"),
                ),
                rx.alert_dialog.action(
                    rx.button(
                        _t["notifications.delete_confirm"],
                        color_scheme="red",
                        on_click=NotificationState.delete_channel(ch.id),
                    ),
                ),
                spacing="3",
                justify="end",
                margin_top="16px",
            ),
            max_width="480px",
        ),
    )


def channel_row(ch: ChannelItem) -> rx.Component:
    return rx.table.row(
        rx.table.cell(ch.name),
        rx.table.cell(ch.channel_type),
        rx.table.cell(ch.events.join(", ")),
        rx.table.cell(
            rx.cond(ch.is_active, rx.badge("On", color_scheme="green"), rx.badge("Off")),
        ),
        rx.table.cell(
            rx.hstack(
                rx.button(
                    rx.icon("power", size=14),
                    on_click=NotificationState.toggle_channel_active(ch.id),
                    variant="ghost",
                    size="1",
                ),
                rx.button(
                    rx.icon("pencil", size=14),
                    on_click=NotificationState.edit_channel(ch.id),
                    variant="ghost",
                    size="1",
                ),
                _delete_channel_dialog(ch),
                spacing="1",
            ),
        ),
    )


def notification_form() -> rx.Component:
    return rx.vstack(
        rx.input(
            placeholder=_t["notifications.name"],
            value=NotificationState.form_name,
            on_change=NotificationState.set_form_name,
            width="100%",
        ),
        rx.select(
            ["slack", "telegram", "email", "webhook"],
            value=NotificationState.form_channel_type,
            on_change=NotificationState.set_form_channel_type,
            width="100%",
        ),
        rx.cond(
            NotificationState.form_channel_type == "slack",
            rx.input(
                placeholder=_t["notifications.webhook_url"],
                value=NotificationState.form_webhook_url,
                on_change=NotificationState.set_form_webhook_url,
                width="100%",
            ),
        ),
        rx.cond(
            NotificationState.form_channel_type == "telegram",
            rx.vstack(
                rx.input(
                    placeholder=_t["notifications.telegram_token"],
                    value=NotificationState.form_telegram_token,
                    on_change=NotificationState.set_form_telegram_token,
                    width="100%",
                ),
                rx.input(
                    placeholder=_t["notifications.telegram_chat_id"],
                    value=NotificationState.form_telegram_chat_id,
                    on_change=NotificationState.set_form_telegram_chat_id,
                    width="100%",
                ),
                spacing="2",
                width="100%",
            ),
        ),
        rx.cond(
            NotificationState.form_channel_type == "email",
            rx.input(
                placeholder=_t["notifications.email_address"],
                value=NotificationState.form_email,
                on_change=NotificationState.set_form_email,
                width="100%",
            ),
        ),
        rx.cond(
            NotificationState.form_channel_type == "webhook",
            rx.input(
                placeholder=_t["notifications.custom_url"],
                value=NotificationState.form_custom_url,
                on_change=NotificationState.set_form_custom_url,
                width="100%",
            ),
        ),
        rx.hstack(
            rx.checkbox(
                _t["notifications.on_failure"],
                checked=NotificationState.form_on_failure,
                on_change=NotificationState.set_form_on_failure,
            ),
            rx.checkbox(
                _t["notifications.on_success"],
                checked=NotificationState.form_on_success,
                on_change=NotificationState.set_form_on_success,
            ),
            spacing="4",
        ),
        rx.hstack(
            rx.button(
                _t["notifications.save"],
                on_click=NotificationState.save_channel,
                size="2",
            ),
            rx.button(
                _t["common.cancel"],
                on_click=NotificationState.toggle_form,
                variant="outline",
                size="2",
            ),
            spacing="2",
        ),
        spacing="3",
        width="100%",
    )


def notifications_card() -> rx.Component:
    return rx.card(
        rx.vstack(
            rx.hstack(
                rx.heading(_t["notifications.title"], size="4"),
                rx.spacer(),
                rx.button(
                    _t["notifications.add"],
                    on_click=NotificationState.toggle_form,
                    size="2",
                ),
                width="100%",
                align="center",
            ),
            # Save / delete / toggle failures all land in
            # `NotificationState.error_message`, which nothing rendered before
            # core#887 — so a channel that failed to save looked like a channel
            # that saved and then vanished from the table.
            error_or_quota_callout(NotificationState),
            rx.cond(
                NotificationState.show_form,
                notification_form(),
            ),
            rx.cond(
                NotificationState.channels.length() == 0,
                rx.text(
                    _t["notifications.no_channels"],
                    color="gray",
                    size="2",
                ),
                rx.table.root(
                    rx.table.header(
                        rx.table.row(
                            rx.table.column_header_cell(_t["common.name"]),
                            rx.table.column_header_cell(_t["notifications.type"]),
                            rx.table.column_header_cell(_t["notifications.events"]),
                            rx.table.column_header_cell(_t["notifications.active"]),
                            rx.table.column_header_cell(""),
                        ),
                    ),
                    rx.table.body(
                        rx.foreach(NotificationState.channels, channel_row),
                    ),
                    width="100%",
                ),
            ),
            spacing="4",
            width="100%",
        ),
        width="100%",
    )


def settings_page() -> rx.Component:
    return page_layout(
        rx.vstack(
            rx.cond(
                SettingsState.error_message != "",
                error_or_quota_callout(SettingsState),
            ),
            # First card on the page: the only user-scoped control here.
            account_card(),
            org_profile_card(),
            # V2 pricing pivot — billing preview, gated by feature flag.
            (billing_preview_modal() if settings.datanika_dual_mode_ux_enabled else rx.fragment()),
            members_card(),
            notifications_card(),
            api_keys_card(),
            backup_restore_card(),
            spacing="6",
            width="100%",
        ),
        title=_t["nav.settings"],
    )
