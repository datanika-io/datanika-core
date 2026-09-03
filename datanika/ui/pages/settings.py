"""Settings page — org profile, members, API keys, notifications, and backup/import."""

import reflex as rx

from datanika.config import settings
from datanika.ui.components.api_key_row import api_key_create_controls, api_key_row
from datanika.ui.components.billing_preview_modal import billing_preview_modal
from datanika.ui.components.layout import page_layout
from datanika.ui.components.quota_callout import error_or_quota_callout
from datanika.ui.state.account_state import AccountState
from datanika.ui.state.api_key_state import ApiKeyItem, ApiKeyState
from datanika.ui.state.auth_state import AuthState
from datanika.ui.state.backup_state import BackupState
from datanika.ui.state.i18n_state import I18nState
from datanika.ui.state.notification_state import ChannelItem, NotificationState
from datanika.ui.state.settings_state import InvitationItem, MemberItem, SettingsState

_t = I18nState.translations


def _delete_account_dialog() -> rx.Component:
    """The typed-confirmation dialog for account erasure (SPEC_PII_SEPARATION D9).

    ``rx.alert_dialog``, not ``rx.dialog``: it renders a real ``role="alertdialog"``, and
    WORKFLOW_RULES §7b applies to the implementation as well as to anyone driving it — the
    destructive handler sits on the dialog's **action**, never on its trigger, so nothing
    destructive is reachable without the dialog open. This is the pattern core#804
    established for `/connections` and `/uploads`.

    **The confirmation is typed, not a second button**, and which text is required depends
    on ``has_password`` — core#623's discriminator (``password_changed_at IS NULL``),
    never ``oauth_provider``, which is backfilled onto password accounts on first social
    login and would demand a password from someone who has never had one.

    Everything the dialog must state before the confirm button is reachable: what is
    deleted, what is **kept** (billing records, 7 years, by law), that warehouse schemas in
    the customer's own account are untouched, that backups age out within 30 days, and that
    it cannot be undone.
    """
    return rx.alert_dialog.root(
        rx.alert_dialog.trigger(
            rx.button(
                _t["account.delete_button"],
                size="2",
                color_scheme="red",
                variant="soft",
                on_click=AccountState.load_delete_preconditions,
            ),
        ),
        rx.alert_dialog.content(
            rx.alert_dialog.title(_t["account.delete_confirm_heading"]),
            rx.alert_dialog.description(_t["account.delete_body"]),
            rx.vstack(
                rx.text(_t["account.delete_what_goes"], size="2"),
                rx.text(_t["account.delete_what_stays"], size="2", color="var(--gray-11)"),
                rx.text(_t["account.delete_backups_note"], size="1", color="var(--gray-9)"),
                # The org consequence, stated BEFORE the confirm control is used. D9
                # requires the choice to be put to the user rather than discovered.
                rx.cond(
                    AccountState.sole_member_org != "",
                    rx.callout(
                        _t["account.delete_org_too"],
                        icon="triangle_alert",
                        color_scheme="amber",
                        width="100%",
                    ),
                ),
                # §9a(1): the refusal, up front, naming both exits. Refusing a sole
                # owner's request with no route out is worse than the current state.
                rx.cond(
                    AccountState.blocking_org != "",
                    rx.callout(
                        _t["account.delete_last_owner"],
                        icon="octagon_alert",
                        color_scheme="red",
                        width="100%",
                    ),
                ),
                rx.cond(
                    AccountState.delete_error != "",
                    rx.callout(
                        AccountState.delete_error,
                        icon="triangle_alert",
                        color_scheme="red",
                        width="100%",
                    ),
                ),
                rx.form(
                    rx.vstack(
                        # A real `<label>` bound by `html_for`, not `rx.text` — core#720:
                        # a sibling `<p>` is not an accessible name, a screen reader
                        # announces nothing for the input, and `page.getByLabel(...)`
                        # cannot find it. Caught here by
                        # `test_input_accessible_names.py`, which is the harness that
                        # found the original eight.
                        rx.el.label(
                            rx.cond(
                                AccountState.has_password,
                                _t["account.delete_confirm_password"],
                                _t["account.delete_confirm_org_name"],
                            ),
                            html_for="delete-account-confirmation",
                            font_size="14px",
                            font_weight="500",
                        ),
                        rx.input(
                            id="delete-account-confirmation",
                            name="confirmation",
                            type=rx.cond(AccountState.has_password, "password", "text"),
                            custom_attrs={"autoComplete": "off"},
                            width="100%",
                        ),
                        rx.flex(
                            rx.alert_dialog.cancel(
                                rx.button(
                                    _t["common.cancel"],
                                    variant="soft",
                                    color_scheme="gray",
                                    type="button",
                                ),
                            ),
                            rx.button(
                                _t["account.delete_button"],
                                color_scheme="red",
                                type="submit",
                                # Disabled while a sole-owner refusal stands: the service
                                # would refuse anyway, and offering a control that cannot
                                # succeed is what §9a(1) calls worse than not offering one.
                                disabled=AccountState.blocking_org != "",
                            ),
                            spacing="3",
                            justify="end",
                            margin_top="16px",
                        ),
                        spacing="3",
                        width="100%",
                    ),
                    on_submit=AccountState.delete_account,
                    reset_on_submit=True,
                    width="100%",
                ),
                spacing="3",
                width="100%",
                margin_top="12px",
            ),
            max_width="520px",
        ),
    )


def delete_account_section() -> rx.Component:
    """Account erasure, visually separated from everything above it (D9).

    ⚠️ This used to say *"the only destructive control on /settings"*, which was
    wrong when written and got wronger: the page also renders ``remove_member``,
    ``delete_channel``, ``revoke_api_key``, ``cancel_invitation`` and
    ``leave_org``. It is the only **irreversible** one — that is the claim worth
    making, and the reason it sits behind a typed confirmation rather than a
    second button.
    """
    return rx.vstack(
        rx.divider(),
        rx.heading(_t["account.delete_heading"], size="3", color_scheme="red"),
        rx.text(_t["account.delete_body"], size="2", color="gray"),
        _delete_account_dialog(),
        spacing="2",
        width="100%",
        margin_top="8px",
    )


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
            # core#700 AC4. The only surface anywhere that reflects
            # `users.email_verified`. `/login?verified=1` reports the click, not
            # the state, so a user who missed that one redirect had no way to
            # learn their address was never confirmed.
            #
            # Rendered only when unverified: a green "confirmed" badge on every
            # account forever is noise, and the actionable case is the other one.
            rx.cond(
                ~AccountState.email_verified,
                rx.vstack(
                    rx.callout(
                        rx.vstack(
                            rx.text(_t["account.email_unverified"], weight="medium"),
                            rx.text(
                                _t["account.email_unverified_help"],
                                size="2",
                            ),
                            spacing="1",
                            align="start",
                        ),
                        icon="mail-warning",
                        color_scheme="amber",
                        width="100%",
                    ),
                    rx.hstack(
                        rx.button(
                            _t["account.resend_verification"],
                            on_click=AccountState.resend_verification,
                            size="2",
                            variant="outline",
                        ),
                        rx.text(AccountState.account_email, size="2", color="gray"),
                        align="center",
                        spacing="3",
                    ),
                    # Every outcome renders. A branch that covered only the
                    # happy path would leave a failed resend looking exactly
                    # like a successful one, which is core#700 itself.
                    rx.cond(
                        AccountState.resend_state == "queued",
                        rx.callout(
                            _t["account.resend_queued"],
                            icon="circle_check",
                            color_scheme="green",
                            width="100%",
                        ),
                    ),
                    rx.cond(
                        AccountState.resend_state == "no_relay",
                        rx.callout(
                            _t["account.resend_no_relay"],
                            icon="info",
                            color_scheme="gray",
                            width="100%",
                        ),
                    ),
                    rx.cond(
                        AccountState.resend_state == "failed",
                        rx.callout(
                            _t["account.resend_failed"],
                            icon="triangle_alert",
                            color_scheme="red",
                            width="100%",
                        ),
                    ),
                    rx.cond(
                        AccountState.resend_state == "rate_limited",
                        rx.callout(
                            _t["account.resend_rate_limited"],
                            icon="clock",
                            color_scheme="amber",
                            width="100%",
                        ),
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
            delete_account_section(),
            spacing="4",
            width="100%",
        ),
        width="100%",
    )


def _org_profile_form() -> rx.Component:
    """The editable org profile. Owner-only — ``update_org`` gates on
    ``_check_role("owner")`` (core#886)."""
    return rx.vstack(
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
    )


def _org_profile_readonly() -> rx.Component:
    """What a non-owner sees instead.

    ⚠️ Deliberately not ``rx.fragment()``. Every one of these three values is
    something an editor legitimately needs — ``default_dbt_schema`` in
    particular is where their transformations land — so hiding the card would
    trade a control that refuses for information that is missing, which is a
    worse bug than the one core#886 is fixing. The gate removes the *inputs*
    and the Save button, not the facts.
    """
    return rx.vstack(
        rx.text(_t["common.name"], size="2", weight="medium"),
        rx.text(SettingsState.org_name, size="2"),
        rx.text(_t["settings.slug"], size="2", weight="medium"),
        rx.text(SettingsState.org_slug, size="2"),
        rx.text(_t["settings.default_dbt_schema"], size="2", weight="medium"),
        rx.text(SettingsState.edit_default_dbt_schema, size="2"),
        spacing="3",
        width="100%",
        align="start",
    )


def org_profile_card() -> rx.Component:
    return rx.card(
        rx.vstack(
            rx.heading(_t["settings.org_profile"], size="4"),
            rx.cond(
                SettingsState.is_owner,
                _org_profile_form(),
                _org_profile_readonly(),
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


def _leave_org_dialog() -> rx.Component:
    """Ask before signing somebody out of the product (core#851, SPEC_MUTATION_FEEDBACK D7a).

    This was the **twelfth** one-click destructive control and the one core#851's
    own sweep could not see: ``leave_org`` takes no arguments, so it is written
    ``on_click=SettingsState.leave_org`` — an ``ast.Attribute``, not an
    ``ast.Call``, and the guard's visitor only had a ``visit_Call`` arm. Widening
    the verb list would never have found it.

    Three things make it unlike every other entry on that list:

    * It is deliberately **not** role-gated (``SPEC_ORG_ROLES`` R6 — leaving is
      the one action every member has), so every member sees it, including the
      ones with no way back.
    * Every other entry deletes a **row**. This one removes the actor's access to
      all of them. The membership is soft-deleted, so an operator can restore it;
      the user cannot, cannot see that it is restorable, and has just been
      ejected from the surface that would have said so.
    * 🚨 **The handler ends in `switch_org` or `logout`, and this one button
      produces both.** Disclosing which is the substance of the fix — a dialog
      that only asks "are you sure?" leaves the more serious outcome undisclosed,
      which is the failure this whole class is about.

    ⚠️ **No success toast, and that is a decision, not an omission.** ``leave_org``
    ends in ``return <event>``; adding a ``yield`` makes it an async generator,
    where ``return`` *with a value* is a ``SyntaxError``. Yielding the events
    instead compiles — and the event is a **navigation**, so the toast would race
    a redirect and render on ``/login`` if at all. The dialog before the act is
    the acknowledgement; the destination is the outcome. See D7a.
    """
    return rx.alert_dialog.root(
        rx.alert_dialog.trigger(
            rx.button(
                _t["settings.leave_org"],
                size="1",
                color_scheme="red",
                variant="ghost",
            ),
        ),
        rx.alert_dialog.content(
            rx.alert_dialog.title(_t["settings.leave_org_title"]),
            rx.alert_dialog.description(_t["settings.leave_org_body"]),
            rx.vstack(
                rx.card(
                    rx.text(SettingsState.org_name, size="2", weight="bold"),
                    rx.text(SettingsState.org_slug, size="1", color="var(--gray-9)"),
                ),
                # Exactly one of these renders. They are branches of the same
                # `rx.cond`, so "both" and "neither" are unreachable by
                # construction rather than by a second rule that could drift.
                rx.cond(
                    SettingsState.leaving_signs_me_out,
                    rx.callout(
                        _t["settings.leave_org_signs_you_out"],
                        icon="log_out",
                        color_scheme="red",
                        width="100%",
                    ),
                    rx.callout(
                        _t["settings.leave_org_switches_you"]
                        + " "
                        + SettingsState.leaving_switches_me_to,
                        icon="arrow_right_left",
                        color_scheme="amber",
                        width="100%",
                    ),
                ),
                rx.text(_t["settings.leave_org_reversible"], size="1", color="var(--gray-9)"),
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
                        _t["settings.leave_org_confirm"],
                        color_scheme="red",
                        on_click=SettingsState.leave_org,
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
                _leave_org_dialog(),
                rx.cond(
                    member.can_manage,
                    _remove_member_dialog(member),
                    rx.fragment(),
                ),
            ),
        ),
    )


def _transfer_ownership_dialog() -> rx.Component:
    """Ask before handing the org away (SPEC_MUTATION_FEEDBACK D7b).

    The only route to ``MemberRole.OWNER``, and it demotes the actor in the same
    transaction — ``transfer_ownership`` re-reads their role immediately
    afterwards. **Only the new owner can transfer it back**, so the undo lives in
    somebody else's hands: ``leave_org``'s shape applied to control rather than
    to access.

    ⚠️ The disabled state is not decoration. ``transfer_ownership`` refuses with
    *"Choose the member who will become the owner"* when the select is empty, and
    offering a confirmation for an act that cannot succeed is the pattern
    ``_delete_account_dialog`` avoids for the sole-owner case.

    ``settings.transfer_ownership_help`` already states the consequence honestly —
    but that is help text on a card, read before the select is touched, not a
    confirmation at the moment of the act. Both, for the same reason core#851
    kept row-content aiming *and* added a dialog: they are different guarantees.
    """
    return rx.alert_dialog.root(
        rx.alert_dialog.trigger(
            rx.button(
                _t["settings.transfer_ownership"],
                size="2",
                color_scheme="amber",
                disabled=SettingsState.transfer_to_email == "",
            ),
        ),
        rx.alert_dialog.content(
            rx.alert_dialog.title(_t["settings.transfer_ownership_title"]),
            rx.alert_dialog.description(_t["settings.transfer_ownership_body"]),
            rx.vstack(
                rx.card(
                    rx.text(SettingsState.transfer_to_email, size="2", weight="bold"),
                    rx.text(SettingsState.org_name, size="1", color="var(--gray-9)"),
                ),
                rx.callout(
                    _t["settings.transfer_ownership_irreversible"],
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
                        _t["settings.transfer_ownership_confirm"],
                        color_scheme="amber",
                        on_click=SettingsState.transfer_ownership,
                    ),
                ),
                spacing="3",
                justify="end",
                margin_top="16px",
            ),
            max_width="480px",
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
                _transfer_ownership_dialog(),
                spacing="2",
            ),
            spacing="3",
            width="100%",
            align="start",
        ),
        rx.fragment(),
    )


def _cancel_invitation_dialog(inv: InvitationItem) -> rx.Component:
    """Ask before revoking a pending invitation (core#851, SPEC_MUTATION_FEEDBACK D7c).

    core#851's **eleventh** site, and the one its sweep missed for a different
    reason from ``leave_org``: the sweep matched destructive *verbs*
    (``delete|revoke|remove|purge``) and this handler is spelled ``cancel_``. Two
    independent blind spots, one predicate and one matcher, in the same census.

    ⚠️ **This is the lightest of the three dialogs on purpose.** Re-inviting fully
    restores the state and nobody loses access they already had, so it is a plain
    confirm rather than a warning — core#851 rated it *"low, and lower than the
    ten already listed"*, which is right.

    It earns one anyway because it sits **between two controls on the same card
    that both confirm** — ``_remove_member_dialog`` above and
    ``_delete_channel_dialog`` below. On a page that has established the pattern,
    an absent dialog reads as a claim that this action is safe.
    """
    return rx.alert_dialog.root(
        rx.alert_dialog.trigger(
            rx.button(
                _t["common.cancel"],
                size="1",
                color_scheme="red",
                variant="ghost",
            ),
        ),
        rx.alert_dialog.content(
            rx.alert_dialog.title(_t["settings.cancel_invitation_title"]),
            rx.vstack(
                rx.card(
                    rx.text("#", inv.id, "  ", inv.email, size="2", weight="bold"),
                    rx.text(inv.role, size="1", color="var(--gray-9)"),
                ),
                rx.text(
                    _t["settings.cancel_invitation_reversible"],
                    size="1",
                    color="var(--gray-9)",
                ),
                spacing="3",
                width="100%",
                margin_top="12px",
            ),
            rx.flex(
                # ⚠️ Not `common.cancel` here: on this one dialog "Cancel" is
                # the *destructive* verb — it labels the trigger. A back-out
                # button reading "Cancel" beside a confirm button reading
                # "Yes, cancel invitation" is a coin flip, not a choice.
                rx.alert_dialog.cancel(
                    rx.button(
                        _t["settings.cancel_invitation_keep"],
                        variant="soft",
                        color_scheme="gray",
                    ),
                ),
                rx.alert_dialog.action(
                    rx.button(
                        _t["settings.cancel_invitation_confirm"],
                        color_scheme="red",
                        on_click=SettingsState.cancel_invitation(inv.id),
                    ),
                ),
                spacing="3",
                justify="end",
                margin_top="16px",
            ),
            max_width="480px",
        ),
    )


def _invitation_row(inv: InvitationItem) -> rx.Component:
    """One pending invitation.

    ⚠️ ``cancel_invitation`` gates on ``_check_role("admin")`` and this button
    had no gate, sitting between two controls on this very card that do —
    ``add_member_by_email`` above it under ``can_manage_members`` and
    ``remove_member`` beside it under ``member.can_manage``. It survived
    core#658's sweep and core#851's because both derived their lists from
    destructive *verbs* (``delete|revoke|remove|purge``) and this handler is
    spelled ``cancel_``. ``can_manage_members`` is the matching gate rather
    than a new one: it is ``bool(assignable_roles(current_role))``, true for
    exactly admin and owner, which is the threshold the handler enforces.
    core#886.
    """
    return rx.table.row(
        rx.table.cell(inv.email),
        rx.table.cell(inv.role),
        rx.table.cell(inv.created_at),
        rx.table.cell(
            rx.cond(
                SettingsState.can_manage_members,
                _cancel_invitation_dialog(inv),
                rx.fragment(),
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
    """API keys management card for Settings page.

    The create block is delegated to the same helper ``/api-keys`` uses, behind
    the same ``can_administer`` gate, so the two surfaces cannot end up with the
    control on one and not the other (core#886). The keys *table* stays
    ungated — listing your org's keys and their last-used dates is a read.
    """
    return rx.card(
        rx.vstack(
            rx.hstack(
                rx.heading(_t["api_keys.title"], size="4"),
                width="100%",
                align="center",
            ),
            rx.cond(
                AuthState.can_administer,
                api_key_create_controls(),
                rx.fragment(),
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


def _channel_actions(ch: ChannelItem) -> rx.Component:
    """Toggle / edit / delete for one alerting channel — admin only.

    ``toggle_channel_active``, ``save_channel``, ``delete_channel`` **and
    ``edit_channel``** all gate on ``_check_role("admin")``.

    🚨 This docstring used to say ``edit_channel`` did not gate "because it
    persists nothing — it copies the row into the form", and that hiding the
    button was enough since "the only thing it leads to is a Save that would
    refuse". Both halves were wrong, and the same reasoning had left
    ``ConnectionState.edit_connection`` open (core#972):

    * **Reading the secret is the harm.** ``edit_channel`` returns the stored
      webhook URL or bot token to the caller; whether it then writes anything is
      beside the point.
    * **Hiding a button is not a gate.** A Reflex event handler is dispatched by
      **name** over the websocket. Which buttons were rendered has no bearing on
      which events can be sent, which is exactly why the three mutating handlers
      here check the role in the handler rather than trusting this ``rx.cond``.

    core#886 for the member-visible list, core#972 for the gate.
    """
    return rx.hstack(
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
    )


def _delivery_badge(ch: ChannelItem) -> rx.Component:
    """Answer "is this channel working?", which is not "is it switched on?".

    The old cell rendered ``is_active`` alone as a green **On** — a green
    affirmative beside an email channel type that had never dispatched anything,
    on any org, in any edition (core#652). A green badge is an assertion, and
    that one was false for the life of the feature.

    Three states, deliberately, because two cannot carry the distinction that
    matters: **off**, **on but never attempted** (grey — we are claiming nothing),
    and **on with a delivery record** (green or red on the real outcome). A
    channel that has never delivered must not look identical to one that has.
    """
    return rx.cond(
        ch.is_active,
        rx.cond(
            ch.last_status == "",
            rx.badge(_t["notifications.never_delivered"], color_scheme="gray"),
            rx.cond(
                ch.last_status == "success",
                rx.badge(_t["notifications.delivering"], color_scheme="green"),
                rx.cond(
                    ch.last_status == "skipped",
                    rx.badge(_t["notifications.not_delivering"], color_scheme="amber"),
                    rx.badge(_t["notifications.delivery_failed"], color_scheme="red"),
                ),
            ),
        ),
        rx.badge(_t["notifications.off"]),
    )


def channel_row(ch: ChannelItem) -> rx.Component:
    return rx.table.row(
        rx.table.cell(ch.name),
        rx.table.cell(ch.channel_type),
        rx.table.cell(ch.events.join(", ")),
        rx.table.cell(
            rx.vstack(
                _delivery_badge(ch),
                # The reason, where there is one. AC5: a webhook returning 500
                # must show as failed **in the UI, without reading a log** — a
                # log line on a box the user cannot read is not feedback.
                rx.cond(
                    ch.last_error != "",
                    rx.text(ch.last_error, size="1", color="var(--gray-9)"),
                    rx.fragment(),
                ),
                spacing="1",
                align="start",
            ),
        ),
        rx.table.cell(
            rx.cond(AuthState.can_administer, _channel_actions(ch), rx.fragment()),
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
    """Alerting channels, rendered for what the viewer may actually do.

    The channel *list* stays visible to everyone — knowing where run failures
    are announced is something an editor needs. Add / edit / toggle / delete
    are admin (core#886).
    """
    return rx.card(
        rx.vstack(
            rx.hstack(
                rx.heading(_t["notifications.title"], size="4"),
                rx.spacer(),
                rx.cond(
                    AuthState.can_administer,
                    rx.button(
                        _t["notifications.add"],
                        on_click=NotificationState.toggle_form,
                        size="2",
                    ),
                    rx.fragment(),
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
                AuthState.can_administer & NotificationState.show_form,
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
            # Both of this card's entry points — `export_backup` and
            # `handle_restore_upload` — gate on `_check_role("admin")`, and
            # unlike the org profile there is nothing here to read: the card is
            # two operations and a hint. So the whole card is gated, not its
            # controls (core#886).
            rx.cond(AuthState.can_administer, backup_restore_card(), rx.fragment()),
            spacing="6",
            width="100%",
        ),
        title=_t["nav.settings"],
    )
