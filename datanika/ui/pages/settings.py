"""Settings page — org profile, member management, and backup/restore."""

import reflex as rx

from datanika.ui.components.layout import page_layout
from datanika.ui.state.backup_state import BackupState
from datanika.ui.state.i18n_state import I18nState
from datanika.ui.state.settings_state import MemberItem, SettingsState

_t = I18nState.translations


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
                rx.button(_t["common.save"], on_click=SettingsState.update_org, size="2"),
                spacing="3",
                width="100%",
            ),
            spacing="4",
            width="100%",
        ),
        width="100%",
    )


def member_row(member: MemberItem) -> rx.Component:
    return rx.table.row(
        rx.table.cell(member.email),
        rx.table.cell(member.full_name),
        rx.table.cell(
            rx.select(
                ["owner", "admin", "editor", "viewer"],
                value=member.role,
                on_change=lambda val: SettingsState.change_member_role(member.id, val),
                size="1",
                width="100%",
            ),
        ),
        rx.table.cell(
            rx.button(
                _t["settings.remove"],
                on_click=SettingsState.remove_member(member.id),
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
            rx.separator(),
            rx.heading(_t["settings.invite_member"], size="3"),
            rx.hstack(
                rx.input(
                    placeholder=_t["settings.ph_email"],
                    value=SettingsState.invite_email,
                    on_change=SettingsState.set_invite_email,
                    width="100%",
                ),
                rx.select(
                    ["owner", "admin", "editor", "viewer"],
                    value=SettingsState.invite_role,
                    on_change=SettingsState.set_invite_role,
                    size="2",
                    width="100%",
                ),
                rx.button(_t["common.add"], on_click=SettingsState.add_member_by_email, size="2"),
                spacing="2",
                align="end",
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
            rx.hstack(
                rx.button(
                    _t["settings.export_backup"],
                    on_click=BackupState.export_backup,
                    size="2",
                ),
                spacing="3",
            ),
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
                BackupState.restore_conflicts.length() > 0,
                rx.vstack(
                    rx.callout(
                        _t["settings.restore_conflicts"],
                        icon="triangle_alert",
                        color_scheme="orange",
                        width="100%",
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


def settings_page() -> rx.Component:
    return page_layout(
        rx.vstack(
            rx.cond(
                SettingsState.error_message != "",
                rx.callout(
                    SettingsState.error_message,
                    icon="triangle_alert",
                    color_scheme="red",
                    width="100%",
                ),
            ),
            org_profile_card(),
            members_card(),
            backup_restore_card(),
            spacing="6",
            width="100%",
        ),
        title=_t["nav.settings"],
    )
