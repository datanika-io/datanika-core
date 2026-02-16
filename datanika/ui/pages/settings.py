"""Settings page — org profile and member management."""

import reflex as rx

from etlfabric.ui.components.layout import page_layout
from etlfabric.ui.state.settings_state import MemberItem, SettingsState


def org_profile_card() -> rx.Component:
    return rx.card(
        rx.vstack(
            rx.heading("Organization Profile", size="4"),
            rx.vstack(
                rx.text("Name", size="2", weight="medium"),
                rx.input(
                    value=SettingsState.edit_org_name,
                    on_change=SettingsState.set_edit_org_name,
                    width="100%",
                ),
                rx.text("Slug", size="2", weight="medium"),
                rx.input(
                    value=SettingsState.edit_org_slug,
                    on_change=SettingsState.set_edit_org_slug,
                    width="100%",
                ),
                rx.button("Save", on_click=SettingsState.update_org, size="2"),
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
            ),
        ),
        rx.table.cell(
            rx.button(
                "Remove",
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
            rx.heading("Members", size="4"),
            rx.table.root(
                rx.table.header(
                    rx.table.row(
                        rx.table.column_header_cell("Email"),
                        rx.table.column_header_cell("Name"),
                        rx.table.column_header_cell("Role"),
                        rx.table.column_header_cell("Actions"),
                    ),
                ),
                rx.table.body(
                    rx.foreach(SettingsState.members, member_row),
                ),
                width="100%",
            ),
            rx.separator(),
            rx.heading("Invite Member", size="3"),
            rx.hstack(
                rx.input(
                    placeholder="Email address",
                    value=SettingsState.invite_email,
                    on_change=SettingsState.set_invite_email,
                ),
                rx.select(
                    ["owner", "admin", "editor", "viewer"],
                    value=SettingsState.invite_role,
                    on_change=SettingsState.set_invite_role,
                    size="2",
                ),
                rx.button("Add", on_click=SettingsState.add_member_by_email, size="2"),
                spacing="2",
                align="end",
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
            spacing="6",
            width="100%",
        ),
        title="Settings",
    )
