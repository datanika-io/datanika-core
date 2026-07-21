"""OAuth consent screen (core#394, Remote-MCP P2).

The one screen a user sees when Claude.ai / ChatGPT / Cursor asks to connect to
their Datanika org. Standalone rather than inside the app shell: the user
arrived from another product mid-handshake, and sidebar navigation here is an
invitation to abandon a flow that cannot be resumed from our side.

The design job is making three things unmissable before the Allow button —
*which* app, *which* org, and that the grant is read-only — plus how to take it
back afterwards. See ``ui/state/mcp_consent_state.py`` for why the app name and
the return address are both resolved server-side rather than read off the URL.
"""

import reflex as rx

from datanika.ui.state.auth_state import AuthState
from datanika.ui.state.i18n_state import I18nState
from datanika.ui.state.mcp_consent_state import McpConsentState

_t = I18nState.translations

_CARD = {
    "width": "100%",
    "max_width": "440px",
    "padding": "32px",
    "border": "1px solid var(--gray-a5)",
    "border_radius": "12px",
    "bg": "var(--color-background)",
}


def _brand() -> rx.Component:
    return rx.hstack(
        rx.image(src="/logo.png", width="32px", height="32px"),
        rx.heading(_t["app.name"], size="5"),
        spacing="2",
        align="center",
    )


def _fact(label: str, body: rx.Component) -> rx.Component:
    """A labelled block in the "what you are agreeing to" list."""
    return rx.vstack(
        rx.text(
            label,
            size="1",
            weight="bold",
            color="gray",
            letter_spacing="0.04em",
            text_transform="uppercase",
        ),
        body,
        spacing="1",
        align="start",
        width="100%",
    )


def _org_block() -> rx.Component:
    """Which org the grant binds to — switchable when there is a choice.

    The grant is bound to the org in the session, so a user in several orgs
    could otherwise hand an agent the keys to the wrong one without any signal
    that a choice was even being made.
    """
    return _fact(
        _t["mcp_consent.org_label"],
        rx.vstack(
            rx.cond(
                AuthState.user_orgs.length() > 1,
                rx.select(
                    AuthState.org_name_options,
                    value=AuthState.current_org.name,
                    on_change=AuthState.switch_org_by_name_in_place,
                    width="100%",
                ),
                rx.text(AuthState.current_org.name, size="3", weight="medium"),
            ),
            rx.text(_t["mcp_consent.org_hint"], size="1", color="gray"),
            spacing="1",
            width="100%",
        ),
    )


def _access_block() -> rx.Component:
    return _fact(
        _t["mcp_consent.access_label"],
        rx.vstack(
            rx.badge(
                rx.icon("eye", size=12),
                _t["mcp_consent.read_only"],
                color_scheme="green",
                variant="soft",
            ),
            rx.text(_t["mcp_consent.can_read"], size="2"),
            rx.hstack(
                rx.icon("shield", size=14, color="var(--gray-9)", flex_shrink="0"),
                rx.text(_t["mcp_consent.cannot_write"], size="2", color="gray"),
                spacing="2",
                align="start",
            ),
            spacing="2",
            align="start",
            width="100%",
        ),
    )


def _revoke_block() -> rx.Component:
    """Where the off switch is. A consent screen that hides it is a worse one."""
    return _fact(
        _t["mcp_consent.revoke_label"],
        rx.vstack(
            rx.text(
                _t["mcp_consent.revoke_creates"],
                " ",
                rx.code(McpConsentState.api_key_name, size="1"),
                size="2",
            ),
            rx.text(
                _t["mcp_consent.revoke_where"],
                " ",
                rx.link(_t["mcp_consent.revoke_link"], href="/settings", underline="always"),
                size="2",
                color="gray",
            ),
            spacing="1",
            align="start",
            width="100%",
        ),
    )


def _prompt() -> rx.Component:
    return rx.vstack(
        _brand(),
        rx.vstack(
            rx.heading(McpConsentState.client_name, size="6"),
            rx.text(_t["mcp_consent.wants_access"], size="2", color="gray"),
            spacing="1",
            align="start",
            width="100%",
        ),
        rx.divider(),
        _org_block(),
        _access_block(),
        _revoke_block(),
        rx.divider(),
        rx.hstack(
            rx.button(
                _t["mcp_consent.deny"],
                on_click=McpConsentState.deny,
                variant="soft",
                color_scheme="gray",
                size="3",
                width="100%",
                disabled=McpConsentState.is_submitting,
            ),
            rx.button(
                rx.cond(
                    McpConsentState.is_submitting,
                    _t["mcp_consent.approving"],
                    _t["mcp_consent.allow"],
                ),
                on_click=McpConsentState.allow,
                size="3",
                width="100%",
                loading=McpConsentState.is_submitting,
            ),
            spacing="3",
            width="100%",
        ),
        rx.text(
            _t["mcp_consent.signed_in_as"],
            " ",
            AuthState.current_user.email,
            size="1",
            color="gray",
            text_align="center",
            width="100%",
        ),
        spacing="4",
        align="start",
        **_CARD,
    )


def _error() -> rx.Component:
    """A refusal, not a prompt.

    Every branch here means the request cannot safely proceed, so the screen
    offers no way to proceed anyway — just what happened and the reassurance
    that nothing was shared.
    """
    return rx.vstack(
        _brand(),
        rx.icon("shield-alert", size=32, color="var(--red-9)"),
        rx.heading(_t["mcp_consent.error_title"], size="5"),
        rx.text(
            rx.match(
                McpConsentState.error_code,
                ("unknown_client", _t["mcp_consent.error_unknown_client"]),
                ("unregistered_redirect", _t["mcp_consent.error_unregistered_redirect"]),
                ("incomplete_request", _t["mcp_consent.error_incomplete"]),
                ("rejected", _t["mcp_consent.error_rejected"]),
                _t["mcp_consent.error_unavailable"],
            ),
            size="2",
            color="gray",
        ),
        rx.cond(
            McpConsentState.error_detail != "",
            rx.callout(McpConsentState.error_detail, icon="info", size="1", width="100%"),
        ),
        rx.text(_t["mcp_consent.error_nothing_shared"], size="2", weight="medium"),
        rx.link(
            rx.button(_t["mcp_consent.back_to_datanika"], variant="soft", width="100%", size="3"),
            href="/",
            width="100%",
        ),
        spacing="3",
        align="center",
        **_CARD,
    )


def oauth_consent_page() -> rx.Component:
    return rx.center(
        rx.cond(
            McpConsentState.is_loading,
            rx.vstack(rx.spinner(size="3"), spacing="3", align="center", **_CARD),
            rx.cond(McpConsentState.has_error, _error(), _prompt()),
        ),
        height="100vh",
        padding="16px",
    )
