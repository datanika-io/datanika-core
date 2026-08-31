"""Sidebar navigation and page wrapper layout."""

import reflex as rx

from datanika.ui.components.language_switcher import language_switcher
from datanika.ui.components.notification_bell import notification_bell
from datanika.ui.state.auth_state import AuthState
from datanika.ui.state.i18n_state import I18nState

_t = I18nState.translations

# Plugin extension point — plugins append (text_key, href, icon) tuples here.
extra_sidebar_links: list[tuple[str, str, str]] = []


def sidebar_link(text: rx.Var[str], href: str, icon: str) -> rx.Component:
    return rx.link(
        rx.hstack(
            rx.icon(icon, size=18),
            rx.text(text, size="3"),
            spacing="2",
            align="center",
            width="100%",
            padding_x="12px",
            padding_y="8px",
            border_radius="6px",
            _hover={"bg": "var(--gray-a3)"},
        ),
        href=href,
        underline="none",
        width="100%",
    )


def sidebar_user_section() -> rx.Component:
    return rx.vstack(
        rx.separator(),
        language_switcher(),
        sidebar_link(_t["nav.settings"], "/settings", "settings"),
        *[sidebar_link(_t[key], href, icon) for key, href, icon in extra_sidebar_links],
        rx.cond(
            AuthState.user_orgs.length() > 1,
            rx.select(
                AuthState.org_name_options,
                value=AuthState.current_org.name,
                on_change=AuthState.switch_org_by_name,
                size="1",
                width="100%",
            ),
        ),
        rx.hstack(
            rx.vstack(
                rx.text(AuthState.current_user.full_name, size="2", weight="medium"),
                rx.text(AuthState.current_org.name, size="1", color="gray"),
                spacing="0",
            ),
            rx.spacer(),
            notification_bell(),
            rx.icon_button(
                rx.icon("log-out", size=16),
                on_click=AuthState.logout,
                variant="ghost",
                size="1",
            ),
            width="100%",
            padding_x="12px",
            padding_y="8px",
            align="center",
        ),
        spacing="1",
        width="100%",
        padding="8px",
    )


#: The published policy documents. Absolute and cross-origin on purpose:
#: a root-relative "/terms/" with ``is_external`` opens app.datanika.io/terms/,
#: a route the Reflex app does not serve (#418). ``is_external`` in Reflex means
#: *new tab*, which is what we want here — a user reading the terms mid-signup
#: must not lose what they typed.
TERMS_URL = "https://datanika.io/terms/"
PRIVACY_URL = "https://datanika.io/privacy/"


def legal_links() -> rx.Component:
    """Terms and Privacy, reachable from inside the product (#656).

    Rendered at the foot of the sidebar, so it is one click away from every
    authenticated page. Before this the application referenced neither document
    anywhere — a logged-in user could not find the terms they had agreed to.
    """
    return rx.hstack(
        rx.link(
            rx.text(_t["legal.terms"], size="1"),
            href=TERMS_URL,
            is_external=True,
            color_scheme="gray",
        ),
        # A list separator between two standalone links, not a connective inside
        # a sentence — so unlike the signup line (#682) it carries no grammar and
        # needs no locale to place it. Do not "fix" this by analogy.
        rx.text("·", size="1", color="gray"),
        rx.link(
            rx.text(_t["legal.privacy"], size="1"),
            href=PRIVACY_URL,
            is_external=True,
            color_scheme="gray",
        ),
        spacing="2",
        align="center",
        justify="center",
        width="100%",
        padding_bottom="8px",
    )


def sidebar() -> rx.Component:
    return rx.box(
        rx.vstack(
            rx.hstack(
                rx.image(src="/logo.png", width="32px", height="32px"),
                rx.heading(_t["app.name"], size="5"),
                spacing="2",
                align="center",
                padding="16px",
            ),
            rx.separator(),
            rx.vstack(
                sidebar_link(_t["nav.dashboard"], "/", "layout-dashboard"),
                sidebar_link(_t["nav.connections"], "/connections", "plug"),
                sidebar_link(_t["nav.uploads"], "/uploads", "upload"),
                sidebar_link(_t["nav.transformations"], "/transformations", "code"),
                sidebar_link(_t["nav.pipelines"], "/pipelines", "git-branch"),
                sidebar_link(_t["nav.models"], "/models", "database"),
                sidebar_link(_t["nav.dependencies"], "/dag", "network"),
                sidebar_link(_t["nav.schedules"], "/schedules", "clock"),
                sidebar_link(_t["nav.runs"], "/runs", "play"),
                sidebar_link(_t["nav.audit_log"], "/audit-log", "scroll-text"),
                spacing="1",
                width="100%",
                padding="8px",
            ),
            rx.spacer(),
            sidebar_user_section(),
            legal_links(),
            spacing="0",
            height="100vh",
        ),
        width="240px",
        border_right="1px solid var(--gray-a5)",
        bg="var(--gray-a2)",
        position="fixed",
        left="0",
        top="0",
    )


def verification_mail_notice() -> rx.Component:
    """Say what happened to the confirmation mail after signup (core#700 AC1).

    Lives in the shell rather than on /signup because signup authenticates the user and
    redirects straight to their destination, so a callout on the signup page would never
    be seen. Dismissible, since it is an acknowledgement rather than a standing state.

    **Nothing renders for ``no_relay``** on purpose: a self-hosted deployment with no SMTP
    relay is a normal deployment, and telling that operator we could not send their mail
    would be a false alarm. Only a real failure gets the warning.
    """
    return rx.fragment(
        rx.cond(
            AuthState.verification_mail_state == "queued",
            rx.callout(
                rx.hstack(
                    rx.text(_t["auth.verification_mail_sent"]),
                    rx.spacer(),
                    rx.button(
                        _t["common.dismiss"],
                        on_click=AuthState.dismiss_verification_notice,
                        size="1",
                        variant="ghost",
                    ),
                    width="100%",
                    align="center",
                ),
                icon="mail_check",
                color_scheme="green",
                width="100%",
            ),
        ),
        rx.cond(
            AuthState.verification_mail_state == "failed",
            rx.callout(
                rx.hstack(
                    rx.vstack(
                        rx.text(_t["auth.verification_mail_failed"], weight="medium"),
                        rx.text(_t["auth.verification_mail_failed_help"], size="2"),
                        spacing="1",
                        align="start",
                    ),
                    rx.spacer(),
                    rx.button(
                        _t["common.dismiss"],
                        on_click=AuthState.dismiss_verification_notice,
                        size="1",
                        variant="ghost",
                    ),
                    width="100%",
                    align="start",
                ),
                icon="mail_warning",
                color_scheme="amber",
                width="100%",
            ),
        ),
    )


def signed_out_panel() -> rx.Component:
    """Shown when a mutating handler discovered the session had ended (#673).

    A handler cannot navigate, so unlike the page-load path it cannot hand the
    user to ``/login?expired=1`` with a query parameter. It clears the session
    instead, which drops this tab into ``is_authenticated``'s false branch —
    previously a bare spinner, and a spinner forever is indistinguishable from a
    hang. This says what happened and offers the way back.
    """
    return rx.center(
        rx.card(
            rx.vstack(
                # "log-out", hyphenated, matching the sidebar's proven spelling.
                # Reflex does not raise on an unknown icon name — it warns on
                # stderr and silently renders ``circle_help``.
                rx.icon("log-out", size=28, color="var(--amber-9)"),
                rx.heading(_t["auth.signed_out_title"], size="5"),
                rx.text(_t["auth.signed_out_body"], size="2", align="center"),
                rx.link(
                    rx.button(_t["auth.signed_out_cta"], size="3"),
                    href="/login?expired=1",
                    underline="none",
                ),
                spacing="3",
                align="center",
            ),
            padding="32px",
            max_width="420px",
        ),
        height="100vh",
    )


def page_layout(*children, title: rx.Var[str] | str = "") -> rx.Component:
    return rx.cond(
        AuthState.is_authenticated,
        rx.box(
            sidebar(),
            rx.box(
                rx.vstack(
                    verification_mail_notice(),
                    rx.cond(title != "", rx.heading(title, size="6"), rx.fragment()),
                    *children,
                    spacing="4",
                    width="100%",
                ),
                margin_left="240px",
                padding="24px",
                width="calc(100% - 240px)",
            ),
            rx.toast.provider(duration=3000),
        ),
        # Not signed in. Two very different reasons land here: the page is still
        # hydrating (spinner), or a handler just ended the session (#673).
        rx.cond(
            AuthState.session_expired,
            signed_out_panel(),
            rx.center(
                rx.spinner(size="3"),
                height="100vh",
            ),
        ),
    )
