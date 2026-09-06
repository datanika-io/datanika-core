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


def invite_notice() -> rx.Component:
    """Say that the invitation did not take (core#981 AC2).

    Lives in the shell for the same reason ``verification_mail_notice`` does: signup
    authenticates and redirects, so a callout on /signup would never be seen.

    **The failure it reports is invisible from anywhere else.** The user clicked an
    invitation link, signed up, and is now signed in — in a personal org, not the team's.
    Nothing about that screen looks wrong. The expired-link case is the common one and was
    indistinguishable from success.

    ⚠️ **Amber, not red, and dismissible.** Nothing the user did failed: their account
    exists, they are signed in, and the only thing that did not happen is the join. A red
    error would send them to check the account they just successfully created.

    One value covers every cause on purpose — see ``AuthState.invite_notice``.
    """
    return rx.cond(
        AuthState.invite_notice == "not_applied",
        rx.callout(
            rx.hstack(
                rx.vstack(
                    rx.text(_t["auth.invite_not_applied"], weight="medium"),
                    rx.text(_t["auth.invite_not_applied_help"], size="2"),
                    spacing="1",
                    align="start",
                ),
                rx.spacer(),
                rx.button(
                    _t["common.dismiss"],
                    on_click=AuthState.dismiss_invite_notice,
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


def app_shell_skeleton() -> rx.Component:
    """Shown while a protected page is entering the backend event path (#1090).

    ``page_layout``'s ``is_authenticated`` false arm is reached for the whole of
    that window, and it used to be ``rx.center(rx.spinner(size="3"))``. Entering
    the event path costs **1933–9499 ms** on production — measured signed out,
    five samples, and the slowest was the *control* page with one no-op
    ``on_load`` handler. Reflex's fast path is keyed on whether any handler
    exists, never on what one costs, so there is no payload to optimise here and
    no "the 3.7 seconds" to quote.

    ``signed_out_panel()`` below already makes the argument this component acts
    on — *a spinner forever is indistinguishable from a hang*. #673 accepted it
    for the session-ended branch and left the hydrating branch beside it as the
    bare spinner it was arguing against.

    🚨 **Chrome only, and that is a product decision rather than minimalism.**
    The visitor's destination is not yet known — that is the entire problem. An
    authenticated user is about to see this chrome filled in; a signed-out one is
    about to be moved to ``/login``. A branded shell is honest in both branches;
    placeholder rows, counts or chart shapes are honest in one and a fabrication
    in the other. Nothing here links anywhere, because a link offered to a
    visitor who may never be signed in is clickable during the window.

    ⚠️ **It removes 0 ms.** It is not a performance fix and must not be described
    as one.

    The product name is deliberately *outside* the ``rx.skeleton`` shimmer:
    Radix's Skeleton hides its children, and a real text node is what makes
    ``first-contentful-paint`` mean *"the user saw something"*. FCP is
    structurally blind to a spinner — no text node, no image, no SVG — which is
    how #1090 came to be filed as a blank screen.
    """
    return rx.box(
        rx.box(
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
                    *[
                        rx.skeleton(height="32px", width="100%", border_radius="6px")
                        for _ in range(8)
                    ],
                    spacing="2",
                    width="100%",
                    padding="8px",
                ),
                spacing="0",
                width="100%",
                height="100vh",
            ),
            width="240px",
            border_right="1px solid var(--gray-a5)",
            bg="var(--gray-a2)",
            position="fixed",
            left="0",
            top="0",
        ),
        rx.box(
            rx.vstack(
                rx.skeleton(height="32px", width="240px", border_radius="6px"),
                rx.skeleton(height="140px", width="100%", border_radius="8px"),
                rx.skeleton(height="140px", width="100%", border_radius="8px"),
                spacing="4",
                width="100%",
            ),
            margin_left="240px",
            padding="24px",
            width="calc(100% - 240px)",
        ),
        width="100%",
        # A shimmer says nothing to assistive technology, and this is the one
        # affordance here that costs no locale key — so AC4.3 does not trade
        # against it.
        custom_attrs={"aria-busy": "true"},
    )


def connection_lost_banner() -> rx.Component:
    """Say so when the websocket is down, on every page (#744).

    Reflex does not send an event synchronously with the click:
    ``addEvents`` → ``queueEvents`` → ``processEvent``, and ``processEvent``
    opens by returning early when the socket is not connected — its own comment
    for that path reads *"otherwise we throw the event into the void"*. The
    event stays queued and is replayed on reconnect, so nothing errors; the
    button simply does nothing for as long as the outage lasts, and is lost
    outright if the page unloads first. QA measured that on staging, four times.

    🚨 **This must stay outside ``page_layout``'s ``is_authenticated``
    conditional.** ``is_authenticated`` reads a state var, so a page whose state
    never arrived evaluates it false and lands on the bare spinner — which
    ``signed_out_panel``'s docstring already names as indistinguishable from a
    hang. That spinner is exactly where a dropped socket puts you, so a banner
    mounted in the true branch is invisible to the only person who needs it.
    ``test_the_banner_is_not_inside_the_authenticated_branch`` pins that.

    Reflex's default overlay does carry a corner ``wifi-off`` pulser and a
    transient toast; neither stays on screen, and neither survives the spinner.

    🔑 **The translated text still renders on a page that never hydrated**,
    which is not obvious and is what makes this work at all. ``compile_state``
    bakes every state var's *default* into the client bundle at build time, and
    ``I18nState.translations`` defaults to the full English dictionary — so the
    lookup resolves from the compiled bundle with no websocket, and upgrades to
    the user's locale once one exists. A banner whose own text needed the
    connection would be blank exactly when it fires.
    """
    return rx.connection_banner(
        rx.box(
            rx.text(_t["app.connection_lost"], size="2", weight="medium", color="black"),
            background_color="var(--amber-9)",
            width="100%",
            padding="8px 16px",
            text_align="center",
            position="fixed",
            top="0",
            left="0",
            z_index="1000",
        )
    )


def action_error_notice() -> rx.Component:
    """Render why the last mutating action was refused (#744).

    ``BaseState._check_role`` wrote the reason to the *substate's*
    ``error_message``, which for 10 of the 15 state classes that assign it is
    rendered by nothing at all (#887). So a Run pressed without the editor role
    produced no run row, no toast and no error — the same user experience as a
    click dropped by a dead socket, from a completely different cause.
    """
    return rx.cond(
        AuthState.action_error != "",
        rx.callout(
            rx.hstack(
                rx.text(AuthState.action_error),
                rx.spacer(),
                rx.button(
                    _t["common.dismiss"],
                    on_click=AuthState.dismiss_action_error,
                    size="1",
                    variant="ghost",
                ),
                width="100%",
                align="center",
            ),
            icon="shield_alert",
            color_scheme="red",
            width="100%",
        ),
    )


def page_layout(*children, title: rx.Var[str] | str = "") -> rx.Component:
    return rx.fragment(
        connection_lost_banner(),
        rx.cond(
            AuthState.is_authenticated,
            rx.box(
                sidebar(),
                rx.box(
                    rx.vstack(
                        verification_mail_notice(),
                        invite_notice(),
                        action_error_notice(),
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
            # Not signed in. Two very different reasons land here, and each gets
            # the state that says which: a handler just ended the session
            # (#673), or the page is still entering the backend event path,
            # which costs 1933-9499 ms on production (#1090, SPEC_PAGE_ENTRY §4).
            rx.cond(
                AuthState.session_expired,
                signed_out_panel(),
                app_shell_skeleton(),
            ),
        ),
    )
