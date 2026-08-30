"""Login page."""

import json

import reflex as rx

from datanika.config import settings
from datanika.ui.components.captcha import captcha_script
from datanika.ui.components.layout import legal_links
from datanika.ui.state.auth_state import AuthState
from datanika.ui.state.i18n_state import I18nState

_backend = settings.oauth_redirect_base_url
_t = I18nState.translations


def _social_login_button(label: str, provider: str) -> rx.Component:
    """Start a social login in the current tab (#418).

    This has to be a real browser navigation: ``/api/auth/login/<provider>``
    is a backend Starlette route that 302s to the provider, so nothing the
    frontend router does can serve it. It also has to stay in *this* tab —
    the previous ``rx.link(..., is_external=True)`` compiled to
    ``target="_blank"``, so the user authenticated in a second tab and ended
    up signed in there while the original still showed the sign-in form.

    Neither ``rx.link`` nor ``rx.el.a`` can express that: both render a
    react-router ``Link``, which treats a *same-origin* absolute URL as an
    in-app route — and in production the backend and frontend share
    ``app.datanika.io``, so the click would be swallowed by the router.
    (In dev the origins differ, so that breakage would not show up locally.)
    ``rx.redirect`` has the same same-origin branch. Hence an explicit
    assignment, which is unambiguous whatever the router does.

    **Sizing (#605).** These two are the only controls in the card that share a
    row, so they are the only ones that must not use ``width="100%"``. They used
    to, and the GitHub button rendered *outside* the card: a Radix button
    computes to ``flex: 0 0 auto``, so ``flex-basis`` resolved to the declared
    100% (294px each) and ``flex-shrink: 0`` forbade the row from reducing them
    — 294 + 12 + 294 laid out in a 294px row. ``flex="1 1 0"`` makes the basis 0
    and lets both grow into equal halves; ``min_width="0"`` overrides a flex
    item's default ``min-width: auto``, which would otherwise floor each button
    at its own label width.
    """
    target = f"{_backend}/api/auth/login/{provider}"
    return rx.button(
        label,
        variant="outline",
        size="3",
        flex="1 1 0",
        min_width="0",
        type="button",
        on_click=rx.call_script(f"window.location.assign({json.dumps(target)})"),
    )


def _forgot_password_link() -> rx.Component:
    """The "Forgot your password?" link — hidden without SMTP (core#623, D9).

    ``EmailService.send()`` returns False when ``smtp_host`` is empty, which is
    the **default** for a self-hosted instance. Offering the link there leads to
    a "check your inbox" screen for an email that was never sent. The Settings
    change-password card still works with no mail server at all, so nothing is
    lost by hiding this.

    Evaluated at import time on purpose: it is instance configuration, not
    per-request state, so there is nothing to react to.
    """
    if not settings.smtp_host:
        return rx.fragment()
    return rx.link(
        _t["auth.forgot_password"],
        href="/forgot-password",
        on_click=AuthState.clear_auth_error,
        size="2",
        color="gray",
    )


def _help_key():
    """Remedy text for a refused social link — it must name a remedy that exists.

    With no relay, ``_forgot_password_link`` above hides the reset entry point
    entirely and no verification mail is ever sent, so "confirm your address"
    names something the instance cannot do. The password still works, which is
    the only true remedy there.

    Import-time, like ``_forgot_password_link``: instance configuration, not
    per-request state.
    """
    if settings.smtp_host:
        return _t["auth.social_link_blocked_help"]
    return _t["auth.social_link_blocked_help_no_email"]


def login_page() -> rx.Component:
    return rx.center(
        rx.vstack(
            rx.hstack(
                rx.image(src="/logo.png", width="48px", height="48px"),
                rx.heading(_t["app.name"], size="7"),
                spacing="3",
                align="center",
            ),
            rx.text(_t["auth.sign_in_heading"], size="3", color="gray"),
            rx.cond(
                AuthState.show_reset_done,
                rx.callout(
                    _t["auth.password_reset_done"],
                    icon="circle_check",
                    color_scheme="green",
                    width="100%",
                ),
            ),
            rx.cond(
                AuthState.show_session_expired,
                rx.callout(
                    _t["auth.session_expired"],
                    icon="clock",
                    color_scheme="amber",
                    width="100%",
                ),
            ),
            rx.cond(
                AuthState.show_link_blocked,
                rx.callout(
                    rx.vstack(
                        rx.text(_t["auth.social_link_blocked"], weight="medium"),
                        rx.text(_help_key(), size="2"),
                        spacing="1",
                        align="start",
                    ),
                    icon="shield_alert",
                    color_scheme="amber",
                    width="100%",
                ),
            ),
            rx.cond(
                AuthState.auth_error != "",
                rx.callout(
                    AuthState.auth_error,
                    icon="triangle_alert",
                    color_scheme="red",
                    width="100%",
                ),
            ),
            rx.form(
                rx.vstack(
                    rx.el.label(
                        rx.text(_t["auth.email"], size="2", weight="medium"),
                        html_for="login-email",
                    ),
                    rx.input(
                        id="login-email",
                        placeholder=_t["auth.ph_email"],
                        name="email",
                        width="100%",
                    ),
                    rx.el.label(
                        rx.text(_t["auth.password"], size="2", weight="medium"),
                        html_for="login-password",
                    ),
                    rx.input(
                        id="login-password",
                        placeholder=_t["auth.ph_password"],
                        name="password",
                        type="password",
                        width="100%",
                    ),
                    rx.button(
                        _t["auth.sign_in"],
                        type="submit",
                        width="100%",
                        size="3",
                    ),
                    captcha_script("login"),
                    spacing="3",
                    width="100%",
                ),
                on_submit=AuthState.login,
            ),
            _forgot_password_link(),
            rx.divider(),
            rx.text(_t["auth.or_continue_with"], size="2", color="gray", text_align="center"),
            rx.hstack(
                _social_login_button("Google", "google"),
                _social_login_button("GitHub", "github"),
                width="100%",
                spacing="3",
            ),
            rx.text(
                _t["auth.no_account"],
                " ",
                rx.link(
                    _t["auth.sign_up"],
                    href="/signup",
                    on_click=AuthState.clear_auth_error,
                ),
                size="2",
                color="gray",
            ),
            # #656. Rendered from the shared component rather than written
            # here, so the `is_external` (= new tab) that these off-site links
            # need stays out of this module — the AST guard in
            # test_external_links.py is about the *social* buttons, and a
            # module-wide ban is easier to keep true than to keep meaningful.
            legal_links(),
            spacing="4",
            width="360px",
            padding="32px",
            border="1px solid var(--gray-a5)",
            border_radius="12px",
            bg="var(--color-background)",
        ),
        height="100vh",
    )
