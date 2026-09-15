"""Social sign-in controls, shared by ``/login`` and ``/signup`` (core#624).

Lifted out of ``pages/login.py`` rather than copied into ``pages/signup.py``. The button carries
two non-obvious fixes that have each already been re-learned once — #418 (navigation) and #605
(sizing) — and a copy would be a second place to regress both (``SPEC_SIGNUP_SOCIAL_AUTH`` §4).
``tests/test_ui/test_login_layout.py`` fails if either page builds a provider URL itself.
"""

import json

import reflex as rx

from datanika.config import settings
from datanika.services.auth_redirects import OAUTH_CONTEXT_KEYS
from datanika.ui.state.i18n_state import I18nState

_t = I18nState.translations


def _navigation_script(target: str) -> str:
    """Navigate to ``target``, forwarding the page's own sign-in context (core#624).

    ``/signup?template=<slug>``, ``?invite_token=`` and ``?next=`` are what the email path honours;
    without this the social path starts from a bare ``/api/auth/login/<provider>`` and drops them.

    Only :data:`OAUTH_CONTEXT_KEYS` are forwarded, and only when present — never the whole query
    string, because ``/signup?invite_token=…&email=…`` also carries an address that nothing on the
    social path consumes. The backend re-validates every value (``oauth_routes._clean_context``),
    so this is transport, not a check.
    """
    return (
        "(function(){"
        f"var url=new URL({json.dumps(target)},window.location.href);"
        "var page=new URLSearchParams(window.location.search);"
        f"{json.dumps(list(OAUTH_CONTEXT_KEYS))}.forEach(function(key){{"
        "var value=page.get(key);if(value){url.searchParams.set(key,value);}"
        "});"
        "window.location.assign(url.toString());"
        "})()"
    )


def social_login_button(label: str, provider: str) -> rx.Component:
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

    **Context (core#624).** The navigation carries the page's ``?template=``, ``?invite_token=``
    and ``?next=`` along — see :func:`_navigation_script`.
    """
    target = f"{settings.oauth_redirect_base_url}/api/auth/login/{provider}"
    return rx.button(
        label,
        variant="outline",
        size="3",
        flex="1 1 0",
        min_width="0",
        type="button",
        on_click=rx.call_script(_navigation_script(target)),
    )


def social_login_row() -> rx.Component:
    """The divider, the "or continue with" line and both provider buttons, in that order.

    Google, then GitHub — the order ``/login`` has always used (§4). Provider names stay
    untranslated (WORKFLOW_RULES §6) and ``auth.or_continue_with`` exists in all nine locales,
    so this adds no strings. A fragment, so the caller's ``vstack`` spacing applies to each
    piece exactly as it did when they were written inline on ``/login``.
    """
    return rx.fragment(
        rx.divider(),
        rx.text(_t["auth.or_continue_with"], size="2", color="gray", text_align="center"),
        rx.hstack(
            social_login_button("Google", "google"),
            social_login_button("GitHub", "github"),
            width="100%",
            spacing="3",
        ),
    )
