"""Reusable callout that shows an upgrade prompt for quota errors."""

import reflex as rx

from datanika.plugin_registry import BILLING_ROUTE
from datanika.ui.state.base_state import BaseState
from datanika.ui.state.i18n_state import I18nState

_t = I18nState.translations


def error_or_quota_callout(state_cls: type[BaseState]) -> rx.Component:
    """Render either a quota upgrade callout or a plain error callout.

    Usage in pages:
        error_or_quota_callout(ConnectionState)
    """
    return rx.cond(
        state_cls.error_message,
        rx.cond(
            state_cls.is_quota_error,
            # Quota exceeded — show upgrade prompt
            rx.callout(
                rx.vstack(
                    rx.text(state_cls.error_message, weight="medium"),
                    rx.hstack(
                        rx.text(_t["quota.upgrade_hint"], color="var(--amber-11)"),
                        rx.link(
                            rx.button(
                                _t["quota.upgrade_button"],
                                size="1",
                                color_scheme="amber",
                                variant="solid",
                            ),
                            href=BILLING_ROUTE,
                        ),
                        align="center",
                        spacing="3",
                    ),
                    spacing="2",
                ),
                icon="arrow_up_circle",
                color_scheme="amber",
            ),
            # Regular error — plain red callout
            rx.callout(
                state_cls.error_message,
                icon="triangle_alert",
                color_scheme="red",
            ),
        ),
    )
