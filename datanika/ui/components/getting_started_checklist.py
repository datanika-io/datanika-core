"""Getting Started checklist widget — 5 steps to first successful pipeline."""

import reflex as rx

from datanika.ui.state.i18n_state import I18nState
from datanika.ui.state.onboarding_state import OnboardingState

_t = I18nState.translations


def _step_row(done: rx.Var[bool], label: rx.Var[str], href: str) -> rx.Component:
    return rx.link(
        rx.hstack(
            rx.cond(
                done,
                rx.icon("circle_check", size=18, color="var(--green-9)"),
                rx.icon("circle", size=18, color="var(--slate-8)"),
            ),
            rx.text(
                label,
                size="2",
                color=rx.cond(done, "var(--slate-10)", "var(--slate-12)"),
                style={"textDecoration": rx.cond(done, "line-through", "none")},
            ),
            align="center",
            spacing="2",
        ),
        href=href,
        underline="none",
        width="100%",
    )


def getting_started_checklist() -> rx.Component:
    return rx.cond(
        OnboardingState.visible,
        rx.card(
            rx.vstack(
                rx.hstack(
                    rx.vstack(
                        rx.heading(_t["onboarding.title"], size="4"),
                        rx.text(
                            _t["onboarding.subtitle"],
                            size="2",
                            color="var(--slate-10)",
                        ),
                        spacing="1",
                        align="start",
                    ),
                    rx.spacer(),
                    rx.hstack(
                        rx.badge(
                            OnboardingState.done_count.to_string()
                            + " / "
                            + OnboardingState.total_count.to_string(),
                            color_scheme=rx.cond(OnboardingState.all_done, "green", "violet"),
                            variant="soft",
                            size="2",
                        ),
                        rx.button(
                            _t["onboarding.dismiss"],
                            on_click=OnboardingState.dismiss_checklist,
                            variant="ghost",
                            size="1",
                            color_scheme="gray",
                        ),
                        align="center",
                        spacing="2",
                    ),
                    align="start",
                    width="100%",
                ),
                rx.divider(),
                _step_row(
                    OnboardingState.progress.has_connection,
                    _t["onboarding.step_connection"],
                    "/connections",
                ),
                _step_row(
                    OnboardingState.progress.has_pipeline,
                    _t["onboarding.step_pipeline"],
                    "/pipelines",
                ),
                _step_row(
                    OnboardingState.progress.has_run,
                    _t["onboarding.step_run"],
                    "/runs",
                ),
                _step_row(
                    OnboardingState.progress.has_transformation,
                    _t["onboarding.step_transformation"],
                    "/transformations",
                ),
                _step_row(
                    OnboardingState.progress.has_schedule,
                    _t["onboarding.step_schedule"],
                    "/schedules",
                ),
                rx.cond(
                    OnboardingState.all_done,
                    rx.callout(
                        _t["onboarding.all_done"],
                        icon="party_popper",
                        color_scheme="green",
                    ),
                    rx.fragment(),
                ),
                spacing="3",
                align="stretch",
            ),
            width="100%",
            margin_bottom="1rem",
        ),
        rx.fragment(),
    )
