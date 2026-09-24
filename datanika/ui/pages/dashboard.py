"""Dashboard page — dynamic stats and recent runs."""

import reflex as rx

from datanika.plugin_registry import BILLING_ROUTE
from datanika.ui.components.getting_started_checklist import getting_started_checklist
from datanika.ui.components.info_tooltip import info_tooltip
from datanika.ui.components.layout import page_layout
from datanika.ui.components.run_status import run_status_color
from datanika.ui.state.dashboard_state import DashboardState
from datanika.ui.state.i18n_state import I18nState

_t = I18nState.translations


def stat_card(title: rx.Var[str], value: rx.Var, icon: str) -> rx.Component:
    return rx.card(
        rx.vstack(
            rx.icon(icon, size=24),
            rx.text(title, weight="bold"),
            rx.heading(value, size="6"),
            align="center",
            spacing="2",
        ),
        width="180px",
    )


def recent_runs_table() -> rx.Component:
    return rx.box(
        rx.heading(_t["dashboard.recent_runs"], size="4"),
        rx.table.root(
            rx.table.header(
                rx.table.row(
                    rx.table.column_header_cell(_t["common.id"]),
                    rx.table.column_header_cell(_t["dashboard.target"]),
                    rx.table.column_header_cell(_t["common.status"]),
                    rx.table.column_header_cell(_t["dashboard.started"]),
                    # core#1170 AC4.3 — "both pages". The dashboard renders the
                    # same normalize-step count under the same bare "Rows"
                    # header, so qualifying it on one page and not the other
                    # leaves the unqualified copy on the page a new user lands on.
                    rx.table.column_header_cell(
                        rx.hstack(
                            rx.text(_t["dashboard.rows"]),
                            info_tooltip("tooltip.rows_loaded"),
                            spacing="1",
                            align="center",
                        )
                    ),
                ),
            ),
            rx.table.body(
                rx.foreach(
                    DashboardState.recent_runs,
                    lambda r: rx.table.row(
                        rx.table.cell(r.id),
                        rx.table.cell(rx.text(r.target_name)),
                        rx.table.cell(
                            rx.badge(r.status, color_scheme=run_status_color(r.status)),
                        ),
                        rx.table.cell(r.started_at),
                        rx.table.cell(r.rows_loaded),
                    ),
                ),
            ),
            width="100%",
        ),
        width="100%",
    )


def _guide_step(title: rx.Var[str], desc: rx.Var[str]) -> rx.Component:
    return rx.box(
        rx.text(title, size="2", weight="bold"),
        rx.text(desc, size="2", color="var(--gray-11)"),
        padding_y="2",
    )


def _runs_dimension() -> rx.Component:
    """The runs meter, self-gating on its own allowance.

    core#1513 / ``SPEC_USAGE_VISIBILITY`` §2.3: a ``NULL`` allowance renders nothing, never
    ``0 / 0``. This used to be implicit in the *card's* visibility test, which is precisely what
    made the runs dimension gate the volume one.
    """
    return rx.cond(DashboardState.has_usage_data, _runs_meter())


def _runs_meter() -> rx.Component:
    return rx.vstack(
        rx.hstack(
            rx.text(
                DashboardState.runs_used,
                " / ",
                DashboardState.runs_limit,
                " ",
                _t["dashboard.usage_runs"],
                size="2",
            ),
            rx.text(
                DashboardState.runs_percent,
                "%",
                size="2",
                weight="bold",
                color=rx.cond(
                    DashboardState.runs_percent >= 80,
                    "var(--red-11)",
                    rx.cond(
                        DashboardState.runs_percent >= 60,
                        "var(--amber-11)",
                        "var(--green-11)",
                    ),
                ),
            ),
            justify="between",
            width="100%",
        ),
        rx.progress(
            value=DashboardState.runs_percent,
            max=100,
            color_scheme=DashboardState.runs_color,
            width="100%",
        ),
        rx.cond(
            DashboardState.runs_percent >= 80,
            rx.hstack(
                rx.icon("triangle-alert", size=14, color="var(--red-11)"),
                rx.link(
                    rx.text(
                        _t["dashboard.usage_upgrade"],
                        size="2",
                        color="var(--red-11)",
                        weight="medium",
                    ),
                    href=BILLING_ROUTE,
                ),
                align="center",
                spacing="1",
            ),
        ),
        spacing="2",
        width="100%",
    )


def _volume_dimension() -> rx.Component:
    return rx.cond(
        DashboardState.has_volume_data,
        rx.vstack(
            rx.hstack(
                rx.text(_t["quota.volume_title"], weight="bold", size="2"),
                # 🚨 SUBSTITUTED, not concatenated. `quota.volume_usage` is a TEMPLATE —
                # "{used} / {limit} GB processed this month" — and substitution here is
                # per-call-site, not automatic: painting `_t[...]` directly puts the braces in
                # the DOM. This component has never rendered in production (the UX flag has
                # been off since V2 P1), which is why nobody ever saw it.
                #
                # `.replace` rather than `i18n_text.interpolate`, deliberately: that helper
                # weaves *components* (the signup sentence's links) into a template and splits
                # sequentially, so it requires every locale to keep the slots in the order they
                # are passed. These are plain values, and `replaceAll` has no ordering
                # constraint — which matters precisely because the locales disagree on order.
                #
                # 🔑 Re-wording the nine values to drop the braces would NOT be equivalent:
                # ru, zh and ar place the numbers mid-phrase ("本月已处理 {used} / {limit} GB"),
                # so a component that hardcodes "<used> / <limit> <sentence>" is wrong in those
                # locales even with no braces left. `Var.replace` compiles to JS `replaceAll`
                # and keeps each locale's own word order.
                rx.text(
                    _t["quota.volume_usage"]
                    .replace("{used}", DashboardState.bytes_used_gb)
                    .replace("{limit}", DashboardState.bytes_limit_gb),
                    size="2",
                ),
                rx.text(
                    DashboardState.bytes_percent,
                    "%",
                    size="2",
                    weight="bold",
                    color=rx.cond(
                        DashboardState.bytes_percent >= 80,
                        "var(--red-11)",
                        rx.cond(
                            DashboardState.bytes_percent >= 60,
                            "var(--amber-11)",
                            "var(--green-11)",
                        ),
                    ),
                ),
                justify="between",
                width="100%",
            ),
            rx.progress(
                value=DashboardState.bytes_percent,
                max=100,
                color_scheme=DashboardState.bytes_color,
                width="100%",
            ),
            rx.cond(
                DashboardState.bytes_percent >= 100,
                rx.hstack(
                    rx.icon("triangle-alert", size=14, color="var(--red-11)"),
                    # SPEC_USAGE_VISIBILITY §2.4 — a cap and an allowance must not look the
                    # same. `hard_cap_bytes = true` means runs STOP; `false` means overage
                    # BILLS (core#713). Telling a hard-capped Free org it is being billed for
                    # overage is false in both halves: nothing is billed and the run is blocked.
                    rx.cond(
                        DashboardState.bytes_hard_cap,
                        # A wall. No placeholders in this key, in any locale.
                        rx.text(
                            _t["quota.volume_quota_reached_title"],
                            size="2",
                            color="var(--red-11)",
                            weight="medium",
                        ),
                        # Overage. Drawn only when the biller supplied every figure the
                        # sentence names — see `has_overage_figures`. Unreachable in production
                        # today: `subscriptions` holds 0 rows, so every org resolves to `free`,
                        # which is hard-capped and takes the branch above.
                        rx.cond(
                            DashboardState.has_overage_figures,
                            rx.text(
                                _t["quota.volume_overage"]
                                .replace("{gb}", DashboardState.bytes_overage_gb)
                                .replace("{rate}", DashboardState.bytes_overage_rate)
                                .replace("{total}", DashboardState.bytes_overage_total),
                                size="2",
                                color="var(--red-11)",
                                weight="medium",
                            ),
                        ),
                    ),
                    align="center",
                    spacing="1",
                ),
            ),
            spacing="2",
            width="100%",
        ),
    )


def usage_bar() -> rx.Component:
    """The dashboard's Plan Usage card (core#1513, ``SPEC_USAGE_VISIBILITY`` §3.1).

    🚨 **The volume dimension is deliberately NOT behind
    ``settings.datanika_dual_mode_ux_enabled`` any more.** Bytes are the billed dimension, and
    for as long as the meter asked that flag we charged in a unit no screen displayed. The flag
    itself is untouched and must stay off — it also mounts the ETL/ELT mode selector, which
    persists nothing (landing#656), so flipping it would publish a broken control to fix an
    unrelated one.

    ⚠️ This is necessary and **not sufficient**. ``has_volume_data`` is ``bytes_limit > 0``, and
    the only thing that assigns ``bytes_limit`` is cloud's ``BillingService.fill_usage_summary``.
    Without cloud's half this card mounts a volume dimension that renders nothing.

    Volume leads and runs follow (§2.2); each dimension gates only itself (§2.1).
    """
    return rx.cond(
        DashboardState.has_any_usage_data,
        rx.card(
            rx.vstack(
                rx.hstack(
                    rx.text(_t["dashboard.usage_title"], weight="bold", size="3"),
                    rx.badge(DashboardState.plan_name, size="1"),
                    align="center",
                    spacing="2",
                ),
                _volume_dimension(),
                # Only a real separator: two meters to separate. Otherwise a plan with one
                # dimension draws a rule under a single row.
                rx.cond(DashboardState.shows_both_dimensions, rx.divider()),
                _runs_dimension(),
                spacing="3",
                width="100%",
            ),
            width="100%",
        ),
    )


def getting_started_card() -> rx.Component:
    return rx.accordion.root(
        rx.accordion.item(
            header=rx.hstack(
                rx.icon("book-open", size=18),
                rx.text(_t["guide.title"], size="3", weight="bold"),
                align="center",
                spacing="2",
            ),
            content=rx.vstack(
                _guide_step(_t["guide.step1_title"], _t["guide.step1_desc"]),
                _guide_step(_t["guide.step2_title"], _t["guide.step2_desc"]),
                _guide_step(_t["guide.step3_title"], _t["guide.step3_desc"]),
                _guide_step(_t["guide.step4_title"], _t["guide.step4_desc"]),
                _guide_step(_t["guide.step5_title"], _t["guide.step5_desc"]),
                _guide_step(_t["guide.step6_title"], _t["guide.step6_desc"]),
                _guide_step(_t["guide.step7_title"], _t["guide.step7_desc"]),
                rx.link(
                    rx.hstack(
                        rx.icon("external-link", size=14),
                        rx.text(_t["guide.docs_link"], size="2"),
                        align="center",
                        spacing="1",
                    ),
                    href="https://datanika.io/docs",
                    is_external=True,
                    padding_top="2",
                ),
                spacing="1",
                width="100%",
            ),
            value="guide",
        ),
        collapsible=True,
        variant="ghost",
        width="100%",
    )


def dashboard_page() -> rx.Component:
    return page_layout(
        rx.vstack(
            rx.card(
                rx.text(_t["dashboard.welcome"], size="4"),
                rx.text(
                    _t["dashboard.welcome_sub"],
                    color="var(--gray-11)",
                ),
                width="100%",
            ),
            getting_started_checklist(),
            getting_started_card(),
            rx.hstack(
                stat_card(
                    _t["dashboard.uploads"],
                    DashboardState.stats.total_uploads,
                    "upload",
                ),
                stat_card(
                    _t["dashboard.transformations"],
                    DashboardState.stats.total_transformations,
                    "code",
                ),
                stat_card(
                    _t["dashboard.pipelines"],
                    DashboardState.stats.total_pipelines,
                    "git-branch",
                ),
                stat_card(
                    _t["dashboard.schedules"],
                    DashboardState.stats.total_schedules,
                    "clock",
                ),
                stat_card(
                    _t["dashboard.runs_ok"],
                    DashboardState.stats.recent_runs_success,
                    "circle_check",
                ),
                stat_card(
                    _t["dashboard.runs_fail"],
                    DashboardState.stats.recent_runs_failed,
                    "circle_x",
                ),
                spacing="4",
                wrap="wrap",
            ),
            usage_bar(),
            recent_runs_table(),
            spacing="6",
            width="100%",
        ),
        title=_t["nav.dashboard"],
    )
