"""How a run's status is coloured, defined once (core#657, ``SPEC_RUN_CANCELLATION`` §4).

Five pages coloured run statuses from their own hand-written chains of ``rx.cond``: three
copies of ``_status_color`` (runs, dashboard, models) and two of ``_run_button_color``
(pipelines, uploads). Each compared the status to string literals and fell through to ``gray``,
so a status added later rendered in the colour of whatever the fall-through happened to mean.
`cancelling` did exactly that — the colour of `pending`, i.e. *queued* — and on the Run button
it read *idle* while the worker was still busy. §4 is the list of what that shape costs.

Both are now derived: :data:`RUN_STATUS_COLORS` is total over :class:`RunStatus` (a test fails
when a status has no colour), and the in-progress colour covers
:data:`NON_TERMINAL_RUN_STATUSES` rather than two literals.
"""

from __future__ import annotations

import reflex as rx

from datanika.models.run import NON_TERMINAL_RUN_STATUSES, RunStatus
from datanika.ui.theme import ACCENT_COLOR

#: Radix colour scheme per status. **Total over the enum**, asserted by
#: ``tests/test_ui/test_runs_cancel_control.py::TestOneColourMap``.
RUN_STATUS_COLORS: dict[RunStatus, str] = {
    RunStatus.PENDING: "gray",
    RunStatus.RUNNING: "blue",
    # A stop was asked for and the worker has not confirmed it. Not `gray` — that is `pending`'s,
    # and it says "queued" about a run that is still working.
    RunStatus.CANCELLING: "amber",
    RunStatus.SUCCESS: "green",
    RunStatus.FAILED: "red",
    RunStatus.CANCELLED: "gray",
}

#: What a status the map has never seen renders as. Unreachable while the map is total.
_UNKNOWN_COLOR = "gray"

#: The Run button's colour while a run of that target is still going, and when none is.
#:
#: ⚠️ The idle colour is the **accent**, not ``gray``, and that is a contrast constraint rather
#: than taste (``docs/specs/SPEC_BUTTON_CONTRAST.md`` §5). Run is a *solid* button, and solid is
#: reserved for scales whose step 9 carries its label at 4.5:1 — the accent and the two
#: dark-label scales. Solid gray is **3.30:1**; solid yellow is 12.89:1, which is why the busy
#: branch is fine as it stands. Naming :data:`ACCENT_COLOR` rather than repeating ``"violet"``
#: is what keeps a future accent change from leaving this one branch behind.
#:
#: 🔑 This is the branch that proves the guard reads computed colours: ``rx.match`` renders to a
#: JavaScript ``switch``, so a checker that reduced the prop to a plain string saw nothing here
#: at all, and ``tests/test_ui/test_buttons_are_legible.py`` reported it as *solid gray, 3.30:1*
#: on ``/pipelines`` and ``/uploads`` only because it reads every arm.
_IN_PROGRESS_COLOR = "yellow"
_IDLE_COLOR = ACCENT_COLOR


def run_status_color(status: rx.Var[str]) -> rx.Var[str]:
    """The badge colour for ``status``, from :data:`RUN_STATUS_COLORS`."""
    arms = [(s.value, color) for s, color in sorted(RUN_STATUS_COLORS.items())]
    return rx.match(status, *arms, _UNKNOWN_COLOR)


def in_progress_values() -> list[str]:
    """The status values that mean *a run of this target is still going* — derived, sorted."""
    return sorted(s.value for s in NON_TERMINAL_RUN_STATUSES)


def run_in_progress_color(status: rx.Var[str]) -> rx.Var[str]:
    """``yellow`` while the target's last run is non-terminal, ``gray`` otherwise."""
    arms = [(value, _IN_PROGRESS_COLOR) for value in in_progress_values()]
    return rx.match(status, *arms, _IDLE_COLOR)
