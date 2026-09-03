"""A second submit while the first is in flight must create nothing (core#872 AC3).

Measured on production: `Create Connection` succeeded and every signal available
to the user said *"nothing happened"* — no toast, no inline confirmation, and a
table still listing the old rows for 5-17 seconds. The natural next action is to
click again.

**Connection quota enforcement is live**, so a duplicate create is not merely an
untidy row: on a Free org at 4/5 the first click silently spends the last slot
and the *second* click is the one refused — the failure and the success arriving
in the opposite order from the one the user perceives.

Why the disabled button is not the fix
--------------------------------------
core#872's AC3 says *"the create control is disabled between submit and
outcome"*. Disabling it is worth doing and **is not a guarantee**: whether the
frame carrying ``disabled`` reaches the browser before a fast second click is a
question about a running stack, a websocket round trip and a human's reflexes.
It cannot be answered by a test, and **a control that looks guarded and is not is
worse than none** — it invites exactly the double-click it appears to prevent.

So the load-bearing half is server-side: ``save_connection`` refuses to run while
``is_saving`` is set. That removes the timing question instead of betting on it,
and it is what these tests assert. The visual half is asserted separately, by
reading the control rather than by driving a browser.

What makes the reds attributable
--------------------------------
``_check_role`` is patched to a **recorder**, not to a stub that lets the body
run. So "the guard refused" is *"the recorder was never called"*, and the control
below proves the recorder fires when the guard is open — otherwise an empty
recorder would be satisfied by a harness that never reached the handler at all.
"""

from __future__ import annotations

import pytest

from datanika.ui.pages import connections as connections_page
from datanika.ui.state.connection_state import ConnectionState


class HarnessError(RuntimeError):
    """A failure of this file, not of the code under test."""


@pytest.fixture
def recorded_roles(monkeypatch) -> list[str]:
    """Record every ``_check_role`` call and refuse, so nothing below touches a DB."""
    calls: list[str] = []

    async def _record(self, role):  # noqa: ANN001, ANN202
        calls.append(role)
        return False  # refuse, so the handler body stops here

    monkeypatch.setattr(ConnectionState, "_check_role", _record)
    return calls


async def _drain(state: ConnectionState) -> list:
    """Run ``save_connection`` to completion and collect what it yielded."""
    return [event async for event in state.save_connection()]


async def test_control_an_open_guard_reaches_the_handler_body(recorded_roles):
    """The recorder must fire when nothing is blocking.

    Without this, *"the guard refused"* and *"this file never reached the
    handler"* are the same observation, and every assertion below is vacuous.
    """
    state = ConnectionState()
    await _drain(state)

    if recorded_roles != ["editor"]:
        raise HarnessError(
            f"_check_role was called {recorded_roles!r}, expected ['editor']. The "
            "handler body was not reached, so the guard test proves nothing."
        )


async def test_a_second_submit_while_saving_does_nothing(recorded_roles):
    """The criterion. A re-entrant submit must not reach the handler body."""
    state = ConnectionState()
    state.is_saving = True

    produced = await _drain(state)

    assert recorded_roles == [], (
        f"a second submit ran the handler body ({recorded_roles!r}). On a Free org "
        "at 4/5 connections that is a silently spent slot and a refusal shown for "
        "the wrong click."
    )
    assert produced == [], f"the refused submit emitted {produced!r}; it must be inert"


async def test_the_flag_is_cleared_on_the_error_path(recorded_roles):
    """``is_saving`` must be released when the save fails, not only when it succeeds.

    ``_check_role`` refuses here, which is the earliest of several early
    returns. A guard cleared only on the success path wedges the form on the
    first validation error and the user cannot submit again at all — a worse
    outcome than the duplicate it was added to prevent.

    ⚠️ Asserted as **set-then-cleared**, not merely as "False at the end".
    ``is_saving`` defaults to ``False``, so the end-state alone is satisfied by
    a version with no guard at all — the test would pass against exactly the
    code it exists to reject.
    """
    state = ConnectionState()
    generator = state.save_connection()

    await generator.asend(None)  # advance to the first yield
    assert state.is_saving is True, (
        "is_saving was never set, so 'it is False afterwards' says nothing — "
        "that is also true of a handler with no guard"
    )

    with pytest.raises(StopAsyncIteration):
        await generator.asend(None)  # run the refused body to completion

    assert state.is_saving is False, (
        "is_saving survived a refused save. Every early return in the body sits "
        "inside the guard's try/finally for exactly this reason."
    )
    assert recorded_roles == ["editor"], "the error path under test was not taken"


async def test_the_flag_is_set_before_the_first_await(recorded_roles):
    """The flag must be observable to a second event, and the first ``yield`` proves it.

    Reflex serialises events per client token, so a second submit is refused only
    if the first has already written ``is_saving``. Setting it after an ``await``
    would leave a window between the check and the set. Stopping at the first
    yield shows the write has already happened while the body has not yet run.
    """
    state = ConnectionState()
    generator = state.save_connection()

    await generator.asend(None)  # advance to the first yield

    assert state.is_saving is True, (
        "is_saving was not set by the time the handler first yielded, so a second "
        "event arriving now would run the body"
    )
    assert recorded_roles == [], (
        "the handler body ran before the first yield, so the disabled control is "
        "pushed after the work rather than before it"
    )

    await generator.aclose()


def test_the_control_is_bound_to_the_flag():
    """AC3's visual half, read off the rendered control rather than a browser.

    ⚠️ Asserted as a *convenience*, deliberately, not as the guard. If this ever
    goes red while the tests above stay green, the product is still correct and
    only the affordance is missing — which is the opposite of the failure mode
    this file exists for.
    """
    fields = ConnectionState.get_fields()
    assert "is_saving" in fields, (
        "ConnectionState has no `is_saving` var, so no control can bind to it"
    )

    source = connections_page.connections_page.__doc__ or ""
    del source  # the page is built by calling it; read the module instead

    import inspect

    page_source = inspect.getsource(connections_page)
    assert "disabled=ConnectionState.is_saving" in page_source, (
        "the save button is not disabled while a save is in flight; AC3's visual half is missing"
    )
