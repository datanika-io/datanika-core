"""The alerting channel list must not carry credentials to the client (core#972).

`NotificationState.channels` is a **public** Reflex state var, so every field of
every `ChannelItem` in it is serialized to the connected browser. The channel
list is deliberately visible to every member (core#886) — *"knowing where run
failures are announced is something an editor needs"* — while add / edit /
toggle / delete are admin. So the payload handed a non-admin member the org's
Slack webhook URL and Telegram **bot token**.

The secret key names are read off ``notification_service._CONFIG_REQUIRED``
rather than typed here, so a new channel type is covered on the day it is added.

⚠️ **Assert on the values, not on the field.** ``ChannelItem`` no longer
declares ``config``, and a test that only checked ``not hasattr(item, "config")``
would go green against an implementation that renamed the field to ``settings``
and shipped exactly the same bytes. What matters is that no credential *value*
appears anywhere in the serialized row.
"""

import ast
import inspect
import textwrap
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from datanika.models.notification_channel import ChannelType, NotificationChannel
from datanika.models.user import Organization
from datanika.services.notification_service import _CONFIG_REQUIRED
from datanika.ui.state import notification_state
from datanika.ui.state.notification_state import ChannelItem, NotificationState

#: Distinctive per-type credential values. Any of these turning up in a
#: serialized row is the bug.
SECRET_VALUES = {
    "webhook_url": "https://hooks.slack.example/T-LEAKED-SLACK-URL",
    "token": "8100000000:AA-LEAKED-TELEGRAM-BOT-TOKEN",
    "chat_id": "-1009999999999",
    "url": "https://webhook.example/LEAKED-CUSTOM-URL",
    "email": "leaked-alerts@example.com",
}


def _unwrap(func):
    """Reflex wraps a state method in an ``EventHandler``; get the function."""
    return getattr(func, "fn", func)


def _code_only(func) -> str:
    """Source of ``func`` with its docstring and comments removed.

    🚨 **A static guard over code that documents itself must exclude the
    documentation.** This bit on the first run of this very file: the fixed
    ``edit_channel`` explains *why* it no longer reads ``self.channels``, and
    the explanation contains the literal ``self.channels`` — so the assertion
    that the string is absent failed against the correct implementation. That is
    the third time this class has appeared in this repo, and the direction is
    what makes it dangerous: **the better the comment, the more likely it fools
    the grep**, in either polarity.

    ``ast.unparse`` drops comments as a side effect of round-tripping and
    normalises string quoting, so assertions built on it must be quote-agnostic
    — which is why the role checks below are asserted structurally, over the
    parse tree, rather than by matching source text.
    """
    tree = ast.parse(textwrap.dedent(inspect.getsource(_unwrap(func))))
    fn = tree.body[0]
    if (
        fn.body
        and isinstance(fn.body[0], ast.Expr)
        and isinstance(fn.body[0].value, ast.Constant)
        and isinstance(fn.body[0].value.value, str)
    ):
        fn.body = fn.body[1:]
    return ast.unparse(fn)


def _awaited_role_gate(func) -> str | None:
    """The role passed to an **awaited** ``self._check_role(...)``, if any.

    Structural rather than textual on purpose. ``_check_role`` is a coroutine,
    so an un-awaited call is a truthy object and ``if not self._check_role(...)``
    refuses nobody — a defect that is invisible to any grep for the method name,
    and that reads in review as a guard being present.
    """
    tree = ast.parse(textwrap.dedent(inspect.getsource(_unwrap(func))))
    for node in ast.walk(tree):
        if not isinstance(node, ast.Await):
            continue
        call = node.value
        if (
            isinstance(call, ast.Call)
            and isinstance(call.func, ast.Attribute)
            and call.func.attr == "_check_role"
            and call.args
            and isinstance(call.args[0], ast.Constant)
        ):
            return call.args[0].value
    return None


@pytest.fixture
def org(db_session):
    o = Organization(name="Leaky Org", slug="leaky-org")
    db_session.add(o)
    db_session.flush()
    return o


def _channel(db_session, org, ct: ChannelType) -> NotificationChannel:
    config = {k: SECRET_VALUES[k] for k in _CONFIG_REQUIRED[ct]}
    ch = NotificationChannel(
        org_id=org.id,
        name=f"{ct.value}-channel",
        channel_type=ct,
        config=config,
        events=["run_failure"],
        is_active=True,
    )
    db_session.add(ch)
    db_session.flush()
    return ch


def test_every_channel_type_has_a_secret_value_in_this_test():
    """Anti-vacuity: a type whose keys are missing here is never exercised.

    Without this, adding a fifth channel type silently drops it out of the
    coverage below while every assertion still passes.
    """
    missing = {
        (ct.value, key)
        for ct, keys in _CONFIG_REQUIRED.items()
        for key in keys
        if key not in SECRET_VALUES
    }
    assert not missing, (
        f"these config keys have no test value, so the leak assertions never see them: {missing}"
    )


class _Loader:
    """Stand-in for ``NotificationState`` — only what ``load_channels`` may touch.

    ⚠️ Deliberately not a ``MagicMock``. The whole finding is about a field
    arriving in a payload nobody inspected; a mock that answers every attribute
    would let the assertion pass against code that never ran.
    """

    def __init__(self, org_id: int):
        self._org_id = org_id
        self.channels: list[ChannelItem] = []

    async def get_state(self, _cls):
        return SimpleNamespace(current_org=SimpleNamespace(id=self._org_id))


async def _run_load_channels(db_session, org_id) -> list[ChannelItem]:
    """Execute the real handler against a real row.

    Running ``load_channels`` itself is the point. An earlier draft of this test
    rebuilt the DTO the way the handler does, and passed *before* the fix — it
    was asserting against a copy of the corrected code rather than the shipped
    code. A guard that re-implements its subject can only ever agree with
    itself.
    """

    @contextmanager
    def _session():
        yield db_session

    state = _Loader(org_id)
    with patch.object(notification_state, "get_sync_session", _session):
        await _unwrap(NotificationState.load_channels)(state)
    return state.channels


@pytest.mark.parametrize("ct", list(_CONFIG_REQUIRED), ids=lambda c: c.value)
async def test_a_listed_channel_carries_no_credential_value(db_session, org, ct):
    """The regression. Red before the fix, for all four channel types."""
    row = _channel(db_session, org, ct)
    items = await _run_load_channels(db_session, org.id)
    assert len(items) == 1, f"expected the fixture channel back, got {items}"

    serialized = items[0].model_dump()
    leaked = sorted(key for key in _CONFIG_REQUIRED[ct] if SECRET_VALUES[key] in repr(serialized))
    assert not leaked, (
        f"a {ct.value} channel row serialized to the client carries {leaked}. "
        "NotificationState.channels is a public state var and the list is visible to "
        "every member (core#886), so this is the org's credential sitting in a "
        f"non-admin's browser. Row: {serialized}"
    )
    # Positive control: what the table renders must survive the removal, or the
    # assertion above is satisfied by a handler that returns nothing useful.
    assert serialized["name"] == row.name
    assert serialized["channel_type"] == ct.value
    assert serialized["events"] == ["run_failure"]
    assert serialized["is_active"] is True


def test_channel_item_declares_no_unconstrained_dict_field():
    """A bare ``dict`` on a wire DTO is a decision nobody made.

    Every credential in this codebase reaches the client through a JSON
    ``config`` column copied verbatim into a rendering model. Naming each field
    forces the question *"should the client see this?"* once per field, which is
    the only moment it gets asked.
    """
    offenders = sorted(
        name
        for name, field in ChannelItem.model_fields.items()
        if field.annotation in (dict, dict[str, object])
    )
    assert not offenders, (
        f"ChannelItem carries unconstrained dict field(s) {offenders}; that is the "
        "shape core#972 was — a provider-shaped payload crossing to the client with "
        "nobody deciding field by field what was safe"
    )


def test_the_docstring_stripper_works_in_both_directions():
    """The control on ``_code_only`` itself.

    Both halves matter. If it stripped nothing, the assertion below would fail
    against the *correct* implementation, whose docstring quotes the very
    literal it rejects — and someone would "fix" it by deleting the
    explanation. If it stripped too much, the assertion would pass against any
    implementation at all.
    """
    code = _code_only(NotificationState.edit_channel)
    assert "core#972" not in code, (
        "the docstring survived stripping, so every assertion built on _code_only "
        "is really being made against the prose"
    )
    assert "get_channel" in code, (
        "the stripper removed executable code as well as the docstring, so the "
        "assertions below can no longer see the implementation"
    )


def test_edit_channel_does_not_read_the_config_off_client_state():
    """``edit_channel`` must re-read the row server-side.

    The old implementation took the token out of ``self.channels`` — which is
    only possible because the token was in ``self.channels``, so the two halves
    of this fix are one change. If a later edit re-introduces the read, the DTO
    has to carry the secret again and this goes red before that happens.
    """
    code = _code_only(NotificationState.edit_channel)
    assert "self.channels" not in code, (
        "edit_channel reads the channel out of self.channels, the public state var. "
        "Re-read the row through NotificationService.get_channel instead — the handler "
        "already has a session."
    )
    assert "get_channel" in code, (
        "edit_channel no longer re-reads the row server-side; the form would be "
        "populated from whatever the client happens to hold"
    )


def test_edit_channel_is_gated_on_an_awaited_admin_check():
    """It hands the stored token back to the caller, so the caller must be checked.

    ⚠️ The docstring on ``_channel_actions`` used to justify leaving this ungated
    with *"it persists nothing — it copies the row into the form"*. That is the
    wrong test: **reading** the secret is the harm, and persistence is beside the
    point. The same reasoning left ``ConnectionState.edit_connection`` open.

    The gate is asserted over the parse tree, so a ``_check_role`` call that is
    present but not awaited — a truthy coroutine that refuses nobody — fails
    here rather than reading as a guard.
    """
    assert inspect.iscoroutinefunction(_unwrap(NotificationState.edit_channel)), (
        "edit_channel must be async; a sync handler cannot await _check_role"
    )
    assert _awaited_role_gate(NotificationState.edit_channel) == "admin", (
        "edit_channel returns the stored webhook URL / bot token to the caller and is "
        "dispatched by name over the websocket, so hiding the pencil button behind "
        "rx.cond(AuthState.can_administer, ...) gates nothing. It needs "
        "`if not await self._check_role('admin'): return`."
    )
