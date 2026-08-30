"""A failed connection test must say why — without leaking the password.

**The class (core#608 + core#625).** Two defects filed a day apart, one function
apart, with the same ending: the user is shown a string that names nothing.

- **#608** — an exception *escaped*, so Reflex showed
  *"An error occurred. Contact the website administrator."*
- **#625** — an exception was *caught and discarded*, so the user was shown
  *"Connection failed — check your credentials and network settings"* for a
  MongoDB connection whose credentials were correct and whose runs succeeded.
  The driver had said "Authentication failed against database X". That sentence
  was the fix, and it was thrown away.

Catching #608's exception and returning another fixed string would have been a
fresh instance of the class it was closing. So the messages carry the driver's
own reason now.

**And that is exactly why it had not been done.** Driver exceptions quote the
connection URI, and the URI contains the password. These tests exist as much for
the redaction as for the message: a helpful error that discloses a credential is
a worse bug than an unhelpful one, on a form that a morning of work
(core#618) was just spent keeping credentials out of.
"""

import pytest

from datanika.models.connection import ConnectionType
from datanika.services.connection_service import (
    ConnectionService,
    describe_connection_failure,
)

_SECRET = "hunter2-correct-horse"


class _DriverError(Exception):
    """An exception whose text quotes the connection URI, like a real driver's."""


def test_the_reason_reaches_the_user():
    """The whole point: the message names the cause."""
    message = describe_connection_failure(
        _DriverError("Authentication failed against database admin"),
        {"user": "svc", "password": _SECRET},
        "Connection failed",
    )
    assert "Authentication failed against database admin" in message
    assert message.startswith("Connection failed")


def test_the_password_never_reaches_the_user():
    """A driver that quotes the URI quotes the password with it."""
    message = describe_connection_failure(
        _DriverError(f"could not connect to mongodb://svc:{_SECRET}@db:27017/orders"),
        {"user": "svc", "password": _SECRET},
        "Connection failed",
    )
    assert _SECRET not in message
    assert "***" in message
    # The useful part survives the redaction.
    assert "mongodb://svc:***@db:27017/orders" in message


def test_the_percent_encoded_password_is_redacted_too():
    """We percent-encode the userinfo when building the URI, so drivers echo that form."""
    secret = "p@ss word/with+chars"
    message = describe_connection_failure(
        _DriverError("bad URI: mongodb://svc:p%40ss+word%2Fwith%2Bchars@db:27017/o"),
        {"password": secret},
        "Connection failed",
    )
    assert "p%40ss+word%2Fwith%2Bchars" not in message
    assert "p@ss word/with+chars" not in message


@pytest.mark.parametrize(
    "key",
    [
        "password",
        "token",
        "api_key",
        "access_token",
        "refresh_token",
        "client_secret",
        "developer_token",
        "aws_secret_access_key",
        "keyfile_json",
        "service_account_json",
    ],
)
def test_every_secret_shaped_key_is_redacted(key):
    """One key missing from the set is one credential on screen.

    Swept rather than sampled for the same reason core#618's autofill test is:
    the gap is uniform and invisible, and the cost of an extra key is nothing.
    """
    message = describe_connection_failure(
        _DriverError(f"rejected value {_SECRET} at endpoint"), {key: _SECRET}, "Failed"
    )
    assert _SECRET not in message, f"{key} was not redacted"


def test_it_fails_closed_when_a_secret_is_too_short_to_remove():
    """A one-character password cannot be substring-replaced without shredding prose.

    The safe answer is to show nothing extra, not to show a mangled sentence and
    certainly not to show the reason unredacted. Worst case is the old
    behaviour.
    """
    message = describe_connection_failure(
        _DriverError("authentication failed"), {"password": "a"}, "Connection failed"
    )
    assert message == "Connection failed"


def test_a_stack_trace_is_trimmed_rather_than_dumped():
    """One line of context, not a wall of text in a UI callout."""
    message = describe_connection_failure(
        _DriverError("x" * 5000), {"password": _SECRET}, "Connection failed"
    )
    assert len(message) < 400
    assert message.endswith("…")


def test_newlines_are_collapsed():
    """Driver messages are often multi-line; the callout renders one paragraph."""
    message = describe_connection_failure(
        _DriverError("first line\n  second line\n\nthird"), {"password": _SECRET}, "Failed"
    )
    assert "\n" not in message
    assert "first line second line third" in message


def test_a_real_missing_dialect_now_explains_itself():
    """End to end through the service, on the unpatched databricks path (core#608)."""
    ok, message = ConnectionService.test_connection(
        {
            "host": "dbc-adc397cf-f20a.cloud.databricks.com",
            "http_path": "/sql/1.0/warehouses/x",
            "token": "dapi-not-a-real-token-but-long-enough",
            "catalog": "main",
        },
        ConnectionType.DATABRICKS,
    )
    assert ok is False
    assert "databricks" in message.lower()
    assert "dapi-not-a-real-token-but-long-enough" not in message


def test_an_engine_build_failure_carries_its_reason_and_not_the_password(monkeypatch):
    """The `create_engine` guard added for core#608 must not be a new fixed string.

    A `ValueError`, not a `NoSuchModuleError`: the latter has its own branch with
    a message that already names the cause ("no database driver for X"). It is
    the *generic* branch — the catch-all that closes #608's escape — that risks
    being the class all over again, so that is the one under test here.
    """
    monkeypatch.setattr(
        "datanika.services.connection_service.create_engine",
        lambda *a, **kw: (_ for _ in ()).throw(
            ValueError(f"invalid port in postgresql://u:{_SECRET}@h:notaport/d")
        ),
    )
    ok, message = ConnectionService.test_connection(
        {"host": "h", "port": "notaport", "user": "u", "password": _SECRET, "database": "d"},
        ConnectionType.POSTGRES,
    )
    assert ok is False
    assert "invalid port" in message
    assert _SECRET not in message
