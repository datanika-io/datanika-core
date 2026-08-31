"""core#700 — a failing mail path must be able to make something go red.

The premise of this file is a measurement, not a worry. On 2026-08-30 Product
proved email verification works in production by **opening a real inbox**. That
was the only available proof, because a *failing* run would have looked identical
at every surface a person can see: same silent signup, same absent confirmation,
same `False` returned and discarded.

The suite was no better placed than the person. Every existing test of this path
mocks the layer below it:

* ``tests/test_tasks/test_email_tasks.py`` patches ``EmailService`` wholesale, so
  it asserts the task *calls* something, never that the something produces mail.
* ``tests/test_services/test_email_verification_wiring.py`` patches
  ``send_verification_email_task``, so it asserts the token is well-formed and
  handed over, never that handing it over sends anything.
* ``tests/test_services/test_email_service.py`` patches ``smtplib``, and never
  reaches the service from the task or the token minter.

Each is correct in isolation. **Composed, they mean the seams have never been
executed against each other** — the same shape as core#492 and core#684, where
every unit passed and the pipe moved nothing. So this file draws the line in one
place instead: the only thing faked is the wire.

Nothing here asserts that mail was *delivered*; that needs a real relay and is
release validation, not CI. It asserts that everything up to the socket happened,
which is what the existing tests could not say and what a break would falsify.

Shown red before being trusted, and — more to the point — shown red on a break
the existing tests **cannot see**. Three obvious breakages (the ``/api`` prefix
dropped from the URL, the wrong token type minted, the task returning ``True``
without sending) turn this file red, but each is already caught elsewhere, so
catching them proves nothing about whether this file earns its place.

The one that does is a mutation of the **plumbing** — the seam no test owns.
In ``email_tasks.py``, the verification task builds its ``EmailService`` out of
``settings``; ``test_email_service.py`` passes those values in by hand and
``test_email_tasks.py`` mocks the class, so neither can see that constructor.
Breaking two of its arguments::

    frontend_url=settings.frontend_url   ->  frontend_url=""
    smtp_from_email=settings.smtp_from_email -> smtp_from_email=settings.smtp_from_name

gives every verification link no host (unclickable) and a From address of
``Datanika <Datanika>`` (rejected by most relays). Measured 2026-08-31:

===============================================================  ==========
``test_email_service`` + ``test_email_tasks`` +
``test_email_verification`` + ``test_email_verification_wiring``  **29 passed**
this file                                                         **FAILED**
===============================================================  ==========

A wholly broken mail path, green across every test that exists. That is the gap,
and it is the reason this file is composed rather than layered.
"""

import ast
import inspect
import textwrap
from unittest.mock import MagicMock, patch

import pytest

from datanika.services.auth import AuthService
from datanika.services.email_verification import (
    VerificationMailResult,
    request_email_verification,
)
from datanika.tasks.email_tasks import send_verification_email_task

VERIFY_SECRET = "test-secret-key-mail-path-observability"


@pytest.fixture
def auth():
    return AuthService(VERIFY_SECRET)


@pytest.fixture
def smtp_settings(monkeypatch):
    """A configured relay, applied to the singleton the task reads at call time.

    The task does ``from datanika.config import settings`` inside its own body,
    so patching the singleton's attributes is what reaches it. Patching a
    module-level name would not.
    """
    from datanika.config import settings

    monkeypatch.setattr(settings, "smtp_host", "smtp.example.test")
    monkeypatch.setattr(settings, "smtp_port", 587)
    monkeypatch.setattr(settings, "smtp_user", "mailer")
    monkeypatch.setattr(settings, "smtp_password", "hunter2")
    monkeypatch.setattr(settings, "smtp_from_email", "no-reply@datanika.io")
    monkeypatch.setattr(settings, "smtp_from_name", "Datanika")
    monkeypatch.setattr(settings, "smtp_use_tls", True)
    monkeypatch.setattr(settings, "frontend_url", "https://app.datanika.io")
    return settings


@pytest.fixture
def wire():
    """Fake the socket and nothing above it.

    Yields the mock that stands in for ``smtplib.SMTP`` so a test can read what
    was actually handed to ``sendmail``.
    """
    with patch("datanika.services.email_service.smtplib.SMTP") as smtp_cls:
        server = MagicMock()
        smtp_cls.return_value.__enter__.return_value = server
        yield server


def _sent_message(server) -> str:
    assert server.sendmail.call_count == 1, (
        f"expected exactly one sendmail call, got {server.sendmail.call_count} — "
        "the composed path did not reach the wire"
    )
    return server.sendmail.call_args.args[2]


class TestTheWholePathReachesTheWire:
    """Token minter -> Celery task -> EmailService -> template -> SMTP.

    Every link here is covered somewhere else with the next one stubbed out.
    This is the only place they run against each other.
    """

    def test_a_verification_request_puts_a_usable_link_on_the_wire(
        self, db_session, auth, smtp_settings, wire
    ):
        """The one assertion that a broken mail path cannot satisfy.

        Deliberately end-to-end rather than layered: it goes red if the token
        type changes, the template's format placeholders drift, ``frontend_url``
        stops being plumbed through, the ``/api/verify-email`` prefix moves, the
        from-address is unset, or the task stops calling the service at all —
        each of which is currently invisible to the suite.
        """
        from datanika.services.user_service import UserService

        user = UserService(auth).register_user(
            db_session, "wire@example.com", "password123", "Wire"
        )

        # `.delay` is the only thing swapped for eager execution: the broker is
        # not the system under test, everything downstream of it is.
        with patch.object(send_verification_email_task, "delay", send_verification_email_task):
            queued = request_email_verification(
                user.id, user.email, auth, smtp_host=smtp_settings.smtp_host
            )

        assert queued is VerificationMailResult.QUEUED
        raw = _sent_message(wire)

        assert "no-reply@datanika.io" in raw, "the From address never reached the message"
        assert "wire@example.com" in raw, "the recipient never reached the message"

        # The link has to be one /api/verify-email will actually accept. Asserting
        # that a URL is present is not enough — core#623's sibling bug was a link
        # that rendered fine and pointed at a route that does not exist.
        assert "https://app.datanika.io/api/verify-email?token=" in raw, (
            "the verification URL is absent or malformed; a mail with no usable "
            "link is indistinguishable from no mail, from the user's side"
        )

        token = raw.split("/api/verify-email?token=")[1].split('"')[0].split("<")[0].strip()
        payload = auth.decode_token(token, expected_type="email_verify")
        assert payload is not None, (
            "the token on the wire is not one /api/verify-email accepts — the "
            "user would click a link and be told verification failed"
        )
        assert payload["user_id"] == user.id
        assert payload["email"] == "wire@example.com"

    def test_an_unreachable_relay_does_not_reach_the_wire(self, db_session, auth, smtp_settings):
        """Guard-the-guard: the test above must be capable of failing.

        If ``sendmail`` is never called the assertion helper raises, so this
        pins that the helper is measuring the wire and not something that is
        true either way.
        """
        from datanika.services.user_service import UserService

        user = UserService(auth).register_user(
            db_session, "downrelay@example.com", "password123", "Down"
        )

        with patch("datanika.services.email_service.smtplib.SMTP") as smtp_cls:
            smtp_cls.side_effect = OSError("connection refused")
            with patch.object(send_verification_email_task, "delay", send_verification_email_task):
                request_email_verification(
                    user.id, user.email, auth, smtp_host=smtp_settings.smtp_host
                )
            assert smtp_cls.return_value.__enter__.return_value.sendmail.call_count == 0


class TestTheOutcomeIsObservable:
    """The defect core#700 is actually about.

    Both of these describe the product core#700 asks for. QA parked them as strict
    xfails so they were green on ``dev`` and would fail the moment the fix landed unless
    the markers came off with it — a regression test parked in a branch is one nobody
    runs. The fix landed; the markers came off.
    """

    def test_signup_does_not_discard_whether_the_mail_was_queued(self):
        """``request_email_verification`` returns a bool. Signup throws it away.

        ``auth_state.py`` calls it as a bare expression statement, so the one
        piece of information the product has about the mail path is destroyed at
        the only place that could act on it. Checked structurally rather than by
        substring: ``"request_email_verification"`` appears in that source
        whatever the call does with the result, which is why the existing
        ``test_signup_calls_it`` stays green through this defect.
        """
        from datanika.ui.state import auth_state as auth_state_module

        # dedent first: inspect.getsource returns the method still indented, and
        # ast.parse raises IndentationError on it. Without this the assertion below
        # never ran — the strict xfail was being satisfied by the harness, not by the
        # defect it names.
        source = textwrap.dedent(inspect.getsource(auth_state_module.AuthState.signup.fn))
        tree = ast.parse(source)
        discarded = [
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.Expr)
            and isinstance(node.value, ast.Call)
            and isinstance(node.value.func, ast.Name)
            and node.value.func.id == "request_email_verification"
        ]
        assert not discarded, (
            "signup() calls request_email_verification and drops the result, so a "
            "queue failure and a success are the same event to the rest of the "
            "product. Assign it and act on it."
        )

    def test_auth_state_carries_the_verification_outcome(self):
        """Something a page can render has to hold the answer.

        AC1 (signup confirms the mail was sent, or says it could not) and AC4
        (verification state is visible somewhere a user can act on) both need a
        field to read. There is none, which is why ``/signup`` shows nothing
        either way.
        """
        from datanika.ui.state.auth_state import AuthState

        # Annotated, non-callable names only. A Reflex state var must be
        # annotated to exist; scanning `vars()` as well would let an ordinary
        # method whose name happens to contain "verify" satisfy this, which
        # would make the test pass without a field anything can render.
        annotated = dict(getattr(AuthState, "__annotations__", {}))
        candidates = {
            n
            for n in annotated
            if ("verif" in n.lower() or "email_sent" in n.lower())
            and not n.startswith("_")
            and "token" not in n.lower()
            and not callable(getattr(AuthState, n, None))
        }
        assert candidates, (
            "AuthState exposes nothing about the verification mail, so no page "
            "can render a confirmation or a failure. core#700 AC1/AC4."
        )


class TestTheDeclaredRetryPolicyIsReachable:
    """A retry policy that cannot fire is worse than none — it reads as coverage."""

    def test_a_transport_error_can_reach_celerys_autoretry(self, smtp_settings):
        """Either the declared exceptions propagate, or they should not be declared.

        Passes under **either** correct fix — letting transport errors escape
        ``EmailService.send`` so the retry works, or dropping the ``autoretry_for``
        declaration that currently promises a behaviour the code prevents. It
        only fails while the two disagree.
        """
        declared = getattr(send_verification_email_task, "autoretry_for", ()) or ()
        if not declared:
            pytest.skip("no retry policy declared — nothing to be unreachable")

        with patch("datanika.services.email_service.smtplib.SMTP") as smtp_cls:
            smtp_cls.side_effect = OSError("connection refused")
            with pytest.raises(tuple(declared)):
                send_verification_email_task("retry@example.com", "any-token")
