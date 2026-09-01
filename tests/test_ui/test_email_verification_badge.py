"""Verification state must be visible and actionable on /settings (core#700 AC4).

AC1, AC2, AC3 and AC5 shipped in #750/#752: signup reports whether the mail was
queued, ``VerificationMailResult`` distinguishes ``NO_RELAY`` from ``FAILED``,
``/login?verified=1`` renders, and the regression tests exist.

**AC4 was left open on purpose**, and the reason is recorded on the issue: a
resend control with no rate limit is a mail amplifier, and that deserved its own
change rather than a rider. This is that change.

Re-derived against ``origin/dev`` before writing anything, because the issue
title still says verification "succeeds silently and fails silently" and that is
no longer the whole truth:

* ``AuthState.verification_mail_state`` exists and **is** rendered —
  ``ui/components/layout.py:158 verification_mail_notice()``, mounted in the app
  shell at line 336. Signup is no longer silent.
* ``users.email_verified`` is read in ``ui/`` **nowhere**. The one hit,
  ``AuthState.show_email_verified``, reads the ``?verified=1`` *query param* —
  it says "you just clicked a link", not "your address is confirmed". So a user
  who missed that one redirect has no way to find out, ever.

⚠️ ``tests/test_ui/test_error_message_is_rendered.py`` does **not** cover this
path, and it was worth checking rather than assuming: that guard walks
``error_message``, and the verification surfaces use their own vars
(``verification_mail_state`` here, ``resend_state`` below). Same *family* —
a state var nothing renders — different variable, so the existing walk is blind
to it by construction. The rendering assertions in
:class:`TestTheBadgeIsActuallyRendered` are this file's equivalent, and they are
the reason the state vars below are not just another silent write.

## The mail-amplification control, and how the shape of it changed

The issue's own note warns that a resend button is "a mail-bomb aimed at any
address a signed-in user can name". That is true of a design where the address
is an **input**. This one reads it off the ``User`` row, so the worst case is a
signed-in user mailing themselves — still worth bounding (it burns the relay
quota and the address may not be theirs any more), but a strictly smaller
exposure. :meth:`TestTheAddressIsNotAttackerControlled` pins the difference so a
later "let the user correct the address here" edit cannot quietly reintroduce
the larger one.
"""

from __future__ import annotations

import pathlib
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import datanika.ui.state.account_state as acc
from datanika.models.user import MemberRole, Membership, Organization
from datanika.services.auth import AuthService
from datanika.services.email_verification import VerificationMailResult
from datanika.services.user_service import UserService
from datanika.ui.state.account_state import AccountState

PAGES = pathlib.Path(acc.__file__).resolve().parents[1] / "pages"


@pytest.fixture
def svc():
    return UserService(AuthService("test-secret"))


@pytest.fixture
def user(db_session, svc):
    u = svc.register_user(db_session, "alice@example.com", "correct horse", "Alice")
    org = Organization(name="Alice Org", slug=f"alice-{u.id}")
    db_session.add(org)
    db_session.flush()
    db_session.add(Membership(user_id=u.id, org_id=org.id, role=MemberRole.OWNER))
    db_session.flush()
    return u


def _state(user_id=0, org_id=0):
    st = MagicMock()
    for name, field in AccountState.__fields__.items():
        default = field.default_factory() if field.default_factory else field.default
        setattr(st, name, default)
    auth = MagicMock()
    auth.current_user.id = user_id
    auth.current_org.id = org_id
    st.get_state = AsyncMock(return_value=auth)
    st._service = lambda: AccountState._service(st)
    return st


class _TestSession:
    """``db_session`` proxy whose ``commit()`` is a ``flush()`` — see
    ``test_account_state.py`` for why the shared fixture needs this."""

    def __init__(self, session):
        self._session = session

    def commit(self):
        self._session.flush()

    def __getattr__(self, name):
        return getattr(self._session, name)


def _session_patch(db_session):
    ctx = MagicMock()
    ctx.__enter__ = MagicMock(return_value=_TestSession(db_session))
    ctx.__exit__ = MagicMock(return_value=False)
    return ctx


def _allowing_limiter():
    limiter = MagicMock()
    limiter.check_window.return_value = MagicMock(allowed=True, retry_after=0)
    return limiter


def _refusing_limiter(retry_after=1800):
    limiter = MagicMock()
    limiter.check_window.return_value = MagicMock(allowed=False, retry_after=retry_after)
    return limiter


async def _resend(st, db_session, limiter=None, mail=None):
    """Drive the handler with the ambient session and limiter replaced."""
    st._rate_limiter = lambda: limiter if limiter is not None else _allowing_limiter()
    with patch.object(acc, "get_sync_session", return_value=_session_patch(db_session)):
        if mail is None:
            return await AccountState.resend_verification.fn(st)
        with patch.object(acc, "request_email_verification", mail):
            return await AccountState.resend_verification.fn(st)


async def _load(st, db_session):
    with patch.object(acc, "get_sync_session", return_value=_session_patch(db_session)):
        return await AccountState.load_account.fn(st)


# ---------------------------------------------------------------------------
# Section 1 — the state is visible at all
# ---------------------------------------------------------------------------


class TestLoadReportsVerificationState:
    @pytest.mark.asyncio
    async def test_an_unverified_account_reports_false(self, db_session, user):
        """Red against unfixed dev: ``AccountState`` has no such attribute.

        ``register_user`` leaves ``email_verified`` False, which is every
        password account until it clicks the link.
        """
        assert user.email_verified is False
        st = _state(user.id, 1)
        await _load(st, db_session)
        assert st.email_verified is False

    @pytest.mark.asyncio
    async def test_control_a_verified_account_reports_true(self, db_session, user):
        """The control that differs in exactly one fact.

        Without it, a handler that hardcoded ``False`` would pass the test
        above — and an always-on "unverified" badge is worse than none, because
        it trains people to ignore it.
        """
        user.email_verified = True
        db_session.flush()
        st = _state(user.id, 1)
        await _load(st, db_session)
        assert st.email_verified is True

    @pytest.mark.asyncio
    async def test_the_default_does_not_flash_a_false_alarm(self):
        """Before ``load_account`` runs, the badge must not claim anything.

        Reflex renders the card with defaults on first paint. Defaulting to
        False would show "unverified" to a verified user for one frame, on
        every page load.
        """
        assert AccountState.__fields__["email_verified"].default is True

    @pytest.mark.asyncio
    async def test_the_address_shown_is_the_one_on_file(self, db_session, user):
        st = _state(user.id, 1)
        await _load(st, db_session)
        assert st.account_email == "alice@example.com"

    @pytest.mark.asyncio
    async def test_a_signed_out_caller_gets_nowhere(self, db_session, user):
        st = _state(0, 0)
        await _load(st, db_session)
        assert st.account_email == ""


# ---------------------------------------------------------------------------
# Section 2 — resend, and every outcome it can have
# ---------------------------------------------------------------------------


class TestResendReportsEveryOutcome:
    """The three-outcome vocabulary AC2 established, plus refusal.

    A bool would collapse ``no_relay`` (a normal self-hosted deployment, nothing
    wrong) into ``failed`` (a real outage) all over again — the exact defect
    #752 fixed one layer down.
    """

    @pytest.mark.asyncio
    async def test_queued_is_recorded(self, db_session, user):
        st = _state(user.id, 1)
        mail = MagicMock(return_value=VerificationMailResult.QUEUED)
        await _resend(st, db_session, mail=mail)
        assert st.resend_state == "queued"
        assert mail.call_count == 1

    @pytest.mark.asyncio
    async def test_no_relay_is_not_reported_as_a_failure(self, db_session, user):
        st = _state(user.id, 1)
        mail = MagicMock(return_value=VerificationMailResult.NO_RELAY)
        await _resend(st, db_session, mail=mail)
        assert st.resend_state == "no_relay"

    @pytest.mark.asyncio
    async def test_failure_is_recorded_rather_than_swallowed(self, db_session, user):
        """The whole issue in one assertion.

        ``request_email_verification`` never raises by design, so a caller that
        drops its return value produces a UI in which a failed send and a
        successful one are the same event.
        """
        st = _state(user.id, 1)
        mail = MagicMock(return_value=VerificationMailResult.FAILED)
        await _resend(st, db_session, mail=mail)
        assert st.resend_state == "failed"

    @pytest.mark.asyncio
    async def test_the_outcomes_are_distinguishable_from_each_other(self, db_session, user):
        """Pairwise, because "it set something" is not evidence it set the
        right thing — three handlers that all wrote ``"done"`` would satisfy
        every test above read individually."""
        seen = {}
        for result in VerificationMailResult:
            st = _state(user.id, 1)
            await _resend(st, db_session, mail=MagicMock(return_value=result))
            seen[result.value] = st.resend_state
        assert len(set(seen.values())) == len(VerificationMailResult), seen


# ---------------------------------------------------------------------------
# Section 3 — the rate limit
# ---------------------------------------------------------------------------


class TestResendIsRateLimited:
    @pytest.mark.asyncio
    async def test_a_refusal_does_not_send(self, db_session, user):
        """The limiter has to gate the *send*, not just the message.

        A handler that mails first and reports "rate limited" afterwards passes
        any assertion about ``resend_state`` alone while still sending every
        time — which is the entire thing the limit exists to prevent.
        """
        st = _state(user.id, 1)
        mail = MagicMock(return_value=VerificationMailResult.QUEUED)
        await _resend(st, db_session, limiter=_refusing_limiter(), mail=mail)
        assert st.resend_state == "rate_limited"
        assert mail.call_count == 0

    @pytest.mark.asyncio
    async def test_the_bucket_is_per_user(self, db_session, user):
        """One person exhausting their allowance must not mute anyone else's."""
        limiter = _allowing_limiter()
        st = _state(user.id, 1)
        await _resend(st, db_session, limiter=limiter, mail=MagicMock())
        bucket = limiter.check_window.call_args.args[0]
        assert str(user.id) in bucket

    @pytest.mark.asyncio
    async def test_the_window_is_bounded_and_not_per_minute(self, db_session, user):
        """A per-minute window is not a mail limit; it is a click limit."""
        limiter = _allowing_limiter()
        st = _state(user.id, 1)
        await _resend(st, db_session, limiter=limiter, mail=MagicMock())
        kwargs = limiter.check_window.call_args.kwargs
        args = limiter.check_window.call_args.args
        window = kwargs.get("window_seconds", args[2] if len(args) > 2 else None)
        limit = kwargs.get("limit", args[1] if len(args) > 1 else None)
        assert window is not None and window >= 600, f"window too short: {window}"
        assert limit is not None and 1 <= limit <= 10, f"implausible limit: {limit}"

    @pytest.mark.asyncio
    async def test_a_limiter_outage_does_not_open_the_floodgates(self, db_session, user):
        """Fail **closed**, unlike the concurrency limiter.

        A Redis outage must not turn the resend button into an unbounded mailer.
        The cost of failing closed is a button that says "try again later" while
        Redis is down; the cost of failing open is our relay reputation.
        """
        limiter = MagicMock()
        limiter.check_window.side_effect = RuntimeError("redis down")
        st = _state(user.id, 1)
        mail = MagicMock(return_value=VerificationMailResult.QUEUED)
        await _resend(st, db_session, limiter=limiter, mail=mail)
        assert mail.call_count == 0
        assert st.resend_state == "rate_limited"


# ---------------------------------------------------------------------------
# Section 4 — what the resend is aimed at
# ---------------------------------------------------------------------------


class TestTheAddressIsNotAttackerControlled:
    @pytest.mark.asyncio
    async def test_the_address_comes_from_the_user_row(self, db_session, user):
        st = _state(user.id, 1)
        st.account_email = "attacker@example.com"
        mail = MagicMock(return_value=VerificationMailResult.QUEUED)
        await _resend(st, db_session, mail=mail)
        sent_to = mail.call_args.args[1]
        assert sent_to == "alice@example.com", (
            "the resend used a state var as the destination; a signed-in user "
            "could then mail an arbitrary address from our relay"
        )

    @pytest.mark.asyncio
    async def test_an_already_verified_account_sends_nothing(self, db_session, user):
        """No verified address should be re-mailable at all."""
        user.email_verified = True
        db_session.flush()
        st = _state(user.id, 1)
        mail = MagicMock(return_value=VerificationMailResult.QUEUED)
        await _resend(st, db_session, mail=mail)
        assert mail.call_count == 0

    @pytest.mark.asyncio
    async def test_a_signed_out_caller_sends_nothing(self, db_session, user):
        st = _state(0, 0)
        mail = MagicMock(return_value=VerificationMailResult.QUEUED)
        await _resend(st, db_session, mail=mail)
        assert mail.call_count == 0


# ---------------------------------------------------------------------------
# Section 5 — it reaches a screen
# ---------------------------------------------------------------------------


class TestTheBadgeIsActuallyRendered:
    """core#700 is *about* state that no surface reflects.

    Shipping two more unrendered state vars to fix it would be the same defect
    with more code. ``test_error_message_is_rendered.py`` cannot see these —
    it walks ``error_message`` — so the check lives here.
    """

    def _settings_source(self) -> str:
        return (PAGES / "settings.py").read_text(encoding="utf-8")

    def test_the_verified_flag_reaches_the_settings_page(self):
        assert "AccountState.email_verified" in self._settings_source()

    def test_the_resend_outcome_reaches_the_settings_page(self):
        assert "AccountState.resend_state" in self._settings_source()

    def test_the_resend_control_is_wired_to_the_handler(self):
        assert "AccountState.resend_verification" in self._settings_source()

    def test_every_value_resend_state_can_take_is_rendered(self):
        """The core#887 lesson applied to a four-valued var, not a bool.

        `rx.cond` branches are per-value, so a handler can report an outcome the
        page has no branch for and the user sees nothing happen -- the same
        defect as an unrendered `error_message`, one level up. Asserted per
        value rather than "the var is mentioned somewhere", which one branch
        satisfies.
        """
        source = self._settings_source()
        for outcome in ("queued", "no_relay", "failed", "rate_limited"):
            assert f'AccountState.resend_state == "{outcome}"' in source, (
                f"resend_state can be {outcome!r} and /settings has no branch for it"
            )

    def test_the_control_finds_the_file_it_thinks_it_does(self):
        """Negative control.

        A wrong ``PAGES`` path would make every assertion above fail loudly, but
        a *stale* one could pass against a file that is no longer served. Assert
        the source really is the settings page.
        """
        source = self._settings_source()
        assert "def account_card()" in source
        assert "AccountState.has_password" in source


class TestEveryOutcomeHasCopy:
    """A state value with no i18n key renders as a blank callout.

    That is this issue's failure mode wearing a different hat: the handler
    reports, the page branches, and the user sees an empty box.
    """

    def test_all_four_outcomes_are_translated(self):
        import json

        # parents[2], not [1]: `acc.__file__` is ui/state/account_state.py, so
        # parents[1] is `ui/` and the i18n directory is a level above that. The
        # off-by-one made this assert against a path that does not exist.
        i18n = pathlib.Path(acc.__file__).resolve().parents[2] / "i18n" / "en.json"
        keys = json.loads(i18n.read_text(encoding="utf-8"))
        for outcome in ("queued", "no_relay", "failed", "rate_limited"):
            assert f"account.resend_{outcome}" in keys, outcome
        assert "account.email_unverified" in keys
        assert "account.resend_verification" in keys
