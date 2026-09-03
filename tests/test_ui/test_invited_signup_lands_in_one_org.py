"""An invited signup joins the inviting org, and nothing else (core#981).

Two live defects on the shipped email path, both in `AuthState.signup`.

**1. Every invited signup created a spurious personal org.** `svc.create_org`
had one call site and no enclosing branch — it ran for every signup, invited or
not — while `accept_invitation` ran 42 lines later and *appended* the invited
org. So a user who accepted an invitation by signing up finished in **two**
orgs: `{full_name}'s Org`, which they own and never asked for, and the team that
invited them. Quota enforcement went live 2026-08-31, so the spare org carries
its own Free-plan limits.

**2. A failed invitation was invisible.** The `except` around
`accept_invitation` was a single `logger.exception` assigning nothing
user-facing. Support got the line; the user got a personal org and no sign that
the link they clicked had not worked. The expired-link case is the common one
and was indistinguishable from success.

🚨 **The flow had no tests, on either path.** `grep -rl invite_token tests/`
returned one file, covering the invitation *email*. So there was nothing to
regress and nothing pinning the old behaviour — which is why every assertion
here is written to be red first.

⚠️ **Assert the count, not the presence.** "the invitee appears in the Members
list" is satisfied by the two-org behaviour and cannot see this bug. The
assertions below count memberships and organizations.
"""

import logging
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from datanika.config import settings
from datanika.models.invitation import Invitation, InvitationStatus
from datanika.models.user import MemberRole, Membership, Organization, User
from datanika.services.auth import AuthService
from datanika.services.invitation_service import InvitationService
from datanika.services.user_service import UserService
from datanika.ui.state.auth_state import AuthState

INVITEE_EMAIL = "invitee@example.com"
INVITEE_PASSWORD = "correct-horse-battery-staple"
INVITEE_NAME = "Ingrid Invitee"


@pytest.fixture(autouse=True)
def _sqlite_returns_aware_datetimes():
    """Make the SQLite harness honour ``DateTime(timezone=True)``.

    🚨 **Without this the happy path fails for a reason production does not
    have, and the failure looks like the feature being broken.**

    `Invitation.expires_at` is `DateTime(timezone=True)`; PostgreSQL returns it
    tz-aware and SQLite — which has no `timestamptz` — silently returns it
    naive. `accept_invitation` compares it to `datetime.now(UTC)`, so the moment
    the instance is re-loaded from the database rather than served from the
    identity map, the comparison raises
    `TypeError: can't compare offset-naive and offset-aware datetimes`.

    ⚠️ The existing `tests/test_services/test_invitation_service.py` does not
    hit this, and that is luck rather than coverage: it creates the invitation
    and accepts it in the same unit of work, so the aware value it wrote is
    still the one in the session. Anything that flushes between the two — which
    a signup does, repeatedly — gets the naive one back.

    Scoped to this module deliberately. A global fix belongs in `conftest.py`
    and would change what ~4,000 other tests are running against, which is not
    a change to make as a side effect of a bug fix.
    """
    from datetime import UTC as _UTC

    from sqlalchemy import event

    def _make_aware(target, _context):
        if target.expires_at is not None and target.expires_at.tzinfo is None:
            target.expires_at = target.expires_at.replace(tzinfo=_UTC)

    event.listen(Invitation, "load", _make_aware, propagate=True)
    try:
        yield
    finally:
        event.remove(Invitation, "load", _make_aware)


@pytest.fixture
def auth():
    return AuthService(settings.secret_key)


@pytest.fixture
def inviting_org(db_session, auth):
    """An org with an owner, ready to invite somebody."""
    svc = UserService(auth)
    owner = svc.register_user(db_session, "owner@example.com", "owner-password-1", "Olive Owner")
    org = svc.create_org(db_session, "Acme Data", "acme-data", owner.id)
    db_session.flush()
    return SimpleNamespace(org=org, owner=owner, org_id=org.id, owner_id=owner.id)


def _invite(db_session, auth, inviting_org, *, email=INVITEE_EMAIL) -> str:
    inv = InvitationService(auth).create_invitation(
        db_session,
        inviting_org.org_id,
        email,
        MemberRole.EDITOR,
        inviting_org.owner_id,
    )
    db_session.flush()
    return inv.token


class _SignupState:
    """Stand-in for ``AuthState`` — only what ``signup`` may touch.

    ⚠️ Deliberately not a bare ``MagicMock``. A mock answers every attribute
    truthily, so an assertion about *which* org the handler ended on would pass
    against a handler that never set one. Everything here is a real value the
    handler is allowed to read or write.
    """

    def __init__(self, service, invite_token=""):
        self.auth_error = ""
        self.signup_blocked = ""
        self.verification_mail_state = ""
        self.invite_notice = ""
        self.access_token = ""
        self.refresh_token = ""
        self.current_user = SimpleNamespace(id=0, email="", full_name="")
        self.current_org = SimpleNamespace(id=0, name="", slug="")
        self.user_orgs = []
        self.current_role = ""
        self.email = ""
        self._service = service
        self.router = SimpleNamespace(
            page=SimpleNamespace(params={"invite_token": invite_token} if invite_token else {})
        )

    #: The **real** helper, bound onto the stand-in rather than reimplemented.
    #: It is where the invitation decision and both log lines live, so faking it
    #: would leave every assertion below testing a copy of the fix.
    _accept_signup_invitation = getattr(
        AuthState._accept_signup_invitation, "fn", AuthState._accept_signup_invitation
    )

    def _client_ip(self):
        return ""

    def _get_user_service(self):
        return self._service

    def _post_auth_redirect_target(self):
        return "/"


def _run_signup(db_session, auth, *, invite_token="", email=INVITEE_EMAIL) -> _SignupState:
    """Execute the real ``signup`` handler against a real database session.

    ⚠️ **``commit`` is redirected to ``flush``, and it is not tidiness.**
    ``signup`` commits its own session. ``db_session`` joins an outer
    transaction the fixture rolls back (``join_transaction_mode=
    "create_savepoint"``), so a real ``commit()`` ends that transaction: the
    rollback then silently does nothing and rows **leak between tests**. It
    surfaced here exactly as it did on core#652 — as
    ``UserServiceError: Email already exists`` in seven tests whose own setup
    was fine, which reads like a fixture-scope bug rather than a lost rollback.
    """
    state = _SignupState(UserService(auth), invite_token=invite_token)

    session_cm = MagicMock()
    session_cm.__enter__.return_value = db_session
    session_cm.__exit__.return_value = False

    with (
        patch("datanika.ui.state.auth_state.CaptchaService") as captcha,
        patch("datanika.ui.state.auth_state.get_sync_session", return_value=session_cm),
        patch("datanika.ui.state.auth_state._allow", return_value=True),
        patch(
            "datanika.ui.state.auth_state.request_email_verification",
            return_value=SimpleNamespace(value="queued"),
        ),
        patch("datanika.ui.state.auth_state.collect_events", return_value=[]),
        patch.object(db_session, "commit", db_session.flush),
    ):
        captcha.return_value.verify.return_value = True
        AuthState.signup.fn(
            state,
            {
                "email": email,
                "password": INVITEE_PASSWORD,
                "full_name": INVITEE_NAME,
                "captcha_token": "tok",
            },
        )
    return state


def _orgs_and_memberships(db_session, email: str):
    user = db_session.query(User).filter(User.email == email).one()
    memberships = (
        db_session.query(Membership)
        .filter(Membership.user_id == user.id, Membership.deleted_at.is_(None))
        .all()
    )
    orgs = [db_session.get(Organization, m.org_id) for m in memberships]
    return user, memberships, orgs


# --------------------------------------------------------------------------
# AC1 — exactly one membership and exactly one org
# --------------------------------------------------------------------------


def test_an_invited_signup_creates_no_personal_org(db_session, auth, inviting_org):
    """The regression. Red before the fix: two memberships, two orgs.

    ``create_org`` had no enclosing branch, so the personal org was created
    before the invitation was even looked at.
    """
    token = _invite(db_session, auth, inviting_org)
    _run_signup(db_session, auth, invite_token=token)

    _user, memberships, orgs = _orgs_and_memberships(db_session, INVITEE_EMAIL)

    assert len(memberships) == 1, (
        "an invited signup produced "
        f"{len(memberships)} memberships in {[o.name for o in orgs]}. It must join the "
        "inviting org and nothing else — the spare personal org clutters the switcher "
        "from the account's first second and carries its own Free-plan quota."
    )
    assert orgs[0].id == inviting_org.org_id, (
        f"the single membership is in {orgs[0].name!r}, not the inviting org"
    )
    assert not any(o.name == f"{INVITEE_NAME}'s Org" for o in orgs), (
        "the personal org was created anyway"
    )
    # And no orphan org exists either — a fix that creates the org and then
    # leaves it unjoined would satisfy the membership count alone.
    assert (
        db_session.query(Organization).filter(Organization.name == f"{INVITEE_NAME}'s Org").count()
        == 0
    ), "the personal org row was created and then abandoned rather than not created"


def test_the_invited_session_lands_on_the_inviting_org(db_session, auth, inviting_org):
    """Session state, not just rows. ``current_org`` was ambiguous immediately
    after signup — set to the personal org and then reassigned."""
    token = _invite(db_session, auth, inviting_org)
    state = _run_signup(db_session, auth, invite_token=token)

    assert state.current_org.id == inviting_org.org_id
    assert [o.id for o in state.user_orgs] == [inviting_org.org_id], (
        f"the org switcher would show {[o.name for o in state.user_orgs]}"
    )
    assert state.current_role == MemberRole.EDITOR.value, (
        "the invited role was not applied to the session"
    )
    assert state.access_token, "the session must carry a token scoped to the invited org"
    assert not state.invite_notice, "a successful invitation must not warn about anything"


# --------------------------------------------------------------------------
# AC3 — the false-positive control: an uninvited signup is unchanged
# --------------------------------------------------------------------------


def test_an_uninvited_signup_still_gets_a_personal_org(db_session, auth):
    """The control. Without it, "create no personal org" is satisfied by a
    handler that creates no org at all, and every new user lands nowhere."""
    state = _run_signup(db_session, auth, email="solo@example.com")

    _user, memberships, orgs = _orgs_and_memberships(db_session, "solo@example.com")
    assert len(memberships) == 1
    assert orgs[0].name == f"{INVITEE_NAME}'s Org"
    assert state.current_org.id == orgs[0].id
    assert state.current_role == "owner"
    assert not state.invite_notice, (
        "a signup with no token must produce no message about invitations — "
        "otherwise every ordinary signup is told something went wrong"
    )


# --------------------------------------------------------------------------
# AC2 — a token that cannot be applied is announced
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    "kind",
    ["expired", "already_accepted", "garbage", "wrong_address"],
    ids=str,
)
def test_an_unusable_token_falls_back_and_says_so(db_session, auth, inviting_org, kind):
    """Red before the fix on every branch: the swallow assigned nothing.

    Four ways a token fails to apply, because they reach ``accept_invitation``'s
    ``None`` return by four different routes and a fix that only handles one of
    them leaves the rest silent. ``garbage`` additionally exercises the
    ``except`` path rather than the ``None`` path.
    """
    if kind == "garbage":
        token = "not-a-real-token"
    else:
        token = _invite(
            db_session,
            auth,
            inviting_org,
            email="someone-else@example.com" if kind == "wrong_address" else INVITEE_EMAIL,
        )
        inv = db_session.query(Invitation).order_by(Invitation.id.desc()).first()
        if kind == "expired":
            from datetime import UTC, datetime, timedelta

            inv.expires_at = datetime.now(UTC) - timedelta(days=1)
        elif kind == "already_accepted":
            inv.status = InvitationStatus.ACCEPTED
        db_session.flush()

    state = _run_signup(db_session, auth, invite_token=token)

    _user, memberships, orgs = _orgs_and_memberships(db_session, INVITEE_EMAIL)

    # AC5 — nobody finishes signup with zero orgs.
    assert len(memberships) == 1, (
        f"an unusable {kind} token left the user in {len(memberships)} orgs; the personal "
        "org is the fallback and must be created when the invitation produced no membership"
    )
    assert orgs[0].name == f"{INVITEE_NAME}'s Org"
    assert state.current_org.id == orgs[0].id

    # AC2 — and it is announced.
    assert state.invite_notice, (
        f"a {kind} invitation token was dropped with no user-visible sign. The user is "
        "signed in, in a personal org, believing they joined a team. Support gets the log "
        "line; the user gets nothing."
    )


def test_the_dropped_invitation_is_still_logged(db_session, auth, inviting_org, caplog):
    """The log line stays — it is what support needs (core#723).

    Making the failure user-visible must not remove the operator's evidence;
    these are two audiences, not one, and a fix that swaps one for the other has
    traded a silent user for a silent operator.
    """
    token = _invite(db_session, auth, inviting_org)
    inv = db_session.query(Invitation).order_by(Invitation.id.desc()).first()
    inv.status = InvitationStatus.ACCEPTED
    db_session.flush()

    with caplog.at_level(logging.INFO, logger="datanika.ui.state.auth_state"):
        _run_signup(db_session, auth, invite_token=token)

    assert any("nvitation" in r.message for r in caplog.records), (
        "no log record mentions the invitation; support has nothing to go on"
    )


def test_the_notice_is_not_the_raw_reason(db_session, auth, inviting_org):
    """It must be an i18n key or state value the UI can translate, not prose.

    ``test_message`` on connections is rendered raw from the service and reads
    in English in all nine locales (core#872 / SPEC_LOCAL_FILE_CONNECTIONS D6).
    This notice must not repeat that: the state carries a *value*, the page
    carries the sentence.
    """
    token = _invite(db_session, auth, inviting_org)
    inv = db_session.query(Invitation).order_by(Invitation.id.desc()).first()
    inv.status = InvitationStatus.ACCEPTED
    db_session.flush()

    state = _run_signup(db_session, auth, invite_token=token)
    # Premise first: an empty notice satisfies "contains no space" and would let
    # this pass against the unfixed code, which assigned nothing at all.
    assert state.invite_notice, "no notice was set, so this assertion is vacuous"
    assert " " not in state.invite_notice, (
        f"invite_notice is {state.invite_notice!r} — a sentence, not a state value. "
        "Nine locales would read it in English. Carry a slug and translate in the page, "
        "the way signup_blocked does."
    )


# --------------------------------------------------------------------------
# The notice has to be RENDERED. A state var nothing reads is core#887's
# defect — ten of fifteen state classes assign `error_message` and no page
# renders it, so the button did nothing and said nothing.
# --------------------------------------------------------------------------


def _build_shell():
    """Render the authenticated shell, capturing Reflex's stderr warnings."""
    import contextlib
    import io

    import reflex as rx

    from datanika.ui.components.layout import page_layout

    buf = io.StringIO()
    with contextlib.redirect_stdout(buf), contextlib.redirect_stderr(buf):
        rendered = str(page_layout(rx.text("body"), title="T"))
    return rendered, buf.getvalue()


class TestTheShellRendersTheNotice:
    def test_page_layout_mounts_it(self):
        """Rendered from ``page_layout``, not from the helper.

        Building the helper directly proves the helper works and says nothing
        about whether the shell mounts it — which is the whole failure mode.
        """
        rendered, _ = _build_shell()
        assert "invite_notice" in rendered, (
            "page_layout does not mount the invitation notice, so the user still sees "
            "nothing — the state var would be set and read by nobody (core#887)"
        )

    def test_the_dismiss_control_is_wired(self):
        rendered, _ = _build_shell()
        assert "dismiss_invite_notice" in rendered

    def test_building_the_shell_substitutes_no_icon(self):
        """Reflex swaps an unknown icon for ``circle_help`` and only warns on stderr.

        So "the layout builds" proves nothing about its icons — a typo would put
        a help bubble on every authenticated page with every test still green.
        """
        _, captured = _build_shell()
        assert "Invalid icon tag" not in captured, f"an icon was substituted: {captured}"


class TestTheSentencesExistInEveryLocale:
    KEYS = ("auth.invite_not_applied", "auth.invite_not_applied_help")

    @pytest.mark.parametrize("locale", ["en", "ru", "el", "de", "fr", "es", "zh", "ar", "sr"])
    def test_both_keys_are_present_and_translated(self, locale):
        """Parity is enforced elsewhere; this checks the values are not English
        copies pasted into nine files, which parity cannot see."""
        import json
        from pathlib import Path

        data = json.loads((Path("datanika/i18n") / f"{locale}.json").read_text(encoding="utf-8"))
        en = json.loads((Path("datanika/i18n") / "en.json").read_text(encoding="utf-8"))
        for key in self.KEYS:
            assert key in data, f"{locale}.json is missing {key}"
            assert data[key].strip(), f"{locale}.json has an empty {key}"
            if locale != "en":
                assert data[key] != en[key], (
                    f"{locale}.json's {key} is the English string verbatim — nine "
                    "locales reading English is the defect SPEC_LOCAL_FILE_CONNECTIONS "
                    "D6 names"
                )
