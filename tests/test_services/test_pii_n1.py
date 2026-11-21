"""Register, invite, and channel create keep personal data on the sidecars.

core#939. N+1 stopped writing the legacy columns. N+2 dropped them, so these
tests no longer read those attributes. The sidecar is the copy that remains.
"""

from __future__ import annotations

from datanika.models.notification_channel import ChannelType
from datanika.models.pii import InvitationPII, NotificationChannelPII, UserPII
from datanika.models.user import MemberRole
from datanika.services.auth import AuthService
from datanika.services.email_change_service import READY, EmailChangeService
from datanika.services.invitation_service import InvitationService, hash_invitation_token
from datanika.services.notification_service import NotificationService
from datanika.services.user_service import UserService
from tests.factories import make_invitation, make_user


def _services():
    auth = AuthService("pii-n1-test-secret")
    return auth, UserService(auth), InvitationService(auth)


def test_register_writes_the_sidecar_and_leaves_the_legacy_columns_empty(db_session):
    _, users, _ = _services()
    user = users.register_user(db_session, "Ada@Example.com", "a fine password", "Ada Lovelace")
    pii = db_session.get(UserPII, user.id)
    assert pii is not None
    assert pii.email == "ada@example.com"
    assert pii.full_name == "Ada Lovelace"


def test_get_user_by_email_misses_when_the_sidecar_row_is_gone(db_session):
    _, users, _ = _services()
    user = make_user(
        db_session, email="legacy-only@example.com", full_name="Legacy", password_hash="x"
    )
    pii = db_session.get(UserPII, user.id)
    assert pii is not None
    db_session.delete(pii)
    db_session.flush()
    assert users.get_user_by_email(db_session, "legacy-only@example.com") is None


def test_oauth_create_keeps_the_provider_on_the_user_and_the_subject_on_the_sidecar(db_session):
    _, users, _ = _services()
    created, is_new = users.find_or_create_oauth_user(
        db_session, "OAuth@Example.com", "Octavia", "github", "gh-subject", email_verified=True
    )
    assert is_new is True
    assert created.oauth_provider == "github"
    pii = db_session.get(UserPII, created.id)
    assert pii is not None
    assert pii.email == "oauth@example.com"
    assert pii.full_name == "Octavia"
    assert pii.oauth_provider_id == "gh-subject"


def test_a_new_invitation_keeps_the_address_and_stops_storing_the_plaintext_token(db_session):
    auth, users, invites = _services()
    owner = users.register_user(db_session, "owner@example.com", "owner password", "Owner")
    org = users.create_org(db_session, "Owners Org", f"org-{owner.id}", owner.id)
    inv = invites.create_invitation(
        db_session, org.id, "Invitee@Example.com", MemberRole.EDITOR, owner.id
    )
    invited = db_session.get(InvitationPII, inv.id)
    assert invited is not None and invited.email == "invitee@example.com"
    raw = inv.plaintext_token
    assert raw
    assert inv.token_hash == hash_invitation_token(raw)
    assert invites.get_invitation_by_token(db_session, raw).id == inv.id
    assert auth is not None


def test_get_invitation_by_token_misses_a_row_whose_only_copy_is_plaintext(db_session):
    _, users, invites = _services()
    owner = users.register_user(db_session, "owner2@example.com", "owner password", "Owner")
    org = users.create_org(db_session, "Owners Org", f"org-{owner.id}", owner.id)
    inv = make_invitation(
        db_session,
        org_id=org.id,
        email="plain@example.com",
        role=MemberRole.VIEWER,
        invited_by_user_id=owner.id,
        token="plaintext-token-value",
    )
    inv.token_hash = "0" * 64
    db_session.flush()
    assert invites.get_invitation_by_token(db_session, "plaintext-token-value") is None


def test_accept_ignores_a_legacy_invitation_address_when_the_sidecar_is_gone(db_session):
    _, users, invites = _services()
    owner = users.register_user(db_session, "owner3@example.com", "owner password", "Owner")
    org = users.create_org(db_session, "Owners Org", f"org-{owner.id}", owner.id)
    users.register_user(db_session, "joiner@example.com", "joiner password", "Joiner")
    inv = make_invitation(
        db_session,
        org_id=org.id,
        email="joiner@example.com",
        role=MemberRole.EDITOR,
        invited_by_user_id=owner.id,
        token="accept-me",
    )
    pii = db_session.get(InvitationPII, inv.id)
    assert pii is not None
    db_session.delete(pii)
    db_session.flush()
    assert invites.accept_invitation(db_session, "accept-me") is None


def test_confirming_an_email_change_moves_the_sidecar_and_leaves_the_legacy_column(db_session):
    _, users, _ = _services()
    user = users.register_user(db_session, "alice@example.com", "alice password", "Alice")
    users.create_org(db_session, "Alice Org", f"org-{user.id}", user.id)
    changes = EmailChangeService(users)
    prepared = changes.prepare_change(db_session, user.id, "new@example.com")
    assert prepared.status == READY
    confirmed = changes.confirm_change(db_session, prepared.token)
    db_session.refresh(user)
    assert confirmed is not None
    assert db_session.get(UserPII, user.id).email == "new@example.com"
    assert user.email_verified is True
    # The stale legacy column must not block a return to the previous address.
    back = changes.prepare_change(db_session, user.id, "alice@example.com")
    assert back.status == READY


def test_an_email_channel_stores_the_recipient_on_the_sidecar_only(db_session):
    _, users, _ = _services()
    owner = users.register_user(db_session, "ops@example.com", "ops password", "Ops")
    org = users.create_org(db_session, "Ops Org", f"org-{owner.id}", owner.id)
    channel = NotificationService().create_channel(
        db_session,
        org.id,
        name="Alerts",
        channel_type=ChannelType.EMAIL,
        config={"email": "alerts@example.com"},
        events=["run_failure"],
        actor_user_id=owner.id,
    )
    assert "email" not in (channel.config or {})
    pii = db_session.get(NotificationChannelPII, channel.id)
    assert pii is not None and pii.recipient == "alerts@example.com"


def test_a_telegram_channel_keeps_the_token_and_moves_the_chat_id(db_session):
    _, users, _ = _services()
    owner = users.register_user(db_session, "ops2@example.com", "ops password", "Ops")
    org = users.create_org(db_session, "Ops Org", f"org-{owner.id}", owner.id)
    channel = NotificationService().create_channel(
        db_session,
        org.id,
        name="Telegram",
        channel_type=ChannelType.TELEGRAM,
        config={"token": "bot-secret", "chat_id": "4242"},
        events=["run_failure"],
        actor_user_id=owner.id,
    )
    assert channel.config.get("token") == "bot-secret"
    assert "chat_id" not in channel.config
    pii = db_session.get(NotificationChannelPII, channel.id)
    assert pii is not None and pii.recipient == "4242"
