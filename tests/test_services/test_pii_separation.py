"""Release N of the PII-separation chain — extraction, erasure and org deletion.

``docs/specs/SPEC_PII_SEPARATION.md``, core#655. Acceptance criteria 1, 2, 3, 5, 6, 8,
8a, 8b, 8c, 8d, 8e, 9, 10, 11, 12.

Two criteria in here are written so that **every other erasure assertion passes on the
broken implementation**, and they are the reason this file is not just a happy-path sweep:

* **8d (§0.2)** — the legacy columns are cleared too, asserted with raw SQL against the
  columns rather than through the ORM's join. An erasure that deletes the ``user_pii`` row
  and stops there satisfies criterion 8 (no PII rows), criterion 3
  (``get_user_by_email`` returns ``None``), makes login structurally impossible and leaves
  no surface rendering anything — while the address sits untouched in ``users.email``.
  **Only a query against the legacy column can see it**, and once N+2 drops those columns
  it becomes untestable, so it is exercised now or never.
* **8e (§0.1)** — the sole-member org is renamed **before** it is soft-deleted, asserted
  *without* a ``deleted_at IS NULL`` filter, because the natural query skips exactly the
  row that carries the problem.
"""

from __future__ import annotations

import pytest
from sqlalchemy import text

from datanika.models.invitation import Invitation, InvitationStatus
from datanika.models.notification_channel import ChannelType
from datanika.models.pii import InvitationPII, NotificationChannelPII, UserPII
from datanika.models.user import MemberRole, Membership, Organization, User
from datanika.services.auth import AuthService
from datanika.services.invitation_service import InvitationService, hash_invitation_token
from datanika.services.notification_service import NotificationService
from datanika.services.user_service import UserService, UserServiceError


@pytest.fixture
def auth():
    return AuthService("test-secret-key-for-pii-separation")


@pytest.fixture
def svc(auth):
    return UserService(auth)


def _register(svc, session, email="alice@example.com", name="Alice Adams"):
    user = svc.register_user(session, email, "sup3rSecret!pw", name)
    org = svc.create_org(session, f"{name}'s Org", f"org-{user.id}", user.id)
    session.flush()
    return user, org


# ---------------------------------------------------------------------------
# Criterion 1 / 2 — the tables exist and the dual-write reaches them
# ---------------------------------------------------------------------------


class TestExtraction:
    def test_registering_writes_the_sidecar_and_the_legacy_columns(self, svc, db_session):
        user, _ = _register(svc, db_session)
        pii = db_session.get(UserPII, user.id)
        assert pii is not None, "register_user did not dual-write user_pii"
        assert pii.email == "alice@example.com"
        assert pii.full_name == "Alice Adams"
        # Release N still writes the legacy columns: the previously deployed code reads
        # them through the blue/green swap. N+1 stops; N+2 drops.
        assert user.email == "alice@example.com"

    def test_every_active_user_has_exactly_one_pii_row(self, svc, db_session):
        """Criterion 2 — the count check the N backfill has to satisfy.

        ⚠️ Scoped to **the rows this test created**, not to the whole table. The first
        version compared `count(users)` against `count(user_pii)` globally; it passed in
        isolation and failed in the full run, because other files legitimately insert
        `User` rows directly (and one in this very file does so on purpose, to reproduce
        the t1 window). A guard whose verdict depends on what else ran is not a guard —
        and this one's failure would have read as "the dual-write is broken".
        """
        created = [
            _register(svc, db_session, "a@example.com", "A Person")[0],
            _register(svc, db_session, "b@example.com", "B Person")[0],
        ]
        for user in created:
            assert user.deleted_at is None
            assert db_session.get(UserPII, user.id) is not None, (
                f"user {user.id} has no user_pii row — the N backfill's count check "
                "(criterion 2) is what this asserts, per user rather than in aggregate"
            )

    def test_get_user_by_email_refuses_a_soft_deleted_user(self, svc, db_session):
        """Criterion 3 — and a join written without the `deleted_at` filter passes every
        other test in this file. Without it, erasure is a security regression: the
        account still authenticates."""
        user, _ = _register(svc, db_session)
        assert svc.get_user_by_email(db_session, "alice@example.com") is not None

        user.deleted_at = __import__("datetime").datetime.now(__import__("datetime").UTC)
        db_session.flush()
        assert svc.get_user_by_email(db_session, "alice@example.com") is None

    def test_the_legacy_column_is_still_readable_during_the_dual_write_window(
        self, svc, db_session
    ):
        """The t1 window, which is the reason `get_user_by_email` reads BOTH columns.

        At the blue/green swap the previously deployed container is still registering
        people, and it writes `users.email` with no `user_pii` row. A join-only read would
        leave every account created during the swap unable to sign in — a failure that
        appears only in production, only during a deploy, and only for the users who
        arrived in that window.
        """
        user = User(
            email="t1-window@example.com",
            password_hash="x",
            full_name="Swap Window",
        )
        db_session.add(user)
        db_session.flush()
        assert db_session.get(UserPII, user.id) is None, "precondition: no sidecar row"
        assert svc.get_user_by_email(db_session, "t1-window@example.com") is not None

    def test_new_org_slug_carries_no_part_of_the_name(self, svc, db_session):
        """Criterion 5. §2c measured `organizations.slug` holding a live `users.full_name`
        in 5 of 5 production rows — a person's name published in a durable, URL-bearing,
        unique key that erasing `users.full_name` does not reach."""
        user, org = _register(svc, db_session, "zebediah@example.com", "Zebediah Quirk")
        assert org.slug == f"org-{user.id}"
        assert "zebediah" not in org.slug.lower()
        assert "quirk" not in org.slug.lower()


class TestInvitationTokenHashing:
    def test_a_new_invitation_stores_a_hash(self, auth, svc, db_session):
        """Criterion 4, first half.

        ⚠️ The second half — *"`SELECT token` yields nothing decodable"* — **cannot be
        true in release N** and is not asserted here. N dual-writes the plaintext token
        because the previously deployed code looks invitations up by equality on it, and
        the column is NOT NULL until this release's migration widens it. It becomes true
        at N+2, when the column is dropped. Raised on core#655.
        """
        owner, org = _register(svc, db_session, "owner@example.com", "Owner One")
        inv_svc = InvitationService(auth)
        inv = inv_svc.create_invitation(
            db_session, org.id, "Invitee@Example.com", MemberRole.EDITOR, owner.id
        )
        assert inv.token_hash == hash_invitation_token(inv.token)
        assert len(inv.token_hash) == 64

        pii = db_session.get(InvitationPII, inv.id)
        assert pii is not None and pii.email == "invitee@example.com"

    def test_the_emailed_link_still_accepts(self, auth, svc, db_session):
        """The whole point of hashing is that the *stored* value changes and the *usable*
        one does not."""
        owner, org = _register(svc, db_session, "owner2@example.com", "Owner Two")
        inv_svc = InvitationService(auth)
        inv = inv_svc.create_invitation(
            db_session, org.id, "joiner@example.com", MemberRole.EDITOR, owner.id
        )
        raw = inv.token
        found = inv_svc.get_invitation_by_token(db_session, raw)
        assert found is not None and found.id == inv.id


class TestNotificationChannelExtraction:
    def test_the_recipient_is_mirrored_into_the_sidecar(self, svc, db_session):
        _, org = _register(svc, db_session, "ops@example.com", "Ops Person")
        ch = NotificationService().create_channel(
            db_session,
            org.id,
            name="Alerts",
            channel_type=ChannelType.EMAIL,
            config={"email": "alerts@example.com"},
            events=["run_failure"],
        )
        pii = db_session.get(NotificationChannelPII, ch.id)
        assert pii is not None and pii.recipient == "alerts@example.com"


# ---------------------------------------------------------------------------
# Erasure
# ---------------------------------------------------------------------------


class TestErasure:
    def test_the_pii_row_and_credentials_go(self, svc, db_session):
        """Criterion 8."""
        user, _ = _register(svc, db_session)
        counts = svc.erase_user(db_session, user.id)
        assert db_session.get(UserPII, user.id) is None
        assert counts["user_pii"] == 1
        assert user.deleted_at is not None
        assert user.is_active is False

    def test_it_happens_the_moment_the_call_returns(self, svc, db_session):
        """Criterion 8a — no job, no queue, no wait (D14.1).

        An implementation that marks-and-defers passes criterion 8 an hour later and
        breaks the `/privacy` sentence, and nothing else here would notice. The
        `/privacy` promise asserts a mechanism — *"the 30 days IS the off-site backup
        retention window"* — and that arithmetic only holds if the live purge is prompt.
        """
        user, _ = _register(svc, db_session)
        svc.erase_user(db_session, user.id)
        # Same transaction, no flush of our own, no waiting.
        assert db_session.get(UserPII, user.id) is None

    def test_the_legacy_columns_are_cleared_too(self, svc, db_session):
        """🚨 Criterion 8d (§0.2) — the one assertion a working-looking implementation fails.

        Read with **raw SQL against the legacy columns**, not through the ORM's join. An
        erasure that deletes the `user_pii` row and stops there satisfies every other
        criterion in this class while leaving the address exactly where it was extracted
        from.
        """
        user, _ = _register(svc, db_session, "leftover@example.com", "Leftover Person")
        user_id = user.id
        svc.erase_user(db_session, user_id)
        db_session.flush()

        row = db_session.execute(
            text("SELECT email, full_name, oauth_provider_id FROM users WHERE id = :i"),
            {"i": user_id},
        ).one()
        assert row.email is None, (
            "users.email still holds the erased address. The user_pii row is gone, "
            "get_user_by_email returns None and login is impossible — and the personal "
            "datum is still in the database. This is the dual-write window's second copy."
        )
        assert row.full_name is None
        assert row.oauth_provider_id is None

    def test_a_sole_member_org_is_renamed_before_it_is_soft_deleted(self, svc, db_session):
        """🚨 Criterion 8e (§0.1).

        Queried **without** a `deleted_at IS NULL` filter, deliberately: the row is
        soft-deleted by the time we look, and the natural query — the shape every
        org-scoped query in this codebase uses — would skip precisely the row that
        carries the problem.
        """
        user, org = _register(svc, db_session, "solo@example.com", "Solo Person")
        org.name = "Solo Person's Org"
        org.slug = "solo-person-1"
        db_session.flush()
        org_id = org.id

        svc.erase_user(db_session, user.id)
        db_session.flush()

        row = db_session.execute(
            text("SELECT name, slug, deleted_at FROM organizations WHERE id = :i"),
            {"i": org_id},
        ).one()
        assert row.deleted_at is not None, "the sole-member org should have been deleted"
        assert row.name == f"Organization {org_id}"
        assert row.slug == f"org-{org_id}"
        assert "solo" not in row.name.lower() and "solo" not in row.slug.lower()

    def test_the_erased_address_can_register_again(self, svc, db_session):
        """Criterion 9. Freeing the address is the point, not a side effect: a tombstone
        that blocked re-registration would be a retained pseudonymous identifier that
        re-identifies the person on lookup."""
        user, _ = _register(svc, db_session, "returning@example.com", "Returning Person")
        old_id = user.id
        svc.erase_user(db_session, old_id)
        db_session.flush()

        new_user, _ = _register(svc, db_session, "returning@example.com", "Returning Person")
        assert new_user.id != old_id
        assert db_session.get(UserPII, new_user.id) is not None

    def test_no_table_holds_the_erased_address(self, svc, db_session):
        """Criterion 10, run as a query over **every** text column rather than over the
        tables anyone remembered. That is what catches a partial implementation."""
        user, org = _register(svc, db_session, "sweepme@example.com", "Sweep Me")
        org.name = "Sweep Me's Org"
        org.slug = "sweep-me-1"
        NotificationService().create_channel(
            db_session,
            org.id,
            name="Alerts",
            channel_type=ChannelType.EMAIL,
            config={"email": "sweepme@example.com"},
            events=["run_failure"],
        )
        db_session.flush()
        svc.erase_user(db_session, user.id)
        db_session.flush()

        from datanika.models.base import Base

        offenders: list[str] = []
        for table in Base.metadata.tables.values():
            text_cols = [
                c.name for c in table.columns if str(c.type).upper().startswith(("VARCHAR", "TEXT"))
            ]
            if not text_cols:
                continue
            for row in db_session.execute(text(f"SELECT * FROM {table.name}")).mappings():
                for col in text_cols:
                    value = row.get(col)
                    if isinstance(value, str) and (
                        "sweepme@example.com" in value or "Sweep Me" in value
                    ):
                        offenders.append(f"{table.name}.{col}")
        assert not offenders, f"the erased address or name survives in: {sorted(set(offenders))}"

    def test_a_sole_owner_of_a_shared_org_is_refused(self, svc, db_session):
        """Criterion 11 and §9a(1). Refusing with no route out would be worse than the
        current state, so the message names both exits."""
        owner, org = _register(svc, db_session, "boss@example.com", "Boss Person")
        member, _ = _register(svc, db_session, "member@example.com", "Member Person")
        db_session.add(Membership(user_id=member.id, org_id=org.id, role=MemberRole.EDITOR))
        db_session.flush()

        with pytest.raises(UserServiceError) as exc:
            svc.erase_user(db_session, owner.id)
        assert "only owner" in str(exc.value).lower()
        assert "transfer ownership" in str(exc.value).lower()

    def test_the_refusal_leaves_everything_byte_identical(self, svc, db_session):
        """Criterion 6's shape, applied to the refusal: a wrong answer must change nothing.

        A refusal that had already renamed the org or deleted a credential before
        discovering it could not finish is worse than one that never started.
        """
        owner, org = _register(svc, db_session, "boss2@example.com", "Boss Two")
        member, _ = _register(svc, db_session, "member2@example.com", "Member Two")
        db_session.add(Membership(user_id=member.id, org_id=org.id, role=MemberRole.EDITOR))
        db_session.flush()
        before = (org.name, org.slug, owner.email, owner.deleted_at)

        with pytest.raises(UserServiceError):
            svc.erase_user(db_session, owner.id)

        assert (org.name, org.slug, owner.email, owner.deleted_at) == before
        assert db_session.get(UserPII, owner.id) is not None

    def test_pending_invitations_this_person_sent_are_revoked(self, auth, svc, db_session):
        """D5 step 3 — the invitee's address is *their* personal data, on a row this user
        authored, and the invitation cannot complete anyway."""
        owner, org = _register(svc, db_session, "inviter@example.com", "Inviter Person")
        inv = InvitationService(auth).create_invitation(
            db_session, org.id, "pending@example.com", MemberRole.EDITOR, owner.id
        )
        inv_id = inv.id
        svc.erase_user(db_session, owner.id)
        db_session.flush()

        refreshed = db_session.get(Invitation, inv_id)
        assert refreshed.status is InvitationStatus.CANCELLED
        assert db_session.get(InvitationPII, inv_id) is None
        assert refreshed.email is None

    def test_the_residual_audit_sweep_reports_its_count(self, svc, db_session):
        """Criterion 8c — it must run and report, not merely not-fail.

        Expected value is zero *because D11 and the redactor already made it impossible*.
        A sweep that reports nothing and a sweep that never ran are the same reading, so
        the criterion is about the report.
        """
        user, _ = _register(svc, db_session, "canary@example.com", "Canary Person")
        counts = svc.erase_user(db_session, user.id)
        assert "audit_payloads_redacted" in counts
        assert counts["audit_payloads_redacted"] == 0

    def test_the_residual_sweep_finds_a_planted_row(self, svc, db_session):
        """🚨 §2c criterion 2 — the sweep proven against a **deliberately planted** row.

        *"Finds zero"* is also what this returns before the feature exists, so a clean run
        is not evidence of anything. The row is planted with raw SQL precisely because the
        application can no longer produce one: `log_action` redacts at the chokepoint, so
        going through the service would test the redactor rather than the sweep.
        """
        user, org = _register(svc, db_session, "planted@example.com", "Planted Person")
        # 'UPDATE', not 'update'. `AuditLog.action` is `Enum(AuditAction,
        # native_enum=False)` with no `values_callable`, so SQLAlchemy persists the enum
        # MEMBER NAME and refuses anything else on read:
        #
        #     LookupError: 'update' is not among the defined enum values
        #
        # Worth writing down rather than just fixing, because it is the exact mechanism
        # that makes D5 step 8's `action="user.erased"` a t1 hazard rather than a typo:
        # the previously deployed code raises this when it *reads* a value it does not
        # know, and the audit page lists rows for a whole org. Measured here by accident,
        # which is better evidence than the argument on core#655.
        db_session.execute(
            text(
                "INSERT INTO audit_logs (org_id, user_id, action, resource_type, new_values) "
                "VALUES (:o, :u, 'UPDATE', 'member', :v)"
            ),
            {"o": org.id, "u": user.id, "v": '{"email": "planted@example.com"}'},
        )
        db_session.flush()

        counts = svc.erase_user(db_session, user.id)
        assert counts["audit_payloads_redacted"] == 1, (
            "the residual sweep did not find a planted PII-bearing payload, so its zero "
            "on a clean run says nothing at all"
        )


class TestOrgDeletion:
    def test_the_org_scoped_table_set_is_derived_and_exact(self):
        """The derivation is pinned, both because a shorter list would leave rows visible
        while every count read zero, and because of what must NOT be in it.

        🚨 `subscriptions`, `usage_ledger` and `charges` are org-scoped and carry a
        `deleted_at`, so a naive walk of `Base.metadata` soft-deletes billing records
        whenever the cloud plugin is installed. `datanika.io/privacy` §6 promises they are
        kept **7 years, as tax law requires**. The filter that excludes them is a
        module-name test, so it works without core importing or naming cloud — and this
        assertion is what keeps it working.
        """
        tables = UserService.org_scoped_core_tables()
        assert set(tables) == {
            "api_keys",
            "catalog_entries",
            "connections",
            "dependencies",
            "invitations",
            "memberships",
            "notification_channels",
            "notifications",
            "oauth_grants",
            "oauth_tokens",
            "pipelines",
            "runs",
            "schedules",
            "sso_configs",
            "transformations",
            "uploaded_files",
            "uploads",
        }
        for forbidden in ("subscriptions", "usage_ledger", "charges"):
            assert forbidden not in tables, (
                f"{forbidden} is a billing record kept for 7 years by law and must never "
                "be swept by an org deletion"
            )
        # `audit_logs` has no `deleted_at` at all — it is append-only by design, and the
        # trail survives with a person removed from it rather than being deleted with them.
        assert "audit_logs" not in tables

    def test_deleting_an_org_marks_the_rows_not_just_the_org(self, svc, db_session):
        """`Organization.deleted_at` is read in exactly one place in the codebase and
        written nowhere, so org-scoped queries do not filter on it. Setting it alone hides
        nothing."""
        user, org = _register(svc, db_session, "orgdel@example.com", "Org Del")
        ch = NotificationService().create_channel(
            db_session,
            org.id,
            name="Alerts",
            channel_type=ChannelType.EMAIL,
            config={"email": "alerts2@example.com"},
            events=["run_failure"],
        )
        counts = svc.delete_org(db_session, org.id)
        db_session.flush()
        assert counts["organizations"] == 1
        assert counts["notification_channels"] >= 1
        db_session.refresh(ch)
        assert ch.deleted_at is not None
        membership = db_session.query(Membership).filter_by(org_id=org.id).one()
        assert membership.deleted_at is not None

    def test_a_subscriber_can_veto_the_deletion(self, svc, db_session):
        """D6 item 3 — the Paddle subscription is cancelled **first**, and a failed
        cancellation aborts the deletion.

        Core cannot call `BillingService` and must not try, so this goes through the hook
        system — `emit`, which propagates a raise, rather than `announce`, which swallows
        one. The reverse order would leave a subscription with no org to attribute it to.
        """
        from datanika import hooks

        user, org = _register(svc, db_session, "billed@example.com", "Billed Person")

        def refuse(**kwargs):
            raise RuntimeError("Paddle cancellation failed")

        hooks.on("org.before_delete", refuse)
        try:
            with pytest.raises(RuntimeError):
                svc.delete_org(db_session, org.id)
        finally:
            hooks.off("org.before_delete", refuse)

        db_session.refresh(org)
        assert org.deleted_at is None, "the org was deleted despite the veto"
        membership = db_session.query(Membership).filter_by(org_id=org.id).one()
        assert membership.deleted_at is None, "rows were touched before the veto point"

    def test_the_tenant_dbt_project_directory_is_removed(self, svc, db_session, tmp_path):
        """Criterion 12 — it lives outside the database and nothing soft-deletes a
        directory."""
        user, org = _register(svc, db_session, "dbtdel@example.com", "Dbt Del")
        project = tmp_path / f"tenant_{org.id}"
        project.mkdir()
        (project / "dbt_project.yml").write_text("name: x", encoding="utf-8")

        counts = svc.delete_org(db_session, org.id, projects_dir=str(tmp_path))
        assert counts["dbt_project_dirs"] == 1
        assert not project.exists()


class TestOrganizationModel:
    def test_a_pii_table_has_no_deleted_at(self):
        """D1, and §0's whole point. A `deleted_at` on a table whose purpose is *hard*
        deletion is a trap waiting to be read as "erased" while the row is still there —
        and a soft-deleted row is still a row in `pg_dump`, in a backup, and to a
        regulator."""
        for model in (UserPII, InvitationPII, NotificationChannelPII):
            assert not hasattr(model, "deleted_at"), (
                f"{model.__name__} has a deleted_at column. These tables are hard-deleted; "
                "a soft-delete marker here is how an erasure silently becomes a hide."
            )

    def test_the_pii_tables_are_public(self):
        """They are person-scoped, like `users` itself — an address belongs to a person,
        not to an org."""
        from datanika.migrations.helpers import PUBLIC_TABLES

        for name in (
            "user_pii",
            "invitation_pii",
            "notification_channel_pii",
            "email_change_requests",
        ):
            assert name in PUBLIC_TABLES

    def test_audit_log_pii_is_not_built(self):
        """§2a / D11, criterion 1. It would protect `audit_logs.ip_address`, which is
        populated in **0 of 117** production rows. Three tables ship, not four; the
        sidecar lands with core#670, when there is data to protect."""
        from datanika.models.base import Base

        assert "audit_log_pii" not in Base.metadata.tables
        assert Organization is not None  # keeps the import honest


class TestTheSettingsControl:
    """D9/D10 — the self-service half. Criteria 6 and 7.

    D10 does not accept `erase_user` with only one entry point: *"a merged erase_user with
    no caller is a fourth instance"* of the audit's recurring machinery-without-an-entry-point
    finding. Both ship — `datanika/scripts/erase_user.py` for a request arriving by email
    from somebody who cannot reach the UI, and this.
    """

    def test_the_confirmation_is_typed_and_scoped_to_the_dialog(self):
        """WORKFLOW_RULES §7b applied to the implementation, not just to whoever drives it.

        The destructive handler must sit on the dialog's form, never on the trigger, so
        nothing destructive is reachable without the dialog open — and the confirmation
        must be *typed*, not a second button (D9).
        """
        import inspect

        from datanika.ui.pages import settings as settings_page

        source = inspect.getsource(settings_page._delete_account_dialog)
        assert "rx.alert_dialog.root" in source, (
            "must be rx.alert_dialog (a real role='alertdialog'), not rx.dialog — "
            "core#804's pattern"
        )
        assert "on_submit=AccountState.delete_account" in source
        trigger = source.split("rx.alert_dialog.trigger(")[1].split("rx.alert_dialog.content")[0]
        assert "delete_account" not in trigger, (
            "the destructive handler is on the dialog TRIGGER, so it fires without the "
            "dialog ever opening — that is the shape §7b is about"
        )
        assert 'name="confirmation"' in source, "the confirmation must be typed"

    def test_an_oauth_only_account_is_offered_the_org_name_not_a_password(self):
        """🚨 Criterion 7, and the spec flags it as a trap on purpose: *a test covering
        only the password variant passes on the broken implementation.*

        The discriminator is `password_changed_at IS NULL` (core#623's), reached through
        `has_usable_password` — never `oauth_provider`, which
        `find_or_create_oauth_user` backfills onto a pre-existing **password** account on
        first social login. Gating on that would demand a current password from someone
        who has one, and offer an org-name field to someone who does not.
        """
        import inspect

        from datanika.ui.pages import settings as settings_page

        source = inspect.getsource(settings_page._delete_account_dialog)
        assert "AccountState.has_password" in source
        assert "account.delete_confirm_org_name" in source, (
            "the OAuth-only branch is missing: an account with no password would be shown "
            "a password field it can never satisfy"
        )
        assert "account.delete_confirm_password" in source

        # Checked against the CODE, with the docstring stripped: the docstring names
        # `oauth_provider` in order to say not to use it, and a naive substring check
        # over the whole source therefore fails on a correct implementation for the
        # opposite of the right reason. (It did, on the first run.)
        code_only = source.split('"""', 2)[-1]
        assert "oauth_provider" not in code_only, (
            "the dialog is discriminating on oauth_provider, which "
            "find_or_create_oauth_user backfills onto password accounts on first social "
            "login (core#623) — so it would demand a password from someone who has one "
            "and offer an org-name field to someone who does not"
        )

    def test_the_dialog_states_what_survives(self):
        """D9 — before the confirm button is reachable the dialog must state what is
        deleted, what is **kept** (billing records, 7 years, by law), that warehouse
        schemas in the customer's own account are untouched, and that backups age out.

        Each of these will otherwise arrive as a support ticket, and two of them are
        promises on `datanika.io/privacy` that the dialog must not contradict.
        """
        import inspect

        from datanika.ui.pages import settings as settings_page

        source = inspect.getsource(settings_page._delete_account_dialog)
        for key in (
            "account.delete_what_goes",
            "account.delete_what_stays",
            "account.delete_backups_note",
            "account.delete_org_too",
            "account.delete_last_owner",
        ):
            assert key in source, f"the dialog does not state {key}"

    def test_preconditions_come_from_the_service_not_a_second_copy_of_the_rule(self):
        """The dialog must not be able to promise a deletion the service will refuse.

        `erasure_preconditions` runs the *same* classifier `erase_user` runs. A state
        layer that re-derived "is this person the last owner" would be a second answer to
        a question that already has one, and the two would drift.
        """
        import inspect

        from datanika.ui.state.account_state import AccountState

        # `.fn`: Reflex wraps a state method in an `EventHandler`, which `inspect` refuses
        # with `TypeError: ... got EventHandler`. The same unwrap the auth-state guards use.
        handler = AccountState.load_delete_preconditions
        source = inspect.getsource(getattr(handler, "fn", handler))
        assert "erasure_preconditions" in source

    def test_the_refusal_and_the_erasure_agree(self, svc, db_session):
        """The two paths give the same verdict on the same state — asserted against the
        service rather than the rendered page, because that is where they could diverge."""
        owner, org = _register(svc, db_session, "agree@example.com", "Agree Person")
        member, _ = _register(svc, db_session, "agree2@example.com", "Agree Two")
        db_session.add(Membership(user_id=member.id, org_id=org.id, role=MemberRole.EDITOR))
        db_session.flush()

        facts = svc.erasure_preconditions(db_session, owner.id)
        assert facts["blocking_org"] == org.name
        with pytest.raises(UserServiceError):
            svc.erase_user(db_session, owner.id)

    def test_a_sole_member_org_is_announced_before_confirmation(self, svc, db_session):
        """D9: the org consequence is *stated to the user before they confirm*, not
        discovered afterwards."""
        user, org = _register(svc, db_session, "alone@example.com", "Alone Person")
        facts = svc.erasure_preconditions(db_session, user.id)
        assert facts["sole_member_org"] == org.name
        assert facts["blocking_org"] == ""
