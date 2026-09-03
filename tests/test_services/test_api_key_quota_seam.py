"""`ApiKeyService` must offer a `before_create` seam like every other quota (core#706).

API keys are the only priced dimension with no `*.before_create` hook. QA
verified the absence three ways: the cloud `Plan` has no `max_api_keys`,
`plugin.py` registers no API-key handler, and **`api_key_service.py` emits no
hooks at all — so there is no seam a quota could attach to even if one were
written.** This file is that seam.

Why it matters more than a missing column: `api_middleware` resolves
`rate_limit_rpm` per **org** and buckets per **key**, so the published per-plan
rate bounds nothing while key creation is unbounded. A Free org with ten keys
sustains 300 rpm — exactly what Enterprise is sold
(`plans/qa/notes/probe-705/`, finding 4). A per-key bucket cannot enforce a
per-plan entitlement; only a cap on keys can.

⚠️ **Core ships no cap.** `plans.max_api_keys` is nullable and NULL on every
plan, and the enforcement handler lives in the cloud plugin. In the core edition
nothing subscribes, `emit` returns immediately, and behaviour is unchanged. That
is deliberate: core is the open-source edition and must not grow a paywall.
"""

import pytest

from datanika import hooks
from datanika.models.api_key import ApiKey
from datanika.models.user import Organization
from datanika.services.api_key_service import ApiKeyService
from tests.factories import make_user


@pytest.fixture
def clean_hooks():
    """Isolate the global bus — it is process-wide state."""
    saved = {event: list(handlers) for event, handlers in hooks._handlers.items()}
    hooks._handlers.clear()
    yield hooks
    hooks._handlers.clear()
    hooks._handlers.update(saved)


@pytest.fixture
def org_and_user(db_session):
    org = Organization(name="Seam", slug="api-key-seam")
    db_session.add(org)
    db_session.flush()
    user = make_user(db_session, email="seam@example.com", full_name="Seam", password_hash="h")
    return org, user


def _count_keys(session, org_id: int) -> int:
    from sqlalchemy import select

    return len(session.execute(select(ApiKey).where(ApiKey.org_id == org_id)).scalars().all())


class TestTheSeamExists:
    def test_creation_emits_before_create(self, db_session, org_and_user, clean_hooks):
        org, user = org_and_user
        seen = []
        clean_hooks.on("api_key.before_create", lambda **kw: seen.append(kw))

        ApiKeyService().create_api_key(db_session, org.id, user.id, "k")

        assert len(seen) == 1, "api_key.before_create was not emitted"
        assert seen[0]["org_id"] == org.id
        assert seen[0]["session"] is db_session, (
            "the handler must receive the caller's session, or a quota check reads a "
            "different transaction than the one about to write the row"
        )

    def test_a_vetoing_subscriber_blocks_creation(self, db_session, org_and_user, clean_hooks):
        """`emit`, not `announce` — a subscriber refusing is the whole point.

        `hooks.emit` propagates; `hooks.announce` swallows and logs. Using the
        wrong verb here is silent: the event fires, the handler raises, the key
        is created anyway, and every test that only asserts the emit stays green.
        """
        org, user = org_and_user

        class RefusedError(Exception):
            pass

        def refuse(**_kw):
            raise RefusedError("over quota")

        clean_hooks.on("api_key.before_create", refuse)

        with pytest.raises(RefusedError):
            ApiKeyService().create_api_key(db_session, org.id, user.id, "k")

    def test_a_vetoed_creation_writes_no_row(self, db_session, org_and_user, clean_hooks):
        """The assertion that pins the *ordering*, not just the exception.

        Emitting after `session.add(api_key)` raises just as loudly while
        leaving the row in the session for the caller's next `flush` to commit —
        a gate that refuses and creates anyway. Counting rows is the only
        assertion that tells the two apart.
        """
        org, user = org_and_user

        def refuse(**_kw):
            raise RuntimeError("over quota")

        clean_hooks.on("api_key.before_create", refuse)

        with pytest.raises(RuntimeError):
            ApiKeyService().create_api_key(db_session, org.id, user.id, "k")

        db_session.flush()
        assert _count_keys(db_session, org.id) == 0, (
            "the refused key was written anyway — the emit runs after session.add"
        )

    def test_creation_still_works_with_no_subscriber(self, db_session, org_and_user, clean_hooks):
        """NEGATIVE CONTROL, and the core edition's actual behaviour.

        Nothing subscribes in core, so `emit` must be a no-op rather than a new
        way for key creation to fail.
        """
        org, user = org_and_user
        key, raw = ApiKeyService().create_api_key(db_session, org.id, user.id, "k")

        assert key.id is not None
        assert raw.startswith("etf_")
        assert _count_keys(db_session, org.id) == 1

    def test_the_gate_runs_before_the_key_is_minted(self, db_session, org_and_user, clean_hooks):
        """A refused request must not burn entropy or leave a hash lying around.

        Minor, but it is the difference between a gate and an undo.
        """
        org, user = org_and_user
        observed: list[int] = []

        def count_rows(**kw):
            observed.append(_count_keys(kw["session"], org.id))

        clean_hooks.on("api_key.before_create", count_rows)
        ApiKeyService().create_api_key(db_session, org.id, user.id, "k")

        assert observed == [0], (
            f"the handler saw {observed} existing keys — it must run before the new row "
            "is added, so a quota check counts the rows it is deciding about"
        )


class TestTheMigrationShipsTheColumn:
    def test_plans_gains_max_api_keys(self) -> None:
        """The cloud `Plan` model declares it; core owns the `plans` table.

        Cloud's own `test_migration_coverage` fails the build when a cloud model
        column has no core migration, so this is the core-side half of that
        contract stated where the migration lives.
        """
        import pathlib

        versions = (
            pathlib.Path(__file__).resolve().parents[2] / "datanika" / "migrations" / "versions"
        )
        hits = [
            p.name for p in versions.glob("*.py") if "max_api_keys" in p.read_text(encoding="utf-8")
        ]
        assert hits, "no migration adds plans.max_api_keys"

    def test_the_column_is_nullable(self) -> None:
        """NULL means uncapped, and every plan ships NULL.

        A NOT NULL column with a default would choose a cap by accident, and
        `SET NOT NULL` is forbidden in the same release as the code needing it
        (`SPEC_EXPAND_CONTRACT_MIGRATIONS.md`).
        """
        import pathlib

        versions = (
            pathlib.Path(__file__).resolve().parents[2] / "datanika" / "migrations" / "versions"
        )
        source = "\n".join(
            p.read_text(encoding="utf-8")
            for p in versions.glob("*.py")
            if "max_api_keys" in p.read_text(encoding="utf-8")
        )
        assert "nullable=True" in source, "plans.max_api_keys must be nullable — NULL = uncapped"
        assert "server_default" not in source.split("max_api_keys")[1][:400], (
            "a server_default on this column would choose a cap nobody decided"
        )
