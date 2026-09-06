"""`/models`' empty state must not tell a user to do what they just did (core#883).

`models.no_models` reads *"No models found. Run an upload or transformation to
populate the catalog."* That is correct for someone who has never run one. Told
to a user whose upload just went green with a row count — which is exactly what
happens when the destination dataset was deleted, renamed or mistyped, because
`get_table_names` answers `[]` for a missing schema — it sends them back around
the same loop. The absent signal is not merely missing; it is replaced by a
confident wrong one.

`ModelState.loaded_without_catalog` is the discriminator, and these tests pin
both directions: it must be True only when rows have been loaded *and* the
catalog is empty.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

import datanika.ui.state.model_state as model_state_module
from datanika.models.catalog_entry import CatalogEntryType
from datanika.models.dependency import NodeType
from datanika.models.user import Organization
from datanika.services.catalog_service import CatalogService
from datanika.services.execution_service import ExecutionService
from datanika.ui.state.model_state import ModelState


class _TestSession:
    """`commit()` becomes `flush()` so the outer test transaction can roll back."""

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


class _St:
    """Minimal stand-in carrying only what `load_models` touches.

    ⚠️ **It answers `get_state`, not just `_get_org_id` (core#1097).** The loader
    used to resolve its org through `BaseState._get_org_id`; since core#1097 it
    reads `current_org.id` and `current_user.id` off `AuthState` directly, so a
    signed-out visitor can be refused before any session is opened — which
    `_get_org_id` cannot express, because it says nothing about the user.

    These four tests are all about an org that *is* resolved, so both ids are
    supplied. `_get_org_id` stays for any other handler that still uses it.
    """

    def __init__(self, org_id: int):
        self._org_id = org_id
        self.models: list = []
        self.loaded_without_catalog = False
        self.error_message = "stale"

    async def _get_org_id(self):
        return self._org_id

    async def get_state(self, _cls):
        return SimpleNamespace(
            current_org=SimpleNamespace(id=self._org_id),
            current_user=SimpleNamespace(id=1),
        )


@pytest.fixture
def org(db_session):
    import uuid

    o = Organization(name="Acme", slug=f"acme-models-{uuid.uuid4().hex[:8]}")
    db_session.add(o)
    db_session.flush()
    return o


def _completed_upload_run(db_session, org_id: int, *, rows: int):
    exec_svc = ExecutionService()
    run = exec_svc.create_run(db_session, org_id, NodeType.UPLOAD, 1)
    exec_svc.complete_run(db_session, org_id, run.id, rows_loaded=rows, logs="ok")
    return run


async def _load(db_session, org_id: int) -> _St:
    st = _St(org_id)
    with (
        patch.object(
            model_state_module, "get_sync_session", return_value=_session_patch(db_session)
        ),
        # `load_models` builds an EncryptionService from settings, and the test
        # config carries the insecure placeholder rather than a Fernet key.
        # Nothing here decrypts anything — there are no connections in play —
        # so the cipher is genuinely irrelevant to what is being asserted.
        patch.object(model_state_module, "EncryptionService", MagicMock()),
    ):
        await ModelState.load_models.fn(st)
    return st


class TestLoadedWithoutCatalog:
    @pytest.mark.asyncio
    async def test_true_when_a_run_loaded_rows_and_the_catalog_is_empty(self, db_session, org):
        """The case core#883 is about: green run, row count, empty catalog."""
        _completed_upload_run(db_session, org.id, rows=10)

        st = await _load(db_session, org.id)

        assert st.models == []
        assert st.loaded_without_catalog is True

    @pytest.mark.asyncio
    async def test_false_for_an_org_that_has_never_run_anything(self, db_session, org):
        """The negative control that keeps the generic copy correct where it IS
        correct. Without it, a flag hardcoded True would pass the test above."""
        st = await _load(db_session, org.id)

        assert st.models == []
        assert st.loaded_without_catalog is False

    @pytest.mark.asyncio
    async def test_false_when_a_run_loaded_zero_rows(self, db_session, org):
        """A genuinely empty load is not a contradiction — same boundary as the
        run-log warning in `upload_tasks`, and it has to agree with it."""
        _completed_upload_run(db_session, org.id, rows=0)

        st = await _load(db_session, org.id)

        assert st.loaded_without_catalog is False

    @pytest.mark.asyncio
    async def test_false_once_the_catalog_has_entries(self, db_session, org):
        """Not merely "has this org loaded" — the flag must go back down when
        the catalog is populated, or the wrong copy would be permanent for
        anyone who ever hit the bug."""
        _completed_upload_run(db_session, org.id, rows=10)
        CatalogService().upsert_entry(
            db_session,
            org.id,
            entry_type=CatalogEntryType.SOURCE_TABLE,
            origin_type=NodeType.UPLOAD,
            origin_id=1,
            table_name="users",
            schema_name="d",
            dataset_name="d",
            columns=[{"name": "id", "data_type": "INTEGER"}],
        )

        st = await _load(db_session, org.id)

        assert st.models != []
        assert st.loaded_without_catalog is False


class TestThePageActuallyReadsIt:
    def test_the_models_page_renders_both_empty_state_strings(self):
        """A state var nothing renders is core#887's whole subject — so assert
        the page reaches both keys, not just that the flag is computed."""
        import inspect

        import datanika.ui.pages.models as models_page_module

        source = inspect.getsource(models_page_module)
        assert "models.no_models_after_load" in source
        assert "ModelState.loaded_without_catalog" in source
