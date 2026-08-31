"""Editing an upload must not silently rewrite its ``dlt_config`` (core#803).

``_populate_form_from_upload`` used to end with an unconditional::

    self.form_use_raw_json = False
    self.form_config = "{}"

so opening the edit form on an upload whose config came from **Use raw JSON
config** discarded it: ``_build_config`` then wrote either a structured config
rebuilt from defaults (checkbox left off) or literally ``{}`` (checkbox ticked).
For a ``rest_api`` / ``openapi`` source that is fatal rather than merely lossy —
``DltRunner._build_rest_api_source`` raises without a ``resources`` list — so a
user who edited a working upload's *description* got an upload that could no
longer run, and found out at the next scheduled run.

**The assertion here is the round trip, not the checkbox.** Asserting only that
``form_use_raw_json`` came back ticked passes on a form that ticks the box over
an empty ``{}`` textarea, which is exactly the state observed on production
(org 27, upload 16 ``githubissues``). So every test below populates the form and
then calls ``_build_config()``, which is what **Save Changes** actually writes.

The fix deliberately asks ``_build_config`` itself whether it can reproduce the
stored config rather than consulting a hand-written list of "advanced" keys. A
list goes stale the moment a connector adds a key; running the builder cannot.
``TestKeysTheStructuredFormCannotExpress`` is the blast-radius list from the
issue, and it is a *consumer* of that property, not the mechanism.
"""

import json
import types
from types import SimpleNamespace

import pytest

from datanika.ui.state.upload_state import UploadState

# Connection option strings the form matches against, in the
# "<id> — <name> (<type>)" shape ``load_uploads`` builds.
SRC_OPTS = [
    "1 — github (rest_api)",
    "2 — sales (postgres)",
    "3 — store (shopify)",
    "4 — drop (csv)",
]
DST_OPTS = ["9 — warehouse (postgres)"]


class _Form:
    """A stand-in for a live ``UploadState``.

    Reflex refuses direct instantiation (``ReflexRuntimeError``) and its
    ``__setattr__`` needs a running app, so the form logic is exercised on a
    plain object carrying the same functions — the same approach as
    ``tests/test_ui/test_upload_format_options.py``.
    """


# Copy every private helper ``UploadState`` defines rather than naming them one
# by one. A hand-written list is a stub that diverges silently: the first draft
# of this file omitted ``_restore_raw_json_fallback`` and *every* test in it
# errored, including the controls that are supposed to pass either way — which
# is a harness failure wearing a product failure's clothes.
for _name, _attr in vars(UploadState).items():
    if (
        _name.startswith("_")
        and not _name.startswith("__")
        and isinstance(_attr, (types.FunctionType, staticmethod, classmethod))
    ):
        setattr(_Form, _name, _attr)

assert callable(_Form._build_config), "the method copy above found nothing"
assert hasattr(_Form, "_populate_form_from_upload")
assert hasattr(_Form, "_restore_raw_json_fallback"), (
    "core#803's fallback is missing from UploadState — the guard below would "
    "otherwise fail for the wrong reason"
)


def _upload(dlt_config: dict, *, source_connection_id: int = 1, description: str = ""):
    return SimpleNamespace(
        name="githubissues",
        description=description,
        source_connection_id=source_connection_id,
        destination_connection_id=9,
        dlt_config=dlt_config,
    )


def _edit(dlt_config: dict, **upload_kwargs) -> _Form:
    """Open the edit form on an upload, exactly as ``edit_upload`` does."""
    form = _Form()
    form._populate_form_from_upload(_upload(dlt_config, **upload_kwargs), SRC_OPTS, DST_OPTS)
    return form


def _save(form: _Form) -> dict:
    """What **Save Changes** would write."""
    return form._build_config()


REST_API_CONFIG = {
    "resources": [
        {
            "name": "github_issues",
            "endpoint": {
                "path": "repos/datanika-io/datanika-core/issues",
                "params": {"state": "all", "per_page": 100},
                "paginator": {"type": "single_page"},
            },
        }
    ]
}


class TestTheProductionCase:
    """Upload 16 ``githubissues``, the one measured broken on production."""

    def test_edit_then_save_preserves_the_config_byte_for_byte(self):
        saved = _save(_edit(REST_API_CONFIG))
        assert json.dumps(saved, sort_keys=True) == json.dumps(REST_API_CONFIG, sort_keys=True)

    def test_the_resources_key_the_runner_requires_survives(self):
        # DltRunner._build_rest_api_source raises without this exact key, so
        # losing it is the difference between a lossy edit and a dead upload.
        assert _save(_edit(REST_API_CONFIG))["resources"] == REST_API_CONFIG["resources"]

    def test_the_form_shows_the_stored_config_not_an_empty_object(self):
        form = _edit(REST_API_CONFIG)
        assert form.form_use_raw_json is True, "raw-JSON mode must be restored"
        assert json.loads(form.form_config) == REST_API_CONFIG, (
            "the textarea held '{}' on production while the checkbox was ticked — "
            "a ticked checkbox alone is not the fix"
        )

    def test_editing_only_the_description_still_leaves_a_runnable_upload(self):
        form = _edit(REST_API_CONFIG, description="before")
        form.form_description = "after"
        saved = _save(form)
        assert saved == REST_API_CONFIG
        assert form.form_description == "after"


class TestKeysTheStructuredFormCannotExpress:
    """The blast radius from core#803, one case per runner key.

    ``rest_api`` / ``openapi`` are the fatal ones because the key is required.
    The rest degrade quietly, which is harder to diagnose, not easier.
    """

    @pytest.mark.parametrize(
        ("config", "source_connection_id"),
        [
            pytest.param({"resources": [{"name": "x"}]}, 1, id="resources"),
            pytest.param({"paginator": {"type": "json_link"}}, 1, id="paginator"),
            pytest.param({"resource_defaults": {"write_disposition": "merge"}}, 1, id="defaults"),
            pytest.param({"headers": {"X-Trace": "1"}}, 1, id="headers"),
            pytest.param({"auth": {"type": "bearer", "token": "t"}}, 1, id="auth"),
            pytest.param({"resource_names": ["pets"]}, 1, id="resource_names"),
            pytest.param({"table_name": "renamed"}, 2, id="table_name"),
            pytest.param({"backend": "pyarrow"}, 2, id="backend"),
            pytest.param({"filters": [{"column": "c", "op": "eq", "value": 1}]}, 2, id="filters"),
        ],
    )
    def test_key_survives_an_edit(self, config, source_connection_id):
        saved = _save(_edit(config, source_connection_id=source_connection_id))
        for key, value in config.items():
            assert saved.get(key) == value, f"editing dropped {key!r}"


class TestValuesTheStructuredFormCannotExpress:
    """A key can be structured and its *value* still unrepresentable.

    ⚠️ Note what this stub can and cannot show. It is a plain object, so it does
    **not** reproduce Reflex's own coercion of a value assigned to a typed var —
    an earlier version of this test asserted that an integer ``initial_value``
    survives being round-tripped through the ``str`` field ``form_initial_value``
    and passed for the wrong reason: on the stub nothing coerced it. The cases
    below are model-independent, i.e. they are losses in ``_build_config``'s own
    logic and would fail identically against a live state.
    """

    def test_a_falsy_incremental_initial_value_is_not_dropped(self):
        # ``if self.form_initial_value:`` drops 0, and 0 is a legitimate place
        # to start an integer cursor. The structured form cannot express it, so
        # the config must survive as raw JSON instead of losing the key.
        config = {
            "mode": "single_table",
            "table": "events",
            "incremental": {"cursor_path": "id", "initial_value": 0},
        }
        assert _save(_edit(config, source_connection_id=2)) == config

    def test_an_explicitly_empty_table_names_list_is_not_dropped(self):
        # ``table_names: []`` and "no table_names at all" mean different things
        # to a reader of the stored config; the structured form collapses both.
        config = {"mode": "full_database", "table_names": []}
        assert _save(_edit(config, source_connection_id=2)) == config


class TestTheStructuredFormIsStillUsedWhereItFits:
    """The fallback must not swallow the ordinary case.

    Flipping every upload into a JSON textarea would 'fix' core#803 by making
    the structured form unreachable, and every test above would still pass.
    """

    def test_an_empty_config_stays_structured(self):
        form = _edit({})
        assert form.form_use_raw_json is False
        assert form.form_config == "{}"

    def test_a_plain_full_database_config_stays_structured(self):
        form = _edit(
            {"mode": "full_database", "write_disposition": "append", "table_names": ["a", "b"]},
            source_connection_id=2,
        )
        assert form.form_use_raw_json is False
        assert form.form_table_names == "a, b"

    def test_a_single_table_incremental_config_stays_structured(self):
        form = _edit(
            {
                "mode": "single_table",
                "write_disposition": "merge",
                "primary_key": "id",
                "table": "orders",
                "incremental": {"cursor_path": "updated_at", "row_order": "asc"},
            },
            source_connection_id=2,
        )
        assert form.form_use_raw_json is False
        assert form.form_enable_incremental is True
        assert form.form_cursor_path == "updated_at"

    def test_a_saas_endpoint_selection_stays_structured(self):
        form = _edit(
            {"mode": "full_database", "endpoints": ["products", "orders"]},
            source_connection_id=3,
        )
        assert form.form_use_raw_json is False
        assert form.form_selected_endpoints == ["products", "orders"]

    def test_a_file_source_format_config_stays_structured(self):
        form = _edit(
            {"mode": "full_database", "file_glob": "*.csv", "delimiter": ";"},
            source_connection_id=4,
        )
        assert form.form_use_raw_json is False
        assert form.form_delimiter == ";"


class TestTheFallbackFailsSafe:
    """When the structured rebuild cannot even run, keep the config."""

    def test_an_unparseable_structured_field_falls_back_rather_than_raising(self):
        # batch_size is read back through int(); a config that reached the DB
        # by some other route (API, MCP, a hand edit) can hold a value the
        # structured form cannot re-parse. Losing it silently is the bug.
        config = {"mode": "full_database", "batch_size": "many"}
        form = _edit(config, source_connection_id=2)
        assert form.form_use_raw_json is True
        assert json.loads(form.form_config) == config
