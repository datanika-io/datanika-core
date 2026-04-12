"""Tests for the connection-state side of pipeline templates.

Covers ``ConnectionState._apply_template_defaults`` — the pure helper that
takes a template slug, looks up the template, and prefills the matching
``form_*`` fields on the state object. This is the bridge between the
static template registry and the existing connection creation form.
"""

import types

from datanika.data.pipeline_templates import get_template


class _FakeConnectionState:
    """Stand-in for ConnectionState that exposes only the form_* fields the
    template prefill logic touches. Mirrors the FakeState pattern in
    ``tests/test_ui/test_state_models.py::TestConnectionBuildConfig``.
    """


def _make_state(**overrides):
    from datanika.ui.state.connection_state import ConnectionState

    defaults = {
        "form_type": "postgres",
        "form_host": "",
        "form_port": "5432",
        "form_user": "",
        "form_password": "",
        "form_database": "",
        "form_schema": "",
        "form_path": "",
        "form_project": "",
        "form_dataset": "",
        "form_keyfile_json": "",
        "form_account": "",
        "form_warehouse": "",
        "form_role": "",
        "form_bucket_url": "",
        "form_base_url": "",
        "form_property_id": "",
        "form_bootstrap_servers": "",
        "form_topics": "",
        "selected_template_slug": "",
    }
    defaults.update(overrides)

    obj = _FakeConnectionState()
    for k, v in defaults.items():
        setattr(obj, k, v)
    obj._apply_template_defaults = types.MethodType(ConnectionState._apply_template_defaults, obj)
    return obj


class TestApplyTemplateDefaults:
    def test_unknown_slug_is_noop(self):
        state = _make_state(form_type="postgres")
        state._apply_template_defaults("does-not-exist")
        # No fields should change
        assert state.form_type == "postgres"
        assert state.selected_template_slug == ""

    def test_empty_slug_is_noop(self):
        state = _make_state(form_type="postgres")
        state._apply_template_defaults("")
        assert state.form_type == "postgres"

    def test_stripe_template_sets_source_type_to_stripe(self):
        state = _make_state(form_type="postgres")
        state._apply_template_defaults("stripe-to-postgres")
        assert state.form_type == "stripe"
        assert state.selected_template_slug == "stripe-to-postgres"

    def test_postgres_template_prefills_port_and_schema(self):
        state = _make_state(form_type="bigquery", form_port="", form_schema="")
        state._apply_template_defaults("postgres-to-bigquery")
        assert state.form_type == "postgres"
        assert state.form_port == "5432"
        assert state.form_schema == "public"

    def test_csv_template_sets_source_type_to_csv(self):
        state = _make_state(form_type="postgres")
        state._apply_template_defaults("csv-to-duckdb")
        assert state.form_type == "csv"
        assert state.selected_template_slug == "csv-to-duckdb"

    def test_template_does_not_overwrite_credentials(self):
        """Template prefill must never touch password/secret/api_key fields."""
        state = _make_state(
            form_type="postgres",
            form_password="user-typed-secret",
            form_user="alice",
        )
        # Even if we apply a template that targets postgres source, the
        # user-entered credentials must survive.
        state._apply_template_defaults("postgres-to-bigquery")
        assert state.form_password == "user-typed-secret"
        assert state.form_user == "alice"

    def test_apply_template_is_idempotent(self):
        state = _make_state()
        state._apply_template_defaults("stripe-to-postgres")
        state._apply_template_defaults("stripe-to-postgres")
        assert state.form_type == "stripe"
        assert state.selected_template_slug == "stripe-to-postgres"


class TestTemplateLookupHelper:
    """Sanity check that get_template (already tested in test_data) is the
    contract the state code relies on. If this fails, the state tests above
    will too — but this localizes the failure mode for debugging."""

    def test_get_template_returns_object_with_source_type_attribute(self):
        tpl = get_template("stripe-to-postgres")
        assert tpl is not None
        assert hasattr(tpl, "source_type")
        assert hasattr(tpl, "source_config_defaults")
