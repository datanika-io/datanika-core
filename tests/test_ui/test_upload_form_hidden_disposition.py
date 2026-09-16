"""The upload form must not store a write disposition it does not show (core#1336).

Product's rule: *a control the form does not render must not govern the load.* The Write
Disposition select renders only for SQL database sources (`uploads.py`, under
`~UploadState.form_is_non_sql_source`). But `_build_config` stored `form_write_disposition`, default
`"append"`, for every structured upload. The runner forwarded it to dlt, which applied it to every
resource. The load-level consequence is measured in
`tests/test_services/test_rerun_lands_each_record_once.py`; these tests pin the form's half.
"""

from __future__ import annotations

import copy
import dataclasses

from datanika.ui.state.connection_state import (
    FILE_SOURCE_TYPES,
    NON_SQL_SOURCE_TYPES,
    SAAS_SOURCE_TYPES,
)
from datanika.ui.state.upload_state import UploadState

#: One of each kind of non-SQL loader the form can pick.
NON_SQL_SAMPLE = ("openapi", "stripe", "csv", "s3", "mongodb", "google_sheets", "rest_api", "kafka")


def _field_default(field):
    """A declared var's default value.

    List vars declare a `default_factory`, and their `default` is the dataclasses MISSING sentinel,
    which `_build_config` would otherwise store as a value (`endpoints: <MISSING>`).
    """
    default = getattr(field, "default", None)
    if default is dataclasses.MISSING:
        factory = getattr(field, "default_factory", dataclasses.MISSING)
        return factory() if factory is not dataclasses.MISSING else None
    return copy.deepcopy(default)


class _UploadForm:
    """A stand-in `self` carrying UploadState's declared vars, so the REAL methods run."""

    _build_config = UploadState._build_config
    _restore_raw_json_fallback = UploadState._restore_raw_json_fallback

    def __init__(self, source_type: str, **values):
        for name, field in UploadState.get_fields().items():
            setattr(self, name, _field_default(field))
        # What `set_form_source_id` (and the edit read-back) set for a source of this type.
        self.form_is_non_sql_source = source_type in NON_SQL_SOURCE_TYPES
        self.form_is_file_source = source_type in FILE_SOURCE_TYPES
        self.form_is_saas_source = source_type in SAAS_SOURCE_TYPES
        for name, value in values.items():
            setattr(self, name, value)


def test_the_sample_really_is_non_sql():
    """Floor: if a sample type were SQL, the next test would pass for the wrong reason."""
    assert set(NON_SQL_SAMPLE) <= NON_SQL_SOURCE_TYPES


def test_the_stub_stores_no_sentinel():
    """Floor for this harness: a declared list var must arrive as a list, not as MISSING."""
    config = _UploadForm("stripe")._build_config()

    assert not any(value is dataclasses.MISSING for value in config.values()), config


def test_a_non_sql_source_stores_no_write_disposition():
    stored = {t: _UploadForm(t)._build_config().get("write_disposition") for t in NON_SQL_SAMPLE}

    assert all(value is None for value in stored.values()), (
        f"the form stored a write disposition for sources it hides the control from: {stored}"
    )


def test_a_hidden_merge_is_not_stored_either():
    """A choice made while a SQL source was selected survives a switch to a non-SQL one, because
    `set_form_source_id` resets no SQL field. It must not reach the stored config."""
    config = _UploadForm(
        "stripe",
        form_write_disposition="merge",
        form_mode="single_table",
        form_primary_key="id",
    )._build_config()

    assert "write_disposition" not in config
    assert "primary_key" not in config


def test_control_a_sql_source_stores_the_visible_choice():
    """The control renders for SQL sources, so what it holds is a choice and is stored."""
    assert _UploadForm("postgres")._build_config()["write_disposition"] == "append"
    replace = _UploadForm("postgres", form_write_disposition="replace")._build_config()
    assert replace["write_disposition"] == "replace"


def test_editing_an_upload_saved_before_the_fix_opens_the_structured_form():
    """QA's stored row, opened for editing, must not be pushed into raw-JSON mode.

    `_restore_raw_json_fallback` switches to raw JSON when the structured form cannot rebuild a
    stored key. Once the form stops writing the hidden disposition, a legacy row would look like
    exactly that, and every SaaS, OpenAPI and file upload saved before this fix would open as a
    JSON editor. Saving it through the structured form is also what drops the stale key.
    """
    stored = {"mode": "full_database", "write_disposition": "append"}
    form = _UploadForm("openapi")

    form._restore_raw_json_fallback(stored)

    assert form.form_use_raw_json is False, (
        "a non-SQL upload saved before core#1336 opened in raw-JSON mode"
    )


def test_control_a_deliberate_raw_json_disposition_still_opens_raw_json():
    """A disposition sent without the form's `mode` key is not the form's hidden default. It came
    from raw JSON or the API, and the structured form can no longer represent it for this source,
    so the edit form must keep it visible rather than dropping it on save."""
    stored = {"write_disposition": "append"}
    form = _UploadForm("openapi")

    form._restore_raw_json_fallback(stored)

    assert form.form_use_raw_json is True
