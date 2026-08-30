"""Regression tests for core#610 — the connection's dataset is where rows land.

**The defect.** The BigQuery connection form asks for **`Dataset *`** — required,
with a `raw_data` placeholder — and then never used it. Rows landed in a dataset
named after the *upload*. Proven on production: the user set `docs_bigquery`,
BigQuery reported `bigqueryfirstrun`, and `docs_bigquery` was never created.

**Where it went.** `_to_dlt_credentials` did ``creds.pop(key, None)`` with the
return value unbound — `dataset` was correctly recognised as *not a credential*
and then dropped on the floor. The comment above that table described the right
intent; only half of it was implemented.

**Why it is worse than a wrong name.** Both the run and the destination look
healthy, and nothing in the UI ever mentions `bigqueryfirstrun`. The data is
simply somewhere else. And every upload gets its own dataset, so a warehouse the
user meant to be one tidy `raw_data` becomes one dataset per pipeline — which our
own connector guide tells them to expect the opposite of.

**Two tests in `tests/test_tasks/test_upload_tasks.py` codified the bug** rather
than catching it: one asserted `dataset_name == "test"` for an upload named
"test", and the other built a BigQuery destination with `dataset="d"` and
asserted the *upload name* won. They are rewritten alongside this file, and the
`to_snake_case` behaviour they were really covering is preserved on the
postgres path, where the upload name is still the only available name.
"""

import pytest

from datanika.services.dlt_runner import (
    _NON_CREDENTIAL_WAREHOUSE_KEYS,
    DltRunnerService,
    destination_dataset_name,
)

#: The three destinations that let the user name where to write, and the key
#: each one stores it under.
WAREHOUSES = [("bigquery", "dataset"), ("databricks", "schema"), ("snowflake", "schema")]


@pytest.mark.parametrize(("destination_type", "key"), WAREHOUSES)
def test_the_users_chosen_dataset_is_returned(destination_type, key):
    assert (
        destination_dataset_name(destination_type, {key: "docs_bigquery", "project": "p"})
        == "docs_bigquery"
    )


@pytest.mark.parametrize(
    "destination_type", ["postgres", "mysql", "mssql", "duckdb", "sqlite", "clickhouse", "redshift"]
)
def test_a_destination_without_the_concept_returns_none(destination_type):
    """``None``, not ``""``.

    An empty string is a valid argument to ``dlt.pipeline(dataset_name=...)`` and
    would silently create a nameless dataset; ``None`` is the signal for the
    caller to use its own default.
    """
    assert destination_dataset_name(destination_type, {"host": "h", "database": "d"}) is None


@pytest.mark.parametrize(("destination_type", "key"), WAREHOUSES)
@pytest.mark.parametrize("value", ["", "   ", None, 0, [], {}])
def test_a_missing_or_blank_value_returns_none(destination_type, key, value):
    """A user who left the field empty gets the fallback, not a blank dataset."""
    assert destination_dataset_name(destination_type, {key: value}) is None


@pytest.mark.parametrize(("destination_type", "key"), WAREHOUSES)
def test_surrounding_whitespace_is_trimmed(destination_type, key):
    assert destination_dataset_name(destination_type, {key: "  raw_data \n"}) == "raw_data"


@pytest.mark.parametrize(("destination_type", "key"), WAREHOUSES)
def test_a_key_stripped_from_the_credentials_is_not_lost(destination_type, key):
    """**The invariant #610 violated, stated directly.**

    ``dataset``/``schema`` must still be kept out of the credentials — dlt
    rejects unknown fields, and three other tests depend on that. What must
    *also* be true, and was not, is that the value goes somewhere. Asserting
    both halves in one test is what stops the next person removing a key from
    the credentials and forgetting the other half.
    """
    svc = DltRunnerService()
    config = {"project": "p", "host": "h", "account": "a", key: "chosen_by_the_user"}

    creds = svc._to_dlt_credentials(destination_type, config)

    assert key not in creds, f"{key} must not be sent to dlt as a credential"
    assert destination_dataset_name(destination_type, config) == "chosen_by_the_user", (
        f"{key} was stripped from the credentials and then dropped — that is core#610"
    )


def test_the_strip_set_is_derived_from_the_routing_table():
    """Structural guard: nothing can be stripped that is not also routed.

    The two tables were one table's worth of knowledge written once. Deriving
    the strip set is what makes "stripped but never used" — the exact shape of
    this bug — unrepresentable.
    """
    assert set(_NON_CREDENTIAL_WAREHOUSE_KEYS) == {d for d, _ in WAREHOUSES}
    for destination_type, key in WAREHOUSES:
        assert _NON_CREDENTIAL_WAREHOUSE_KEYS[destination_type] == {key}
        # ...and the same key is the one the router reads.
        assert destination_dataset_name(destination_type, {key: "x"}) == "x"
