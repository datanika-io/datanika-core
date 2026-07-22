"""Contracts between the connector-type sets that must agree but aren't linked.

`SOURCE_TYPES` (what may be a source), `NON_SQL_SOURCE_TYPES` (what the upload
form treats as non-SQL) and `dlt_runner.SUPPORTED_SAAS_TYPES` (what the loader
dispatches to the SaaS builder) are three separate literals in three modules.
Nothing binds them, so adding a connector to one and forgetting another is
silent — which is exactly what happened in core#503: pipedrive, freshdesk and
asana shipped as SaaS at run time but were never classified in the UI, so the
upload form rendered Load Mode / Write Disposition / Source schema / Table
names for an HTTP API.

These tests are the binding.
"""

from datanika.models.connection import ConnectionType
from datanika.services.connection_schemas import CONFIG_SCHEMAS
from datanika.services.connection_service import SOURCE_TYPES, WITHDRAWN_SOURCE_TYPES
from datanika.services.dlt_runner import SUPPORTED_SAAS_TYPES
from datanika.ui.pages.connections import PICKER_TYPES
from datanika.ui.state.connection_state import (
    FILE_SOURCE_TYPES,
    NON_SQL_SOURCE_TYPES,
    SAAS_DEFAULT_ENDPOINTS,
    SAAS_SOURCE_TYPES,
)

# The source types that really are SQL databases — the only ones for which the
# upload form's Load Mode / Write Disposition / Source schema / Table names
# block means anything. Listed explicitly rather than derived, so adding a
# connector forces a deliberate choice instead of inheriting one.
SQL_SOURCE_TYPES = {
    "postgres",
    "mysql",
    "mssql",
    "oracle",
    "sqlite",
    "clickhouse",
    "duckdb",
}


def test_every_source_type_is_classified_sql_or_non_sql():
    """A source type must be one or the other. Unclassified defaults to SQL.

    `form_is_non_sql_source = conn_type in NON_SQL_SOURCE_TYPES`, and the SQL
    block renders under `~form_is_non_sql_source` — so *forgetting* a connector
    silently gives it SQL controls. The failure mode is a default, not an error,
    which is why this needs a test rather than care.
    """
    unclassified = SOURCE_TYPES - NON_SQL_SOURCE_TYPES - SQL_SOURCE_TYPES
    assert not unclassified, (
        f"Source types {sorted(unclassified)} are in neither NON_SQL_SOURCE_TYPES nor "
        "SQL_SOURCE_TYPES. The upload form will render Load Mode / Write Disposition / "
        "Source schema / Table names for them by default. Add them to "
        "NON_SQL_SOURCE_TYPES (connection_state.py) if they are not SQL databases."
    )


def test_no_type_is_both_sql_and_non_sql():
    assert not (NON_SQL_SOURCE_TYPES & SQL_SOURCE_TYPES)


def test_runner_saas_types_are_all_non_sql():
    """The loader's view and the form's view must not contradict each other.

    This is the assertion that fails on core#503 as filed: `SUPPORTED_SAAS_TYPES`
    carries pipedrive/freshdesk/asana, and none of them were in
    NON_SQL_SOURCE_TYPES.
    """
    sql_looking_saas = SUPPORTED_SAAS_TYPES - NON_SQL_SOURCE_TYPES
    assert not sql_looking_saas, (
        f"{sorted(sql_looking_saas)} are dispatched to the SaaS builder by dlt_runner "
        "but are not classified as non-SQL by the upload form, so the form shows them "
        "SQL controls that the loader ignores."
    )


def test_the_form_and_the_loader_agree_on_which_types_are_saas():
    """Tightened from subset to equality once core#532 landed, as noted here.

    It was a subset while the endpoint picker was inert: forcing
    pipedrive/freshdesk/asana into `SAAS_SOURCE_TYPES` would have rendered a
    control that did nothing. The selection is honoured now, so the two sets
    must match exactly — a type in one and not the other is either a picker
    with no loader behind it or a loader the picker can't configure.
    """
    assert SAAS_SOURCE_TYPES == SUPPORTED_SAAS_TYPES


def test_every_ui_saas_type_has_endpoints_to_offer():
    """A SaaS type with no endpoint list renders an empty checkbox group.

    Withdrawn types are exempt: they are never rendered, so there is nothing to
    offer. They stay *classified* as SaaS on purpose — see
    `WITHDRAWN_SOURCE_TYPES` — so this invariant has to know the difference
    between "not offered" and "offered with nothing in it".
    """
    offered = SAAS_SOURCE_TYPES - WITHDRAWN_SOURCE_TYPES
    missing = {t for t in offered if not SAAS_DEFAULT_ENDPOINTS.get(t)}
    assert not missing, f"{sorted(missing)} would render an empty endpoint selector"


def test_a_withdrawn_type_is_not_offered_anywhere():
    """Withdrawal has to hold on every surface, not just the one we remembered.

    core#555's first attempt removed google_ads from the loader's dispatch set
    as well, which gave connections already stored the generic "Unsupported
    source type" instead of the developer-token explanation. The split matters:
    withdrawn means *cannot be created*, not *becomes unexplainable*.
    """
    for withdrawn in WITHDRAWN_SOURCE_TYPES:
        assert withdrawn not in PICKER_TYPES, f"{withdrawn} is still in the connections picker"
        assert withdrawn not in CONFIG_SCHEMAS, f"{withdrawn} still has a config schema"
        assert withdrawn not in SOURCE_TYPES, f"{withdrawn} is still a selectable source type"


def test_a_withdrawn_type_still_resolves_for_stored_connections():
    """The other half — it must keep its identity and its dispatch."""
    for withdrawn in WITHDRAWN_SOURCE_TYPES:
        assert withdrawn in {c.value for c in ConnectionType}, (
            f"{withdrawn} lost its ConnectionType member; rows already stored would not load"
        )
        assert withdrawn in SUPPORTED_SAAS_TYPES, (
            f"{withdrawn} lost loader dispatch, so a stored connection fails with a generic "
            "'Unsupported source type' instead of an error that explains itself"
        )


def test_file_source_types_are_non_sql():
    assert FILE_SOURCE_TYPES <= NON_SQL_SOURCE_TYPES
