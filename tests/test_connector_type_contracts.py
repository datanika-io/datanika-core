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


# ── The transform-destination contract (core#825, core#862) ─────────────────────
#
# `DESTINATION_TYPES` (what dlt can LOAD into) and `SUPPORTED_ADAPTERS` (what dbt
# can TRANSFORM in) are two literals in two modules describing different
# capabilities. Until core#825 they were textually identical — the same eleven
# strings — so nothing had ever diverged and nothing bound them.
#
# They are not identical any more, and the failure mode is expensive:
# `generate_profiles_yml` raises AFTER `run.before_execute` has fired and after
# `start_run`, so offering a destination dbt cannot build in consumes the
# tenant's quota on a run that was never capable of succeeding.


def test_transform_destinations_are_exactly_the_loadable_destinations_dbt_can_build_in():
    """The explicit set must equal the computed intersection.

    `TRANSFORM_DESTINATION_TYPES` is written out longhand so that adding a
    connector forces a deliberate choice rather than inheriting one, and so that
    `connection_service` does not have to import `dbt.cli.main`. This is what
    stops the longhand from rotting.
    """
    from datanika.services.connection_service import (
        DESTINATION_TYPES,
        TRANSFORM_DESTINATION_TYPES,
    )
    from datanika.services.dbt_project import SUPPORTED_ADAPTERS

    expected = DESTINATION_TYPES & SUPPORTED_ADAPTERS
    assert expected == TRANSFORM_DESTINATION_TYPES, (
        "TRANSFORM_DESTINATION_TYPES has drifted from DESTINATION_TYPES & "
        f"SUPPORTED_ADAPTERS.\n  missing: {sorted(expected - TRANSFORM_DESTINATION_TYPES)}"
        f"\n  extra:   {sorted(TRANSFORM_DESTINATION_TYPES - expected)}"
    )


def test_mysql_specifically_is_no_longer_offered_as_a_transform_destination():
    """The one member core#825 removes, named rather than left to the set algebra.

    The test above would keep passing if BOTH sets regained mysql together, which
    is exactly what re-adding `dbt-mysql` would do — and that would silently drag
    the whole dbt stack back to 1.7 and six advisories with it. This one names
    the member, so the two tests fail on different mistakes.
    """
    from datanika.services.connection_service import (
        DESTINATION_TYPES,
        TRANSFORM_DESTINATION_TYPES,
    )

    # 🚨 This assertion used to read `"mysql" in DESTINATION_TYPES`, on the
    # grounds that "dlt loads into MySQL through SQLAlchemy/pymysql". **That was
    # Product's own recommendation and it was retracted the same night**
    # (core#865): dlt has no `mysql` destination factory, `build_destination`'s
    # unconditional getattr raises, and no MySQL load has ever moved a row.
    #
    # 🔑 It was an OVER-CORRECTION GUARD — written to stop the dbt-adapter
    # removal being flattened into "MySQL is unsupported" — and the capability it
    # protected did not exist. Its effect was therefore to pin a false claim in
    # place and fail anyone who removed it. **An over-correction guard asserts a
    # POSITIVE capability, so it is only as true as that capability, and it needs
    # the same evidence as a page that says the same thing.** The identical
    # mistake shipped on the landing side in landing#429, from the same author,
    # in the same week.
    #
    # The over-correction is still worth guarding — deleting MySQL from the
    # product is still wrong — so the replacement asserts the half that IS
    # measured: extraction, verified in `test_mysql_after_dbt_mysql_removal.py`
    # against a real MySQL 8.4 container moving real rows.
    assert "mysql" in SOURCE_TYPES, (
        "mysql must remain an EXTRACT SOURCE. That capability is real and is "
        "measured against a live MySQL server; only the destination and "
        "transform roles were withdrawn (core#825, core#865)."
    )
    assert "mysql" not in DESTINATION_TYPES, (
        "mysql is advertised as a load destination again. dlt has no `mysql` "
        "destination factory, so `build_destination` raises AttributeError "
        "before a socket is opened (core#865). Check "
        "`hasattr(dlt.destinations, ...)` before re-adding it — set membership "
        "is a claim, not a capability."
    )
    assert "mysql" not in TRANSFORM_DESTINATION_TYPES, (
        "mysql is offered as a dbt transformation destination, but no dbt-mysql "
        "adapter is installed (core#825). generate_profiles_yml raises AFTER "
        "run.before_execute fires, so the run consumes quota before failing."
    )


# ── Ask the layer beneath (core#862, core#865) ───────────────────────────
#
# 🚨 The four sets above are hand-maintained CLAIMS about layers they do not
# control. `"mysql" in SUPPORTED_DESTINATION_TYPES` was True for the life of the
# entry and meant nothing. Only checks that ask dlt and dbt directly
# discriminate, and these are the whole reason this file exists.
#
# 🔑 **Both were RED before core#862 landed**, measured 2026-09-01 against the
# core venv on `origin/dev` (dlt 1.21.0). That red is the negative control, and
# it is written down here because it stops existing once the fix lands — a guard
# nobody can tell has ever failed is a guard nobody should trust:
#
#     dlt factories   advertised 11 -> RESOLVED 9,  ABSENT: mysql, sqlite
#     dbt adapters    listed     10 -> INSTALLED 7, MISSING: databricks,
#                                                            sqlite, synapse
#     controls        hasattr(dlt.destinations, "notareal")     -> False
#                     find_spec("dbt.adapters.notarealadapter") -> None
#
# The controls are what make those four failures facts about those connectors
# rather than facts about the probe.


def test_every_advertised_destination_has_a_dlt_factory():
    """dlt is asked, not the set. This is what core#865 needed and did not have.

    `build_destination` is an unconditional `getattr(dlt.destinations, type)`,
    so a member with no factory is not degraded — it raises `AttributeError`
    before a socket is opened, inside a Celery task, after the run row and the
    quota charge already exist.
    """
    import dlt.destinations as dlt_destinations

    from datanika.services.dlt_runner import DltRunnerService

    # Negative control first: a set-membership check cannot fail, and this is
    # what shows `hasattr` here discriminates at all.
    assert not hasattr(dlt_destinations, "notarealdestination")

    absent = sorted(
        t for t in DltRunnerService.SUPPORTED_DESTINATION_TYPES if not hasattr(dlt_destinations, t)
    )
    assert not absent, (
        f"advertised as dlt destinations but no factory exists: {absent}. "
        "Every load into one of these raises AttributeError. Ask "
        "`hasattr(dlt.destinations, t)` before adding a member."
    )


def test_every_supported_adapter_is_actually_installed():
    """dbt is asked, not the set (core#862).

    `pyproject.toml` documented `dbt-databricks` and `dbt-synapse` as
    deliberately absent while `SUPPORTED_ADAPTERS` advertised them, and
    `_build_profile_output` had purpose-built branches writing profiles dbt
    cannot load. The manifest and the constant disagreed for the life of the
    project, and nothing could notice.
    """
    import importlib.util

    from datanika.services.dbt_project import SUPPORTED_ADAPTERS

    # `mssql` is served by dbt-sqlserver; every other adapter matches its name.
    module_for = {"mssql": "sqlserver"}

    assert importlib.util.find_spec("dbt.adapters.notarealadapter") is None

    missing = sorted(
        a
        for a in SUPPORTED_ADAPTERS
        if importlib.util.find_spec(f"dbt.adapters.{module_for.get(a, a)}") is None
    )
    assert not missing, (
        f"listed as dbt transform targets but no adapter is installed: {missing}. "
        "`generate_profiles_yml` would write a profile dbt cannot load, and it "
        "raises AFTER run.before_execute has charged the tenant's quota. Install "
        "the adapter before widening this set."
    )


def test_the_ui_never_offers_a_destination_the_runner_would_refuse():
    """`DESTINATION_TYPES` (what the pickers show) must not exceed dlt's set.

    Two independent literals in two modules describing one capability. This is
    the binding, and it is the check that would have caught core#865 at any
    point in the years `mysql` sat in both.
    """
    from datanika.services.connection_service import DESTINATION_TYPES
    from datanika.services.dlt_runner import DltRunnerService

    extra = sorted(DESTINATION_TYPES - DltRunnerService.SUPPORTED_DESTINATION_TYPES)
    assert not extra, (
        f"the UI offers {extra} as load destinations, which `build_destination` refuses outright."
    )


def test_the_three_adapterless_types_are_no_longer_transform_targets():
    """The three core#862 removed, named.

    The intersection test alone would not notice them coming back together, and
    `sqlite` is the one that has NEVER had an adapter — unlike mysql, which had
    an abandoned one.
    """
    from datanika.services.connection_service import (
        DESTINATION_TYPES,
        TRANSFORM_DESTINATION_TYPES,
    )

    for t in ("sqlite", "databricks", "synapse"):
        assert t not in TRANSFORM_DESTINATION_TYPES, f"{t} has no installed dbt adapter"

    # Databricks and Synapse ARE loadable — the narrowing is per-capability, and
    # flattening them to "unsupported" is the over-correction this guards.
    for t in ("databricks", "synapse"):
        assert t in DESTINATION_TYPES, (
            f"{t} is a working dlt LOAD destination; only the dbt transform role "
            "was withdrawn. Do not delete it from the product."
        )


def test_sqlite_keeps_the_role_it_really_has():
    """Same shape as the mysql assertion above: guard the withdrawal AND the
    over-correction, and assert only capabilities that were measured."""
    from datanika.services.connection_service import DESTINATION_TYPES, SOURCE_TYPES

    assert "sqlite" in SOURCE_TYPES, "sqlite remains a valid extract source"
    assert "sqlite" not in DESTINATION_TYPES, (
        "dlt has no `sqlite` destination factory either (core#865) — identical "
        "defect to mysql, one cause, one fix."
    )
