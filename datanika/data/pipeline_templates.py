"""Pipeline templates — pre-configured source→destination starters.

Each template captures the *non-credential* configuration decisions a new
user would otherwise have to make on their own:

- which source connector to set up
- which destination connector to use
- sensible default settings for both connections (schemas, regions, etc.)
- which dlt resources/tables to extract and the right write disposition

The user still enters their own credentials. Templates do not run pipelines
end-to-end on their own — they prefill forms in the connection creation flow
so a Google Ads visitor can go from "empty dashboard" to "first run" in
roughly two minutes instead of stalling on configuration decisions.

This module is intentionally pure data: a frozen dataclass plus a small
registry. Templates are not stored in the database, do not require a
migration, and do not have an admin UI. To add a template, append to
LAUNCH_TEMPLATES, add the matching i18n keys to all locale files, and add a
test row to ``tests/test_data/test_pipeline_templates.py``.
"""

from dataclasses import dataclass, field
from typing import Any

from datanika.models.connection import ConnectionType


@dataclass(frozen=True)
class PipelineTemplate:
    """A pre-configured source→destination starter pipeline.

    Attributes:
        slug: URL-safe stable identifier (e.g. ``"stripe-to-postgres"``).
        name_key: i18n key for the human-readable display name.
        description_key: i18n key for the one-line description shown on the card.
        source_type: ConnectionType the template's source connection will use.
        destination_type: ConnectionType the template's destination uses.
        source_config_defaults: Non-credential connection-form fields to
            prefill on the source side (e.g. default schema, region, port).
        destination_config_defaults: Same for the destination connection.
        dlt_config_defaults: Default extract/load configuration — at minimum
            ``write_disposition``, plus connector-specific keys like
            ``resources`` (Stripe) or ``tables`` (Postgres).
        icon_source: Connector slug used to look up the source icon in the UI.
        icon_destination: Connector slug used to look up the destination icon.
    """

    slug: str
    name_key: str
    description_key: str
    source_type: ConnectionType
    destination_type: ConnectionType
    icon_source: str
    icon_destination: str
    source_config_defaults: dict[str, Any] = field(default_factory=dict)
    destination_config_defaults: dict[str, Any] = field(default_factory=dict)
    dlt_config_defaults: dict[str, Any] = field(default_factory=dict)


LAUNCH_TEMPLATES: tuple[PipelineTemplate, ...] = (
    PipelineTemplate(
        slug="stripe-to-postgres",
        name_key="templates.stripe_to_postgres.name",
        description_key="templates.stripe_to_postgres.description",
        source_type=ConnectionType.STRIPE,
        destination_type=ConnectionType.POSTGRES,
        icon_source="stripe",
        icon_destination="postgres",
        source_config_defaults={
            # Stripe connections only require the API key — nothing else to
            # prefill on the source side. Listed explicitly so future fields
            # have an obvious place to land.
        },
        destination_config_defaults={
            "schema": "raw_stripe",
            "port": "5432",
        },
        dlt_config_defaults={
            "resources": ["Customer", "Charge", "Invoice", "Subscription", "Product", "Price"],
            "write_disposition": "merge",
        },
    ),
    PipelineTemplate(
        slug="postgres-to-bigquery",
        name_key="templates.postgres_to_bigquery.name",
        description_key="templates.postgres_to_bigquery.description",
        source_type=ConnectionType.POSTGRES,
        destination_type=ConnectionType.BIGQUERY,
        icon_source="postgres",
        icon_destination="bigquery",
        source_config_defaults={
            "port": "5432",
            "schema": "public",
        },
        destination_config_defaults={
            "dataset": "raw_postgres",
        },
        dlt_config_defaults={
            "write_disposition": "merge",
        },
    ),
    PipelineTemplate(
        slug="csv-to-duckdb",
        name_key="templates.csv_to_duckdb.name",
        description_key="templates.csv_to_duckdb.description",
        source_type=ConnectionType.CSV,
        destination_type=ConnectionType.DUCKDB,
        icon_source="csv",
        icon_destination="duckdb",
        source_config_defaults={},
        destination_config_defaults={
            "schema": "raw_csv",
        },
        dlt_config_defaults={
            "write_disposition": "replace",
        },
    ),
)


def list_templates() -> list[PipelineTemplate]:
    """Return a fresh list of all launch templates.

    Returns a *copy* so callers can mutate the list freely without
    corrupting the registry.
    """
    return list(LAUNCH_TEMPLATES)


def get_template(slug: str) -> PipelineTemplate | None:
    """Look up a template by slug. Returns ``None`` if no template matches."""
    if not slug:
        return None
    for tpl in LAUNCH_TEMPLATES:
        if tpl.slug == slug:
            return tpl
    return None
