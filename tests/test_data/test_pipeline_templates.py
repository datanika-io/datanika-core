"""Tests for the pipeline templates registry — static data validation."""

import pytest

from datanika.data.pipeline_templates import (
    LAUNCH_TEMPLATES,
    PipelineTemplate,
    get_template,
    list_templates,
)
from datanika.models.connection import ConnectionType


class TestPipelineTemplateDataclass:
    def test_construct_with_all_required_fields(self):
        tpl = PipelineTemplate(
            slug="test",
            name_key="templates.test.name",
            description_key="templates.test.description",
            source_type=ConnectionType.STRIPE,
            destination_type=ConnectionType.POSTGRES,
            source_config_defaults={"start_date": "2024-01-01"},
            destination_config_defaults={"schema": "raw_stripe"},
            dlt_config_defaults={
                "resources": ["customers", "charges"],
                "write_disposition": "merge",
            },
            icon_source="stripe",
            icon_destination="postgres",
        )
        assert tpl.slug == "test"
        assert tpl.source_type == ConnectionType.STRIPE
        assert tpl.destination_type == ConnectionType.POSTGRES

    def test_template_is_immutable(self):
        """Templates are static data — instances should be frozen."""
        tpl = LAUNCH_TEMPLATES[0]
        with pytest.raises((AttributeError, TypeError)):
            tpl.slug = "mutated"  # type: ignore[misc]


class TestLaunchTemplates:
    def test_three_launch_templates(self):
        """MVP ships exactly 3 templates."""
        assert len(LAUNCH_TEMPLATES) == 3

    def test_launch_template_slugs(self):
        slugs = {t.slug for t in LAUNCH_TEMPLATES}
        assert slugs == {
            "stripe-to-postgres",
            "postgres-to-bigquery",
            "csv-to-duckdb",
        }

    def test_all_slugs_are_unique(self):
        slugs = [t.slug for t in LAUNCH_TEMPLATES]
        assert len(slugs) == len(set(slugs))

    def test_all_required_fields_present(self):
        for tpl in LAUNCH_TEMPLATES:
            assert tpl.slug
            assert tpl.name_key
            assert tpl.description_key
            assert tpl.source_type
            assert tpl.destination_type
            assert tpl.icon_source
            assert tpl.icon_destination

    def test_source_and_destination_types_are_valid(self):
        """Every template's source and destination must be a real ConnectionType."""
        valid_types = {ct for ct in ConnectionType}
        for tpl in LAUNCH_TEMPLATES:
            assert tpl.source_type in valid_types, (
                f"{tpl.slug} has invalid source_type {tpl.source_type}"
            )
            assert tpl.destination_type in valid_types, (
                f"{tpl.slug} has invalid destination_type {tpl.destination_type}"
            )

    def test_i18n_keys_follow_naming_convention(self):
        """Name and description keys should be under 'templates.<slug>.*'."""
        for tpl in LAUNCH_TEMPLATES:
            normalized = tpl.slug.replace("-", "_")
            assert tpl.name_key == f"templates.{normalized}.name", (
                f"{tpl.slug} name_key={tpl.name_key} doesn't match convention"
            )
            assert tpl.description_key == f"templates.{normalized}.description", (
                f"{tpl.slug} description_key={tpl.description_key} doesn't match convention"
            )

    def test_dlt_config_has_write_disposition(self):
        """Every template must specify a write_disposition for its dlt config."""
        valid_dispositions = {"append", "replace", "merge"}
        for tpl in LAUNCH_TEMPLATES:
            assert "write_disposition" in tpl.dlt_config_defaults, (
                f"{tpl.slug} missing write_disposition"
            )
            assert tpl.dlt_config_defaults["write_disposition"] in valid_dispositions

    def test_stripe_to_postgres_template(self):
        tpl = get_template("stripe-to-postgres")
        assert tpl.source_type == ConnectionType.STRIPE
        assert tpl.destination_type == ConnectionType.POSTGRES
        # Stripe template should preselect at least the core resources
        assert "resources" in tpl.dlt_config_defaults
        resources = tpl.dlt_config_defaults["resources"]
        assert "Customer" in resources or "customers" in resources

    def test_postgres_to_bigquery_template(self):
        tpl = get_template("postgres-to-bigquery")
        assert tpl.source_type == ConnectionType.POSTGRES
        assert tpl.destination_type == ConnectionType.BIGQUERY

    def test_csv_to_duckdb_template(self):
        """The zero-credentials quickstart."""
        tpl = get_template("csv-to-duckdb")
        assert tpl.source_type == ConnectionType.CSV
        assert tpl.destination_type == ConnectionType.DUCKDB


class TestListTemplates:
    def test_returns_all_launch_templates(self):
        templates = list_templates()
        assert len(templates) == len(LAUNCH_TEMPLATES)

    def test_returns_pipeline_template_instances(self):
        for tpl in list_templates():
            assert isinstance(tpl, PipelineTemplate)

    def test_returns_a_copy_not_the_underlying_list(self):
        """Mutating the returned list must not corrupt the registry."""
        templates = list_templates()
        original_len = len(LAUNCH_TEMPLATES)
        templates.clear()
        assert len(LAUNCH_TEMPLATES) == original_len


class TestGetTemplate:
    def test_returns_template_for_valid_slug(self):
        tpl = get_template("stripe-to-postgres")
        assert tpl is not None
        assert tpl.slug == "stripe-to-postgres"

    def test_returns_none_for_unknown_slug(self):
        assert get_template("does-not-exist") is None

    def test_returns_none_for_empty_slug(self):
        assert get_template("") is None
