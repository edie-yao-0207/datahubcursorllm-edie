"""
Comprehensive tests for extract_utils module.

Tests cover the select_extract_pattern function with various database types,
custom extract templates, and error conditions.
"""

from unittest.mock import patch

import pytest
from dataweb import get_databases
from dataweb.userpkgs.constants import Database
from dataweb.userpkgs.extract_utils import (
    DATAWEB_EXTRACT,
    KINESIS_STATS_EXTRACT,
    select_extract_pattern,
)
from dataweb.userpkgs.firmware.metric import Metric, StatMetadata


@pytest.fixture
def mock_database_mappings():
    """Mock database mappings for consistent testing."""
    return {
        "kinesisstats": "kinesisstats_test",
        "kinesisstats_history": "kinesisstats_history_test",
        "product_analytics_staging": "product_analytics_staging_test",
        "product_analytics": "product_analytics_test",
        "other_database": "mapped_other_database",
    }


class TestSelectExtractPattern:
    """Test cases for the select_extract_pattern function."""

    @patch("dataweb.userpkgs.extract_utils.get_databases")
    def test_kinesis_stats_extract_default_template(
        self,
        mock_get_databases,
        sample_kinesis_metric,
        mock_format_kwargs,
        mock_database_mappings,
    ):
        """Test that KinesisStats database uses the default KINESIS_STATS_EXTRACT template."""
        mock_get_databases.return_value = mock_database_mappings
        result = select_extract_pattern(sample_kinesis_metric, **mock_format_kwargs)

        # Expected database mapping: kinesisstats -> kinesisstat_test
        expected_table = "kinesisstats_test.test_table"

        # Check that the result contains expected kinesis-specific elements
        assert "object_id AS device_id" in result
        assert "NOT value.is_end" in result
        assert "NOT value.is_databreak" in result
        assert expected_table in result
        assert "test_field" in result
        assert "2024-01-01" in result
        assert "2024-01-31" in result

    @patch("dataweb.userpkgs.extract_utils.get_databases")
    def test_kinesis_stats_history_extract_default_template(
        self,
        mock_get_databases,
        sample_kinesis_history_metric,
        mock_format_kwargs,
        mock_database_mappings,
    ):
        """Test that KinesisStats History database uses the default KINESIS_STATS_EXTRACT template."""
        mock_get_databases.return_value = mock_database_mappings
        result = select_extract_pattern(
            sample_kinesis_history_metric, **mock_format_kwargs
        )

        # Expected database mapping: kinesisstats_history -> kinesisstats_history_test
        expected_table = "kinesisstats_history_test.history_table"

        # Check that the result contains expected kinesis-specific elements
        assert "object_id AS device_id" in result
        assert "NOT value.is_end" in result
        assert "NOT value.is_databreak" in result
        assert expected_table in result
        assert "history_field" in result

    @patch("dataweb.userpkgs.extract_utils.get_databases")
    def test_dataweb_extract_default_template(
        self,
        mock_get_databases,
        sample_dataweb_metric,
        mock_format_kwargs,
        mock_database_mappings,
    ):
        """Test that DataWeb database uses the default DATAWEB_EXTRACT template."""
        mock_get_databases.return_value = mock_database_mappings
        result = select_extract_pattern(sample_dataweb_metric, **mock_format_kwargs)

        # Expected database mapping: product_analytics_staging -> product_analytics_staging_test
        expected_table = "product_analytics_staging_test.test_analytics_table"

        # Check that the result contains expected dataweb-specific elements
        assert "device_id" in result  # Direct device_id, not object_id AS device_id
        assert expected_table in result
        assert "analytics_field" in result
        assert "2024-01-01" in result
        assert "2024-01-31" in result

    @patch("dataweb.userpkgs.extract_utils.get_databases")
    def test_custom_kinesis_extract_template(
        self, mock_get_databases, sample_kinesis_metric, mock_database_mappings
    ):
        """Test using a custom KinesisStats extract template."""
        mock_get_databases.return_value = mock_database_mappings
        custom_kinesis_template = """
        SELECT custom_field
        FROM {source_table}
        WHERE custom_condition = '{source.field}'
        """

        result = select_extract_pattern(
            sample_kinesis_metric,
            kinesis_stats_extract=custom_kinesis_template,
            date_start="2024-01-01",
            date_end="2024-01-31",
        )

        # Expected database mapping: kinesisstats -> kinesisstats_test
        expected_table = "kinesisstats_test.test_table"

        assert "SELECT custom_field" in result
        assert "custom_condition = 'test_field'" in result
        assert expected_table in result

    @patch("dataweb.userpkgs.extract_utils.get_databases")
    def test_custom_dataweb_extract_template(
        self, mock_get_databases, sample_dataweb_metric, mock_database_mappings
    ):
        """Test using a custom DataWeb extract template."""
        mock_get_databases.return_value = mock_database_mappings
        custom_dataweb_template = """
        SELECT custom_analytics_field
        FROM {source_table}
        WHERE analytics_condition = '{source.field}'
        """

        result = select_extract_pattern(
            sample_dataweb_metric,
            dataweb_extract=custom_dataweb_template,
            date_start="2024-01-01",
            date_end="2024-01-31",
        )

        # Expected database mapping: product_analytics_staging -> product_analytics_staging_test
        expected_table = "product_analytics_staging_test.test_analytics_table"

        assert "SELECT custom_analytics_field" in result
        assert "analytics_condition = 'analytics_field'" in result
        assert expected_table in result

    def test_both_custom_templates(self, sample_kinesis_metric):
        """Test providing both custom templates (only kinesis should be used for kinesis metric)."""
        custom_kinesis_template = "SELECT kinesis_custom FROM {source_table}"
        custom_dataweb_template = "SELECT dataweb_custom FROM {source_table}"

        result = select_extract_pattern(
            sample_kinesis_metric,
            kinesis_stats_extract=custom_kinesis_template,
            dataweb_extract=custom_dataweb_template,
        )

        assert "kinesis_custom" in result
        assert "dataweb_custom" not in result

    def test_unsupported_database_type(self):
        """Test that unsupported database types raise ValueError."""
        unsupported_metric = Metric(
            type="unsupported_db.test_table",
            field="test_field",
            label="Unsupported Metric",
        )

        with pytest.raises(ValueError) as excinfo:
            select_extract_pattern(unsupported_metric)

        assert "Unknown database type: unsupported_db" in str(excinfo.value)

    def test_format_kwargs_propagation(self, sample_kinesis_metric):
        """Test that additional format kwargs are properly propagated to the template."""
        custom_kwargs = {
            "date_start": "2024-02-01",
            "date_end": "2024-02-29",
            "custom_filter": "AND custom_condition = 'test'",
            "limit_clause": "LIMIT 1000",
        }

        custom_template = """
        SELECT *
        FROM {source_table}
        WHERE date BETWEEN "{date_start}" AND "{date_end}"
        {custom_filter}
        {limit_clause}
        """

        result = select_extract_pattern(
            sample_kinesis_metric,
            kinesis_stats_extract=custom_template,
            **custom_kwargs,
        )

        assert "2024-02-01" in result
        assert "2024-02-29" in result
        assert "AND custom_condition = 'test'" in result
        assert "LIMIT 1000" in result

    @patch("dataweb.userpkgs.extract_utils.get_databases")
    def test_metric_field_substitution(
        self, mock_get_databases, sample_dataweb_metric, mock_database_mappings
    ):
        """Test that metric field is properly substituted in templates."""
        mock_get_databases.return_value = mock_database_mappings
        # The metric has field "analytics_field"
        result = select_extract_pattern(
            sample_dataweb_metric, date_start="2024-01-01", date_end="2024-01-31"
        )

        # Should contain the field name in the CAST statement
        assert "CAST(analytics_field AS DOUBLE)" in result

    @patch("dataweb.userpkgs.extract_utils.get_databases")
    def test_source_table_substitution(
        self, mock_get_databases, sample_kinesis_metric, mock_database_mappings
    ):
        """Test that source table is properly substituted in templates."""
        mock_get_databases.return_value = mock_database_mappings
        # The metric has type "kinesisstats.test_table"
        result = select_extract_pattern(
            sample_kinesis_metric, date_start="2024-01-01", date_end="2024-01-31"
        )

        # Expected database mapping: kinesisstats -> kinesisstats_test
        expected_table = "kinesisstats_test.test_table"

        # Should contain the full table name
        assert expected_table in result

    @patch("dataweb.userpkgs.extract_utils.get_databases")
    def test_empty_format_kwargs(
        self, mock_get_databases, sample_dataweb_metric, mock_database_mappings
    ):
        """Test that function works with minimal required format kwargs."""
        mock_get_databases.return_value = mock_database_mappings
        result = select_extract_pattern(
            sample_dataweb_metric, date_start="2024-01-01", date_end="2024-01-31"
        )

        # Expected database mapping: product_analytics_staging -> product_analytics_staging_test
        expected_table = "product_analytics_staging_test.test_analytics_table"

        # Should still work and contain basic template elements
        assert "SELECT" in result
        assert "FROM" in result
        assert "WHERE" in result
        assert expected_table in result

    def test_database_mapping_behavior(self):
        """Test that database mapping is working correctly."""
        database_mappings = get_databases()

        # Test kinesis databases (should remain unchanged in most environments)
        kinesis_metric = Metric(
            type="kinesisstats.test_table",
            field="test_field",
            label="Test Kinesis Metric",
        )

        result = select_extract_pattern(
            kinesis_metric, date_start="2024-01-01", date_end="2024-01-31"
        )

        expected_kinesis_db = database_mappings[Database.KINESISSTATS]
        assert f"{expected_kinesis_db}.test_table" in result

        # Test product analytics staging (likely mapped to datamodel_dev in dev)
        staging_metric = Metric(
            type="product_analytics_staging.test_staging_table",
            field="test_field",
            label="Test Staging Metric",
        )

        result = select_extract_pattern(
            staging_metric, date_start="2024-01-01", date_end="2024-01-31"
        )

        expected_staging_db = database_mappings[Database.PRODUCT_ANALYTICS_STAGING]
        assert f"{expected_staging_db}.test_staging_table" in result

    def test_database_mapping_with_complex_table_names(self):
        """Test database mapping with multi-part table names."""
        # Test a table name with multiple dots
        complex_metric = Metric(
            type="product_analytics_staging.schema.complex_table_name",
            field="test_field",
            label="Complex Table Metric",
        )

        result = select_extract_pattern(
            complex_metric, date_start="2024-01-01", date_end="2024-01-31"
        )

        database_mappings = get_databases()
        expected_db = database_mappings[Database.PRODUCT_ANALYTICS_STAGING]
        expected_table = f"{expected_db}.schema.complex_table_name"
        assert expected_table in result

    @patch("dataweb.userpkgs.extract_utils.get_databases")
    def test_unknown_database_raises_error(self, mock_get_databases):
        """Test that unknown database types raise appropriate errors."""
        # Set up mock with limited mappings
        mock_mappings = {
            "kinesisstats": "kinesisstats_prod",
            "product_analytics_staging": "analytics_test_db",
        }
        mock_get_databases.return_value = mock_mappings

        # Create metric with unknown database
        unknown_metric = Metric(
            type="unknown_database.some_table",
            field="some_field",
            label="Unknown Metric",
        )

        # Should raise ValueError for unknown database
        with pytest.raises(ValueError, match="Unknown database type: unknown_database"):
            select_extract_pattern(
                unknown_metric, date_start="2024-01-01", date_end="2024-01-31"
            )
