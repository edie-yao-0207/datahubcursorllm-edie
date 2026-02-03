"""
Targeted tests for Spark trace processing utilities.

Focuses on testing functions that don't require can_recompiler imports,
which are meant for distributed Spark execution environments.
"""

import pytest
from unittest.mock import Mock, patch

from ....infra.spark.trace_processing import (
    generate_application_struct_sql,
    create_passthrough_dataframe,
)


class TestGenerateApplicationStructSQL:
    """Test SQL generation for application struct - fully testable locally."""

    def test_generate_uds_application_sql(self):
        """Test SQL generation for UDS protocol."""
        sql = generate_application_struct_sql(
            "uds", service_id="34", data_identifier="cast(0x123 as int)", is_response="true"
        )

        # Should contain named_struct with all protocol sections
        assert "named_struct(" in sql
        assert "'uds'" in sql
        assert "'j1939'" in sql
        assert "'none'" in sql

        # UDS section should have the provided fields
        assert "service_id" in sql
        assert "data_identifier" in sql
        assert "is_response" in sql

        # Other protocols should be null structs
        assert "cast(null as struct<" in sql

        assert "cast(null as struct<" in sql

    def test_generate_j1939_application_sql(self):
        """Test SQL generation for J1939 protocol."""
        sql = generate_application_struct_sql(
            "j1939", pgn="cast(0xFEE0 as int)", source_address="72", data_identifier_type="6"
        )

        assert "named_struct(" in sql
        assert "'j1939'" in sql
        assert "pgn" in sql
        assert "source_address" in sql
        assert "data_identifier_type" in sql

    def test_generate_none_application_sql(self):
        """Test SQL generation for None protocol."""
        sql = generate_application_struct_sql(
            "none", arbitration_id="cast(0x123 as long)", payload_length="8"
        )

        assert "named_struct(" in sql
        assert "'none'" in sql
        assert "arbitration_id" in sql
        assert "payload_length" in sql

        # Other protocols should be null
        assert "cast(null as struct<" in sql

    def test_generate_empty_protocol_data(self):
        """Test handling of empty protocol data."""
        sql = generate_application_struct_sql("uds")

        # Should still generate valid structure
        assert "named_struct(" in sql
        assert "'uds'" in sql

    def test_sql_structure_validity(self):
        """Test that generated SQL has valid structure."""
        protocols = ["uds", "j1939", "none"]

        for protocol in protocols:
            sql = generate_application_struct_sql(protocol, test_field="'value'")

            # All generated SQL should be structurally valid
            assert "named_struct(" in sql
            assert sql.count("'") % 2 == 0  # Paired quotes
            assert sql.count("(") == sql.count(")")  # Balanced parentheses

    def test_protocol_field_handling(self):
        """Test that protocol-specific fields are handled correctly."""
        # Test with multiple fields for UDS
        sql = generate_application_struct_sql(
            "uds",
            service_id="cast(34 as int)",
            data_identifier="cast(0xF190 as int)",
            is_response="cast(true as boolean)",
            is_negative_response="cast(false as boolean)",
        )

        # All fields should appear in the UDS section
        assert "service_id" in sql
        assert "data_identifier" in sql
        assert "is_response" in sql
        assert "is_negative_response" in sql

        # Should maintain structure with named_struct
        assert "'uds', named_struct(" in sql or "'uds', cast(" in sql


class TestCreatePassthroughDataframe:
    """Test passthrough DataFrame creation - testable with mocks."""

    def test_create_passthrough_basic_structure(self):
        """Test basic passthrough DataFrame creation."""
        # Mock Spark DataFrame
        mock_df = Mock()
        mock_df.selectExpr.return_value = mock_df

        result = create_passthrough_dataframe(mock_df)

        # Should call selectExpr
        mock_df.selectExpr.assert_called_once()
        assert result == mock_df

    def test_passthrough_selectexpr_call_structure(self):
        """Test the structure of selectExpr call."""
        mock_df = Mock()
        mock_df.selectExpr.return_value = mock_df

        create_passthrough_dataframe(mock_df)

        # Should be called with multiple arguments
        call_args = mock_df.selectExpr.call_args[0]
        assert len(call_args) > 1

        # Second-to-last argument should be the application struct (last is mmyef_id)
        application_arg = call_args[-2]
        assert "AS application" in application_arg

        # Last argument should be mmyef_id
        last_arg = call_args[-1]
        assert last_arg == "mmyef_id"

    def test_passthrough_with_method_chaining(self):
        """Test passthrough DataFrame with method chaining."""
        mock_df = Mock()
        mock_selectexpr_result = Mock()
        mock_df.selectExpr.return_value = mock_selectexpr_result

        result = create_passthrough_dataframe(mock_df)

        assert result == mock_selectexpr_result
        mock_df.selectExpr.assert_called_once()

    def test_passthrough_generates_application_sql(self):
        """Test that passthrough generates application struct SQL."""
        mock_df = Mock()
        mock_df.selectExpr.return_value = mock_df

        create_passthrough_dataframe(mock_df)

        call_args = mock_df.selectExpr.call_args[0]

        # Find the application struct argument (must end with "AS application", not just contain it)
        application_arg = None
        for arg in call_args:
            if arg.strip().endswith("AS application"):
                application_arg = arg
                break

        assert application_arg is not None
        # Should contain named_struct for 'none' protocol
        assert "named_struct(" in application_arg or "cast(" in application_arg


class TestSQLGenerationEdgeCases:
    """Test edge cases and error conditions in SQL generation."""

    def test_sql_with_complex_expressions(self):
        """Test SQL generation with complex field expressions."""
        sql = generate_application_struct_sql(
            "uds",
            service_id="case when payload[0] > 128 then payload[0] - 64 else payload[0] end",
            data_identifier="cast(hex(substring(payload, 2, 2)) as int)",
            is_response="payload[0] >= 64",
        )

        # Should handle complex expressions
        assert "case when" in sql
        assert "cast(" in sql
        assert "substring(" in sql

    def test_sql_with_null_values(self):
        """Test SQL generation with explicit null values."""
        sql = generate_application_struct_sql(
            "j1939", pgn="cast(null as int)", source_address="cast(null as int)"
        )

        assert "cast(null as int)" in sql
        assert "'j1939'" in sql

    def test_all_protocols_sql_generation(self):
        """Test that SQL can be generated for all supported protocols."""
        protocols = ["none", "uds", "j1939"]

        for protocol in protocols:
            sql = generate_application_struct_sql(protocol, test_field="'value'")
            # Each should generate valid SQL
            assert isinstance(sql, str)
            assert len(sql) > 0
            assert "named_struct(" in sql
            assert f"'{protocol}'" in sql

    def test_passthrough_dataframe_edge_cases(self):
        """Test passthrough DataFrame with edge cases."""
        # Test with None DataFrame (should handle gracefully or raise appropriate error)
        try:
            result = create_passthrough_dataframe(None)
            # If it doesn't raise an error, it should return None or handle gracefully
        except (AttributeError, TypeError):
            # Expected behavior for None input
            pass

        # Test with mock that has no selectExpr method
        invalid_mock = Mock(spec=[])  # Empty spec - no methods

        with pytest.raises(AttributeError):
            create_passthrough_dataframe(invalid_mock)
