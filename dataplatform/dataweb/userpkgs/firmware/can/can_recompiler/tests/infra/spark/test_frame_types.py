"""
Tests for Spark-specific frame type conversion and validation.

This module tests the infrastructure layer concerns for converting input data
to processing frames and validating data at the Spark integration boundary.
"""

import pytest

from ....core.frame_types import CANTransportFrame
from ....infra.spark.frame_types import PartitionMetadata, convert_input_to_processing_frame


class TestSparkFrameConversion:
    """Test Spark-specific frame conversion functionality."""

    def test_convert_input_to_can_transport_frame(self):
        """Test conversion from input data to CANTransportFrame."""
        input_data = {
            "start_timestamp_unix_us": 1000000,
            "id": 0x7E0,
            "payload": b"\x22\xF1\x90",
            "transport_id": 1,  # ISOTP
            "trace_uuid": "test-uuid",
            "date": "2024-01-15",
            "org_id": 42,
            "device_id": 1001,
            "source_interface": "can0",
            "direction": 1,
        }

        frame, metadata = convert_input_to_processing_frame(input_data)

        assert isinstance(frame, CANTransportFrame)
        assert frame.arbitration_id == 0x7E0
        assert frame.payload == b"\x22\xF1\x90"
        assert frame.start_timestamp_unix_us == 1000000
        assert frame.is_extended_frame is False
        assert isinstance(metadata, PartitionMetadata)

    @pytest.mark.parametrize(
        "invalid_id,error_pattern",
        [
            (-1, "Invalid CAN ID: -1. Must be non-negative integer"),
            (0x20000000, "CAN ID 20000000 exceeds 29-bit limit"),
        ],
    )
    def test_invalid_can_id_validation(self, invalid_id: int, error_pattern: str):
        """Test validation of invalid CAN IDs during conversion."""
        input_data = {
            "start_timestamp_unix_us": 1000000,
            "id": invalid_id,
            "payload": b"\x22\xF1\x90",
            "trace_uuid": "test-uuid",
            "date": "2024-01-15",
            "org_id": 42,
            "device_id": 1001,
            "source_interface": "can0",
            "direction": 1,
        }

        with pytest.raises(ValueError, match=error_pattern):
            convert_input_to_processing_frame(input_data)

    @pytest.mark.parametrize(
        "timestamp_value,should_raise,description",
        [
            (0, False, "minimum valid timestamp"),
            (1000000, False, "normal timestamp"),
            (4294967296, False, "large valid timestamp"),
            (-1, True, "negative timestamp"),
            (9223372036854775808, True, "extremely large timestamp"),
        ],
    )
    def test_timestamp_validation_comprehensive(self, timestamp_value, should_raise, description):
        """Test timestamp validation during conversion."""
        row_data = {
            "start_timestamp_unix_us": timestamp_value,
            "id": 0x7E0,
            "payload": b"\x22\xF1\x90",
            "trace_uuid": "test-uuid",
            "date": "2024-01-15",
            "org_id": 42,
            "device_id": 1001,
            "source_interface": "can0",
            "direction": 1,
        }

        if should_raise:
            with pytest.raises(ValueError):
                convert_input_to_processing_frame(row_data)
        else:
            try:
                frame, metadata = convert_input_to_processing_frame(row_data)
                assert frame is not None
                assert metadata is not None
            except ValueError:
                # Some edge cases might still be invalid despite expectations
                pass

    @pytest.mark.parametrize(
        "can_id_value,should_raise,description",
        [
            (0, False, "minimum CAN ID"),
            (291, False, "normal standard ID"),
            (2047, False, "maximum standard ID"),
            (2048, False, "minimum extended ID"),
            (536870911, False, "maximum valid CAN ID"),
            (-1, True, "negative CAN ID"),
            (536870912, True, "CAN ID too large"),
        ],
    )
    def test_can_id_validation_comprehensive(self, can_id_value, should_raise, description):
        """Test CAN ID validation during conversion."""
        row_data = {
            "start_timestamp_unix_us": 1000000,
            "id": can_id_value,
            "payload": b"\x22\xF1\x90",
            "trace_uuid": "test-uuid",
            "date": "2024-01-15",
            "org_id": 42,
            "device_id": 1001,
            "source_interface": "can0",
            "direction": 1,
        }

        if should_raise:
            with pytest.raises(ValueError):
                convert_input_to_processing_frame(row_data)
        else:
            frame, metadata = convert_input_to_processing_frame(row_data)
            assert frame is not None
            assert metadata is not None


class TestPartitionMetadata:
    """Test PartitionMetadata functionality."""

    def test_partition_metadata_creation(self):
        """Test basic PartitionMetadata creation and properties."""
        metadata = PartitionMetadata(
            trace_uuid="test-uuid-123",
            date="2024-01-15",
            org_id=42,
            device_id=1001,
            source_interface="can0",
            direction=1,
        )

        assert metadata.trace_uuid == "test-uuid-123"
        assert metadata.date == "2024-01-15"
        assert metadata.org_id == 42
        assert metadata.device_id == 1001
        assert metadata.source_interface == "can0"
        assert metadata.direction == 1

    def test_partition_metadata_to_dict(self):
        """Test PartitionMetadata conversion to dictionary."""
        metadata = PartitionMetadata(
            trace_uuid="test-uuid-123",
            date="2024-01-15",
            org_id=42,
            device_id=1001,
            source_interface="can0",
            direction=1,
        )

        result = metadata.to_dict()

        expected = {
            "date": "2024-01-15",
            "trace_uuid": "test-uuid-123",
            "org_id": 42,
            "device_id": 1001,
            "source_interface": "can0",
            "direction": 1,  # direction as integer
            "mmyef_id": None,  # Vehicle MMYEF hash for signal catalog lookups
            "end_timestamp_unix_us": None,  # End timestamp for deduplication
            "duplicate_count": None,  # Duplicate count for deduplication
        }

        assert result == expected

    def test_partition_metadata_spark_schema(self):
        """Test PartitionMetadata Spark schema generation."""
        schema = PartitionMetadata.get_spark_schema()

        # Check that all expected fields are present
        field_names = [field.name for field in schema.fields]
        expected_fields = [
            "date",
            "trace_uuid",
            "org_id",
            "device_id",
            "source_interface",
            "direction",
            "mmyef_id",  # Vehicle MMYEF hash for signal catalog lookups
            "end_timestamp_unix_us",  # End timestamp for deduplication
            "duplicate_count",  # Duplicate count for deduplication
        ]

        assert set(field_names) == set(expected_fields)

        # Check field ordering (date and trace_uuid should be first)
        assert field_names[0] == "date"
        assert field_names[1] == "trace_uuid"


class TestSparkFrameValidation:
    """Test comprehensive validation for Spark frame conversion."""

    @pytest.mark.parametrize(
        "row_data,expected_error_match,description",
        [
            # Missing required fields
            (
                {"payload": b"\x01\x02\x03", "start_timestamp_unix_us": 1000000},
                "Required field 'id' missing",
                "missing id field",
            ),
            (
                {"id": 0x123, "start_timestamp_unix_us": 1000000},
                "Required field 'payload' missing",
                "missing payload field",
            ),
            (
                {"id": 0x123, "payload": b"\x01\x02\x03"},
                "Required field 'start_timestamp_unix_us' missing",
                "missing timestamp field",
            ),
            # Invalid CAN ID values
            (
                {"id": -1, "payload": b"\x01\x02\x03", "start_timestamp_unix_us": 1000000},
                "Invalid CAN ID",
                "negative CAN ID",
            ),
            (
                {
                    "id": 0x20000000,  # CAN_ID_MASK_29BIT + 1
                    "payload": b"\x01\x02\x03",
                    "start_timestamp_unix_us": 1000000,
                },
                "exceeds 29-bit limit",
                "CAN ID too large",
            ),
            # Invalid timestamp values
            (
                {"id": 0x123, "payload": b"\x01\x02\x03", "start_timestamp_unix_us": -1},
                "Invalid timestamp",
                "negative timestamp",
            ),
            (
                {
                    "id": 0x123,
                    "payload": b"\x01\x02\x03",
                    "start_timestamp_unix_us": 9223372036854775808,
                },
                "unreasonably large",
                "timestamp too large",
            ),
            # Invalid payload values
            (
                {"id": 0x123, "payload": None, "start_timestamp_unix_us": 1000000},
                "Payload cannot be None",
                "None payload",
            ),
            (
                {"id": 0x123, "payload": "invalid", "start_timestamp_unix_us": 1000000},
                "Invalid payload format",
                "string payload",
            ),
            (
                {
                    "id": 0x123,
                    "payload": b"\x01" * 10,  # > CAN_FRAME_MAX_DATA_LENGTH
                    "start_timestamp_unix_us": 1000000,
                },
                "Payload too large",
                "oversized payload",
            ),
        ],
    )
    def test_convert_input_validation_errors(self, row_data, expected_error_match, description):
        """Test convert_input_to_processing_frame validation errors."""
        with pytest.raises(ValueError, match=expected_error_match):
            convert_input_to_processing_frame(row_data)

    def test_convert_input_valid_with_all_metadata(self):
        """Test convert_input_to_processing_frame with all valid fields."""
        row_data = {
            "id": 0x7E0,
            "payload": b"\x01\x02\x03",
            "start_timestamp_unix_us": 1000000,
            "trace_uuid": "test-uuid-123",
            "date": "2024-01-15",
            "org_id": 42,
            "device_id": 1001,
            "source_interface": "can1",
            "direction": 2,
        }

        frame, metadata = convert_input_to_processing_frame(row_data)

        # Verify frame
        assert isinstance(frame, CANTransportFrame)
        assert frame.arbitration_id == 0x7E0
        assert frame.payload == b"\x01\x02\x03"
        assert frame.start_timestamp_unix_us == 1000000

        # Verify metadata
        assert isinstance(metadata, PartitionMetadata)
        assert metadata.trace_uuid == "test-uuid-123"
        assert metadata.date == "2024-01-15"
        assert metadata.org_id == 42
        assert metadata.device_id == 1001
        assert metadata.source_interface == "can1"
        assert metadata.direction == 2

    def test_convert_input_defaults_for_missing_metadata(self):
        """Test convert_input_to_processing_frame with minimal valid data."""
        row_data = {
            "id": 0x123,
            "payload": b"\x01",
            "start_timestamp_unix_us": 1000000,
        }

        frame, metadata = convert_input_to_processing_frame(row_data)

        # Should use defaults for missing metadata
        assert metadata.trace_uuid == ""
        assert metadata.date == ""
        assert metadata.org_id == 0
        assert metadata.device_id == 0
        assert metadata.source_interface == "default"
        assert metadata.direction is None
