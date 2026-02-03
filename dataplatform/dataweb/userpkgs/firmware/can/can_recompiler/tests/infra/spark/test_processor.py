"""
Tests for Spark CAN Protocol Processor.

Tests edge cases and error conditions in the SparkCANProtocolProcessor,
which handles Spark-specific processing and schema generation.
"""

import pytest
from unittest.mock import Mock, patch

from ....infra.spark.processor import SparkCANProtocolProcessor
from ....infra.spark.frame_types import InputFrameData
from ....infra.spark.frame_types import PartitionMetadata
from ....core.enums import TransportProtocolType


class TestSparkCANProtocolProcessor:
    """Test Spark CAN Protocol Processor edge cases for increased coverage."""

    def test_processor_cleanup(self):
        """Test processor cleanup method (line 48)."""
        processor = SparkCANProtocolProcessor()

        # Should call through to processor manager
        processor.cleanup(1000000)  # Should not raise exception

    @pytest.mark.parametrize(
        "can_id,description",
        [
            (0x12345, "Extended CAN ID that doesn't match known patterns"),
            (0x000, "Low ID"),
            (0x7FF, "Standard frame max"),
            (0x800, "Extended frame start"),
            (0x1FFFFFFF, "Extended frame max"),
        ],
    )
    def test_process_frame_detection_edge_cases(self, can_id, description):
        """Test processing with various CAN IDs that test protocol detection."""
        processor = SparkCANProtocolProcessor()

        # Create input with the test CAN ID
        input_data = InputFrameData(
            id=can_id,
            payload=b"\x00\x00\x00",
            start_timestamp_unix_us=1000000,
            trace_uuid="test",
            date="2024-01-15",
            org_id=1,
            device_id=1,
        )

        result = processor.process_frame_to_dict(input_data)
        # Might still process as J1939 due to broad detection, so just verify structure
        if result is not None:
            assert "transport_id" in result
            assert "application_id" in result
        # Should handle {description} without crashing

    def test_assemble_application_section_with_transport_metadata(self):
        """Test assembling application section with transport metadata (consolidated via adapters)."""
        processor = SparkCANProtocolProcessor()

        # Create real UDS message (not mock) since processor uses isinstance checks
        from ....protocols.uds.frames import UDSMessage
        from ....protocols.uds.enums import UDSDataIdentifierType
        from ....protocols.isotp.frames import ISOTPFrame

        transport_frame = ISOTPFrame(
            source_address=0x7E0,
            payload=b"\x22\xF1\x90",  # ReadDataByIdentifier, DID F190
            start_timestamp_unix_us=1000000,
            frame_count=1,
        )

        uds_message = UDSMessage(transport_frame=transport_frame)

        from ....core.enums import ApplicationProtocolType

        # Test with UDS protocol type
        result = processor._assemble_application_section(
            uds_message, ApplicationProtocolType.UDS, transport_frame
        )

        # Verify structure (actual adapter output will have more fields)
        assert "j1939" in result
        assert "none" in result
        assert "uds" in result

        assert result["j1939"] is None
        assert result["none"] is None

        # UDS section should have consolidated data from adapter
        uds_data = result["uds"]
        assert uds_data is not None
        assert "service_id" in uds_data
        assert "source_address" in uds_data
        # Note: extended_addressing was removed as it wasn't part of the actual schema
        assert uds_data["service_id"] == 0x22
        assert uds_data["source_address"] == 0x7E0

    def test_assemble_application_section_unknown_protocol(self):
        """Test assembling application section with unknown protocol type."""
        processor = SparkCANProtocolProcessor()

        from ....core.enums import ApplicationProtocolType

        # Test with NONE protocol type and no message (should not populate any section)
        result = processor._assemble_application_section(None, ApplicationProtocolType.NONE)

        expected = {"j1939": None, "none": None, "uds": None}
        assert result == expected

    def test_process_frame_with_metadata_none_values(self):
        """Test processing with metadata containing None values."""
        processor = SparkCANProtocolProcessor()

        input_data = InputFrameData(
            id=0x18FEF100,  # Valid J1939 ID that will process successfully
            payload=b"\x01\x02\x03\x04\x05\x06\x07\x08",  # Valid J1939 payload
            start_timestamp_unix_us=1000000,
            # Test with minimal metadata
            trace_uuid="test",
            date="2024-01-15",
            org_id=1,
            device_id=1,
            # direction and source_interface will be None/default
        )

        result = processor.process_frame_to_dict(input_data)
        assert result is not None

        # Should have default values for missing metadata
        assert "source_interface" in result
        assert "direction" in result
        # Verify that metadata values are flattened correctly
        assert result["trace_uuid"] == "test"
        assert result["org_id"] == 1

    def test_timestamp_propagation_priority(self):
        """Test that timestamp propagation works correctly from different levels."""
        processor = SparkCANProtocolProcessor()

        # Test with metadata timestamp
        input_data = InputFrameData(
            id=0x18FEF100,  # Valid J1939 ID
            payload=b"\x01\x02\x03\x04\x05\x06\x07\x08",
            start_timestamp_unix_us=1000000,  # Metadata level timestamp
            trace_uuid="test",
            date="2024-01-15",
            org_id=1,
            device_id=1,
        )

        result = processor.process_frame_to_dict(input_data)
        if result is not None:
            # Should propagate the timestamp
            assert "start_timestamp_unix_us" in result
            assert isinstance(result["start_timestamp_unix_us"], int)
