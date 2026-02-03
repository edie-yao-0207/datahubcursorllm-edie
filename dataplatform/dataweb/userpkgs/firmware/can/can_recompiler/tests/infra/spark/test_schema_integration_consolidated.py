"""
Consolidated Schema Integration Tests

This consolidates the repetitive schema integration testing patterns across
UDS, and J1939 protocols using parameterization.
"""

import pytest
from typing import Dict, Any

from ....infra.spark.processor import SparkCANProtocolProcessor
from ....infra.spark.frame_types import PartitionMetadata, InputFrameData
from ....protocols.isotp.frames import ISOTPFrame

# Protocol-specific test configurations
PROTOCOL_TEST_CASES = [
    pytest.param(
        {
            "protocol": "UDS",
            "input_config": {
                "id": 0x7E8,  # Response address for 0x7E0
                "payload": b"\x05\x62\xF1\x90\x12\x34",  # Positive response to Read Data By Identifier
                "start_timestamp_unix_us": 1000000,
            },
            "expected_active_protocol": "uds",
            "expected_app_fields": ["service_id", "data_identifier"],
            "expected_transport_fields": ["source_address", "frame_count", "expected_length"],
            "expected_protocols": ["uds", "j1939", "none"],
            "description": "UDS Read Data By Identifier Response",
        },
        id="uds_topological_processing",
    ),
    pytest.param(
        {
            "protocol": "UDS",
            "input_config": {
                "id": 0x7E8,  # ECU response address
                "payload": b"\x04\x41\x0C\x1A\xF8",  # Mode 41 (response), PID 0C, data
                "start_timestamp_unix_us": 1000000,
            },
            "expected_active_protocol": "uds",
            "expected_app_fields": ["service_id", "data_identifier"],
            "expected_transport_fields": ["source_address", "frame_count", "expected_length"],
            "expected_protocols": ["uds", "j1939", "none"],
            "description": "UDS Response",
        },
        id="uds_topological_processing",
    ),
    pytest.param(
        {
            "protocol": "J1939",
            "input_config": {
                "id": 0x18FEF100,  # J1939 PGN with standard addressing
                "payload": b"\x01\x02\x03\x04\x05\x06\x07\x08",
                "start_timestamp_unix_us": 1000000,
            },
            "expected_active_protocol": "j1939",
            "expected_app_fields": ["pgn", "source_address", "destination_address"],
            "expected_transport_fields": [],  # J1939 doesn't use ISO-TP transport
            "expected_protocols": ["uds", "j1939", "none"],
            "description": "J1939 standard message",
        },
        id="j1939_topological_processing",
    ),
    pytest.param(
        {
            "protocol": "None",
            "input_config": {
                "id": 0x123,  # Non-standard address
                "payload": b"\xFF\xFF\xFF\xFF",  # Unknown payload
                "start_timestamp_unix_us": 1000000,
            },
            "expected_active_protocol": "none",
            "expected_app_fields": ["arbitration_id", "data_payload"],
            "expected_transport_fields": [],  # No transport processing for unrecognized frames
            "expected_protocols": ["uds", "j1939", "none"],
            "description": "Unrecognized frame type",
        },
        id="none_topological_processing",
    ),
]


@pytest.fixture
def sample_isotp_frame():
    """Sample ISO-TP frame for testing."""
    return ISOTPFrame(
        source_address=0x7E0,
        target_address=0x7E8,
        payload=b"\x22\xF1\x90",
        start_timestamp_unix_us=1000000,
        frame_count=1,
        expected_length=3,
    )


@pytest.fixture
def sample_metadata():
    """Sample partition metadata."""
    return PartitionMetadata(
        trace_uuid="test-trace-123",
        date="2023-01-01",
        org_id=12345,  # Changed to integer
        device_id=67890,  # Changed to integer
        source_interface="can0",
        direction=1,  # 1 = rx (receive direction)
    )


class TestConsolidatedSchemaIntegration:
    """Consolidated schema integration tests using parameterization."""

    @pytest.mark.parametrize("test_case", PROTOCOL_TEST_CASES)
    def test_topological_output_structure(
        self, test_case: Dict[str, Any], sample_metadata: PartitionMetadata
    ):
        """Test protocol topological output structure via main processor."""
        processor = SparkCANProtocolProcessor()

        # Create input data with metadata
        metadata_dict = sample_metadata.to_dict()
        input_data = InputFrameData(**test_case["input_config"], **metadata_dict)

        output_dict = processor.process_frame_to_dict(input_data)

        # Handle None frames (unrecognized) - processor returns None for efficiency
        if test_case["protocol"] == "None":
            assert (
                output_dict is None
            ), f"Expected None return for unrecognized frame: {test_case['description']}"
            return  # Skip further validation for None frames

        assert (
            output_dict is not None
        ), f"Failed to process {test_case['protocol']} frame: {test_case['description']}"

        # Verify metadata fields are flattened at top level
        expected_metadata_fields = [
            "trace_uuid",
            "date",
            "org_id",
            "device_id",
            "source_interface",
            "direction",
        ]
        for metadata_field in expected_metadata_fields:
            assert (
                metadata_field in output_dict
            ), f"Missing metadata field {metadata_field} for {test_case['protocol']}"

        # Verify consolidated structure (no separate transport section)
        assert (
            "application" in output_dict
        ), f"Missing application section for {test_case['protocol']}"
        assert "transport_id" in output_dict, f"Missing transport_id for {test_case['protocol']}"
        assert (
            "application_id" in output_dict
        ), f"Missing application_id for {test_case['protocol']}"
        assert (
            "start_timestamp_unix_us" in output_dict
        ), f"Missing timestamp for {test_case['protocol']}"

        # Transport section should not exist (consolidated into application)
        assert (
            "transport" not in output_dict
        ), f"Found deprecated transport section for {test_case['protocol']}"

        # Verify consolidated application structure
        application = output_dict["application"]
        for protocol_name in test_case["expected_protocols"]:
            assert (
                protocol_name in application
            ), f"Missing {protocol_name} in application section for {test_case['protocol']}"

    @pytest.mark.parametrize("test_case", PROTOCOL_TEST_CASES)
    def test_active_protocol_population(
        self, test_case: Dict[str, Any], sample_metadata: PartitionMetadata
    ):
        """Test that the correct protocol section is populated with data."""
        processor = SparkCANProtocolProcessor()

        # Create input data
        metadata_dict = sample_metadata.to_dict()
        input_data = InputFrameData(**test_case["input_config"], **metadata_dict)

        output_dict = processor.process_frame_to_dict(input_data)

        # Handle None frames (unrecognized) - processor returns None for efficiency
        if test_case["protocol"] == "None":
            assert (
                output_dict is None
            ), f"Expected None return for unrecognized frame: {test_case['description']}"
            return  # Skip further validation for None frames

        application = output_dict["application"]

        active_protocol = test_case["expected_active_protocol"]
        active_data = application[active_protocol]

        # Active protocol should have data
        assert (
            active_data is not None
        ), f"Expected {active_protocol} to be populated for {test_case['description']}"

        # Verify expected application fields are present
        for field_name in test_case["expected_app_fields"]:
            assert (
                field_name in active_data
            ), f"Missing {field_name} in {active_protocol} data for {test_case['description']}"

        # Verify expected transport fields are present (if applicable)
        for field_name in test_case["expected_transport_fields"]:
            assert (
                field_name in active_data
            ), f"Missing transport field {field_name} in {active_protocol} data for {test_case['description']}"

        # Other protocols should be None (except for the active one)
        for protocol_name in test_case["expected_protocols"]:
            if protocol_name != active_protocol:
                assert (
                    application[protocol_name] is None
                ), f"Expected {protocol_name} to be None for {test_case['protocol']} processing"

    @pytest.mark.parametrize("test_case", PROTOCOL_TEST_CASES)
    def test_protocol_enum_consistency(
        self, test_case: Dict[str, Any], sample_metadata: PartitionMetadata
    ):
        """Test that protocol enum fields are correctly set."""
        processor = SparkCANProtocolProcessor()

        # Create input data
        metadata_dict = sample_metadata.to_dict()
        input_data = InputFrameData(**test_case["input_config"], **metadata_dict)

        output_dict = processor.process_frame_to_dict(input_data)

        # Handle None frames (unrecognized) - processor returns None for efficiency
        if test_case["protocol"] == "None":
            assert (
                output_dict is None
            ), f"Expected None return for unrecognized frame: {test_case['description']}"
            return  # Skip further validation for None frames

        # Check protocol enum consistency
        transport_protocol = output_dict["transport_id"]
        application_protocol = output_dict["application_id"]

        # Verify protocol enums are integers (enum values)
        assert isinstance(
            transport_protocol, int
        ), f"Transport protocol should be integer for {test_case['protocol']}"
        assert isinstance(
            application_protocol, int
        ), f"Application protocol should be integer for {test_case['protocol']}"

        # Protocol-specific enum validation (using enum values)
        if test_case["protocol"] == "UDS":
            assert (
                transport_protocol == 1  # TransportProtocolType.ISOTP
            ), f"Expected ISOTP transport (1) for {test_case['protocol']}, got {transport_protocol}"
            assert (
                application_protocol == 1  # ApplicationProtocolType.UDS
            ), f"Expected UDS application (1) for {test_case['protocol']}, got {application_protocol}"
        elif test_case["protocol"] == "J1939":
            assert (
                transport_protocol == 2  # TransportProtocolType.J1939
            ), f"Expected J1939 transport (2) for {test_case['protocol']}, got {transport_protocol}"
            assert (
                application_protocol == 3  # ApplicationProtocolType.J1939
            ), f"Expected J1939 application (3) for {test_case['protocol']}, got {application_protocol}"

    def test_topological_schema_alignment(self, sample_metadata: PartitionMetadata):
        """Test that topological schema matches actual output structure."""
        from ....infra.spark.schema import get_topological_schema

        processor = SparkCANProtocolProcessor()
        schema = get_topological_schema()

        # Test with UDS response frame to get real output
        metadata_dict = sample_metadata.to_dict()
        input_data = InputFrameData(
            id=0x7E8,  # Response address for 0x7E0
            payload=b"\x05\x62\xF1\x90\x12\x34",  # Positive response to Read Data By Identifier
            start_timestamp_unix_us=1000000,
            **metadata_dict,
        )

        output_dict = processor.process_frame_to_dict(input_data)

        # Verify schema field names match output dictionary keys
        schema_field_names = [field.name for field in schema.fields]

        for key in output_dict.keys():
            if key != "application":  # Application section has nested structure
                assert key in schema_field_names, f"Schema missing field: {key}"

        # Check application section structure
        application_schema = next(field for field in schema.fields if field.name == "application")
        assert application_schema is not None, "Schema missing application section"

        # Verify nested protocol structures exist
        if hasattr(application_schema.dataType, "fields"):
            nested_field_names = [field.name for field in application_schema.dataType.fields]
            expected_protocols = ["uds", "j1939", "none"]
            for protocol in expected_protocols:
                assert (
                    protocol in nested_field_names
                ), f"Schema missing {protocol} in application section"


# Edge case tests for schema validation
class TestSchemaEdgeCases:
    """Test schema edge cases and error handling."""

    def test_empty_payload_handling(self, sample_metadata: PartitionMetadata):
        """Test handling of empty payload frames."""
        processor = SparkCANProtocolProcessor()

        metadata_dict = sample_metadata.to_dict()
        input_data = InputFrameData(
            id=0x123, payload=b"", start_timestamp_unix_us=1000000, **metadata_dict  # Empty payload
        )

        output_dict = processor.process_frame_to_dict(input_data)
        # Empty payload with unrecognized ID should return None (unrecognized frame)
        assert output_dict is None, "Empty payload with unrecognized ID should return None"

    @pytest.mark.parametrize(
        "invalid_input,description",
        [({"id": 0x7E0}, "missing payload"), ({"payload": b"\x22\xF1\x90"}, "missing id")],
    )
    def test_malformed_input_handling(
        self, invalid_input: Dict[str, Any], description: str, sample_metadata: PartitionMetadata
    ):
        """Test handling of malformed input data."""
        processor = SparkCANProtocolProcessor()

        metadata_dict = sample_metadata.to_dict()

        # Create input with missing required fields
        input_config = {"start_timestamp_unix_us": 1000000, **metadata_dict, **invalid_input}

        with pytest.raises((KeyError, TypeError, ValueError)):
            input_data = InputFrameData(**input_config)
            processor.process_frame_to_dict(input_data)
