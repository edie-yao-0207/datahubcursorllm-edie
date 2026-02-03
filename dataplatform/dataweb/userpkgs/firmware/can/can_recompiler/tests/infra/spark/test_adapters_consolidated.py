"""
Consolidated and Parameterized Spark Adapter Tests

This consolidates the repetitive adapter testing patterns across UDS, and J1939
protocols using comprehensive parameterization for better maintainability.
"""

import pytest
from typing import Dict, Any

from ....protocols.uds.frames import UDSMessage
from ....protocols.isotp.frames import ISOTPFrame
from ....protocols.j1939.frames import J1939TransportMessage
from ....infra.spark.adapters import (
    UDSMessageSparkAdapter,
    J1939ApplicationSparkAdapter,
    ISOTPFrameSparkAdapter,
)


@pytest.fixture
def sample_isotp_frame():
    """Sample ISO-TP frame for testing."""
    return ISOTPFrame(
        source_address=0x7E0,
        payload=b"\x22\xF1\x90",
        start_timestamp_unix_us=1000000,
        frame_count=1,
        expected_length=3,
    )


# Comprehensive test data for different adapter types
ADAPTER_TEST_CASES = [
    pytest.param(
        {
            "protocol": "UDS",
            "message_class": UDSMessage,
            "adapter_class": UDSMessageSparkAdapter,
            "message_kwargs": {
                "transport_frame": ISOTPFrame(
                    source_address=0x7E0,
                    payload=b"\x22\xF1\x90\x01\x02",
                    start_timestamp_unix_us=1000000,
                    frame_count=1,
                    expected_length=5,
                ),
            },
            "expected_app_fields": {
                "service_id": 0x22,
                "data_identifier": 0xF190,  # Just the DID (Data Identifier)
                "is_response": False,
                "is_negative_response": False,
            },
            "expected_transport_fields": {
                "source_address": 0x7E0,
                "frame_count": 1,
                "expected_length": 3,
            },
            "default_transport_values": {
                "source_address": 0,
                "frame_count": 1,
                "expected_length": 0,
            },
        },
        id="uds_message_adapter",
    ),
    pytest.param(
        {
            "protocol": "J1939",
            "message_class": J1939TransportMessage,
            "adapter_class": J1939ApplicationSparkAdapter,
            "message_kwargs": {
                "pgn": 0xFEF1,
                "source_address": 0x00,
                "destination_address": 0xFF,
                "priority": 6,
                "payload": b"\x01\x02\x03\x04\x05\x06\x07\x08",
                "start_timestamp_unix_us": 1000000,
                "frame_count": 1,
                "expected_length": 8,
                "is_broadcast": True,
                "transport_protocol_used": None,
            },
            "expected_app_fields": {
                "pgn": 0xFEF1,
                "source_address": 0x00,
                "destination_address": 0xFF,
                "priority": 6,
                "is_broadcast": True,
            },
            "expected_transport_fields": {
                "frame_count": 1,
                "expected_length": 8,
            },
            "default_transport_values": {},  # J1939 doesn't use ISO-TP transport
        },
        id="j1939_message_adapter",
    ),
]


class TestConsolidatedMessageAdapters:
    """Consolidated tests for message adapters using comprehensive parameterization."""

    @pytest.mark.parametrize("test_case", ADAPTER_TEST_CASES)
    def test_consolidated_dict_with_transport(
        self, test_case: Dict[str, Any], sample_isotp_frame: ISOTPFrame
    ):
        """Test adapter creates consolidated dictionary with transport metadata."""
        # Create message instance
        message = test_case["message_class"](**test_case["message_kwargs"])

        # Create adapter with transport frame (skip for J1939 which doesn't use ISO-TP)
        if test_case["protocol"] == "J1939":
            adapter = test_case["adapter_class"](message)
        else:
            adapter = test_case["adapter_class"](message, sample_isotp_frame)

        result = adapter.to_consolidated_dict()

        # Verify application-specific fields
        for field_name, expected_value in test_case["expected_app_fields"].items():
            assert (
                result[field_name] == expected_value
            ), f"{test_case['protocol']}: Field {field_name} mismatch"

        # Verify transport metadata fields (for ISO-TP based protocols)
        if test_case["protocol"] != "J1939":
            for field_name, expected_value in test_case["expected_transport_fields"].items():
                assert (
                    result[field_name] == expected_value
                ), f"{test_case['protocol']}: Transport field {field_name} mismatch"

    @pytest.mark.parametrize("test_case", ADAPTER_TEST_CASES)
    def test_consolidated_dict_without_transport(self, test_case: Dict[str, Any]):
        """Test adapter creates dictionary with default transport values."""
        # Create message instance
        message = test_case["message_class"](**test_case["message_kwargs"])

        # Create adapter without transport frame
        adapter = test_case["adapter_class"](message)
        result = adapter.to_consolidated_dict()

        # Verify application-specific fields
        for field_name, expected_value in test_case["expected_app_fields"].items():
            assert (
                result[field_name] == expected_value
            ), f"{test_case['protocol']}: Field {field_name} mismatch"

        # Verify default transport values (for ISO-TP based protocols)
        if test_case["protocol"] != "J1939":
            for field_name, expected_value in test_case["default_transport_values"].items():
                assert (
                    result[field_name] == expected_value
                ), f"{test_case['protocol']}: Default transport field {field_name} mismatch"

    @pytest.mark.parametrize("test_case", ADAPTER_TEST_CASES)
    def test_spark_schema_generation(self, test_case: Dict[str, Any]):
        """Test Spark schema generation for different adapter types."""
        adapter_class = test_case["adapter_class"]
        schema = adapter_class.get_consolidated_spark_schema()

        # Verify schema structure
        assert hasattr(
            schema, "fields"
        ), f"Schema missing fields attribute for {test_case['protocol']}"
        assert len(schema.fields) > 0, f"Empty schema for {test_case['protocol']}"

        # Verify key application fields are present in schema
        schema_field_names = [field.name for field in schema.fields]
        for field_name in test_case["expected_app_fields"].keys():
            assert (
                field_name in schema_field_names
            ), f"{test_case['protocol']}: Schema missing {field_name} field"

    @pytest.mark.parametrize("test_case", ADAPTER_TEST_CASES)
    def test_adapter_type_safety(self, test_case: Dict[str, Any]):
        """Test adapter maintains type safety in conversions."""
        # Create message instance
        message = test_case["message_class"](**test_case["message_kwargs"])
        adapter = test_case["adapter_class"](message)
        result = adapter.to_consolidated_dict()

        # Verify critical fields have expected types
        for field_name, expected_value in test_case["expected_app_fields"].items():
            actual_value = result[field_name]
            assert type(actual_value) == type(expected_value), (
                f"{test_case['protocol']}: Type mismatch for {field_name}: "
                f"expected {type(expected_value)}, got {type(actual_value)}"
            )

    @pytest.mark.parametrize(
        "protocol_type,has_transport_metadata", [("UDS", True), ("J1939", False)]
    )
    def test_transport_metadata_presence(self, protocol_type: str, has_transport_metadata: bool):
        """Test transport metadata presence based on protocol type."""
        # Find test case for this protocol
        test_case = next(
            tc
            for tc in [case.values[0] for case in ADAPTER_TEST_CASES]
            if tc["protocol"] == protocol_type
        )

        message = test_case["message_class"](**test_case["message_kwargs"])
        adapter = test_case["adapter_class"](message)
        result = adapter.to_consolidated_dict()

        # Check for transport metadata fields that are actually present in schemas
        # Note: target_address, extended_addressing, and physical_address were removed
        # as they were not part of the actual schema
        isotp_fields = ["source_address", "frame_count", "expected_length"]

        for field in isotp_fields:
            if has_transport_metadata:
                assert field in result, f"{protocol_type} should have {field}"
            else:
                # J1939 might have its own addressing fields, so check contextually
                if protocol_type == "J1939" and field in ["source_address"]:
                    assert field in result, f"{protocol_type} should have its own {field}"
                elif protocol_type == "J1939":
                    # Don't expect ISO-TP specific fields in J1939
                    pass


# Additional edge case tests for adapter error handling
class TestAdapterEdgeCases:
    """Test adapter edge cases and error handling."""

    def test_isotp_adapter_standalone(self, sample_isotp_frame):
        """Test ISO-TP frame adapter works independently."""
        adapter = ISOTPFrameSparkAdapter(sample_isotp_frame)
        result = adapter.to_spark_dict()

        # Should contain ISO-TP specific fields
        expected_fields = [
            "source_address",
            "frame_count",
            "expected_length",
            "payload",
            "payload_length",
        ]
        for field in expected_fields:
            assert field in result, f"ISO-TP adapter missing {field}"

        assert result["source_address"] == 0x7E0
        assert result["frame_count"] == 1
        assert result["expected_length"] == 3
