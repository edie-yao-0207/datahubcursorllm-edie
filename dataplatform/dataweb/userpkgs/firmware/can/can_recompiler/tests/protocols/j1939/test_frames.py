"""
J1939 Application Frame Tests

Tests for J1939 application-layer processing using transport frames directly.
The application layer now uses J1939TransportMessage directly instead of 
a separate J1939ApplicationMessage.
"""

import pytest
from typing import Dict, Any

from ....protocols.j1939.frames import J1939TransportMessage
from ....protocols.j1939.enums import J1939DataIdentifierType


@pytest.fixture
def sample_j1939_transport():
    """Sample J1939 transport message for testing."""
    return J1939TransportMessage(
        pgn=0xFEF1,
        source_address=0x00,
        destination_address=0xFF,
        priority=6,
        payload=b"\x01\x02\x03\x04\x05\x06\x07\x08",
        start_timestamp_unix_us=1000000,
        frame_count=1,
        expected_length=8,
        is_broadcast=True,
        transport_protocol_used=None,
    )


J1939_TEST_CASES = [
    pytest.param(
        {
            "pgn": 0xFEF1,  # Normal J1939 PGN
            "expected_data_identifier_type": J1939DataIdentifierType.NORMAL_71,
            "payload": b"\x01\x02\x03\x04",
        },
        id="normal_pgn_test",
    ),
    pytest.param(
        {
            "pgn": 0xFECA,  # DM1 diagnostic PGN
            "expected_data_identifier_type": J1939DataIdentifierType.DM_MESSAGES,
            "payload": b"\xff\x00\x00\x00",
        },
        id="diagnostic_pgn_test",
    ),
    pytest.param(
        {
            "pgn": 0xF004,  # J1939-73 diagnostic range
            "expected_data_identifier_type": J1939DataIdentifierType.DIAGNOSTIC_73,
            "payload": b"\x00\x01\x02\x03",
        },
        id="j1939_73_diagnostic_test",
    ),
]


class TestJ1939TransportAsApplication:
    """Test J1939 transport messages used directly for application processing."""

    @pytest.mark.parametrize("test_case", J1939_TEST_CASES)
    def test_j1939_transport_adapter_conversion(self, test_case: Dict[str, Any]):
        """Test J1939 transport message conversion to application dictionary via adapter."""
        # Create J1939 transport message (this is now our application message too)
        transport_message = J1939TransportMessage(
            pgn=test_case["pgn"],
            source_address=0x00,
            destination_address=0xFF,
            priority=6,
            payload=test_case["payload"],
            start_timestamp_unix_us=1000000,
            frame_count=1,
            expected_length=len(test_case["payload"]),
            is_broadcast=True,
            transport_protocol_used=None,
        )

        # Convert to output dictionary using J1939 adapter
        from ....infra.spark.adapters import J1939ApplicationSparkAdapter

        adapter = J1939ApplicationSparkAdapter(transport_message)
        output_dict = adapter.to_consolidated_dict()

        # Verify consolidated output dictionary structure (J1939 app + transport metadata)
        # J1939 application fields (extracted from transport)
        assert "pgn" in output_dict
        assert "data_identifier_type" in output_dict

        # J1939 transport metadata fields
        assert "source_address" in output_dict
        assert "destination_address" in output_dict
        assert "priority" in output_dict
        assert "is_broadcast" in output_dict
        assert "frame_count" in output_dict
        assert "expected_length" in output_dict
        assert "transport_protocol_used" in output_dict

        # Verify J1939 application data (computed from transport)
        assert output_dict["pgn"] == test_case["pgn"]

        # Verify data identifier type is computed correctly
        from ....protocols.j1939.enums import classify_j1939_pgn

        expected_type = test_case["expected_data_identifier_type"]
        actual_type = classify_j1939_pgn(transport_message.pgn)
        assert actual_type == expected_type
        assert output_dict["data_identifier_type"] == expected_type.value

        # Verify transport metadata
        assert output_dict["source_address"] == transport_message.source_address
        assert output_dict["destination_address"] == transport_message.destination_address

    def test_j1939_transport_type_safety(self, sample_j1939_transport: J1939TransportMessage):
        """Test that J1939 transport message maintains type safety in adapter conversion."""
        from ....infra.spark.adapters import J1939ApplicationSparkAdapter

        # Test adapter initialization and conversion
        adapter = J1939ApplicationSparkAdapter(sample_j1939_transport)
        output_dict = adapter.to_consolidated_dict()

        # Test type safety of output dictionary
        assert isinstance(output_dict["pgn"], int)
        assert isinstance(output_dict["data_identifier_type"], int)

        # Transport metadata type safety
        assert isinstance(output_dict["source_address"], int)
        assert isinstance(output_dict["destination_address"], int)
        assert isinstance(output_dict["priority"], int)
        assert isinstance(output_dict["is_broadcast"], bool)
        assert isinstance(output_dict["frame_count"], int)
        assert isinstance(output_dict["expected_length"], int)
