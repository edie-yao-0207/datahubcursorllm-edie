"""
Integration tests for cross-package protocol processing.

This module tests end-to-end protocol processing across transport and application layers.
"""

import pytest
from typing import Dict, Any

from ...protocols.uds.frames import UDSMessage
from ...protocols.isotp.frames import ISOTPFrame
from ...protocols.j1939.frames import J1939TransportMessage
from ...infra.spark.frame_types import PartitionMetadata
from ...core.enums import TransportProtocolType, ApplicationProtocolType
from ...protocols.uds.enums import UDSDataIdentifierType
from ...protocols.j1939.enums import J1939DataIdentifierType


@pytest.fixture
def sample_metadata():
    """Sample partition metadata for testing."""
    return PartitionMetadata(
        trace_uuid="test-uuid-123",
        date="2024-01-15",
        org_id=42,
        device_id=1001,
        source_interface="can0",
        direction=1,
    )


@pytest.fixture
def sample_isotp_frame(sample_metadata):
    """Sample ISO-TP frame for testing."""
    return ISOTPFrame(
        source_address=0x7E0,
        payload=b"\x22\xF1\x90",
        start_timestamp_unix_us=1000000,
        frame_count=1,
        expected_length=3,
    )


@pytest.fixture
def sample_j1939_transport(sample_metadata):
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


class TestProtocolIntegration:
    """Test cross-package protocol integration."""

    def test_protocol_message_conversion_type_safety(self, sample_isotp_frame):
        """Test that protocol message conversion maintains type safety across packages."""
        # Create different message types with transport frames
        uds_transport = ISOTPFrame(
            source_address=0x7E0, payload=b"\x22\xF1\x90", start_timestamp_unix_us=1000000
        )

        uds_message = UDSMessage(transport_frame=uds_transport)

        # Test type-specific conversions using adapters
        from userpkgs.firmware.can.can_recompiler.infra.spark.adapters import UDSMessageSparkAdapter

        uds_adapter = UDSMessageSparkAdapter(uds_message)

        uds_dict = uds_adapter.to_consolidated_dict()

        # Verify consolidated structure and protocol-specific fields (with embedded transport metadata)
        assert "service_id" in uds_dict
        assert "data_identifier" in uds_dict
        assert "source_address" in uds_dict  # Transport metadata embedded

        # Verify protocol-specific data
        assert uds_dict["service_id"] == 0x22
        assert uds_dict["data_identifier"] == 0xF190  # Just the DID (Data Identifier)

    @pytest.mark.parametrize(
        "message_type,expected_app_fields",
        [
            ("UDS", ["service_id", "data_identifier", "is_response"]),
            ("J1939", ["pgn", "source_address"]),
        ],
    )
    def test_output_dictionary_field_completeness(
        self,
        message_type: str,
        expected_app_fields: list,
        sample_isotp_frame: ISOTPFrame,
        sample_j1939_transport: J1939TransportMessage,
    ):
        """Test that output dictionaries contain all expected protocol-specific fields."""
        if message_type == "UDS":
            # Create transport frame with proper payload for UDS ReadDataByIdentifier
            transport_frame = ISOTPFrame(
                source_address=sample_isotp_frame.source_address,
                payload=b"\x22\xF1\x90",  # SID + DID
                start_timestamp_unix_us=sample_isotp_frame.start_timestamp_unix_us,
            )
            message = UDSMessage(transport_frame=transport_frame)
        elif message_type == "J1939":
            # J1939 uses transport message directly (no separate application message)
            message = sample_j1939_transport

        # Convert using appropriate adapter
        from userpkgs.firmware.can.can_recompiler.infra.spark.adapters import (
            UDSMessageSparkAdapter,
            J1939ApplicationSparkAdapter,
        )

        if message_type == "UDS":
            adapter = UDSMessageSparkAdapter(message, sample_isotp_frame)
        elif message_type == "J1939":
            adapter = J1939ApplicationSparkAdapter(message)

        output_dict = adapter.to_consolidated_dict()

        # Verify application-specific structure exists (with embedded transport metadata)
        for field in expected_app_fields:
            assert (
                field in output_dict
            ), f"Missing {field} in {message_type} consolidated application data"
