"""
Tests for self-parsing UDSMessage.

This module contains focused tests for the self-parsing UDSMessage functionality
after merging decoder logic into the message class.
"""

import pytest
from typing import Optional

from ....protocols.uds.frames import UDSMessage
from ....protocols.uds.enums import UDSDataIdentifierType
from ....protocols.isotp.frames import ISOTPFrame


class TestUDSSelfParsing:
    """Test UDSMessage self-parsing functionality."""

    @pytest.mark.parametrize(
        "payload,expected_sid,expected_negative,expected_did,expected_type,expected_payload,description",
        [
            # Basic service requests (SERVICE_ONLY)
            (
                b"\x10\x01",
                0x10,
                False,
                16,
                UDSDataIdentifierType.SERVICE_ONLY,
                b"\x01",
                "DiagnosticSessionControl",
            ),
            (
                b"\x11\x01",
                0x11,
                False,
                17,
                UDSDataIdentifierType.SERVICE_ONLY,
                b"\x01",
                "ECU Reset",
            ),
            (
                b"\x27\x01",
                0x27,
                False,
                39,
                UDSDataIdentifierType.SERVICE_ONLY,
                b"\x01",
                "SecurityAccess",
            ),
            # Read Data by Identifier (with DIDs) - DID is just the 2-byte identifier, not embedded with service
            (
                b"\x22\x01\x02",
                0x22,
                False,
                0x0102,  # Just the DID, not (SID << 16) | DID
                UDSDataIdentifierType.DID,
                b"",
                "ReadDataByIdentifier 2-byte",
            ),
            (
                b"\x22\x01\x02\x03",
                0x22,
                False,
                0x0102,  # Just the DID, not (SID << 16) | DID
                UDSDataIdentifierType.DID,
                b"\x03",
                "ReadDataByIdentifier with data",
            ),
            (
                b"\x22\x01\x02\xAA\xBB\xCC",
                0x22,
                False,
                0x0102,  # Just the DID, not (SID << 16) | DID
                UDSDataIdentifierType.DID,
                b"\xAA\xBB\xCC",
                "ReadDataByIdentifier with more data",
            ),
            # Positive responses - service_id now returns base request service ID, data_identifier is just the DID
            (
                b"\x50\x01",
                0x10,  # Base request service ID (0x50 - 0x40)
                False,
                16,
                UDSDataIdentifierType.SERVICE_ONLY,
                b"\x01",
                "DiagnosticSessionControl response",
            ),
            (
                b"\x62\x01\x02\xDE\xAD\xBE\xEF",
                0x22,  # Base request service ID (0x62 - 0x40)
                False,
                0x0102,  # Just the DID, not embedded with service
                UDSDataIdentifierType.DID,
                b"\xDE\xAD\xBE\xEF",
                "ReadDataByIdentifier response",
            ),
            # Negative responses - DID encoded as (0x7F << 8) | failed_sid, header length = 3
            (
                b"\x7F\x22\x11",
                0x7F,
                True,
                0x7F22,
                UDSDataIdentifierType.NRC,
                b"",
                "Negative response",
            ),
            # Edge cases
            (
                b"\x3E\x00",
                0x3E,
                False,
                62,
                UDSDataIdentifierType.SERVICE_ONLY,
                b"\x00",
                "TesterPresent",
            ),
        ],
    )
    def test_uds_self_parsing_comprehensive(
        self,
        payload: bytes,
        expected_sid: int,
        expected_negative: bool,
        expected_did: Optional[int],
        expected_type: UDSDataIdentifierType,
        expected_payload: bytes,
        description: str,
    ):
        """Test comprehensive self-parsing of UDS messages."""
        transport_frame = ISOTPFrame(
            source_address=0x7E0, payload=payload, start_timestamp_unix_us=1000000
        )

        message = UDSMessage(transport_frame=transport_frame)

        # Test parsed fields
        assert message.service_id == expected_sid, f"SID mismatch for {description}"
        assert (
            message.is_negative_response == expected_negative
        ), f"Negative response flag mismatch for {description}"
        assert message.data_identifier == expected_did, f"DID mismatch for {description}"
        assert (
            message.data_identifier_type == expected_type
        ), f"Data ID type mismatch for {description}"

        # Test clean payload (no headers)
        assert message.payload == expected_payload, f"Clean payload mismatch for {description}"

        # Test full transport payload access (if needed for debugging)
        assert (
            message.transport_frame.payload == payload
        ), f"Transport payload mismatch for {description}"

        # Test is_response logic (based on raw payload, not converted service_id)
        raw_sid = payload[0] if payload else 0
        expected_is_response = raw_sid == 0x7F or (0x40 <= raw_sid < 0x7F)
        assert (
            message.is_response == expected_is_response
        ), f"Response flag mismatch for {description}"

    def test_uds_payload_vs_transport_payload(self):
        """Test the difference between clean payload and full transport payload."""
        # ReadDataByIdentifier response with headers and data
        transport_frame = ISOTPFrame(
            source_address=0x7E8,
            payload=b"\x62\x01\x02\xDE\xAD\xBE\xEF\x12\x34",  # SID, DID, and 6 bytes of data
            start_timestamp_unix_us=1000000,
        )

        message = UDSMessage(transport_frame=transport_frame)

        # Clean payload should have no headers (no SID, no DID)
        assert message.payload == b"\xDE\xAD\xBE\xEF\x12\x34"  # Just the service data

        # Full transport payload has all headers (accessed via transport_frame)
        assert (
            message.transport_frame.payload == b"\x62\x01\x02\xDE\xAD\xBE\xEF\x12\x34"
        )  # Everything

    def test_uds_empty_payload(self):
        """Test handling of empty or invalid payloads."""
        transport_frame = ISOTPFrame(
            source_address=0x7E0, payload=b"", start_timestamp_unix_us=1000000  # Empty payload
        )

        message = UDSMessage(transport_frame=transport_frame)

        assert message.service_id == 0  # Default for empty payload
        assert message.is_negative_response is False
        assert message.is_response is False
        assert message.data_identifier is None
        assert message.data_identifier_type == UDSDataIdentifierType.NONE
        assert message.payload == b""
        assert message.transport_frame.payload == b""

    def test_uds_cached_properties(self):
        """Test that expensive parsing is cached."""
        transport_frame = ISOTPFrame(
            source_address=0x7E0, payload=b"\x22\x01\x02\xAA\xBB", start_timestamp_unix_us=1000000
        )

        message = UDSMessage(transport_frame=transport_frame)

        # Access properties multiple times
        sid1 = message.service_id
        sid2 = message.service_id
        did1 = message.data_identifier
        did2 = message.data_identifier

        # Should return same values (cached)
        assert sid1 == sid2 == 0x22
        assert did1 == did2 == 0x0102  # Just the DID, not embedded with service

        # The caching is handled by @cached_property decorator
        # We can't easily test that it's actually cached, but we can test consistency

    def test_uds_negative_response_special_case(self):
        """Test negative response handling (SID = 0x7F)."""
        transport_frame = ISOTPFrame(
            source_address=0x7E8,
            payload=b"\x7F\x22\x13",  # Negative response to ReadDataByIdentifier, NRC 0x13
            start_timestamp_unix_us=1000000,
        )

        message = UDSMessage(transport_frame=transport_frame)

        assert message.service_id == 0x7F
        assert message.is_negative_response is True
        assert message.is_response is True  # Negative responses are still responses
        assert message.data_identifier == 0x7F22  # Encoded as (0x7F << 8) | failed_SID
        assert message.payload == b""  # Clean payload is empty (header length = 3)
