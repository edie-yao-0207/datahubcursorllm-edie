"""
J1939 Transport PGN Disambiguation Tests

Tests for correctly distinguishing between TP.ACK (transport acknowledgment) 
and Request PGN (application request) which both use PGN 0xEA00.
"""

import pytest
from ....protocols.j1939.transport import J1939Transport
from ....core.frame_types import CANTransportFrame
from ....protocols.j1939.constants import J1939_TP_ACK_ACK, J1939_TP_ACK_NACK


class TestJ1939TransportPGNDisambiguation:
    """Test J1939 transport layer PGN disambiguation."""

    def test_transport_ack_recognized(self):
        """Test that TP.ACK is correctly identified as transport protocol."""
        transport = J1939Transport()

        # Create a TP.ACK frame with proper structure
        tp_ack_frame = CANTransportFrame(
            arbitration_id=0x18EA0017,  # PGN 0xEA00 from SA=0x17 to DA=0x00
            payload=bytes(
                [
                    J1939_TP_ACK_ACK,  # Control byte: ACK
                    0x05,  # Number of packets that can be received (low byte)
                    0x00,  # Number of packets that can be received (high byte)
                    0x02,  # Next packet number to be sent
                    0xFF,  # Reserved byte 4
                    0xFF,  # Reserved byte 5
                    0xFF,  # Reserved byte 6
                    0xFF,  # Reserved byte 7
                ]
            ),
            start_timestamp_unix_us=1000000,
            is_extended_frame=True,
        )

        # Should be recognized as TP.ACK and processed by transport layer
        result = transport.process(tp_ack_frame)

        # TP.ACK processing returns None (it's internal transport state)
        assert result is None

    def test_transport_nack_recognized(self):
        """Test that TP.NACK is correctly identified as transport protocol."""
        transport = J1939Transport()

        # Create a TP.NACK frame
        tp_nack_frame = CANTransportFrame(
            arbitration_id=0x18EA0017,  # PGN 0xEA00 from SA=0x17 to DA=0x00
            payload=bytes(
                [
                    J1939_TP_ACK_NACK,  # Control byte: NACK
                    0x01,  # Connection abort reason
                    0x00,  # Reserved
                    0x00,  # Reserved
                    0xFF,  # Reserved byte 4
                    0xFF,  # Reserved byte 5
                    0xFF,  # Reserved byte 6
                    0xFF,  # Reserved byte 7
                ]
            ),
            start_timestamp_unix_us=1000000,
            is_extended_frame=True,
        )

        # Should be recognized as TP.NACK and processed by transport layer
        result = transport.process(tp_nack_frame)

        # TP.NACK processing returns None (it's internal transport state)
        assert result is None

    def test_request_pgn_passed_through(self):
        """Test that Request PGN is passed through as single-frame application message."""
        transport = J1939Transport()

        # Create a Request PGN frame (different payload structure)
        request_frame = CANTransportFrame(
            arbitration_id=0x18EA0017,  # PGN 0xEA00 from SA=0x17 to DA=0x00
            payload=b"\x04\xF0\x00",  # Requested PGN 0xF004 (LSB first)
            start_timestamp_unix_us=1000000,
            is_extended_frame=True,
        )

        # Should be passed through as single-frame application message
        result = transport.process(request_frame)

        # Should return J1939TransportMessage for application layer processing
        assert result is not None
        assert result.pgn == 0xEA00
        assert result.source_address == 0x17
        assert result.destination_address == 0x00
        assert result.payload == b"\x04\xF0\x00"
        assert result.frame_count == 1

    def test_short_payload_treated_as_request(self):
        """Test that short payloads on 0xEA00 are treated as Request PGN, not TP.ACK."""
        transport = J1939Transport()

        # Create frame with short payload (less than 8 bytes)
        short_frame = CANTransportFrame(
            arbitration_id=0x18EA0017,  # PGN 0xEA00 from SA=0x17 to DA=0x00
            payload=b"\x04\xF0\x00\x12\x34",  # 5 bytes - too short for TP.ACK
            start_timestamp_unix_us=1000000,
            is_extended_frame=True,
        )

        # Should be treated as application message, not TP.ACK
        result = transport.process(short_frame)

        # Should return J1939TransportMessage for application layer
        assert result is not None
        assert result.pgn == 0xEA00
        assert result.payload == b"\x04\xF0\x00\x12\x34"

    def test_invalid_control_byte_treated_as_request(self):
        """Test that invalid control bytes are treated as Request PGN, not TP.ACK."""
        transport = J1939Transport()

        # Create frame with invalid control byte for TP.ACK
        invalid_frame = CANTransportFrame(
            arbitration_id=0x18EA0017,  # PGN 0xEA00 from SA=0x17 to DA=0x00
            payload=bytes(
                [
                    0x99,  # Invalid control byte (not ACK or NACK)
                    0x05,
                    0x00,
                    0x02,  # Valid remaining TP.ACK structure
                    0xFF,
                    0xFF,
                    0xFF,
                    0xFF,
                ]
            ),
            start_timestamp_unix_us=1000000,
            is_extended_frame=True,
        )

        # Should be treated as application message, not TP.ACK
        result = transport.process(invalid_frame)

        # Should return J1939TransportMessage for application layer
        assert result is not None
        assert result.pgn == 0xEA00
        assert result.payload[0] == 0x99

    def test_tp_ack_with_variable_bytes_recognized(self):
        """Test that TP.ACK is recognized even with variable bytes 4-7."""
        transport = J1939Transport()

        # Create frame with valid control byte and variable bytes 4-7 (as per real TP.ACK format)
        tp_ack_frame = CANTransportFrame(
            arbitration_id=0x18EA0017,  # PGN 0xEA00 from SA=0x17 to DA=0x00
            payload=bytes(
                [
                    J1939_TP_ACK_ACK,  # Valid ACK control byte
                    0x05,
                    0x00,
                    0x02,  # Valid TP.ACK fields
                    0x12,
                    0x34,
                    0x56,
                    0x78,  # Variable bytes (PGN or other data)
                ]
            ),
            start_timestamp_unix_us=1000000,
            is_extended_frame=True,
        )

        # Should be recognized as TP.ACK despite variable bytes 4-7
        result = transport.process(tp_ack_frame)

        # TP.ACK processing returns None (transport layer handling)
        assert result is None
