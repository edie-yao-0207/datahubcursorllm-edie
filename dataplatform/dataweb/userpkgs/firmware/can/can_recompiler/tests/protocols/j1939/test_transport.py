"""
J1939 Transport Layer Tests

Comprehensive tests for J1939 transport layer processing including:
- Single frame processing
- Multi-frame TP.CM/TP.DT protocols (RTS/CTS and BAM)
- TP.ACK processing
- Error handling and session management
"""

import pytest
from typing import List, Optional
from dataclasses import dataclass

from ....core.frame_types import CANTransportFrame
from ....protocols.j1939.transport import J1939Transport, J1939Session
from ....protocols.j1939.frames import J1939TransportMessage
from ....protocols.j1939.enums import J1939TransportProtocolType
from ....protocols.j1939.constants import (
    J1939_TP_CM_PGN,
    J1939_TP_DT_PGN,
    J1939_TP_ACK_PGN,
    J1939_TP_CM_RTS,
    J1939_TP_CM_CTS,
    J1939_TP_CM_ENDOFMSGACK,
    J1939_TP_CM_BAM,
    J1939_TP_CM_ABORT,
    J1939_TP_ACK_ACK,
    J1939_TP_ACK_NACK,
    J1939_MAX_SINGLE_FRAME_SIZE,
    J1939_SESSION_TIMEOUT_US,
)


@pytest.fixture
def j1939_transport():
    """Create a fresh J1939Transport instance."""
    return J1939Transport()


@pytest.fixture
def sample_single_frame():
    """Sample single-frame J1939 message."""
    return CANTransportFrame(
        arbitration_id=0x18FEF100,  # PGN 0xFEF1, SA=0x00
        payload=b"\x01\x02\x03\x04\x05\x06\x07\x08",
        start_timestamp_unix_us=1000000,
        is_extended_frame=True,
    )


@pytest.fixture
def sample_rts_frame():
    """Sample RTS (Request To Send) frame."""
    return CANTransportFrame(
        arbitration_id=0x18EC0000,  # TP.CM PGN, SA=0x00
        payload=b"\x10\x0D\x00\x02\xFF\xF1\xFE\x00",  # RTS, 13 bytes, 2 packets, PGN 0xFEF1
        start_timestamp_unix_us=1000000,
        is_extended_frame=True,
    )


@pytest.fixture
def sample_bam_frame():
    """Sample BAM (Broadcast Announce Message) frame."""
    return CANTransportFrame(
        arbitration_id=0x18ECFF00,  # TP.CM PGN, DA=0xFF (broadcast), SA=0x00
        payload=b"\x20\x0D\x00\x02\xFF\xF1\xFE\x00",  # BAM, 13 bytes, 2 packets, PGN 0xFEF1
        start_timestamp_unix_us=1000000,
        is_extended_frame=True,
    )


def sample_tp_dt_frame(
    sequence: int, data: bytes, sa: int = 0x00, da: int = 0x01
) -> CANTransportFrame:
    """Create a TP.DT (Transport Protocol Data Transfer) frame."""
    return CANTransportFrame(
        arbitration_id=0x18EB0000 | (da << 8) | sa,  # TP.DT PGN
        payload=bytes([sequence]) + data + b"\x00" * (7 - len(data)),  # Pad to 7 data bytes
        start_timestamp_unix_us=1000000 + sequence * 1000,  # Increment time
        is_extended_frame=True,
    )


class TestJ1939Transport:
    """Test J1939 transport layer functionality."""

    def test_transport_initialization(self, j1939_transport):
        """Test transport layer initialization."""
        assert j1939_transport.sessions == {}
        assert j1939_transport._frame_count == 0
        assert hasattr(j1939_transport, "_cleanup_interval")

    def test_single_frame_processing(self, j1939_transport, sample_single_frame):
        """Test processing of single-frame J1939 messages."""
        result = j1939_transport.process(sample_single_frame)

        assert result is not None
        assert isinstance(result, J1939TransportMessage)
        assert result.pgn == 0xFEF1
        assert result.source_address == 0x00
        assert result.destination_address == 0xFF  # Broadcast
        assert result.priority == 6
        assert result.payload == sample_single_frame.payload
        assert result.frame_count == 1
        assert result.expected_length == len(sample_single_frame.payload)
        assert result.is_broadcast is True
        assert result.transport_protocol_used == J1939TransportProtocolType.NONE

    def test_large_single_frame_accepted(self, j1939_transport):
        """Test that J1939 accepts any valid CAN frame as single frame."""
        # J1939 doesn't have separate single frame size limits beyond CAN (8 bytes)
        max_frame = CANTransportFrame(
            arbitration_id=0x18FEF100,
            payload=b"\x00" * 8,  # Max CAN frame size
            start_timestamp_unix_us=1000000,
            is_extended_frame=True,
        )

        result = j1939_transport.process(max_frame)
        assert result is not None
        assert len(result.payload) == 8

    def test_rts_session_start(self, j1939_transport, sample_rts_frame):
        """Test RTS (Request To Send) session initiation."""
        result = j1939_transport.process(sample_rts_frame)

        # RTS should not produce immediate result
        assert result is None

        # Should create a session
        assert len(j1939_transport.sessions) == 1
        session_key = list(j1939_transport.sessions.keys())[0]
        session = j1939_transport.sessions[session_key]

        assert session.size == 13  # From RTS payload
        assert session.target_pgn == 0xFEF1
        assert session.source_sa == 0x00
        assert session.dest_sa == 0x00  # Extracted from arbitration_id
        assert session.transport_type == J1939TransportProtocolType.RTS_CTS

    def test_bam_session_start(self, j1939_transport, sample_bam_frame):
        """Test BAM (Broadcast Announce Message) session initiation."""
        result = j1939_transport.process(sample_bam_frame)

        # BAM should not produce immediate result
        assert result is None

        # Should create a session
        assert len(j1939_transport.sessions) == 1
        session_key = list(j1939_transport.sessions.keys())[0]
        session = j1939_transport.sessions[session_key]

        assert session.size == 13  # From BAM payload
        assert session.target_pgn == 0xFEF1
        assert session.source_sa == 0x00
        assert session.dest_sa == 0xFF  # Broadcast
        assert session.transport_type == J1939TransportProtocolType.BAM
        assert session.is_broadcast is True

    def test_tp_dt_processing(self, j1939_transport, sample_rts_frame):
        """Test TP.DT (Transport Protocol Data Transfer) processing."""
        # First start a session with RTS
        result_rts = j1939_transport.process(sample_rts_frame)
        assert result_rts is None  # RTS creates session but returns None
        assert len(j1939_transport.sessions) == 1

        # Send TP.DT frames matching the session addressing
        dt_frame1 = sample_tp_dt_frame(1, b"\x01\x02\x03\x04\x05\x06\x07", sa=0x00, da=0x00)
        dt_frame2 = sample_tp_dt_frame(2, b"\x08\x09\x0A\x0B\x0C\x0D", sa=0x00, da=0x00)

        result1 = j1939_transport.process(dt_frame1)
        assert result1 is None  # Not complete yet

        result2 = j1939_transport.process(dt_frame2)
        assert result2 is not None  # Should be complete

        assert isinstance(result2, J1939TransportMessage)
        assert result2.pgn == 0xFEF1
        assert result2.payload == b"\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0A\x0B\x0C\x0D"
        assert result2.frame_count == 3  # RTS + 2 TP.DT frames
        assert result2.transport_protocol_used == J1939TransportProtocolType.RTS_CTS

    def test_bam_data_processing(self, j1939_transport, sample_bam_frame):
        """Test BAM data frame processing."""
        # Start BAM session
        j1939_transport.process(sample_bam_frame)

        # Send TP.DT frames for BAM (destination address should be broadcast)
        dt_frame1 = sample_tp_dt_frame(1, b"\x01\x02\x03\x04\x05\x06\x07", sa=0x00, da=0xFF)
        dt_frame2 = sample_tp_dt_frame(2, b"\x08\x09\x0A\x0B\x0C\x0D", sa=0x00, da=0xFF)

        result1 = j1939_transport.process(dt_frame1)
        assert result1 is None

        result2 = j1939_transport.process(dt_frame2)
        assert result2 is not None
        assert result2.is_broadcast is True
        assert result2.transport_protocol_used == J1939TransportProtocolType.BAM
        assert result2.frame_count == 3  # BAM + 2 TP.DT frames

    def test_out_of_sequence_tp_dt(self, j1939_transport, sample_rts_frame):
        """Test handling of out-of-sequence TP.DT frames."""
        # Start session
        j1939_transport.process(sample_rts_frame)

        # Send sequence 2 before sequence 1 (out of order)
        dt_frame2 = sample_tp_dt_frame(2, b"\x08\x09\x0A\x0B\x0C\x0D")
        result = j1939_transport.process(dt_frame2)

        # Should reject out-of-sequence frame
        assert result is None

        # Session should still exist but not progressed
        session = list(j1939_transport.sessions.values())[0]
        assert session.next_seq == 1  # Still expecting sequence 1

    def test_tp_ack_processing(self, j1939_transport):
        """Test TP.ACK frame processing."""
        tp_ack_frame = CANTransportFrame(
            arbitration_id=0x18EA0001,  # TP.ACK PGN (0xEA00), SA=0x01
            payload=b"\x00\xFF\xFF\xFF\xFF\xF1\xFE\x00",  # ACK, PGN 0xFEF1
            start_timestamp_unix_us=1000000,
            is_extended_frame=True,
        )

        # TP.ACK processing should return None (just logs)
        result = j1939_transport.process(tp_ack_frame)
        assert result is None

    def test_unsupported_tp_cm_control_byte(self, j1939_transport):
        """Test handling of unsupported TP.CM control bytes."""
        unsupported_frame = CANTransportFrame(
            arbitration_id=0x18EC0000,
            payload=b"\xFF\x14\x00\x02\xFF\xF1\xFE\x00",  # Unsupported control byte 0xFF
            start_timestamp_unix_us=1000000,
            is_extended_frame=True,
        )

        result = j1939_transport.process(unsupported_frame)
        assert result is None

    def test_non_transport_frame_processing(self, j1939_transport):
        """Test that non-transport frames are processed as single frames."""
        # Non-transport PGN (not TP.CM, TP.DT, or TP.ACK)
        regular_frame = CANTransportFrame(
            arbitration_id=0x18F00456,  # Regular J1939 frame (not transport)
            payload=b"\x01\x02\x03\x04",
            start_timestamp_unix_us=1000000,
            is_extended_frame=True,
        )

        result = j1939_transport.process(regular_frame)
        assert result is not None  # Should process as single frame
        assert isinstance(result, J1939TransportMessage)

    def test_session_cleanup(self, j1939_transport, sample_rts_frame):
        """Test session timeout and cleanup."""
        # Create a session
        j1939_transport.process(sample_rts_frame)
        assert len(j1939_transport.sessions) == 1

        # Cleanup with timestamp far in future
        future_time = sample_rts_frame.start_timestamp_unix_us + J1939_SESSION_TIMEOUT_US + 1000
        j1939_transport.cleanup(future_time)

        # Session should be cleaned up
        assert len(j1939_transport.sessions) == 0

    def test_session_not_cleaned_if_recent(self, j1939_transport, sample_rts_frame):
        """Test that recent sessions are not cleaned up."""
        # Create a session
        j1939_transport.process(sample_rts_frame)
        assert len(j1939_transport.sessions) == 1

        # Cleanup with recent timestamp
        recent_time = sample_rts_frame.start_timestamp_unix_us + 1000
        j1939_transport.cleanup(recent_time)

        # Session should still exist
        assert len(j1939_transport.sessions) == 1

    def test_tp_dt_without_session(self, j1939_transport):
        """Test TP.DT frame without active session."""
        dt_frame = sample_tp_dt_frame(1, b"\x01\x02\x03\x04")
        result = j1939_transport.process(dt_frame)

        # Should be ignored without active session
        assert result is None

    def test_malformed_tp_cm_frame(self, j1939_transport):
        """Test malformed TP.CM frame handling."""
        short_frame = CANTransportFrame(
            arbitration_id=0x18EC0000,
            payload=b"\x10\x14",  # Too short for TP.CM
            start_timestamp_unix_us=1000000,
            is_extended_frame=True,
        )

        result = j1939_transport.process(short_frame)
        assert result is None

    def test_malformed_tp_dt_frame(self, j1939_transport, sample_rts_frame):
        """Test malformed TP.DT frame handling."""
        # Start session
        j1939_transport.process(sample_rts_frame)

        # Send malformed TP.DT (no sequence number)
        malformed_dt = CANTransportFrame(
            arbitration_id=0x18EB0000,
            payload=b"",  # Empty payload
            start_timestamp_unix_us=1000000,
            is_extended_frame=True,
        )

        result = j1939_transport.process(malformed_dt)
        assert result is None

    def test_j1939_session_dataclass(self):
        """Test J1939Session dataclass functionality."""
        session = J1939Session(
            data=bytearray(b"\x01\x02"),
            size=100,
            next_seq=1,
            target_pgn=0xFEF1,
            source_sa=0x00,
            dest_sa=0x01,
            start_time=1000000,
        )

        assert len(session.data) == 2
        assert session.size == 100
        assert session.next_seq == 1
        assert session.target_pgn == 0xFEF1
        assert session.priority == 6  # Default value
        assert session.is_broadcast is False  # Default value
        assert session.transport_type == J1939TransportProtocolType.RTS_CTS  # Default
        assert isinstance(session.frames, list)
        assert len(session.frames) == 0  # Default empty list

    def test_multiple_concurrent_sessions(self, j1939_transport):
        """Test handling of multiple concurrent transport sessions."""
        # Create RTS for different source addresses
        rts1 = CANTransportFrame(
            arbitration_id=0x18EC0001,  # SA=0x01
            payload=b"\x10\x0A\x00\x01\xFF\xF1\xFE\x00",
            start_timestamp_unix_us=1000000,
            is_extended_frame=True,
        )

        rts2 = CANTransportFrame(
            arbitration_id=0x18EC0002,  # SA=0x02
            payload=b"\x10\x0B\x00\x01\xFF\xF2\xFE\x00",  # Different PGN
            start_timestamp_unix_us=1000000,
            is_extended_frame=True,
        )

        # Process both RTS frames
        j1939_transport.process(rts1)
        j1939_transport.process(rts2)

        # Should have two sessions
        assert len(j1939_transport.sessions) == 2

        # Sessions should have different keys
        session_keys = list(j1939_transport.sessions.keys())
        assert len(set(session_keys)) == 2  # All unique

    @pytest.mark.parametrize(
        "control_byte,expected_handled",
        [
            (J1939_TP_CM_RTS, True),
            (J1939_TP_CM_CTS, False),  # CTS is logged but not fully handled
            (J1939_TP_CM_ENDOFMSGACK, False),  # ENDOFMSGACK is logged but not fully handled
            (J1939_TP_CM_BAM, True),
            (J1939_TP_CM_ABORT, False),  # ABORT is logged but not fully handled
            (0xFF, False),  # Unsupported control byte
        ],
    )
    def test_tp_cm_control_byte_handling(self, j1939_transport, control_byte, expected_handled):
        """Test different TP.CM control byte handling."""
        tp_cm_frame = CANTransportFrame(
            arbitration_id=0x18EC0000,
            payload=bytes([control_byte]) + b"\x14\x00\x02\xFF\xF1\xFE\x00",
            start_timestamp_unix_us=1000000,
            is_extended_frame=True,
        )

        result = j1939_transport.process(tp_cm_frame)

        if expected_handled:
            # Should create session for RTS/BAM
            if control_byte in [J1939_TP_CM_RTS, J1939_TP_CM_BAM]:
                assert len(j1939_transport.sessions) >= 1
        else:
            # Other control bytes don't create sessions currently
            pass

        # All TP.CM frames return None (no immediate message)
        assert result is None
