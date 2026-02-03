"""
Protocol-Specific J1939 Transport Layer Processor

This module implements the BaseProtocolTransportProcessor for J1939,
handling J1939 frame reassembly and session management while returning
protocol-specific J1939TransportMessage objects.
"""

import logging
from typing import Dict, Optional
from dataclasses import dataclass, field

from ...core.frame_types import CANTransportFrame
from .frames import J1939TransportMessage
from .enums import J1939TransportProtocolType
from dataclasses import field
from .constants import (
    J1939_SESSION_TIMEOUT_US,
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
)
from ...core.constants import SESSION_CLEANUP_INTERVAL

logger = logging.getLogger("can_recompiler.protocols.j1939.transport")

J1939_MAX_MULTIFRAME_SIZE = 1785  # 255 packets * 7 bytes per packet


@dataclass
class J1939Session:
    """J1939 transport session state."""

    data: bytearray
    size: int
    next_seq: int
    target_pgn: int
    source_sa: int
    dest_sa: int
    start_time: int
    priority: int = 6
    is_broadcast: bool = False
    transport_type: J1939TransportProtocolType = field(default=J1939TransportProtocolType.RTS_CTS)
    frames: list[CANTransportFrame] = field(default_factory=list)


class J1939Transport:
    """
    J1939 transport layer handler.

    Handles J1939 frame reassembly and session management using
    TP.CM/TP.DT protocols, producing J1939TransportMessage objects.
    """

    def __init__(self):
        self.sessions: Dict[tuple, J1939Session] = {}
        self._frame_count = 0
        self._cleanup_interval = SESSION_CLEANUP_INTERVAL

    def process(self, frame: CANTransportFrame) -> Optional[J1939TransportMessage]:
        """
        Process a J1939 frame and return protocol-specific transport result.

        Args:
            frame: Raw CAN frame to process

        Returns:
            J1939TransportMessage if complete, None if incomplete
        """
        self._frame_count += 1

        # Periodic cleanup
        if self._frame_count % self._cleanup_interval == 0:
            self.cleanup(frame.start_timestamp_unix_us)

        # Extract J1939 addressing
        priority = (frame.arbitration_id >> 26) & 0x7
        pgn = (frame.arbitration_id >> 8) & 0x3FFFF
        source_sa = frame.arbitration_id & 0xFF

        # Determine destination SA for non-broadcast messages
        if pgn >= 0xF000:
            # PDU Format 2 (broadcast)
            dest_sa = 0xFF
            is_broadcast = True
        else:
            # PDU Format 1 (peer-to-peer)
            dest_sa = pgn & 0xFF
            pgn = pgn & 0xFF00
            is_broadcast = False

        logger.debug(
            f"Processing J1939 frame: PGN=0x{pgn:04X}, SA=0x{source_sa:02X}, DA=0x{dest_sa:02X}"
        )

        # Handle transport protocol messages
        if pgn == J1939_TP_CM_PGN:
            return self._process_tp_cm(frame, priority, source_sa, dest_sa)
        elif pgn == J1939_TP_DT_PGN:
            return self._process_tp_dt(frame, priority, source_sa, dest_sa)
        elif pgn == J1939_TP_ACK_PGN:
            # Distinguish between TP.ACK (transport acknowledgment) and Request PGN (application request)
            if self._is_transport_ack(frame):
                return self._process_tp_ack(frame, priority, source_sa, dest_sa)
            else:
                # This is a Request PGN or other application message using 0xEA00
                return self._process_single_frame(
                    frame, priority, pgn, source_sa, dest_sa, is_broadcast
                )
        else:
            # Single frame message
            return self._process_single_frame(
                frame, priority, pgn, source_sa, dest_sa, is_broadcast
            )

    def _process_tp_cm(
        self, frame: CANTransportFrame, priority: int, source_sa: int, dest_sa: int
    ) -> Optional[J1939TransportMessage]:
        """Process J1939 TP.CM (Connection Management) message."""
        if len(frame.payload) < 8:
            logger.warning("J1939 TP.CM message too short")
            return None

        control_byte = frame.payload[0]

        if control_byte == J1939_TP_CM_RTS:
            return self._handle_rts(frame, priority, source_sa, dest_sa)
        elif control_byte == J1939_TP_CM_BAM:
            return self._handle_bam(frame, priority, source_sa, dest_sa)
        elif control_byte == J1939_TP_CM_CTS:
            return self._handle_cts(frame, priority, source_sa, dest_sa)
        elif control_byte == J1939_TP_CM_ENDOFMSGACK:
            return self._handle_endofmsgack(frame, priority, source_sa, dest_sa)
        elif control_byte == J1939_TP_CM_ABORT:
            return self._handle_abort(frame, priority, source_sa, dest_sa)
        else:
            logger.warning(f"Unknown J1939 TP.CM control byte: 0x{control_byte:02X}")
            return None

    def _handle_rts(
        self, frame: CANTransportFrame, priority: int, source_sa: int, dest_sa: int
    ) -> Optional[J1939TransportMessage]:
        """Handle J1939 Request To Send (RTS) message."""
        if len(frame.payload) < 8:
            return None

        size = (frame.payload[2] << 8) | frame.payload[1]
        packets = frame.payload[3]
        target_pgn = (frame.payload[7] << 16) | (frame.payload[6] << 8) | frame.payload[5]

        if size > J1939_MAX_MULTIFRAME_SIZE:
            logger.warning(f"J1939 RTS message size too large: {size}")
            return None

        # Create session
        session_key = (target_pgn, source_sa, dest_sa)
        self.sessions[session_key] = J1939Session(
            data=bytearray(),
            size=size,
            next_seq=1,
            target_pgn=target_pgn,
            source_sa=source_sa,
            dest_sa=dest_sa,
            start_time=frame.start_timestamp_unix_us,
            priority=priority,
            is_broadcast=False,
            transport_type=J1939TransportProtocolType.RTS_CTS,
            frames=[frame],
        )

        logger.debug(f"Started J1939 RTS session: PGN=0x{target_pgn:04X}, size={size}")
        return None

    def _handle_bam(
        self, frame: CANTransportFrame, priority: int, source_sa: int, dest_sa: int
    ) -> Optional[J1939TransportMessage]:
        """Handle J1939 Broadcast Announce Message (BAM)."""
        if len(frame.payload) < 8:
            return None

        size = (frame.payload[2] << 8) | frame.payload[1]
        packets = frame.payload[3]
        target_pgn = (frame.payload[7] << 16) | (frame.payload[6] << 8) | frame.payload[5]

        if size > J1939_MAX_MULTIFRAME_SIZE:
            logger.warning(f"J1939 BAM message size too large: {size}")
            return None

        # Create session
        session_key = (target_pgn, source_sa, 0xFF)  # BAM always broadcasts
        self.sessions[session_key] = J1939Session(
            data=bytearray(),
            size=size,
            next_seq=1,
            target_pgn=target_pgn,
            source_sa=source_sa,
            dest_sa=0xFF,
            start_time=frame.start_timestamp_unix_us,
            priority=priority,
            is_broadcast=True,
            transport_type=J1939TransportProtocolType.BAM,
            frames=[frame],
        )

        logger.debug(f"Started J1939 BAM session: PGN=0x{target_pgn:04X}, size={size}")
        return None

    def _handle_cts(
        self, frame: CANTransportFrame, priority: int, source_sa: int, dest_sa: int
    ) -> Optional[J1939TransportMessage]:
        """Handle J1939 Clear To Send (CTS) message."""
        # CTS doesn't complete a session, just flow control
        logger.debug(f"Received J1939 CTS from SA=0x{source_sa:02X}")
        return None

    def _handle_endofmsgack(
        self, frame: CANTransportFrame, priority: int, source_sa: int, dest_sa: int
    ) -> Optional[J1939TransportMessage]:
        """Handle J1939 End of Message Acknowledgment."""
        # End of message ACK doesn't complete a session from our perspective
        logger.debug(f"Received J1939 EndOfMsgACK from SA=0x{source_sa:02X}")
        return None

    def _handle_abort(
        self, frame: CANTransportFrame, priority: int, source_sa: int, dest_sa: int
    ) -> Optional[J1939TransportMessage]:
        """Handle J1939 Connection Abort message."""
        if len(frame.payload) < 8:
            return None

        target_pgn = (frame.payload[7] << 16) | (frame.payload[6] << 8) | frame.payload[5]

        # Remove any existing session
        session_key = (target_pgn, dest_sa, source_sa)  # Note: swapped SA/DA for abort
        if session_key in self.sessions:
            logger.debug(f"Aborting J1939 session: PGN=0x{target_pgn:04X}")
            del self.sessions[session_key]

        return None

    def _process_tp_dt(
        self, frame: CANTransportFrame, priority: int, source_sa: int, dest_sa: int
    ) -> Optional[J1939TransportMessage]:
        """Process J1939 TP.DT (Data Transfer) message."""
        if len(frame.payload) < 2:
            logger.warning("J1939 TP.DT message too short")
            return None

        seq_num = frame.payload[0]
        data = frame.payload[1:]

        # Find matching session
        matching_session = None
        session_key = None

        for key, session in self.sessions.items():
            if session.source_sa == source_sa and (
                session.dest_sa == dest_sa or session.is_broadcast
            ):
                matching_session = session
                session_key = key
                break

        if not matching_session:
            logger.warning(f"No J1939 session found for TP.DT from SA=0x{source_sa:02X}")
            return None

        # Validate sequence number
        if seq_num != matching_session.next_seq:
            logger.warning(
                f"J1939 TP.DT sequence mismatch: expected {matching_session.next_seq}, got {seq_num}"
            )
            return None

        # Add data to session
        matching_session.data.extend(data)
        matching_session.next_seq += 1
        matching_session.frames.append(frame)

        # Check if session is complete
        if len(matching_session.data) >= matching_session.size:
            return self._complete_session(
                session_key, matching_session, frame.start_timestamp_unix_us
            )

        return None

    def _is_transport_ack(self, frame: CANTransportFrame) -> bool:
        """
        Distinguish between TP.ACK (transport acknowledgment) and Request PGN.

        Both use PGN 0xEA00, but have different payload structures:
        - TP.ACK: Transport protocol acknowledgment with specific byte structure
        - Request PGN: Application request with 3-byte requested PGN in payload

        Args:
            frame: The frame to examine

        Returns:
            True if this is a transport protocol acknowledgment, False if Request PGN
        """
        if len(frame.payload) < 8:
            # TP.ACK should have 8 bytes, Request PGN typically has 3-8 bytes
            # If less than 8 bytes, more likely to be Request PGN
            return False

        # TP.ACK payload structure (J1939-21):
        # Byte 0: Control byte (values like J1939_TP_ACK_ACK, J1939_TP_ACK_NACK)
        # Bytes 1-2: Number of packets that can be received (ACK) or Connection Abort reason (NACK)
        # Byte 3: Next packet number to be sent (ACK) or Reserved (NACK)
        # Bytes 4-7: Varies by control byte type

        control_byte = frame.payload[0]

        # Check if control byte matches known TP.ACK values
        if control_byte in [J1939_TP_ACK_ACK, J1939_TP_ACK_NACK]:
            return True

        # If control byte doesn't match, likely Request PGN or other application message
        return False

    def _process_tp_ack(
        self, frame: CANTransportFrame, priority: int, source_sa: int, dest_sa: int
    ) -> Optional[J1939TransportMessage]:
        """Process J1939 TP.ACK message."""
        if len(frame.payload) < 8:
            return None

        control_byte = frame.payload[0]
        target_pgn = (frame.payload[7] << 16) | (frame.payload[6] << 8) | frame.payload[5]

        if control_byte == J1939_TP_ACK_ACK:
            logger.debug(f"Received J1939 ACK for PGN=0x{target_pgn:04X}")
        elif control_byte == J1939_TP_ACK_NACK:
            logger.debug(f"Received J1939 NACK for PGN=0x{target_pgn:04X}")

        # ACK messages don't complete sessions from our perspective
        return None

    def _process_single_frame(
        self,
        frame: CANTransportFrame,
        priority: int,
        pgn: int,
        source_sa: int,
        dest_sa: int,
        is_broadcast: bool,
    ) -> Optional[J1939TransportMessage]:
        """Process a single-frame J1939 message."""
        if len(frame.payload) > J1939_MAX_SINGLE_FRAME_SIZE:
            logger.warning(f"J1939 single frame too large: {len(frame.payload)} bytes")
            return None

        # Create J1939 transport message
        j1939_message = self._create_j1939_message(
            frame=frame,
            priority=priority,
            pgn=pgn,
            source_sa=source_sa,
            dest_sa=dest_sa,
            payload=frame.payload,
            is_broadcast=is_broadcast,
            transport_protocol_used=J1939TransportProtocolType.NONE,  # Single frame
            frame_count=1,
            expected_length=len(frame.payload),
        )

        logger.debug(f"Completed J1939 single frame: PGN=0x{pgn:04X}")
        return j1939_message

    def _complete_session(
        self, session_key: tuple, session: J1939Session, start_timestamp_unix_us: int
    ) -> J1939TransportMessage:
        """Complete a J1939 multi-frame session."""
        try:
            # Truncate to expected size
            data = bytes(session.data[: session.size])

            # Create J1939 transport message
            j1939_message = self._create_j1939_message(
                frame=session.frames[0],  # Use first frame for metadata
                priority=session.priority,
                pgn=session.target_pgn,
                source_sa=session.source_sa,
                dest_sa=session.dest_sa,
                payload=data,
                is_broadcast=session.is_broadcast,
                transport_protocol_used=session.transport_type,
                frame_count=len(session.frames),
                expected_length=session.size,
            )

            logger.debug(
                f"Completed J1939 {session.transport_type} session: PGN=0x{session.target_pgn:04X}"
            )
            return j1939_message

        finally:
            # Clean up session
            if session_key in self.sessions:
                del self.sessions[session_key]

    def _create_j1939_message(
        self,
        frame: CANTransportFrame,
        priority: int,
        pgn: int,
        source_sa: int,
        dest_sa: int,
        payload: bytes,
        is_broadcast: bool,
        transport_protocol_used: Optional[str],
        frame_count: int,
        expected_length: int,
    ) -> J1939TransportMessage:
        """Create a J1939TransportMessage from frame data."""
        return J1939TransportMessage(
            pgn=pgn,
            source_address=source_sa,
            destination_address=dest_sa,
            priority=priority,
            payload=payload,
            start_timestamp_unix_us=frame.start_timestamp_unix_us,
            frame_count=frame_count,
            expected_length=expected_length,
            is_broadcast=is_broadcast,
            transport_protocol_used=transport_protocol_used,
        )

    def cleanup(self, start_timestamp_unix_us: int) -> None:
        """Clean up expired J1939 transport sessions."""
        expired_keys = []
        for key, session in self.sessions.items():
            if start_timestamp_unix_us - session.start_time > J1939_SESSION_TIMEOUT_US:
                expired_keys.append(key)

        for key in expired_keys:
            logger.debug(f"Cleaning up expired J1939 transport session: {key}")
            del self.sessions[key]
