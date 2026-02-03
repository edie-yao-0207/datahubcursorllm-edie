"""
Protocol-Specific ISO-TP Transport Processor

This module provides an ISO-TP transport processor that works with
concrete types using the type-safe processor framework.
"""

import logging
from dataclasses import dataclass, field
from typing import Dict, Optional

from .frames import ISOTPFrame
from ...core.frame_types import CANTransportFrame
from .constants import (
    ISOTP_PCI_SINGLE_FRAME,
    ISOTP_PCI_FIRST_FRAME,
    ISOTP_PCI_CONSECUTIVE_FRAME,
    ISOTP_SINGLE_FRAME_MAX_LENGTH,
    ISOTP_SINGLE_FRAME_PRACTICAL_MAX,
    ISOTP_MULTI_FRAME_MAX_TOTAL_LENGTH,
    ISOTP_SESSION_TIMEOUT_US,
    ISOTP_PCI_SHIFT,
    ISOTP_SINGLE_FRAME_PAYLOAD_OFFSET,
    ISOTP_FIRST_FRAME_PAYLOAD_OFFSET,
    ISOTP_CONSECUTIVE_FRAME_PAYLOAD_OFFSET,
    ISOTP_SEQUENCE_INCREMENT,
    ISOTP_INITIAL_SEQUENCE_NUMBER,
)
from ...core.constants import (
    LOWER_NIBBLE_MASK,
    CAN_BYTE_SHIFT,
    MAX_CONCURRENT_SESSIONS,
    SESSION_CLEANUP_INTERVAL,
)

logger = logging.getLogger("can_recompiler.protocols.isotp.transport")


@dataclass
class ISOTPSession:
    """Represents an active multi-frame ISO-TP session."""

    data: bytearray
    expected_len: int
    next_seq: int
    source_address: int
    start_time: int = 0
    frames: list = field(default_factory=list)


class ISOTPTransport:
    """
    ISO-TP transport layer handler.

    Handles ISO-TP frame reassembly and session management, producing
    ISOTPFrame objects from incoming CAN frames.
    """

    def __init__(self):
        self.sessions: Dict[int, ISOTPSession] = {}
        self._frame_count = 0
        self._cleanup_interval = SESSION_CLEANUP_INTERVAL

    def process(self, frame: CANTransportFrame) -> Optional[ISOTPFrame]:
        """
        Process ISO-TP frame at transport layer.

        Handles single frames immediately and manages multi-frame session
        state for proper message reconstruction.
        """
        addr = frame.arbitration_id
        data = frame.payload
        start_timestamp_unix_us = frame.start_timestamp_unix_us

        # Guard clauses for early exit
        if len(data) < 1:
            return None

        # Note: Extended addressing is not supported in this implementation

        pci = data[0] >> ISOTP_PCI_SHIFT

        # Performance optimization: only clean up periodically
        self._frame_count += 1
        if self._frame_count % self._cleanup_interval == 0:
            self.cleanup(start_timestamp_unix_us)

        # Route to specific frame type handlers
        if pci == ISOTP_PCI_SINGLE_FRAME:
            return self._process_single_frame(frame, addr, data, start_timestamp_unix_us)

        if pci == ISOTP_PCI_FIRST_FRAME:
            self._process_first_frame(frame, addr, data, start_timestamp_unix_us)
            return None  # First frame doesn't produce output yet

        if pci == ISOTP_PCI_CONSECUTIVE_FRAME and addr in self.sessions:
            return self._process_consecutive_frame(frame, addr, data, start_timestamp_unix_us)

        return None

    def _process_single_frame(
        self, frame: CANTransportFrame, addr: int, data: bytes, start_timestamp_unix_us: int
    ) -> Optional[ISOTPFrame]:
        """Process ISO-TP single frame."""
        length = data[0] & LOWER_NIBBLE_MASK

        if length == 0 or length > ISOTP_SINGLE_FRAME_MAX_LENGTH:
            return None

        if length > ISOTP_SINGLE_FRAME_PRACTICAL_MAX:
            logger.info(
                f"ISO-TP single frame from 0x{addr:X} exceeds practical limit: {length} > {ISOTP_SINGLE_FRAME_PRACTICAL_MAX}"
            )
            return None

        raw_payload = data[
            ISOTP_SINGLE_FRAME_PAYLOAD_OFFSET : ISOTP_SINGLE_FRAME_PAYLOAD_OFFSET + length
        ]

        isotp_frame = self._create_isotp_frame(
            frame, addr, raw_payload, length, start_timestamp_unix_us
        )

        logger.debug(f"ISO-TP single frame complete: 0x{addr:X}, len={length}")
        return isotp_frame

    def _process_first_frame(
        self, frame: CANTransportFrame, addr: int, data: bytes, start_timestamp_unix_us: int
    ) -> None:
        """Process ISO-TP first frame."""
        total_len = ((data[0] & LOWER_NIBBLE_MASK) << CAN_BYTE_SHIFT) | data[1]

        # Validate first frame total length limits
        if total_len <= ISOTP_SINGLE_FRAME_PRACTICAL_MAX:
            logger.warning(
                f"Invalid first frame length from 0x{addr:X}: {total_len} <= {ISOTP_SINGLE_FRAME_PRACTICAL_MAX}"
            )
            return None

        if total_len > ISOTP_MULTI_FRAME_MAX_TOTAL_LENGTH:
            logger.warning(
                f"ISO-TP first frame from 0x{addr:X} exceeds maximum total length: {total_len} > {ISOTP_MULTI_FRAME_MAX_TOTAL_LENGTH}"
            )
            return None

        raw_payload = data[ISOTP_FIRST_FRAME_PAYLOAD_OFFSET:]

        # Check session limits before creating new session
        if not self._ensure_session_capacity(start_timestamp_unix_us):
            return

        session = ISOTPSession(
            data=bytearray(raw_payload),
            expected_len=total_len,
            next_seq=ISOTP_INITIAL_SEQUENCE_NUMBER,
            source_address=addr,
            frames=[frame],
            start_time=start_timestamp_unix_us,
        )
        self.sessions[addr] = session

        logger.debug(f"Started ISO-TP multi-frame session 0x{addr:X}, expected_len={total_len}")

    def _process_consecutive_frame(
        self, frame: CANTransportFrame, addr: int, data: bytes, start_timestamp_unix_us: int
    ) -> Optional[ISOTPFrame]:
        """Process ISO-TP consecutive frame."""
        seq = data[0] & LOWER_NIBBLE_MASK
        sess = self.sessions[addr]

        if seq != sess.next_seq:
            logger.warning(
                f"ISO-TP sequence mismatch from 0x{addr:X}: got {seq}, expected {sess.next_seq}"
            )
            del self.sessions[addr]
            return None

        raw_payload = data[ISOTP_CONSECUTIVE_FRAME_PAYLOAD_OFFSET:]
        sess.data.extend(raw_payload)
        sess.frames.append(frame)
        sess.next_seq = (sess.next_seq + ISOTP_SEQUENCE_INCREMENT) & LOWER_NIBBLE_MASK

        # Check if session is complete
        if len(sess.data) < sess.expected_len:
            return None

        # Session complete - create ISO-TP frame
        isotp_frame = self._create_isotp_frame(
            frame, sess.source_address, bytes(sess.data), sess.expected_len, start_timestamp_unix_us
        )
        isotp_frame.frame_count = len(sess.frames)

        logger.debug(
            f"Completed ISO-TP multi-frame 0x{sess.source_address:X}, len={sess.expected_len}"
        )

        del self.sessions[addr]
        return isotp_frame

    def _create_isotp_frame(
        self,
        original_frame: CANTransportFrame,
        source_address: int,
        payload: bytes,
        expected_length: int,
        start_timestamp_unix_us: int,
    ) -> ISOTPFrame:
        """Create an ISOTPFrame from reassembled data."""
        return ISOTPFrame(
            source_address=source_address,
            payload=payload,
            start_timestamp_unix_us=start_timestamp_unix_us,
            frame_count=1,
            expected_length=expected_length,
        )

    def _ensure_session_capacity(self, start_timestamp_unix_us: int) -> bool:
        """Ensure there's capacity for a new session."""
        if len(self.sessions) < MAX_CONCURRENT_SESSIONS:
            return True

        logger.warning(f"ISO-TP session limit reached ({MAX_CONCURRENT_SESSIONS}), forcing cleanup")
        self.cleanup(start_timestamp_unix_us)

        if len(self.sessions) >= MAX_CONCURRENT_SESSIONS:
            logger.error(f"Cannot create new ISO-TP session: limit exceeded")
            return False

        return True

    def cleanup(self, start_timestamp_unix_us: int):
        """Clean up expired sessions."""
        expired_addrs = [
            a
            for a, s in self.sessions.items()
            if (start_timestamp_unix_us - s.start_time) > ISOTP_SESSION_TIMEOUT_US
        ]
        for a in expired_addrs:
            logger.debug(f"ISO-TP session expired: 0x{a:X}")
            del self.sessions[a]
