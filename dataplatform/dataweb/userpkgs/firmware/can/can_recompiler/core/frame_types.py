"""
CAN Frame Data Types (Pure Domain Objects)

This module contains pure domain objects for CAN frame processing without any
infrastructure dependencies. These types provide structured interfaces for
frame data, parsing results, and protocol information focused solely on
networking protocol semantics.
"""

from dataclasses import dataclass
from typing import Optional

from .constants import (
    CAN_FRAME_MAX_DATA_LENGTH,
    CAN_ID_MASK_29BIT,
    CAN_ID_STANDARD_FRAME_LIMIT,
    MAX_TIMESTAMP_MICROSECONDS,
)


@dataclass
class CANTransportFrame:
    """
    Base CAN transport frame representation.

    Provides common CAN frame data and validation methods, focused on the
    transport layer of the CAN stack before protocol-specific processing.
    """

    arbitration_id: int
    payload: bytes
    start_timestamp_unix_us: int
    direction: Optional[int] = None
    source_interface: Optional[str] = None
    is_extended_frame: bool = False
    transport_id: Optional[int] = None  # Pre-classified transport protocol (if available)

    def __post_init__(self):
        """Validate CAN frame data integrity."""
        self._validate_can_id()
        self._validate_payload()
        self._validate_timestamp()

    def _validate_can_id(self):
        """Validate CAN arbitration ID is within valid range."""
        if self.arbitration_id < 0:
            raise ValueError(f"Invalid CAN ID: {self.arbitration_id}. Must be non-negative integer")

        if self.arbitration_id > CAN_ID_MASK_29BIT:
            raise ValueError(f"CAN ID {self.arbitration_id:08X} exceeds 29-bit limit")

        # Set extended frame flag based on ID value
        if self.arbitration_id > CAN_ID_STANDARD_FRAME_LIMIT:
            self.is_extended_frame = True

    def _validate_payload(self):
        """Validate payload is bytes and within CAN frame size limits."""
        if self.payload is None:
            raise ValueError("Payload cannot be None")

        if not isinstance(self.payload, bytes):
            raise ValueError(f"Invalid payload format: {type(self.payload)}. Expected bytes")

        if len(self.payload) > CAN_FRAME_MAX_DATA_LENGTH:
            raise ValueError("Payload too large for CAN frame")

    def _validate_timestamp(self):
        """Validate timestamp is within reasonable bounds."""
        if self.start_timestamp_unix_us < 0:
            raise ValueError(
                f"Invalid timestamp: {self.start_timestamp_unix_us}. Must be non-negative"
            )

        if self.start_timestamp_unix_us > MAX_TIMESTAMP_MICROSECONDS:
            raise ValueError(f"Timestamp {self.start_timestamp_unix_us} unreasonably large")
