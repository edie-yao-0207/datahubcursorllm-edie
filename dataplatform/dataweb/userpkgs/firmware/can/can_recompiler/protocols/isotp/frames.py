"""
ISO-TP Protocol Frame Types

This module contains pure ISO-TP domain objects focused solely on 
networking protocol logic. All serialization concerns are handled
by adapters in the infra package.
"""

from dataclasses import dataclass
from typing import Optional
from ...core.constants import CAN_ID_STANDARD_FRAME_LIMIT


@dataclass
class ISOTPFrame:
    """
    ISO-TP (ISO 15765-2) Transport Protocol frame.

    Represents a complete ISO-TP message after reassembly, containing
    the transport-level information specific to ISO-TP protocol.
    """

    # ISO-TP specific fields (required fields first)
    source_address: int  # Source address (CAN arbitration ID)

    # Common transport fields (from BaseTransportFrame)
    start_timestamp_unix_us: int = 0
    frame_count: int = 1  # Number of CAN frames that made up this message
    expected_length: int = 0  # Expected payload length from transport protocol
    payload: bytes = b""  # Reassembled transport payload
