"""
J1939 Protocol Frame Types

This module contains pure J1939 domain objects focused solely on 
J1939 transport protocol logic. All serialization concerns are handled
by adapters in the infra package.
"""

from dataclasses import dataclass
from typing import Optional

from ...core.enums import TransportProtocolType
from .enums import J1939TransportProtocolType


@dataclass
class J1939TransportMessage:
    """
    J1939 Transport Protocol message.

    Represents a complete J1939 message after transport protocol processing,
    containing J1939-specific addressing and PGN information.
    """

    # J1939 specific fields
    pgn: int  # Parameter Group Number
    source_address: int  # Source address (SA)
    destination_address: int  # Destination address (DA) - 0xFF for broadcast
    priority: int = 6  # Message priority (0-7, default 6)

    # Message data
    payload: bytes = b""  # Reassembled J1939 message data

    # Frame metadata
    start_timestamp_unix_us: int = 0
    frame_count: int = 1  # Number of CAN frames that made up this message
    expected_length: int = 0  # Expected payload length from transport protocol

    # Transport protocol info
    is_broadcast: bool = False  # True if destination is 0xFF
    transport_protocol_used: Optional[
        J1939TransportProtocolType
    ] = None  # BAM, RTS/CTS, or None for single frame
