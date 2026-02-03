"""
Core CAN Protocol Processing Framework (Pure Domain Logic)

This module contains the pure domain objects and orchestration logic for CAN 
protocol processing, without any infrastructure dependencies. All Spark-specific
logic has been moved to the infra package.
"""

# Pure domain types and utilities
from .frame_types import CANTransportFrame
from .enums import TransportProtocolType, ApplicationProtocolType
from .constants import CAN_ID_MASK_29BIT, CAN_FRAME_MAX_DATA_LENGTH

__all__ = [
    # Core types
    "CANTransportFrame",
    # Core Enums (protocol-agnostic only)
    "TransportProtocolType",
    "ApplicationProtocolType",
    # Constants
    "CAN_ID_MASK_29BIT",
    "CAN_FRAME_MAX_DATA_LENGTH",
]
