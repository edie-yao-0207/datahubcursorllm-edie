"""
CAN Frame Processing Enumerations

This module contains shared enumerations used for CAN frame transport and application
protocol classification. These enums are designed to be integer-based for efficient
storage and processing.
"""

from enum import Enum


class TransportProtocolType(int, Enum):
    """Transport layer protocol types for CAN frames."""

    NONE = 0
    ISOTP = 1  # ISO 15765-2 over 11-bit CAN (UDS/OBD-II)
    J1939 = 2  # 29-bit extended frame PGN-based messages

    def __str__(self):
        return self.name


class ApplicationProtocolType(int, Enum):
    """Application layer protocol types."""

    NONE = 0
    UDS = 1  # Unified Diagnostic Services
    J1939 = 3  # SAE J1939 top-level

    def __str__(self):
        return self.name
