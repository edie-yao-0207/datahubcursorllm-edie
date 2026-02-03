"""
Signal Decoder Enumerations

This module contains enumerations used for CAN signal decoding operations.
These enums are designed to be integer-based for efficient storage and processing.
"""

from enum import IntEnum


class EndiannessType(IntEnum):
    """Endianness enumeration for signal byte order."""

    INVALID = 0
    LITTLE = 1
    BIG = 2


class SignType(IntEnum):
    """Sign enumeration for signal interpretation."""

    INVALID = 0
    SIGNED = 1
    UNSIGNED = 2


class DecodeReasonType(IntEnum):
    """Enumeration for signal decode validation reasons."""

    OK = 0
    INVALID_BIT_LENGTH = 1
    NEGATIVE_BIT_START = 2
    UNKNOWN_ENDIAN = 3
    PAYLOAD_TOO_SMALL = 4
