"""
J1939 Protocol Transport Enumerations

J1939-specific transport protocol types and message classifications.
These enums are specific to the J1939 transport layer protocol.
"""

from enum import Enum
from .constants import (
    J1939_TRANSPORT_PGN_MIN,
    J1939_TRANSPORT_PGN_MAX,
    J1939_STANDARD_DATA_PGN_MIN,
    J1939_STANDARD_DATA_PGN_MAX,
    J1939_APP_DIAGNOSTIC_PGN_MIN,
    J1939_APP_DIAGNOSTIC_PGN_MAX,
    J1939_APP_NORMAL_PGN_MIN,
    J1939_APP_NORMAL_PGN_MAX,
    J1939_APP_PROPRIETARY_PGN_MIN,
    J1939_APP_PROPRIETARY_PGN_MAX,
    J1939_DM_MESSAGE_PGNS,
)


class J1939TransportProtocolType(int, Enum):
    """J1939 transport protocol types."""

    NONE = 0  # Single frame message
    BAM = 1  # Broadcast Announce Message
    RTS_CTS = 2  # Request to Send / Clear to Send

    def __str__(self) -> str:
        return self.name


class J1939DataIdentifierType(int, Enum):
    """
    J1939-specific data identifier classification based on PGN ranges.

    Classifies J1939 Parameter Group Numbers (PGNs) by their logical ranges
    according to SAE J1939 standards.
    """

    NONE = 0  # No identifier or unrecognized
    STANDARD = 1  # Standard J1939 range (0x0001-0xDFFF)
    REQUEST = 2  # Request PGN range (0x00EA00)
    TRANSPORT = 3  # Transport Protocol range (0xEA00-0xEC00)
    DIAGNOSTIC_73 = 4  # SAE J1939-73 Diagnostic range (0xF000-0xF0FF)
    DM_MESSAGES = 5  # Diagnostic Message range (0xFECA-0xFEF3)
    NORMAL_71 = 6  # SAE J1939-71 Normal range (0xFE00-0xFEFF, excluding DM messages)
    PROPRIETARY = 7  # Proprietary range (0xFF00-0xFFFF)
    UNKNOWN = 8  # PGNs that cannot be classified

    def __str__(self) -> str:
        return self.name


def classify_j1939_pgn(pgn: int) -> J1939DataIdentifierType:
    """
    Classify a J1939 PGN based on its application-level purpose.

    Args:
        pgn: Parameter Group Number to classify

    Returns:
        J1939DataIdentifierType enum value based on PGN ranges and purpose
    """
    # Transport Protocol range (includes Request PGN 0xEA00, TP.DT 0xEB00, TP.CM 0xEC00)
    if J1939_TRANSPORT_PGN_MIN <= pgn <= J1939_TRANSPORT_PGN_MAX:
        return J1939DataIdentifierType.TRANSPORT
    # SAE J1939-73 Diagnostic range
    elif J1939_APP_DIAGNOSTIC_PGN_MIN <= pgn <= J1939_APP_DIAGNOSTIC_PGN_MAX:
        return J1939DataIdentifierType.DIAGNOSTIC_73
    # Specific DM Messages (diagnostic messages - scattered PGNs)
    elif pgn in J1939_DM_MESSAGE_PGNS:
        return J1939DataIdentifierType.DM_MESSAGES
    # SAE J1939-71 Normal range (excluding DM messages)
    elif J1939_APP_NORMAL_PGN_MIN <= pgn <= J1939_APP_NORMAL_PGN_MAX:
        return J1939DataIdentifierType.NORMAL_71
    # Proprietary range
    elif J1939_APP_PROPRIETARY_PGN_MIN <= pgn <= J1939_APP_PROPRIETARY_PGN_MAX:
        return J1939DataIdentifierType.PROPRIETARY
    # Standard J1939 range (below transport protocols)
    elif J1939_STANDARD_DATA_PGN_MIN <= pgn <= J1939_STANDARD_DATA_PGN_MAX:
        return J1939DataIdentifierType.STANDARD
    else:
        return J1939DataIdentifierType.UNKNOWN
