"""
UDS Protocol Enumerations

UDS-specific data identifier types and service classifications.
These enums are specific to the UDS application layer protocol.
"""

from enum import Enum


class UDSDataIdentifierType(int, Enum):
    """
    UDS-specific data identifier classification.

    Classifies different types of UDS data identifiers and service patterns
    for proper handling and schema generation.
    """

    NONE = 0  # No identifier or unrecognized
    DID = 1  # Data Identifier (0x22 ReadDataByIdentifier, 0x2E WriteDataByIdentifier)
    NRC = 2  # Negative Response Code (0x7F)
    DTC_SUB = 3  # DTC subfunction (0x19 ReadDTCInformation)
    UDS_GENERIC = 4  # Generic UDS service fallback
    UDS_LOCAL_IDENTIFIER = 5  # Local identifier (0x21)
    SERVICE_ONLY = 6  # Service-only operations (0x10, 0x11, 0x14, 0x27, etc.)
    ROUTINE = 7  # Routine control (0x31)
    OBD_PID = 8  # OBD-II Parameter ID (0x01-0x09 modes)

    def __str__(self) -> str:
        return self.name
