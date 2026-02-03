"""
ISO-TP Protocol Constants

Constants specific to ISO Transport Protocol (ISO 15765-2) implementation,
including UDS addressing, frame sizes, timeouts, and protocol control information.
"""

# === ISO-TP Protocol Control Information (PCI) ===
ISOTP_PCI_SINGLE_FRAME = 0x0
ISOTP_PCI_FIRST_FRAME = 0x1
ISOTP_PCI_CONSECUTIVE_FRAME = 0x2

# === UDS Addressing (Standard 11-bit CAN IDs) ===
# Standard OBD-II ranges
UDS_REQUEST_ADDR_MIN = 0x7E0
UDS_REQUEST_ADDR_MAX = 0x7E7
UDS_BROADCAST_ADDR = 0x7DF
UDS_RESPONSE_ADDR_MIN = 0x7E8
UDS_RESPONSE_ADDR_MAX = 0x7EF

# Extended diagnostic ranges for automotive ECUs
UDS_EXTENDED_REQUEST_MIN = 0x700  # Extended request range start
UDS_EXTENDED_REQUEST_MAX = 0x7DE  # Up to broadcast (0x7DF excluded)
UDS_EXTENDED_RESPONSE_MIN = 0x708  # Typical request+8 pattern starts here
UDS_EXTENDED_RESPONSE_MAX = 0x7FE  # Extended response range (up to 0x7FE, avoiding J1939)

# === UDS Extended Addressing (29-bit CAN IDs) ===
UDS_EXTENDED_RANGE_MIN = 0x18DA0000
UDS_EXTENDED_RANGE_MAX = 0x18DAFFFF
UDS_EXTENDED_DIAGNOSTIC_RESPONSE_EXAMPLE = (
    0x18DB33F1  # Example: ECU F1 responding to tester 33 (PF=0xDB diagnostic response)
)
UDS_EXTENDED_PF_DA = 0xDA
UDS_EXTENDED_ADDR_RANGE_MIN = 0xDA00
UDS_EXTENDED_ADDR_RANGE_MAX = 0xDFFF

# === ISO-TP Frame Size Limits ===
ISOTP_SINGLE_FRAME_MAX_LENGTH = 15  # Maximum single frame data length (4-bit length field: 0x0F)
ISOTP_SINGLE_FRAME_PRACTICAL_MAX = 7  # Practical max for single frame in 8-byte CAN frame
ISOTP_MULTI_FRAME_MIN_TOTAL_LENGTH = (
    ISOTP_SINGLE_FRAME_PRACTICAL_MAX + 1
)  # Minimum length requiring multi-frame
ISOTP_MULTI_FRAME_MAX_TOTAL_LENGTH = 4095  # Maximum multi-frame total length (12-bit field: 0xFFF)

# === ISO-TP Session Management ===
# Import shared timeout from core constants to avoid duplication
from ...core.constants import SESSION_TIMEOUT_US

ISOTP_SESSION_TIMEOUT_US = SESSION_TIMEOUT_US  # 2 second timeout in microseconds

# === ISO-TP Frame Processing ===
ISOTP_PCI_SHIFT = 4  # PCI is in upper 4 bits of first byte
ISOTP_SINGLE_FRAME_PAYLOAD_OFFSET = 1  # Payload starts at byte 1
ISOTP_FIRST_FRAME_PAYLOAD_OFFSET = 2  # Payload starts at byte 2
ISOTP_CONSECUTIVE_FRAME_PAYLOAD_OFFSET = 1  # Payload starts at byte 1
ISOTP_SEQUENCE_INCREMENT = 1  # Consecutive frame sequence increment
ISOTP_INITIAL_SEQUENCE_NUMBER = 1  # Starting sequence number for multi-frame
