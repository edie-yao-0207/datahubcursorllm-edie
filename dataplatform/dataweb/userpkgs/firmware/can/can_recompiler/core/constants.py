"""
Core CAN Constants

This module contains core CAN-level constants and shared session management constants.
"""

# =============================================================================
# CAN Frame Constants
# =============================================================================

# CAN ID limits and masks
CAN_ID_MASK_29BIT = 0x1FFFFFFF  # 29-bit extended frame mask
CAN_ID_STANDARD_FRAME_LIMIT = 0x7FF  # 11-bit standard frame upper limit
CAN_FRAME_MAX_DATA_LENGTH = 8  # Maximum CAN frame data length

# Bit manipulation constants
BYTE_MASK = 0xFF  # Single byte mask
LOWER_NIBBLE_MASK = 0x0F  # Lower 4 bits mask
RESERVED_BIT_MASK = 0x1  # Single bit mask

# =============================================================================
# Session Management Constants (Shared across protocols)
# =============================================================================

# Session management constants
MAX_CONCURRENT_SESSIONS = 1000  # Maximum active sessions per processor
SESSION_CLEANUP_INTERVAL = 100  # Clean up sessions every N frames
SESSION_TIMEOUT_US = 2_000_000  # 2 second timeout in microseconds (shared across protocols)

# Timestamp validation constants
MAX_TIMESTAMP_MICROSECONDS = 2**63 - 1  # Maximum valid timestamp (64-bit signed integer)

# =============================================================================
# Bit Shift Constants for CAN Address Extraction
# =============================================================================

# Bit shifts for extracting specific fields from CAN arbitration IDs
CAN_BYTE_SHIFT = 8  # General byte-level extraction (8 bits)

# =============================================================================
# =============================================================================

POSITIVE_RESPONSE_OFFSET = 0x40  # Add to request SID for positive response
