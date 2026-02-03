"""
UDS Application Constants

Constants specific to UDS (Unified Diagnostic Services) application layer,
including service identifiers, response codes, and data identifier patterns.
"""

from ...core.constants import POSITIVE_RESPONSE_OFFSET

# === UDS Service Identifiers ===
UDS_READ_DATA_BY_IDENTIFIER = 0x22
UDS_WRITE_DATA_BY_IDENTIFIER = 0x2E
UDS_INPUT_OUTPUT_CONTROL_BY_IDENTIFIER = 0x2F

# === UDS Response Patterns ===
UDS_POSITIVE_RESPONSE_OFFSET = POSITIVE_RESPONSE_OFFSET  # Reuse shared constant
UDS_NEGATIVE_RESPONSE_SID = 0x7F

# === UDS Data Identifier Services (for extract_data_identifier) ===
UDS_DID_SERVICES = {0x22, 0x2E, 0x2F}  # Services that use Data Identifiers
