"""
UDS Protocol Frame Types

This module contains pure UDS domain objects focused solely on 
diagnostic services protocol logic. All serialization concerns are handled
by adapters in the infra package.
"""

from dataclasses import dataclass
from typing import Optional
from functools import cached_property

from .enums import UDSDataIdentifierType
from ...protocols.isotp.frames import ISOTPFrame


@dataclass
class UDSMessage:
    """
    Self-parsing UDS (Unified Diagnostic Services) application message.

    Creates a UDS message from a transport frame and provides lazy access
    to all parsed fields and clean payload data.
    """

    # Transport context (single source of truth)
    transport_frame: ISOTPFrame

    @cached_property
    def service_id(self) -> int:
        """Extract UDS request service ID from transport payload."""
        if not self.transport_frame.payload:
            return 0

        raw_sid = self.transport_frame.payload[0]
        # Convert response service ID back to request service ID for semantic consistency
        return self._convert_response_to_request_sid(raw_sid)

    @cached_property
    def is_negative_response(self) -> bool:
        """Determine if this is a negative response (raw SID = 0x7F)."""
        if not self.transport_frame.payload:
            return False
        raw_sid = self.transport_frame.payload[0]
        return raw_sid == 0x7F

    @cached_property
    def is_response(self) -> bool:
        """Determine if this is a response (positive or negative)."""
        if not self.transport_frame.payload:
            return False
        raw_sid = self.transport_frame.payload[0]
        return raw_sid == 0x7F or (0x40 <= raw_sid < 0x7F)

    @cached_property
    def _data_info(self) -> tuple[Optional[int], UDSDataIdentifierType, int]:
        """Extract data identifier information (cached)."""
        payload = self.transport_frame.payload
        if not payload:
            return None, UDSDataIdentifierType.NONE, 0

        sid = payload[0]
        length = len(payload)
        request_sid = self._convert_response_to_request_sid(sid)

        # === UDS DID-based
        if request_sid in {0x22, 0x2E, 0x2F} and length >= 3:
            did = (payload[1] << 8) | payload[2]
            return did, UDSDataIdentifierType.DID, 3  # Just return the DID, not (service<<16)|did

        # === UDS Local Identifier
        if request_sid == 0x21 and length >= 2:
            local_id = payload[1]
            return local_id, UDSDataIdentifierType.UDS_LOCAL_IDENTIFIER, 2

        # === UDS Routine Control
        if request_sid == 0x31 and length >= 3:
            routine_id = (payload[1] << 8) | payload[2]
            return routine_id, UDSDataIdentifierType.ROUTINE, 3

        # === UDS DTC Subfunction
        if request_sid == 0x19 and length >= 2:
            subfunction = payload[1]
            return subfunction, UDSDataIdentifierType.DTC_SUB, 2

        # === Negative Response
        if sid == 0x7F and length >= 3:
            failed_sid = payload[1]
            return (sid << 8) | failed_sid, UDSDataIdentifierType.NRC, 3

        # === UDS SID-only services
        SID_ONLY_UDS = {0x10, 0x11, 0x14, 0x27, 0x28, 0x3E, 0x85}
        if request_sid in SID_ONLY_UDS:
            return request_sid, UDSDataIdentifierType.SERVICE_ONLY, 1

        # Check if raw SID is in service-only set (handles both request and response)
        if sid in SID_ONLY_UDS:
            return sid, UDSDataIdentifierType.SERVICE_ONLY, 1

        # === OBD-II specific handling
        # OBD-II service-only operations (no parameters)
        if request_sid in {0x03, 0x04, 0x07, 0x0A} and length >= 1:
            return None, UDSDataIdentifierType.SERVICE_ONLY, 1  # Service ID only

        # OBD-II PID-based operations (with parameters)
        if request_sid in {0x01, 0x02, 0x05, 0x06, 0x08, 0x09} and length >= 2:
            pid = payload[1]
            return pid, UDSDataIdentifierType.OBD_PID, 2  # Service ID + PID

        # === UDS Generic fallback
        if 0x10 <= sid <= 0x7F:
            return sid, UDSDataIdentifierType.UDS_GENERIC, 1

        # Not a recognized UDS pattern
        return request_sid, UDSDataIdentifierType.NONE, 1

    def _convert_response_to_request_sid(self, sid: int) -> int:
        """Convert response SID back to request SID (subtract 0x40 for responses)."""
        return sid - 0x40 if 0x40 <= sid <= 0x7E else sid

    @cached_property
    def data_identifier(self) -> Optional[int]:
        """Extract UDS data identifier if present."""
        return self._data_info[0]

    @cached_property
    def data_identifier_type(self) -> UDSDataIdentifierType:
        """Determine UDS data identifier type."""
        return self._data_info[1]

    @property
    def payload(self) -> bytes:
        """Clean application data payload - no protocol headers."""
        transport_payload = self.transport_frame.payload
        header_len = self._data_info[2]  # Length from UDS data identifier parsing
        return transport_payload[header_len:] if len(transport_payload) > header_len else b""
