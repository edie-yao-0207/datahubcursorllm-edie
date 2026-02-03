"""
Tests for UDS protocol constants.

This module contains tests for UDS-specific constants and their usage.
"""

import pytest

from ....protocols.uds.constants import (
    UDS_READ_DATA_BY_IDENTIFIER,
    UDS_WRITE_DATA_BY_IDENTIFIER,
    UDS_INPUT_OUTPUT_CONTROL_BY_IDENTIFIER,
    UDS_POSITIVE_RESPONSE_OFFSET,
    UDS_NEGATIVE_RESPONSE_SID,
    UDS_DID_SERVICES,
)


class TestUDSConstants:
    """Test UDS protocol constants."""

    def test_uds_service_identifiers(self):
        """Test UDS service identifier constants."""
        assert UDS_NEGATIVE_RESPONSE_SID == 0x7F
        assert UDS_READ_DATA_BY_IDENTIFIER == 0x22
        assert UDS_WRITE_DATA_BY_IDENTIFIER == 0x2E
        assert UDS_INPUT_OUTPUT_CONTROL_BY_IDENTIFIER == 0x2F

    def test_uds_response_patterns(self):
        """Test UDS response pattern constants."""
        assert UDS_POSITIVE_RESPONSE_OFFSET == 0x40
        assert UDS_NEGATIVE_RESPONSE_SID == 0x7F

    def test_uds_did_services(self):
        """Test UDS DID services set."""
        expected_services = {0x22, 0x2E, 0x2F}
        assert UDS_DID_SERVICES == expected_services

        # Test that all expected services are present
        assert UDS_READ_DATA_BY_IDENTIFIER in UDS_DID_SERVICES
        assert UDS_WRITE_DATA_BY_IDENTIFIER in UDS_DID_SERVICES
        assert UDS_INPUT_OUTPUT_CONTROL_BY_IDENTIFIER in UDS_DID_SERVICES

    @pytest.mark.parametrize(
        "service_id,uses_did",
        [
            (0x22, True),  # Read Data By Identifier
            (0x2E, True),  # Write Data By Identifier
            (0x2F, True),  # Input/Output Control By Identifier
            (0x10, False),  # Diagnostic Session Control
            (0x27, False),  # Security Access
        ],
    )
    def test_service_did_usage(self, service_id: int, uses_did: bool):
        """Test which services use Data Identifiers."""
        if uses_did:
            assert service_id in UDS_DID_SERVICES
        else:
            assert service_id not in UDS_DID_SERVICES
