"""
Tests for ISO-TP protocol constants.

This module contains tests for ISO-TP specific constants and their usage.
"""

import pytest

from ....protocols.isotp.constants import (
    UDS_REQUEST_ADDR_MIN,
    UDS_REQUEST_ADDR_MAX,
    UDS_RESPONSE_ADDR_MIN,
    UDS_RESPONSE_ADDR_MAX,
    UDS_BROADCAST_ADDR,
    UDS_EXTENDED_RANGE_MIN,
    UDS_EXTENDED_RANGE_MAX,
    ISOTP_PCI_SINGLE_FRAME,
    ISOTP_PCI_FIRST_FRAME,
    ISOTP_PCI_CONSECUTIVE_FRAME,
    ISOTP_SINGLE_FRAME_MAX_LENGTH,
    ISOTP_MULTI_FRAME_MAX_TOTAL_LENGTH,
)


class TestISOTPConstants:
    """Test ISO-TP protocol constants."""

    def test_uds_address_ranges(self):
        """Test UDS addressing range constants."""
        # Request range should be valid
        assert UDS_REQUEST_ADDR_MIN <= UDS_REQUEST_ADDR_MAX
        assert UDS_REQUEST_ADDR_MIN == 0x7E0
        assert UDS_REQUEST_ADDR_MAX == 0x7E7

        # Response range should be valid
        assert UDS_RESPONSE_ADDR_MIN <= UDS_RESPONSE_ADDR_MAX
        assert UDS_RESPONSE_ADDR_MIN == 0x7E8
        assert UDS_RESPONSE_ADDR_MAX == 0x7EF

        # Broadcast address should be separate
        assert UDS_BROADCAST_ADDR == 0x7DF

    def test_uds_extended_ranges(self):
        """Test UDS extended addressing range constants."""
        assert UDS_EXTENDED_RANGE_MIN <= UDS_EXTENDED_RANGE_MAX
        assert UDS_EXTENDED_RANGE_MIN == 0x18DA0000
        assert UDS_EXTENDED_RANGE_MAX == 0x18DAFFFF

    def test_isotp_pci_values(self):
        """Test ISO-TP PCI (Protocol Control Information) constants."""
        assert ISOTP_PCI_SINGLE_FRAME == 0x0
        assert ISOTP_PCI_FIRST_FRAME == 0x1
        assert ISOTP_PCI_CONSECUTIVE_FRAME == 0x2

        # PCI values should be distinct
        pci_values = {
            ISOTP_PCI_SINGLE_FRAME,
            ISOTP_PCI_FIRST_FRAME,
            ISOTP_PCI_CONSECUTIVE_FRAME,
        }
        assert len(pci_values) == 3

    def test_isotp_frame_limits(self):
        """Test ISO-TP frame size limit constants."""
        assert ISOTP_SINGLE_FRAME_MAX_LENGTH == 15
        assert ISOTP_MULTI_FRAME_MAX_TOTAL_LENGTH == 4095

        # Multi-frame should accommodate larger messages than single frame
        assert ISOTP_MULTI_FRAME_MAX_TOTAL_LENGTH > ISOTP_SINGLE_FRAME_MAX_LENGTH

    @pytest.mark.parametrize(
        "addr,expected_range",
        [
            (0x7E0, "request"),
            (0x7E7, "request"),
            (0x7E8, "response"),
            (0x7EF, "response"),
            (0x7DF, "broadcast"),
        ],
    )
    def test_uds_address_classification(self, addr: int, expected_range: str):
        """Test UDS address classification."""
        if expected_range == "request":
            assert UDS_REQUEST_ADDR_MIN <= addr <= UDS_REQUEST_ADDR_MAX
        elif expected_range == "response":
            assert UDS_RESPONSE_ADDR_MIN <= addr <= UDS_RESPONSE_ADDR_MAX
        elif expected_range == "broadcast":
            assert addr == UDS_BROADCAST_ADDR
