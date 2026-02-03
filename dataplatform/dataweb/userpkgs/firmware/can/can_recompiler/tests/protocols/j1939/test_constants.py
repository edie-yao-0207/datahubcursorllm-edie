"""
Tests for J1939 protocol constants.

This module contains tests for J1939 specific constants and their usage.
"""

import pytest

from ....protocols.j1939.constants import (
    J1939_TP_CM_PGN,
    J1939_TP_DT_PGN,
    J1939_TP_ACK_PGN,
    J1939_TP_CM_RTS,
    J1939_TP_CM_CTS,
    J1939_TP_CM_BAM,
    J1939_TP_CM_ABORT,
    J1939_TP_ACK_ACK,
    J1939_TP_ACK_NACK,
    J1939_MAX_SINGLE_FRAME_SIZE,
    J1939_STANDARD_PGN_MIN,
    J1939_STANDARD_PGN_MAX,
    J1939_DIAGNOSTIC_PGN_MIN,
    J1939_DIAGNOSTIC_PGN_MAX,
    J1939_NORMAL_PGN_MIN,
    J1939_NORMAL_PGN_MAX,
    J1939_PROPRIETARY_PGN_MIN,
    J1939_PROPRIETARY_PGN_MAX,
    J1939_PF_PS_THRESHOLD,
    J1939_BROADCAST_DESTINATION,
)


class TestJ1939Constants:
    """Test J1939 protocol constants."""

    def test_transport_protocol_pgns(self):
        """Test J1939 transport protocol PGN constants."""
        assert J1939_TP_CM_PGN == 0xEC00
        assert J1939_TP_DT_PGN == 0xEB00
        assert J1939_TP_ACK_PGN == 0xEA00

        # PGNs should be distinct
        pgns = {J1939_TP_CM_PGN, J1939_TP_DT_PGN, J1939_TP_ACK_PGN}
        assert len(pgns) == 3

    def test_tp_cm_control_bytes(self):
        """Test J1939 TP.CM control byte constants."""
        assert J1939_TP_CM_RTS == 16
        assert J1939_TP_CM_CTS == 17
        assert J1939_TP_CM_BAM == 32
        assert J1939_TP_CM_ABORT == 255

        # Control bytes should be distinct
        control_bytes = {J1939_TP_CM_RTS, J1939_TP_CM_CTS, J1939_TP_CM_BAM, J1939_TP_CM_ABORT}
        assert len(control_bytes) == 4

    def test_tp_ack_control_bytes(self):
        """Test J1939 TP.ACK control byte constants."""
        assert J1939_TP_ACK_ACK == 0
        assert J1939_TP_ACK_NACK == 1

        # ACK values should be distinct
        assert J1939_TP_ACK_ACK != J1939_TP_ACK_NACK

    def test_pgn_ranges(self):
        """Test J1939 PGN range constants."""
        # Standard PGN range
        assert J1939_STANDARD_PGN_MIN == 0x0001
        assert J1939_STANDARD_PGN_MAX == 0xEFFF
        assert J1939_STANDARD_PGN_MIN <= J1939_STANDARD_PGN_MAX

        # Diagnostic PGN range
        assert J1939_DIAGNOSTIC_PGN_MIN == 0xF000
        assert J1939_DIAGNOSTIC_PGN_MAX == 0xF0FF
        assert J1939_DIAGNOSTIC_PGN_MIN <= J1939_DIAGNOSTIC_PGN_MAX

        # Normal PGN range
        assert J1939_NORMAL_PGN_MIN == 0xFE00
        assert J1939_NORMAL_PGN_MAX == 0xFEFF
        assert J1939_NORMAL_PGN_MIN <= J1939_NORMAL_PGN_MAX

        # Proprietary PGN range
        assert J1939_PROPRIETARY_PGN_MIN == 0xFF00
        assert J1939_PROPRIETARY_PGN_MAX == 0xFFFF
        assert J1939_PROPRIETARY_PGN_MIN <= J1939_PROPRIETARY_PGN_MAX

    def test_frame_size_limits(self):
        """Test J1939 frame size limit constants."""
        assert J1939_MAX_SINGLE_FRAME_SIZE == 8

    def test_pf_ps_threshold(self):
        """Test J1939 PF/PS threshold constant."""
        assert J1939_PF_PS_THRESHOLD == 240

    def test_broadcast_destination(self):
        """Test J1939 broadcast destination constant."""
        assert J1939_BROADCAST_DESTINATION == 0xFF

    @pytest.mark.parametrize(
        "pgn,expected_range",
        [
            (0x0100, "standard"),
            (0xEFFF, "standard"),
            (0xF000, "diagnostic"),
            (0xF0FF, "diagnostic"),
            (0xFE00, "normal"),
            (0xFEFF, "normal"),
            (0xFF00, "proprietary"),
            (0xFFFF, "proprietary"),
        ],
    )
    def test_pgn_range_classification(self, pgn: int, expected_range: str):
        """Test PGN range classification."""
        if expected_range == "standard":
            assert J1939_STANDARD_PGN_MIN <= pgn <= J1939_STANDARD_PGN_MAX
        elif expected_range == "diagnostic":
            assert J1939_DIAGNOSTIC_PGN_MIN <= pgn <= J1939_DIAGNOSTIC_PGN_MAX
        elif expected_range == "normal":
            assert J1939_NORMAL_PGN_MIN <= pgn <= J1939_NORMAL_PGN_MAX
        elif expected_range == "proprietary":
            assert J1939_PROPRIETARY_PGN_MIN <= pgn <= J1939_PROPRIETARY_PGN_MAX
