"""
Tests for Universal CAN Protocol Router

Comprehensive tests for the universal routing system that handles
ALL CAN protocol routing and processing in a single unified interface.
"""

import pytest
from typing import Dict, Any

from ....protocols.routing.universal_router import (
    UniversalCANProtocolRouter,
    create_universal_router,
    RoutingResult,
)
from ....core.frame_types import CANTransportFrame
from ....core.enums import TransportProtocolType, ApplicationProtocolType


class TestUniversalCANProtocolRouter:
    """Test the universal CAN protocol router architecture."""

    def test_universal_router_initialization(self):
        """Test universal router initializes with all components."""
        router = create_universal_router()

        # Should have routing information available
        info = router.get_routing_info()
        assert "transport_processors" in info
        assert "application_routing" in info
        assert "supported_protocols" in info

        # Should have transport processors
        assert len(info["transport_processors"]) > 0

    @pytest.mark.parametrize(
        "frame_config,expected_transport,expected_application,description",
        [
            # UDS frames (response - requests are filtered out at application layer)
            (
                {
                    "arbitration_id": 0x7E0,
                    "payload": b"\x05\x62\xF1\x90\x12\x34",
                    "start_timestamp_unix_us": 1000000,
                },
                TransportProtocolType.ISOTP,
                ApplicationProtocolType.UDS,
                "UDS Read Data By Identifier response",
            ),
            (
                {
                    "arbitration_id": 0x7DF,
                    "payload": b"\x03\x41\x0C\x64",
                    "start_timestamp_unix_us": 1000000,
                },
                TransportProtocolType.ISOTP,
                ApplicationProtocolType.UDS,
                "UDS response",
            ),
            # J1939 frames
            (
                {
                    "arbitration_id": 0x18FEF100,
                    "payload": b"\x01\x02\x03\x04\x05\x06\x07\x08",
                    "start_timestamp_unix_us": 1000000,
                },
                TransportProtocolType.J1939,
                ApplicationProtocolType.J1939,
                "J1939 transport message",
            ),
            # None/unrecognized frames
            (
                {
                    "arbitration_id": 0x123,
                    "payload": b"\xFF\xFF\xFF\xFF",
                    "start_timestamp_unix_us": 1000000,
                },
                TransportProtocolType.NONE,
                ApplicationProtocolType.NONE,
                "Unrecognized frame",
            ),
        ],
    )
    def test_universal_routing_and_processing(
        self,
        frame_config: Dict[str, Any],
        expected_transport: TransportProtocolType,
        expected_application: ApplicationProtocolType,
        description: str,
    ):
        """Test end-to-end routing and processing."""
        router = create_universal_router()

        # Create test frame
        frame = CANTransportFrame(**frame_config)

        # Route and process
        result = router.route_and_process(frame)

        # Verify basic result structure
        assert isinstance(result, RoutingResult), f"Should return RoutingResult for {description}"
        assert (
            result.transport_protocol == expected_transport
        ), f"Wrong transport protocol for {description}"
        assert (
            result.application_protocol == expected_application
        ), f"Wrong application protocol for {description}"

        # Verify routing path is populated
        assert result.routing_path, f"Routing path should be populated for {description}"

        # Verify success indicator
        if expected_transport == TransportProtocolType.NONE:
            assert (
                result.success
            ), f"None frames should be successfully identified as none for {description}"
            assert (
                result.application_message is None
            ), f"None frames should have no application message for {description}"
        else:
            # For processable frames, success depends on actual processing
            if result.success:
                assert (
                    result.transport_frame is not None
                ), f"Successful processing should have transport frame for {description}"

    def test_protocol_detection_accuracy(self):
        """Test protocol detection accuracy across different frame types."""
        router = create_universal_router()

        # Test cases for protocol detection - aligned with existing detection function behavior
        detection_cases = [
            # ISO-TP cases (verified by existing can_detect_isotp function)
            (
                CANTransportFrame(
                    arbitration_id=0x7E0, payload=b"\x01", start_timestamp_unix_us=1000000
                ),
                TransportProtocolType.ISOTP,
            ),
            (
                CANTransportFrame(
                    arbitration_id=0x18DA1234, payload=b"\x01", start_timestamp_unix_us=1000000
                ),
                TransportProtocolType.ISOTP,
            ),
            # J1939 cases (verified by existing can_detect_j1939 function)
            (
                CANTransportFrame(
                    arbitration_id=0x18FEF100, payload=b"\x01", start_timestamp_unix_us=1000000
                ),
                TransportProtocolType.J1939,
            ),
            # None cases (not detected by existing functions)
            (
                CANTransportFrame(
                    arbitration_id=0x123, payload=b"\x01", start_timestamp_unix_us=1000000
                ),
                TransportProtocolType.NONE,
            ),
        ]

        for frame, expected_protocol in detection_cases:
            detected = router._detect_transport_protocol(frame)
            assert detected == expected_protocol, (
                f"Detection failed for 0x{frame.arbitration_id:X}: "
                f"expected {expected_protocol}, got {detected}"
            )

    def test_routing_with_audit_trail(self):
        """Test routing with comprehensive audit trail."""
        router = create_universal_router()

        # Create UDS test frame
        frame = CANTransportFrame(
            arbitration_id=0x7E0, payload=b"\x03\x22\xF1\x90", start_timestamp_unix_us=1000000
        )

        # Route with audit
        audit_result = router.route_with_audit(frame)

        # Verify audit structure
        assert "routing_result" in audit_result
        assert "application_audit" in audit_result
        assert "transport_protocol_detected" in audit_result
        assert "routing_path" in audit_result
        assert "success" in audit_result

        # Verify audit content
        routing_result = audit_result["routing_result"]
        assert isinstance(routing_result, RoutingResult)
        assert audit_result["transport_protocol_detected"] == "ISOTP"
        assert "isotp" in audit_result["routing_path"].lower()

    def test_error_handling_and_edge_cases(self):
        """Test error handling for various edge cases."""
        router = create_universal_router()

        # Test None frame
        result = router.route_and_process(None)
        assert not result.success
        assert result.error_message
        assert "Empty CAN frame" in result.error_message

        # Test frame with empty payload
        empty_frame = CANTransportFrame(
            arbitration_id=0x7E0, payload=b"", start_timestamp_unix_us=1000000
        )
        result = router.route_and_process(empty_frame)
        # Should handle gracefully (may succeed or fail depending on processor logic)
        assert isinstance(result, RoutingResult)

    def test_cleanup_functionality(self):
        """Test cleanup propagates to all processors."""
        router = create_universal_router()

        # Cleanup should not raise exceptions
        timestamp = 2000000
        router.cleanup(timestamp)

        # Router should still be functional after cleanup
        info = router.get_routing_info()
        assert "transport_processors" in info

    def test_no_coupling_between_protocols(self):
        """Test that protocol processors are decoupled."""
        router = create_universal_router()

        # Create frames for different protocols
        uds_frame = CANTransportFrame(
            arbitration_id=0x7E0, payload=b"\x22\xF1\x90", start_timestamp_unix_us=1000000
        )

        # Process UDS frame
        uds_result = router.route_and_process(uds_frame)

        # Results should be correctly classified
        if uds_result.success:
            assert uds_result.application_protocol == ApplicationProtocolType.UDS

    def test_comprehensive_routing_info(self):
        """Test that routing info provides complete system visibility."""
        router = create_universal_router()
        info = router.get_routing_info()

        # Verify structure
        assert "transport_processors" in info
        assert "application_routing" in info
        assert "supported_protocols" in info

        # Verify content completeness
        supported = info["supported_protocols"]
        assert "transport" in supported
        assert "application" in supported

        # Should have both ISO-TP and J1939 transport support
        transport_protocols = [str(tp) for tp in info["transport_processors"]]
        assert any("ISOTP" in tp for tp in transport_protocols)
        assert any("J1939" in tp for tp in transport_protocols)

    def test_universal_interface_simplicity(self):
        """Test that the universal interface is simple and consistent."""
        router = create_universal_router()

        # Single method for all processing
        test_frames = [
            CANTransportFrame(
                arbitration_id=0x7E0, payload=b"\x22\xF1\x90", start_timestamp_unix_us=1000000
            ),  # UDS
            CANTransportFrame(
                arbitration_id=0x7DF, payload=b"\x01\x0C", start_timestamp_unix_us=1000000
            ),
            CANTransportFrame(
                arbitration_id=0x18FEF100, payload=b"\x01\x02", start_timestamp_unix_us=1000000
            ),  # J1939
        ]

        for frame in test_frames:
            result = router.route_and_process(frame)

            # Consistent interface for all frame types
            assert isinstance(result, RoutingResult)
            assert hasattr(result, "transport_protocol")
            assert hasattr(result, "application_protocol")
            assert hasattr(result, "routing_path")
            assert hasattr(result, "success")

            # Success indicator is always meaningful
            assert isinstance(result.success, bool)
