"""
Tests for Application Protocol Router

Tests the improved routing architecture that's properly separated from
transport-specific concerns.
"""

import pytest
from typing import Optional
from unittest.mock import Mock

from ....protocols.routing.application_router import (
    ApplicationProtocolRouter,
    create_application_router,
)
from ....protocols.routing.routing_rules import RoutingRule
from ....protocols.routing.protocol_registry import ProtocolRegistry
from ....protocols.isotp.frames import ISOTPFrame


class MockApplicationProcessor:
    """Mock processor for testing router functionality."""

    def __init__(self, name: str, should_succeed: bool = True):
        self.name = name
        self.should_succeed = should_succeed
        self.processed_frames = []
        self.cleanup_calls = []

    def process(self, transport_frame) -> Optional[object]:
        """Mock process method."""
        self.processed_frames.append(transport_frame)
        if self.should_succeed:
            return f"{self.name}_result"
        return None

    def cleanup(self, start_timestamp_unix_us: int) -> None:
        """Mock cleanup method."""
        self.cleanup_calls.append(start_timestamp_unix_us)


class TestApplicationProtocolRouter:
    """Test the improved application protocol router architecture."""

    def test_router_with_clean_separation(self):
        """Test that router properly separates routing from transport concerns."""
        # Create router with default setup
        router = create_application_router()

        # Should have default rules and processors
        routing_info = router.get_routing_info()
        assert len(routing_info["routing_rules"]) > 0
        assert len(routing_info["registered_processors"]) > 0

        # Should have expected protocols
        assert "uds" in routing_info["registered_processors"]

    @pytest.mark.parametrize(
        "frame_config,expected_processor,description",
        [
            # UDS frames
            ({"source_address": 0x7E0, "payload": b"\x22\xF1\x90"}, "uds", "UDS service"),
            ({"source_address": 0x700, "payload": b"\x2E\xF1\x90\x01"}, "uds", "UDS write"),
        ],
    )
    def test_transport_agnostic_routing(
        self, frame_config: dict, expected_processor: str, description: str
    ):
        """Test that router works with any transport frame type."""
        router = ApplicationProtocolRouter()

        # Register mock processors to avoid actual processing
        uds_proc = MockApplicationProcessor("uds")
        router.register_processor("uds", uds_proc)

        # Create ISO-TP frame (could be any transport)
        frame = ISOTPFrame(start_timestamp_unix_us=1000000, **frame_config)

        result = router.route_frame(frame)

        # Should route correctly regardless of transport type
        assert result == f"{expected_processor}_result", f"Wrong routing for {description}"

    def test_routing_audit_functionality(self):
        """Test the routing audit feature for debugging."""
        router = ApplicationProtocolRouter()

        # Register mock processors
        uds_proc = MockApplicationProcessor("uds")
        router.register_processor("uds", uds_proc)

        # Create test frame
        frame = ISOTPFrame(
            source_address=0x7E0, payload=b"\x22\xF1\x90", start_timestamp_unix_us=1000000
        )

        # Route with audit
        audit = router.route_frame_with_audit(frame)

        # Should have detailed audit information
        assert "result" in audit
        assert "evaluated_rules" in audit
        assert "matched_rule" in audit
        assert "processor_used" in audit

        # Should show successful routing
        assert audit["result"] == "uds_result"
        assert audit["matched_rule"] is not None
        assert audit["processor_used"] == "uds"
        assert len(audit["evaluated_rules"]) > 0

    def test_registry_separation(self):
        """Test that processor registry is properly separated."""
        # Create separate registry and rules
        registry = ProtocolRegistry()
        custom_rules = []

        # Create router with custom components
        router = ApplicationProtocolRouter(registry=registry, routing_rules=custom_rules)

        # Should start empty
        assert len(router.registry.list_registered()) == 0
        assert len(router.routing_rules) == 0

        # Add processor and rule
        test_proc = MockApplicationProcessor("test")
        router.register_processor("test", test_proc)

        test_rule = RoutingRule(
            name="Test_Rule",
            priority=10,
            address_ranges=[(0x100, 0x200)],
            payload_validators=[lambda p: True],
            processor_key="test",
            description="Test rule",
        )
        router.add_routing_rule(test_rule)

        # Should be properly registered
        assert "test" in router.registry.list_registered()
        assert len(router.routing_rules) == 1

    def test_no_transport_coupling(self):
        """Test that routing has no coupling to specific transport types."""
        router = ApplicationProtocolRouter()

        # Create mock transport frame (not ISO-TP specific)
        class MockTransportFrame:
            def __init__(self, source_address: int, payload: bytes):
                self.source_address = source_address
                self.payload = payload
                self.start_timestamp_unix_us = 1000000

        # Register mock processor
        mock_proc = MockApplicationProcessor("mock")
        router.register_processor("uds", mock_proc)

        # Create mock frame
        mock_frame = MockTransportFrame(source_address=0x700, payload=b"\x22\xF1\x90")

        # Should route successfully regardless of transport type
        result = router.route_frame(mock_frame)
        assert result == "mock_result"

        # Processor should receive the frame
        assert len(mock_proc.processed_frames) == 1
        assert mock_proc.processed_frames[0] == mock_frame

    def test_routing_info_completeness(self):
        """Test that routing info provides complete debugging information."""
        router = create_application_router()
        info = router.get_routing_info()

        # Should have all expected sections
        expected_keys = [
            "routing_rules",
            "registered_processors",
            "processor_count",
            "processor_types",
        ]

        for key in expected_keys:
            assert key in info, f"Missing key: {key}"

        # Rule info should be complete
        if info["routing_rules"]:
            rule = info["routing_rules"][0]
            rule_keys = ["name", "priority", "processor_key", "description", "address_ranges"]
            for key in rule_keys:
                assert key in rule, f"Missing rule key: {key}"
