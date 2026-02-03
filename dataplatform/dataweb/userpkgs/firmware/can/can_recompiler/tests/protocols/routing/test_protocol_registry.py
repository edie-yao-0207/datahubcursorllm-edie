"""
Tests for Protocol Registry System

Tests for the protocol registry that manages application protocol processors
and provides processor lookup and lifecycle management.
"""

import pytest
from unittest.mock import Mock, patch
from typing import Optional, Any

from ....protocols.routing.protocol_registry import ProtocolRegistry, create_default_registry
from ....protocols.isotp.frames import ISOTPFrame
from ....protocols.uds.frames import UDSMessage


class MockProcessor:
    """Mock processor for testing."""

    def __init__(self, should_fail: bool = False, cleanup_fail: bool = False):
        self.should_fail = should_fail
        self.cleanup_fail = cleanup_fail
        self.cleanup_called = False

    def process(self, transport_frame: Any) -> Optional[object]:
        if self.should_fail:
            raise ValueError("Mock processor error")
        return f"processed_{transport_frame}"

    def cleanup(self, start_timestamp_unix_us: int) -> None:
        self.cleanup_called = True
        if self.cleanup_fail:
            raise RuntimeError("Mock cleanup error")


class TestProtocolRegistry:
    """Test ProtocolRegistry functionality."""

    @pytest.fixture
    def registry(self):
        """Create a fresh registry for testing."""
        return ProtocolRegistry()

    @pytest.fixture
    def mock_processor(self):
        """Create a mock processor."""
        return MockProcessor()

    def test_registry_initialization(self, registry):
        """Test registry starts empty."""
        assert len(registry.processors) == 0
        assert registry.list_registered() == []

    def test_register_processor(self, registry, mock_processor):
        """Test processor registration."""
        registry.register("test_proc", mock_processor)
        assert "test_proc" in registry.processors
        assert registry.processors["test_proc"] is mock_processor
        assert "test_proc" in registry.list_registered()

    def test_register_duplicate_processor(self, registry, mock_processor):
        """Test registering duplicate processor overwrites existing."""
        # Register first time
        registry.register("test_proc", mock_processor)
        original_processor = registry.processors["test_proc"]

        # Register same key again
        new_processor = MockProcessor()
        registry.register("test_proc", new_processor)

        # Should have new processor (overwrites)
        assert registry.processors["test_proc"] is new_processor
        assert registry.processors["test_proc"] is not original_processor

    def test_get_processor_existing(self, registry, mock_processor):
        """Test getting existing processor."""
        registry.register("test_proc", mock_processor)

        result = registry.get("test_proc")
        assert result is mock_processor

    def test_get_processor_nonexistent(self, registry):
        """Test getting non-existent processor returns None."""
        result = registry.get("nonexistent")
        assert result is None

    def test_unregister_existing_processor(self, registry, mock_processor):
        """Test unregistering existing processor."""
        registry.register("test_proc", mock_processor)

        result = registry.unregister("test_proc")
        assert result is True
        assert "test_proc" not in registry.processors
        assert "test_proc" not in registry.list_registered()

    def test_unregister_nonexistent_processor(self, registry):
        """Test unregistering non-existent processor returns False."""
        result = registry.unregister("nonexistent")
        assert result is False

    def test_cleanup_all_success(self, registry):
        """Test cleanup all processors successfully."""
        proc1 = MockProcessor()
        proc2 = MockProcessor()

        registry.register("proc1", proc1)
        registry.register("proc2", proc2)

        registry.cleanup_all(123456789)

        assert proc1.cleanup_called is True
        assert proc2.cleanup_called is True

    def test_cleanup_all_with_failures(self, registry):
        """Test cleanup all processors with some failures."""
        proc1 = MockProcessor()
        proc2 = MockProcessor(cleanup_fail=True)  # This one will fail
        proc3 = MockProcessor()

        registry.register("proc1", proc1)
        registry.register("proc2", proc2)
        registry.register("proc3", proc3)

        # Should not raise exception, just log warnings
        registry.cleanup_all(123456789)

        assert proc1.cleanup_called is True
        assert proc2.cleanup_called is True
        assert proc3.cleanup_called is True

    def test_get_registry_info(self, registry):
        """Test getting registry information."""
        proc1 = MockProcessor()
        proc2 = MockProcessor()

        registry.register("proc1", proc1)
        registry.register("proc2", proc2)

        info = registry.get_registry_info()

        assert isinstance(info, dict)
        assert "registered_processors" in info
        assert "processor_count" in info
        assert "processor_types" in info

        assert set(info["registered_processors"]) == {"proc1", "proc2"}
        assert info["processor_count"] == 2
        assert len(info["processor_types"]) == 2

        # Check processor type info
        for proc_info in info["processor_types"]:
            assert "key" in proc_info
            assert "type" in proc_info
            assert "module" in proc_info
            assert proc_info["type"] == "MockProcessor"

    def test_multiple_operations(self, registry):
        """Test multiple registry operations in sequence."""
        proc1 = MockProcessor()
        proc2 = MockProcessor()
        proc3 = MockProcessor()

        # Register multiple
        registry.register("proc1", proc1)
        registry.register("proc2", proc2)
        registry.register("proc3", proc3)

        # Check list
        registered = registry.list_registered()
        assert set(registered) == {"proc1", "proc2", "proc3"}

        # Unregister one
        assert registry.unregister("proc2") is True
        assert set(registry.list_registered()) == {"proc1", "proc3"}

        # Get remaining
        assert registry.get("proc1") is proc1
        assert registry.get("proc2") is None
        assert registry.get("proc3") is proc3


class TestDefaultRegistryCreation:
    """Test default registry creation and built-in processors."""

    def test_create_default_registry(self):
        """Test creating default registry with built-in processors."""
        registry = create_default_registry()

        assert isinstance(registry, ProtocolRegistry)

        registered = registry.list_registered()
        assert "uds" in registered

        # Check processor types
        uds_proc = registry.get("uds")
        assert uds_proc is not None

    def test_uds_processor_functionality(self):
        """Test built-in UDS processor works correctly."""
        registry = create_default_registry()
        uds_proc = registry.get("uds")

        # Create sample ISOTP frame (UDS response)
        isotp_frame = ISOTPFrame(
            source_address=0x123,
            payload=b"\x62\x12\x34\x56\x78",  # Read Data By Identifier response (0x62 = 0x22 + 0x40)
            start_timestamp_unix_us=1000000,
        )

        result = uds_proc.process(isotp_frame)

        assert result is not None
        assert isinstance(result, UDSMessage)
        assert result.transport_frame is isotp_frame

    def test_uds_request_filtering(self):
        """Test that UDS requests are filtered out at application layer."""
        registry = create_default_registry()
        uds_proc = registry.get("uds")

        # Create UDS request frame (should be filtered out)
        request_frame = ISOTPFrame(
            source_address=0x123,
            payload=b"\x22\x12\x34",  # Read Data By Identifier request
            start_timestamp_unix_us=1000000,
        )

        result = uds_proc.process(request_frame)

        # Requests should be filtered out, returning None
        assert result is None

    def test_uds_negative_response_filtering(self):
        """Test that UDS negative responses are filtered out at application layer."""
        registry = create_default_registry()
        uds_proc = registry.get("uds")

        # Create UDS negative response frame (should be filtered out)
        negative_response_frame = ISOTPFrame(
            source_address=0x7E8,
            payload=b"\x7F\x22\x11",  # Negative Response: failed service 0x22, NRC 0x11
            start_timestamp_unix_us=1000000,
        )

        result = uds_proc.process(negative_response_frame)

        # Negative responses should be filtered out, returning None
        assert result is None

    def test_processor_cleanup(self):
        """Test built-in processors cleanup."""
        registry = create_default_registry()
        uds_proc = registry.get("uds")
        # Should not raise exceptions (stateless processors)
        uds_proc.cleanup(123456789)

    def test_registry_cleanup_all(self):
        """Test registry cleanup all processors."""
        registry = create_default_registry()

        # Should not raise exceptions
        registry.cleanup_all(123456789)

    def test_registry_info_with_default_processors(self):
        """Test registry info includes default processor information."""
        registry = create_default_registry()
        info = registry.get_registry_info()

        assert info["processor_count"] >= 1
        assert "uds" in info["registered_processors"]

        # Check that processor types are documented
        type_names = [proc_info["type"] for proc_info in info["processor_types"]]
        assert "UDSProcessor" in type_names
