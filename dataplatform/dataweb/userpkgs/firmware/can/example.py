#!/usr/bin/env python3
"""
CAN Recompiler Library - Executable Example

This runnable example demonstrates import handling, error cases,
and protocol enumeration that's difficult to show in README snippets.
Run with: python example.py
"""

import os
import sys

# Enable debug logging for demonstration
os.environ["CAN_RECOMPILER_DEBUG"] = "true"

# Handle import flexibility for different environments
try:
    from can_recompiler import CANFrameProcessor, TransportProtocolType, ApplicationProtocolType

    import_source = "installed package"
except ImportError:
    # Fallback to local development imports
    sys.path.insert(0, "can_recompiler")
    try:
        from can_recompiler import CANFrameProcessor, TransportProtocolType, ApplicationProtocolType

        import_source = "local development"
    except ImportError as e:
        print(f"❌ Could not import can_recompiler: {e}")
        print("Run: cd can_recompiler && make setup && make test")
        sys.exit(1)


def run_comprehensive_example():
    """Comprehensive example with error handling and edge cases."""
    print(f"🔧 Running CAN Recompiler Example (via {import_source})")

    processor = CANFrameProcessor()

    # Test cases: successful parsing, incomplete sessions, error conditions
    test_cases = [
        {
            "name": "UDS Single Frame",
            "data": {
                "trace_uuid": "test-trace-1",
                "start_timestamp_unix_us": 1640995200000000,
                "date": "2025-01-01",
                "org_id": 12345,
                "device_id": 67890,
                "id": 0x7E8,
                "payload": bytes([0x03, 0x62, 0x01, 0x23]),
                "source_interface": "can0",
                "direction": 1,
                "transport_id": TransportProtocolType.ISOTP.value,
            },
        },
        {
            "name": "J1939 Frame",
            "data": {
                "trace_uuid": "test-trace-2",
                "start_timestamp_unix_us": 1640995201000000,
                "date": "2025-01-01",
                "org_id": 12345,
                "device_id": 67890,
                "id": 0x18FEE600,
                "payload": bytes([0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]),
                "source_interface": "can0",
                "direction": 1,
                "transport_id": TransportProtocolType.J1939.value,
            },
        },
    ]

    for i, test_case in enumerate(test_cases, 1):
        print(f"\n{i}. Testing {test_case['name']}:")
        try:
            result = processor.parse_frame(test_case["data"])
            if result:
                print(f"   ✅ Transport: {result.transport_id.name}")
                print(f"   ✅ Application: {result.application_id.name}")
                print(
                    f"   ✅ Data ID: 0x{result.data_id:04X}"
                    if result.data_id
                    else "   ✅ Data ID: None"
                )
            else:
                print("   ⏳ Multi-frame session in progress")
        except Exception as e:
            print(f"   ❌ Error: {e}")


def show_protocol_enums():
    """Display available protocol enumerations."""
    print("\n📋 Available Protocol Types:")

    print("\nTransport Protocols:")
    for tp in TransportProtocolType:
        print(f"   {tp.name} = {tp.value}")

    print("\nApplication Protocols:")
    for ap in ApplicationProtocolType:
        print(f"   {ap.name} = {ap.value}")


if __name__ == "__main__":
    run_comprehensive_example()
    show_protocol_enums()
