"""
End-to-End Integration Tests for CAN Recompiler

These tests demonstrate realistic scenarios where multiple CAN frames 
are processed through the complete pipeline to produce final data rows.
Shows the full journey: Raw CAN frames → Transport processing → 
Application parsing → Spark-ready output.
"""

import pytest
from typing import List, Dict, Any, Optional

from ...core.frame_types import CANTransportFrame
from ...infra.spark.processor import SparkCANProtocolProcessor
from ...infra.spark.frame_types import InputFrameData, convert_input_to_processing_frame
from ...core.enums import TransportProtocolType, ApplicationProtocolType

# Test case structure: (scenario_name, input_frames, expected_results)
END_TO_END_SCENARIOS = [
    (
        "UDS Multi-Frame Read Data Response",
        # Input frames: UDS multi-frame response for VIN (0xF190)
        [
            {
                "id": 0x7E8,
                "payload": b"\x10\x14\x62\xF1\x90\x31\x48\x47",
                "start_timestamp_unix_us": 1000000,
                "trace_uuid": "test-uds-multi",
                "date": "2023-01-01",
                "org_id": 12345,
                "device_id": 67890,
                "source_interface": "can0",
                "direction": 1,
                "frame_id": 1,
            },
            {
                "id": 0x7E8,
                "payload": b"\x21\x43\x4D\x38\x32\x36\x33\x33",
                "start_timestamp_unix_us": 1001000,
                "trace_uuid": "test-uds-multi",
                "date": "2023-01-01",
                "org_id": 12345,
                "device_id": 67890,
                "source_interface": "can0",
                "direction": 1,
                "frame_id": 2,
            },
            {
                "id": 0x7E8,
                "payload": b"\x22\x41\x31\x32\x33\x34\x35\x36",
                "start_timestamp_unix_us": 1002000,
                "trace_uuid": "test-uds-multi",
                "date": "2023-01-01",
                "org_id": 12345,
                "device_id": 67890,
                "source_interface": "can0",
                "direction": 1,
                "frame_id": 3,
            },
        ],
        # Expected: Single complete UDS response
        [
            {
                "start_timestamp_unix_us": 1002000,  # Last frame timestamp
                "payload": b"\x31\x48\x47\x43\x4D\x38\x32\x36\x33\x33\x41\x31\x32\x33\x34\x35\x36",  # VIN data
                "payload_length": 17,  # Actual length after protocol parsing
                "trace_uuid": "test-uds-multi",
                "date": "2023-01-01",
                "org_id": 12345,
                "device_id": 67890,
                "source_interface": "can0",
                "direction": 1,
                "transport_id": TransportProtocolType.ISOTP.value,
                "application_id": ApplicationProtocolType.UDS.value,
                "application": {
                    "uds": {
                        "service_id": 0x22,  # Base request service ID, not response service ID
                        "data_identifier": 0xF190,  # Just the DID, not embedded with service
                        "is_response": True,
                        "is_negative_response": False,
                        "source_address": 0x7E8,
                        "frame_count": 3,
                        "expected_length": 20,  # Total transport payload length
                        "data_identifier_type": 1,  # UDSDataIdentifierType.DID
                    },
                    "j1939": None,
                    "none": None,
                },
            }
        ],
    ),
    (
        "OBD-II PID Response",
        [
            {
                "id": 0x7E8,
                "payload": b"\x04\x41\x0C\x1A\xF8",  # Mode 0x41, PID 0x0C, RPM=0x1AF8
                "start_timestamp_unix_us": 1000000,
                "trace_uuid": "test-uds",
                "date": "2023-01-01",
                "org_id": 12345,
                "device_id": 67890,
                "source_interface": "can0",
                "direction": 1,
                "frame_id": 1,
            }
        ],
        [
            {
                "start_timestamp_unix_us": 1000000,
                "payload": b"\x1A\xF8",  # Just RPM data (mode/PID stripped)
                "payload_length": 2,  # Actual payload after mode/PID stripped
                "trace_uuid": "test-uds",
                "date": "2023-01-01",
                "org_id": 12345,
                "device_id": 67890,
                "source_interface": "can0",
                "direction": 1,
                "transport_id": TransportProtocolType.ISOTP.value,
                "application_id": ApplicationProtocolType.UDS.value,
                "application": {
                    "uds": {
                        "service_id": 0x01,  # OBD-II mode 0x01 (converted from response 0x41)
                        "data_identifier": 0x0C,  # PID 0x0C
                        "is_response": True,
                        "source_address": 0x7E8,
                        "frame_count": 1,
                        "expected_length": 4,  # Total transport payload length
                        "data_identifier_type": 8,  # OBD_PID enum value
                    },
                    "j1939": None,
                    "none": None,
                },
            }
        ],
    ),
    (
        "OBD-II Service 0x03 - Request DTCs",
        [
            {
                "id": 0x7E8,
                "payload": b"\x01\x43",  # Mode 0x43 (0x03 + 0x40 response), no parameters
                "start_timestamp_unix_us": 1000000,
                "trace_uuid": "test-obd-03",
                "date": "2023-01-01",
                "org_id": 12345,
                "device_id": 67890,
                "source_interface": "can0",
                "direction": 1,
                "frame_id": 1,
            }
        ],
        [
            {
                "start_timestamp_unix_us": 1000000,
                "payload": b"",  # No data after service ID (service-only operation)
                "payload_length": 0,  # No data after service ID
                "trace_uuid": "test-obd-03",
                "date": "2023-01-01",
                "org_id": 12345,
                "device_id": 67890,
                "source_interface": "can0",
                "direction": 1,
                "transport_id": TransportProtocolType.ISOTP.value,
                "application_id": ApplicationProtocolType.UDS.value,
                "application": {
                    "uds": {
                        "service_id": 0x03,  # OBD-II mode 0x03 (converted from response 0x43)
                        "data_identifier": None,  # No data identifier for service-only operations
                        "is_response": True,
                        "source_address": 0x7E8,
                        "frame_count": 1,
                        "expected_length": 1,  # Total transport payload length
                        "data_identifier_type": 6,  # SERVICE_ONLY enum value
                    },
                    "j1939": None,
                    "none": None,
                },
            }
        ],
    ),
    (
        "OBD-II Service 0x07 - Request Pending DTCs",
        [
            {
                "id": 0x7E8,
                "payload": b"\x01\x47",  # Mode 0x47 (0x07 + 0x40 response), no parameters
                "start_timestamp_unix_us": 1000000,
                "trace_uuid": "test-obd-07",
                "date": "2023-01-01",
                "org_id": 12345,
                "device_id": 67890,
                "source_interface": "can0",
                "direction": 1,
                "frame_id": 1,
            }
        ],
        [
            {
                "start_timestamp_unix_us": 1000000,
                "payload": b"",  # No data after service ID (service-only operation)
                "payload_length": 0,  # No data after service ID
                "trace_uuid": "test-obd-07",
                "date": "2023-01-01",
                "org_id": 12345,
                "device_id": 67890,
                "source_interface": "can0",
                "direction": 1,
                "transport_id": TransportProtocolType.ISOTP.value,
                "application_id": ApplicationProtocolType.UDS.value,
                "application": {
                    "uds": {
                        "service_id": 0x07,  # OBD-II mode 0x07 (converted from response 0x47)
                        "data_identifier": None,  # No data identifier for service-only operations
                        "is_response": True,
                        "source_address": 0x7E8,
                        "frame_count": 1,
                        "expected_length": 1,  # Total transport payload length
                        "data_identifier_type": 6,  # SERVICE_ONLY enum value
                    },
                    "j1939": None,
                    "none": None,
                },
            }
        ],
    ),
    (
        "OBD-II Service 0x0A - Request Permanent DTCs",
        [
            {
                "id": 0x7E8,
                "payload": b"\x01\x4A",  # Mode 0x4A (0x0A + 0x40 response), no parameters
                "start_timestamp_unix_us": 1000000,
                "trace_uuid": "test-obd-0A",
                "date": "2023-01-01",
                "org_id": 12345,
                "device_id": 67890,
                "source_interface": "can0",
                "direction": 1,
                "frame_id": 1,
            }
        ],
        [
            {
                "start_timestamp_unix_us": 1000000,
                "payload": b"",  # No data after service ID (service-only operation)
                "payload_length": 0,  # No data after service ID
                "trace_uuid": "test-obd-0A",
                "date": "2023-01-01",
                "org_id": 12345,
                "device_id": 67890,
                "source_interface": "can0",
                "direction": 1,
                "transport_id": TransportProtocolType.ISOTP.value,
                "application_id": ApplicationProtocolType.UDS.value,
                "application": {
                    "uds": {
                        "service_id": 0x0A,  # OBD-II mode 0x0A (converted from response 0x4A)
                        "data_identifier": None,  # No data identifier for service-only operations
                        "is_response": True,
                        "source_address": 0x7E8,
                        "frame_count": 1,
                        "expected_length": 1,  # Total transport payload length
                        "data_identifier_type": 6,  # SERVICE_ONLY enum value
                    },
                    "j1939": None,
                    "none": None,
                },
            }
        ],
    ),
    (
        "OBD-II Service 0x03 Request (Not Response)",
        [
            {
                "id": 0x7DF,
                "payload": b"\x01\x03",  # Mode 0x03 request, no parameters
                "start_timestamp_unix_us": 1000000,
                "trace_uuid": "test-obd-03-req",
                "date": "2023-01-01",
                "org_id": 12345,
                "device_id": 67890,
                "source_interface": "can0",
                "direction": 1,
                "frame_id": 1,
            }
        ],
        [
            # No results expected - UDS processor filters out requests
        ],
    ),
    (
        "OBD-II Service 0x02 Response - Show Freeze Frame Data",
        [
            {
                "id": 0x7E8,
                "payload": b"\x04\x42\x0C\x1A\xF8",  # Mode 0x42 (0x02 + 0x40), PID 0x0C, RPM=0x1AF8
                "start_timestamp_unix_us": 1000000,
                "trace_uuid": "test-obd-02-resp",
                "date": "2023-01-01",
                "org_id": 12345,
                "device_id": 67890,
                "source_interface": "can0",
                "direction": 1,
                "frame_id": 1,
            }
        ],
        [
            {
                "start_timestamp_unix_us": 1000000,
                "payload": b"\x1A\xF8",  # Data after service ID and PID stripped
                "payload_length": 2,  # Actual payload after service ID and PID stripped
                "trace_uuid": "test-obd-02-resp",
                "date": "2023-01-01",
                "org_id": 12345,
                "device_id": 67890,
                "source_interface": "can0",
                "direction": 1,
                "transport_id": 1,  # ISOTP
                "application_id": 1,  # UDS
                "application": {
                    "uds": {
                        "service_id": 0x02,  # OBD-II mode 0x02 (converted from response 0x42)
                        "data_identifier": 0x0C,  # PID 0x0C
                        "is_response": True,
                        "source_address": 0x7E8,
                        "frame_count": 1,
                        "expected_length": 4,  # Total transport payload length
                        "data_identifier_type": 8,  # OBD_PID enum value
                    },
                    "j1939": None,
                    "none": None,
                },
            }
        ],
    ),
    (
        "OBD-II Service 0x04 Response - Clear DTCs",
        [
            {
                "id": 0x7E8,
                "payload": b"\x01\x44",  # Mode 0x44 (0x04 + 0x40), no parameters
                "start_timestamp_unix_us": 1000000,
                "trace_uuid": "test-obd-04-resp",
                "date": "2023-01-01",
                "org_id": 12345,
                "device_id": 67890,
                "source_interface": "can0",
                "direction": 1,
                "frame_id": 1,
            }
        ],
        [
            {
                "start_timestamp_unix_us": 1000000,
                "payload": b"",  # No data after service ID (service-only operation)
                "payload_length": 0,  # No payload after service ID
                "trace_uuid": "test-obd-04-resp",
                "date": "2023-01-01",
                "org_id": 12345,
                "device_id": 67890,
                "source_interface": "can0",
                "direction": 1,
                "transport_id": 1,  # ISOTP
                "application_id": 1,  # UDS
                "application": {
                    "uds": {
                        "service_id": 0x04,  # OBD-II mode 0x04 (converted from response 0x44)
                        "data_identifier": None,  # No data identifier for service-only operations
                        "is_response": True,
                        "source_address": 0x7E8,
                        "frame_count": 1,
                        "expected_length": 1,  # Total transport payload length
                        "data_identifier_type": 6,  # SERVICE_ONLY enum value
                    },
                    "j1939": None,
                    "none": None,
                },
            }
        ],
    ),
    (
        "OBD-II Service 0x05 Response - Test Results",
        [
            {
                "id": 0x7E8,
                "payload": b"\x03\x45\x01\x00",  # Mode 0x45 (0x05 + 0x40), PID 0x01, test result=0x00
                "start_timestamp_unix_us": 1000000,
                "trace_uuid": "test-obd-05-resp",
                "date": "2023-01-01",
                "org_id": 12345,
                "device_id": 67890,
                "source_interface": "can0",
                "direction": 1,
                "frame_id": 1,
            }
        ],
        [
            {
                "start_timestamp_unix_us": 1000000,
                "payload": b"\x00",  # Test result data after service ID and PID stripped
                "payload_length": 1,  # Actual payload after service ID and PID stripped
                "trace_uuid": "test-obd-05-resp",
                "date": "2023-01-01",
                "org_id": 12345,
                "device_id": 67890,
                "source_interface": "can0",
                "direction": 1,
                "transport_id": 1,  # ISOTP
                "application_id": 1,  # UDS
                "application": {
                    "uds": {
                        "service_id": 0x05,  # OBD-II mode 0x05 (converted from response 0x45)
                        "data_identifier": 0x01,  # PID 0x01
                        "is_response": True,
                        "source_address": 0x7E8,
                        "frame_count": 1,
                        "expected_length": 3,  # Total transport payload length
                        "data_identifier_type": 8,  # OBD_PID enum value
                    },
                    "j1939": None,
                    "none": None,
                },
            }
        ],
    ),
    (
        "OBD-II Service 0x06 Response - Test Results",
        [
            {
                "id": 0x7E8,
                "payload": b"\x03\x46\x01\x00",  # Mode 0x46 (0x06 + 0x40), PID 0x01, test result=0x00
                "start_timestamp_unix_us": 1000000,
                "trace_uuid": "test-obd-06-resp",
                "date": "2023-01-01",
                "org_id": 12345,
                "device_id": 67890,
                "source_interface": "can0",
                "direction": 1,
                "frame_id": 1,
            }
        ],
        [
            {
                "start_timestamp_unix_us": 1000000,
                "payload": b"\x00",  # Test result data after service ID and PID stripped
                "payload_length": 1,  # Actual payload after service ID and PID stripped
                "trace_uuid": "test-obd-06-resp",
                "date": "2023-01-01",
                "org_id": 12345,
                "device_id": 67890,
                "source_interface": "can0",
                "direction": 1,
                "transport_id": 1,  # ISOTP
                "application_id": 1,  # UDS
                "application": {
                    "uds": {
                        "service_id": 0x06,  # OBD-II mode 0x06 (converted from response 0x46)
                        "data_identifier": 0x01,  # PID 0x01
                        "is_response": True,
                        "source_address": 0x7E8,
                        "frame_count": 1,
                        "expected_length": 3,  # Total transport payload length
                        "data_identifier_type": 8,  # OBD_PID enum value
                    },
                    "j1939": None,
                    "none": None,
                },
            }
        ],
    ),
    (
        "OBD-II Service 0x08 Response - Control System Tests",
        [
            {
                "id": 0x7E8,
                "payload": b"\x03\x48\x01\x00",  # Mode 0x48 (0x08 + 0x40), PID 0x01, test result=0x00
                "start_timestamp_unix_us": 1000000,
                "trace_uuid": "test-obd-08-resp",
                "date": "2023-01-01",
                "org_id": 12345,
                "device_id": 67890,
                "source_interface": "can0",
                "direction": 1,
                "frame_id": 1,
            }
        ],
        [
            {
                "start_timestamp_unix_us": 1000000,
                "payload": b"\x00",  # Test result data after service ID and PID stripped
                "payload_length": 1,  # Actual payload after service ID and PID stripped
                "trace_uuid": "test-obd-08-resp",
                "date": "2023-01-01",
                "org_id": 12345,
                "device_id": 67890,
                "source_interface": "can0",
                "direction": 1,
                "transport_id": 1,  # ISOTP
                "application_id": 1,  # UDS
                "application": {
                    "uds": {
                        "service_id": 0x08,  # OBD-II mode 0x08 (converted from response 0x48)
                        "data_identifier": 0x01,  # PID 0x01
                        "is_response": True,
                        "source_address": 0x7E8,
                        "frame_count": 1,
                        "expected_length": 3,  # Total transport payload length
                        "data_identifier_type": 8,  # OBD_PID enum value
                    },
                    "j1939": None,
                    "none": None,
                },
            }
        ],
    ),
    (
        "OBD-II Service 0x09 Response - Vehicle Information",
        [
            {
                "id": 0x7E8,
                "payload": b"\x04\x49\x02\x48\x69",  # Mode 0x49 (0x09 + 0x40), PID 0x02, VIN="Hi"
                "start_timestamp_unix_us": 1000000,
                "trace_uuid": "test-obd-09-resp",
                "date": "2023-01-01",
                "org_id": 12345,
                "device_id": 67890,
                "source_interface": "can0",
                "direction": 1,
                "frame_id": 1,
            }
        ],
        [
            {
                "start_timestamp_unix_us": 1000000,
                "payload": b"\x48\x69",  # VIN data after service ID and PID stripped
                "payload_length": 2,  # Actual payload after service ID and PID stripped
                "trace_uuid": "test-obd-09-resp",
                "date": "2023-01-01",
                "org_id": 12345,
                "device_id": 67890,
                "source_interface": "can0",
                "direction": 1,
                "transport_id": 1,  # ISOTP
                "application_id": 1,  # UDS
                "application": {
                    "uds": {
                        "service_id": 0x09,  # OBD-II mode 0x09 (converted from response 0x49)
                        "data_identifier": 0x02,  # PID 0x02
                        "is_response": True,
                        "source_address": 0x7E8,
                        "frame_count": 1,
                        "expected_length": 4,  # Total transport payload length
                        "data_identifier_type": 8,  # OBD_PID enum value
                    },
                    "j1939": None,
                    "none": None,
                },
            }
        ],
    ),
    (
        "J1939 BAM Multi-Frame Message",
        # Input frames: J1939 BAM with 3 data frames
        [
            {
                "id": 0x18ECFF00,  # BAM Connection Management
                "payload": b"\x20\x09\x00\x03\xFF\xEE\xFE\x00",
                "start_timestamp_unix_us": 1000000,
                "trace_uuid": "test-j1939",
                "date": "2023-01-01",
                "org_id": 12345,
                "device_id": 67890,
                "source_interface": "can0",
                "direction": 1,
                "frame_id": 1,
            },
            {
                "id": 0x18EBFF00,  # Data Transfer 1
                "payload": b"\x01\x8C\x7D\x95\x42\x8F\x73\x9A",
                "start_timestamp_unix_us": 1001000,
                "trace_uuid": "test-j1939",
                "date": "2023-01-01",
                "org_id": 12345,
                "device_id": 67890,
                "source_interface": "can0",
                "direction": 1,
                "frame_id": 2,
            },
            {
                "id": 0x18EBFF00,  # Data Transfer 2
                "payload": b"\x02\x6B\x44\xC2\xFF\xFF\xFF\xFF",
                "start_timestamp_unix_us": 1002000,
                "trace_uuid": "test-j1939",
                "date": "2023-01-01",
                "org_id": 12345,
                "device_id": 67890,
                "source_interface": "can0",
                "direction": 1,
                "frame_id": 3,
            },
            {
                "id": 0x18EBFF00,  # Data Transfer 3
                "payload": b"\x03\x91\xFF\xFF\xFF\xFF\xFF\xFF",
                "start_timestamp_unix_us": 1003000,
                "trace_uuid": "test-j1939",
                "date": "2023-01-01",
                "org_id": 12345,
                "device_id": 67890,
                "source_interface": "can0",
                "direction": 1,
                "frame_id": 4,
            },
        ],
        # Expected: Single complete J1939 message
        [
            {
                "start_timestamp_unix_us": 1000000,  # BAM frame timestamp
                "payload": b"\x8C\x7D\x95\x42\x8F\x73\x9A\x6B\x44",  # 9 bytes reconstructed (corrected last byte)
                "payload_length": 9,
                "trace_uuid": "test-j1939",
                "date": "2023-01-01",
                "org_id": 12345,
                "device_id": 67890,
                "source_interface": "can0",
                "direction": 1,
                "transport_id": TransportProtocolType.J1939.value,
                "application_id": ApplicationProtocolType.J1939.value,
                "application": {
                    "j1939": {
                        "data_identifier_type": 6,  # J1939DataIdentifierType.NORMAL_71
                        "pgn": 0xFEEE,
                        "source_address": 0x00,
                        "destination_address": 0xFF,
                        "priority": 6,  # Default priority
                        "is_broadcast": True,
                        "frame_count": 3,  # Actual data frame count
                        "expected_length": 9,
                        "transport_protocol_used": 1,  # BAM
                    },
                    "uds": None,
                    "none": None,
                },
            }
        ],
    ),
    (
        "Mixed Protocol Batch",
        # Input frames: UDS, J1939, and unrecognized frames
        [
            {  # UDS negative response
                "id": 0x7E8,
                "payload": b"\x03\x7F\x22\x31",
                "start_timestamp_unix_us": 1000000,
                "trace_uuid": "test-mixed",
                "date": "2023-01-01",
                "org_id": 12345,
                "device_id": 67890,
                "source_interface": "can0",
                "direction": 1,
                "frame_id": 1,
            },
            {
                "id": 0x7E9,
                "payload": b"\x06\x41\x05\x7B\x41\x0F\x5D",
                "start_timestamp_unix_us": 1001000,
                "trace_uuid": "test-mixed",
                "date": "2023-01-01",
                "org_id": 12345,
                "device_id": 67890,
                "source_interface": "can0",
                "direction": 1,
                "frame_id": 2,
            },
            {  # J1939 single frame
                "id": 0x18FEF017,
                "payload": b"\x00\x4F\x00\xFF\xFF\xFF\xFF\xFF",
                "start_timestamp_unix_us": 1002000,
                "trace_uuid": "test-mixed",
                "date": "2023-01-01",
                "org_id": 12345,
                "device_id": 67890,
                "source_interface": "can0",
                "direction": 1,
                "frame_id": 3,
            },
            {  # Unrecognized frame
                "id": 0x123,
                "payload": b"\xFF\xFF\xFF\xFF",
                "start_timestamp_unix_us": 1003000,
                "trace_uuid": "test-mixed",
                "date": "2023-01-01",
                "org_id": 12345,
                "device_id": 67890,
                "source_interface": "can0",
                "direction": 1,
                "frame_id": 4,
            },
        ],
        [
            {
                "start_timestamp_unix_us": 1001000,
                "payload": b"\x7B\x41\x0F\x5D",  # PID data after mode/PID stripped
                "payload_length": 4,  # Data after mode/PID
                "trace_uuid": "test-mixed",
                "date": "2023-01-01",
                "org_id": 12345,
                "device_id": 67890,
                "source_interface": "can0",
                "direction": 1,
                "transport_id": TransportProtocolType.ISOTP.value,
                "application_id": ApplicationProtocolType.UDS.value,
                "application": {
                    "uds": {
                        "service_id": 0x01,  # OBD-II mode 0x01 (converted from response 0x41)
                        "data_identifier": 0x05,
                        "is_response": True,
                        "source_address": 0x7E9,
                        "frame_count": 1,
                        "expected_length": 6,  # Total transport payload length
                        "data_identifier_type": 8,  # OBD_PID
                    },
                    "j1939": None,
                    "none": None,
                },
            },
            {  # J1939 single frame
                "start_timestamp_unix_us": 1002000,
                "payload": b"\x00\x4F\x00\xFF\xFF\xFF\xFF\xFF",
                "payload_length": 8,
                "trace_uuid": "test-mixed",
                "date": "2023-01-01",
                "org_id": 12345,
                "device_id": 67890,
                "source_interface": "can0",
                "direction": 1,
                "transport_id": TransportProtocolType.J1939.value,
                "application_id": ApplicationProtocolType.J1939.value,
                "application": {
                    "j1939": {
                        "data_identifier_type": 6,  # J1939DataIdentifierType.NORMAL_71
                        "pgn": 0xFEF0,
                        "source_address": 0x17,
                        "destination_address": 255,  # Actual destination address from CAN ID
                        "priority": 6,  # Default
                        "is_broadcast": True,  # J1939 frame is detected as broadcast
                        "frame_count": 1,
                        "expected_length": 8,
                        "transport_protocol_used": None,  # No transport protocol used for single frames
                    },
                    "uds": None,
                    "none": None,
                },
            },
        ],
    ),
    (
        "Incomplete Multi-Frame Sequence",
        # Input frames: Start UDS multi-frame but never complete
        [
            {
                "id": 0x7E8,
                "payload": b"\x10\x20\x62\xF1\x86\x01\x02\x03",  # First frame only
                "start_timestamp_unix_us": 1000000,
                "trace_uuid": "test-incomplete",
                "date": "2023-01-01",
                "org_id": 12345,
                "device_id": 67890,
                "source_interface": "can0",
                "direction": 1,
                "frame_id": 1,
            },
            {  # Unrelated frame instead of continuation
                "id": 0x123,
                "payload": b"\xFF\xFF\xFF\xFF",
                "start_timestamp_unix_us": 2000000,
                "trace_uuid": "test-incomplete",
                "date": "2023-01-01",
                "org_id": 12345,
                "device_id": 67890,
                "source_interface": "can0",
                "direction": 1,
                "frame_id": 2,
            },
        ],
        # Expected: No results (incomplete UDS filtered out, NONE protocol returns None)
        [],
    ),
    (
        "Request Filtering and Error Recovery",
        # Input frames: Mix of requests (filtered) and valid responses
        [
            {  # UDS request (should be filtered)
                "id": 0x7E0,
                "payload": b"\x02\x22\xF1\x90",
                "start_timestamp_unix_us": 1000000,
                "trace_uuid": "test-filter",
                "date": "2023-01-01",
                "org_id": 12345,
                "device_id": 67890,
                "source_interface": "can0",
                "direction": 1,
                "frame_id": 1,
            },
            {
                "id": 0x7DF,
                "payload": b"\x02\x01\x0C",
                "start_timestamp_unix_us": 1001000,
                "trace_uuid": "test-filter",
                "date": "2023-01-01",
                "org_id": 12345,
                "device_id": 67890,
                "source_interface": "can0",
                "direction": 1,
                "frame_id": 2,
            },
            {  # Valid UDS multi-frame response
                "id": 0x7E8,
                "payload": b"\x10\x0F\x62\xF1\x90\x31\x48\x47",
                "start_timestamp_unix_us": 1002000,
                "trace_uuid": "test-filter",
                "date": "2023-01-01",
                "org_id": 12345,
                "device_id": 67890,
                "source_interface": "can0",
                "direction": 1,
                "frame_id": 3,
            },
            {
                "id": 0x7E8,
                "payload": b"\x21\x43\x4D\x38\x32\x36\x33\x00",
                "start_timestamp_unix_us": 1003000,
                "trace_uuid": "test-filter",
                "date": "2023-01-01",
                "org_id": 12345,
                "device_id": 67890,
                "source_interface": "can0",
                "direction": 1,
                "frame_id": 4,
            },
            {  # Invalid frame (empty payload)
                "id": 0x7E8,
                "payload": b"",
                "start_timestamp_unix_us": 1004000,
                "trace_uuid": "test-filter",
                "date": "2023-01-01",
                "org_id": 12345,
                "device_id": 67890,
                "source_interface": "can0",
                "direction": 1,
                "frame_id": 5,
            },
        ],
        # Expected: No results - frames with no usable application data are filtered out completely
        [],
    ),
    (
        "Metadata Propagation",
        # Input frames: Single J1939 frame to verify metadata flow
        [
            {
                "id": 0x18FEE048,
                "payload": b"\x19\x00\x4F\x00\xFF\xFF\xFF\xFF",
                "start_timestamp_unix_us": 1234567890,
                "trace_uuid": "test-metadata",
                "date": "2023-01-01",
                "org_id": 12345,
                "device_id": 67890,
                "source_interface": "can0",
                "direction": 1,
                "frame_id": 1,
            }
        ],
        # Expected: Single result with all metadata preserved
        [
            {
                "start_timestamp_unix_us": 1234567890,
                "payload": b"\x19\x00\x4F\x00\xFF\xFF\xFF\xFF",
                "payload_length": 8,
                "trace_uuid": "test-metadata",
                "date": "2023-01-01",
                "org_id": 12345,
                "device_id": 67890,
                "source_interface": "can0",
                "direction": 1,
                "transport_id": TransportProtocolType.J1939.value,
                "application_id": ApplicationProtocolType.J1939.value,
                "application": {
                    "j1939": {
                        "data_identifier_type": 5,  # J1939DataIdentifierType.DM_MESSAGES
                        "pgn": 0xFEE0,
                        "source_address": 0x48,
                        "destination_address": 255,  # Actual destination address from CAN ID
                        "priority": 6,  # Default
                        "is_broadcast": True,  # J1939 frame is broadcast
                        "frame_count": 1,
                        "expected_length": 8,
                        "transport_protocol_used": None,  # None
                    },
                    "uds": None,
                    "none": None,
                },
            }
        ],
    ),
]


class TestEndToEndScenarios:
    """Test realistic end-to-end CAN processing scenarios."""

    def setup_method(self):
        """Setup processor for each test."""
        self.processor = SparkCANProtocolProcessor()

    @pytest.mark.parametrize("scenario_name,input_frames,expected_results", END_TO_END_SCENARIOS)
    def test_end_to_end_scenarios(
        self, scenario_name: str, input_frames: List[Dict], expected_results: List[Dict]
    ):
        """
        Parameterized test for end-to-end CAN processing scenarios.

        Tests realistic scenarios where multiple CAN frames are processed
        through the complete pipeline to produce final data rows.
        """
        print(f"\n=== Testing Scenario: {scenario_name} ===")

        # Process each frame through the pipeline
        actual_results = []
        for frame_data in input_frames:
            input_data = InputFrameData(**frame_data)
            result = self.processor.process_frame_to_dict(input_data)
            if result:  # Only complete/valid messages return data
                actual_results.append(result)

        # Check we have the expected number of results
        assert len(actual_results) == len(
            expected_results
        ), f"Expected {len(expected_results)} results, got {len(actual_results)}"

        # Compare each result against expected
        for i, (actual, expected) in enumerate(zip(actual_results, expected_results)):
            print(f"\nResult {i+1} comparison:")
            print(f"Expected keys: {sorted(expected.keys())}")
            print(f"Actual keys: {sorted(actual.keys())}")

            # Deep comparison - compare each expected field
            self._assert_dict_matches(actual, expected, f"Result {i+1}")

    def _assert_dict_matches(self, actual: Dict[str, Any], expected: Dict[str, Any], context: str):
        """Helper to recursively compare dictionaries with useful error messages."""
        for key, expected_value in expected.items():
            assert (
                key in actual
            ), f"{context}: Missing key '{key}'. Available keys: {list(actual.keys())}"

            actual_value = actual[key]

            if isinstance(expected_value, dict):
                assert isinstance(
                    actual_value, dict
                ), f"{context}.{key}: Expected dict, got {type(actual_value)}"
                self._assert_dict_matches(actual_value, expected_value, f"{context}.{key}")
            elif isinstance(expected_value, bytes):
                assert isinstance(
                    actual_value, bytes
                ), f"{context}.{key}: Expected bytes, got {type(actual_value)}"
                assert (
                    actual_value == expected_value
                ), f"{context}.{key}: Expected {expected_value.hex()}, got {actual_value.hex()}"
            else:
                assert (
                    actual_value == expected_value
                ), f"{context}.{key}: Expected {expected_value!r}, got {actual_value!r}"

        print(f"✓ {context}: All expected fields match")
