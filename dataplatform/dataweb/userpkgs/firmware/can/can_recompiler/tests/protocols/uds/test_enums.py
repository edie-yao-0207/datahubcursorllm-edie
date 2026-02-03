"""
Tests for UDS protocol enums.

This module contains tests for UDS-specific enumerations.
"""

import pytest

from ....protocols.uds.enums import UDSDataIdentifierType


class TestUDSDataIdentifierType:
    """Test UDS data identifier type enumeration."""

    @pytest.mark.parametrize(
        "enum_value,expected_str",
        [
            (UDSDataIdentifierType.NONE, "NONE"),
            (UDSDataIdentifierType.DID, "DID"),
            (UDSDataIdentifierType.UDS_LOCAL_IDENTIFIER, "UDS_LOCAL_IDENTIFIER"),
            (UDSDataIdentifierType.ROUTINE, "ROUTINE"),
            (UDSDataIdentifierType.DTC_SUB, "DTC_SUB"),
        ],
    )
    def test_enum_str_methods(self, enum_value: UDSDataIdentifierType, expected_str: str):
        """Test string representation of UDS data identifier types."""
        assert str(enum_value) == expected_str

    @pytest.mark.parametrize(
        "enum_value,expected_value,description",
        [
            (UDSDataIdentifierType.NONE, 0, "No data identifier"),
            (UDSDataIdentifierType.DID, 1, "Data identifier"),
            (UDSDataIdentifierType.NRC, 2, "Negative Response Code"),
            (UDSDataIdentifierType.DTC_SUB, 3, "DTC subfunction"),
            (UDSDataIdentifierType.UDS_GENERIC, 4, "UDS generic"),
            (UDSDataIdentifierType.UDS_LOCAL_IDENTIFIER, 5, "UDS local identifier"),
            (UDSDataIdentifierType.SERVICE_ONLY, 6, "Service only"),
            (UDSDataIdentifierType.ROUTINE, 7, "Routine control identifier"),
        ],
    )
    def test_enum_values(
        self, enum_value: UDSDataIdentifierType, expected_value: int, description: str
    ):
        """Test UDS data identifier type enum values."""
        assert enum_value.value == expected_value, f"Value mismatch for {description}"
        assert isinstance(enum_value.value, int), f"Non-integer value for {description}"

    def test_enum_uniqueness(self):
        """Test that all UDS data identifier type enum values are unique."""
        enum_values = [e.value for e in UDSDataIdentifierType]
        assert len(enum_values) == len(set(enum_values)), "Duplicate enum values found"

    def test_enum_completeness(self):
        """Test enum completeness and expected members."""
        expected_members = {
            "NONE",
            "DID",
            "NRC",
            "DTC_SUB",
            "UDS_GENERIC",
            "UDS_LOCAL_IDENTIFIER",
            "SERVICE_ONLY",
            "ROUTINE",
            "OBD_PID",
        }
        actual_members = {e.name for e in UDSDataIdentifierType}
        assert actual_members == expected_members, "Enum members don't match expected set"

    @pytest.mark.parametrize(
        "enum_pair,should_be_equal",
        [
            ((UDSDataIdentifierType.DID, UDSDataIdentifierType.DID), True),
            ((UDSDataIdentifierType.DID, UDSDataIdentifierType.ROUTINE), False),
            ((UDSDataIdentifierType.NONE, UDSDataIdentifierType.NONE), True),
        ],
    )
    def test_enum_comparisons(self, enum_pair, should_be_equal: bool):
        """Test UDS data identifier type enum comparisons."""
        enum1, enum2 = enum_pair
        if should_be_equal:
            assert enum1 == enum2
            assert not (enum1 != enum2)
        else:
            assert enum1 != enum2
            assert not (enum1 == enum2)

    def test_enum_in_collections(self):
        """Test UDS data identifier types in collections."""
        # Test in sets
        identifier_set = {
            UDSDataIdentifierType.DID,
            UDSDataIdentifierType.ROUTINE,
            UDSDataIdentifierType.DTC_SUB,
        }
        assert UDSDataIdentifierType.DID in identifier_set
        assert UDSDataIdentifierType.NONE not in identifier_set

        # Test in lists
        identifier_list = [UDSDataIdentifierType.DID, UDSDataIdentifierType.ROUTINE]
        assert UDSDataIdentifierType.DID in identifier_list
        assert UDSDataIdentifierType.UDS_LOCAL_IDENTIFIER not in identifier_list

    def test_enum_serialization(self):
        """Test UDS data identifier type serialization."""
        # Test that enum values can be serialized/deserialized
        for enum_value in UDSDataIdentifierType:
            serialized = enum_value.value
            assert isinstance(serialized, int)

            # Test reconstruction from value
            reconstructed = UDSDataIdentifierType(serialized)
            assert reconstructed == enum_value
