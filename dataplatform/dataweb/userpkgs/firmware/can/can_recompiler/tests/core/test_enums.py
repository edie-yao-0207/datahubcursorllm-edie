"""
Tests for enums.py module.

This module contains tests for the enums used throughout the package,
including:
- TransportProtocolType
- DataIdentifierType
"""

from ...core.enums import TransportProtocolType, ApplicationProtocolType

import pytest


@pytest.mark.parametrize(
    "enum_value,expected_str",
    [
        (TransportProtocolType.NONE, "NONE"),
        (TransportProtocolType.ISOTP, "ISOTP"),
        (TransportProtocolType.J1939, "J1939"),
        (ApplicationProtocolType.NONE, "NONE"),
        (ApplicationProtocolType.UDS, "UDS"),
        (ApplicationProtocolType.J1939, "J1939"),
    ],
)
def test_enum_str_methods(enum_value, expected_str) -> None:
    """Test string representation of enums."""
    assert str(enum_value) == expected_str


@pytest.mark.parametrize(
    "enum_value,description",
    [
        (TransportProtocolType.ISOTP, "transport ISOTP"),
        (TransportProtocolType.J1939, "transport J1939"),
        (ApplicationProtocolType.UDS, "application UDS"),
        (ApplicationProtocolType.J1939, "application J1939"),
    ],
)
def test_enum_properties_comprehensive(enum_value, description) -> None:
    """Test comprehensive enum properties and methods."""
    # Test all enums have expected properties
    assert isinstance(enum_value.value, int), f"Value check failed for {description}"
    assert isinstance(enum_value.name, str), f"Name check failed for {description}"
    assert isinstance(str(enum_value), str), f"String conversion failed for {description}"
    # These core enums are simple and don't have to_dict_entry method


@pytest.mark.parametrize(
    "enum_pair,should_be_equal",
    [
        ((TransportProtocolType.ISOTP, TransportProtocolType.ISOTP), True),
        ((TransportProtocolType.ISOTP, TransportProtocolType.J1939), False),
        ((ApplicationProtocolType.UDS, ApplicationProtocolType.UDS), True),
        ((ApplicationProtocolType.UDS, ApplicationProtocolType.J1939), False),
    ],
)
def test_enum_comparisons(enum_pair, should_be_equal) -> None:
    """Test enum comparison operations."""
    enum1, enum2 = enum_pair
    if should_be_equal:
        assert enum1 == enum2
    else:
        assert enum1 != enum2


def test_enum_collections() -> None:
    """Test enum usage in collections."""
    # Test enums in sets
    transport_set = {TransportProtocolType.ISOTP, TransportProtocolType.J1939}
    assert TransportProtocolType.ISOTP in transport_set

    app_set = {ApplicationProtocolType.UDS, ApplicationProtocolType.J1939}
    assert ApplicationProtocolType.UDS in app_set


def test_enum_serialization() -> None:
    """Test enum serialization for coverage."""
    # Test enum value extraction
    isotp_val = TransportProtocolType.ISOTP.value
    assert isinstance(isotp_val, int)

    uds_val = ApplicationProtocolType.UDS.value
    assert isinstance(uds_val, int)
