"""Shared pytest fixtures for DataWeb userpkgs testing."""

import pytest
from dataweb.userpkgs.constants import Database
from dataweb.userpkgs.firmware.metric import Metric, StatMetadata


@pytest.fixture
def sample_kinesis_metric() -> Metric:
    """Create a sample metric for KinesisStats database testing."""
    metadata = StatMetadata(
        product_area="test_area", signal_name="test_signal", default_priority="high"
    )
    return Metric(
        type="kinesisstats.test_table",
        field="test_field",
        label="Test Metric",
        metadata=metadata,
    )


@pytest.fixture
def sample_kinesis_history_metric() -> Metric:
    """Create a sample metric for KinesisStats History database testing."""
    metadata = StatMetadata(
        product_area="test_area",
        signal_name="history_signal",
        default_priority="medium",
    )
    return Metric(
        type="kinesisstats_history.history_table",
        field="history_field",
        label="History Test Metric",
        metadata=metadata,
    )


@pytest.fixture
def sample_dataweb_metric() -> Metric:
    """Create a sample metric for DataWeb/product_analytics_staging database testing."""
    metadata = StatMetadata(
        product_area="analytics", signal_name="analytics_signal", default_priority="low"
    )
    return Metric(
        type="product_analytics_staging.test_analytics_table",
        field="analytics_field",
        label="Analytics Test Metric",
        metadata=metadata,
    )


@pytest.fixture
def mock_format_kwargs():
    """Standard format kwargs for testing."""
    return {
        "date_start": "2024-01-01",
        "date_end": "2024-01-31",
        "org_id": "test_org",
        "device_id": "test_device",
    }
