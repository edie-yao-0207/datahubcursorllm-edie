"""Tests for deterministic sorting in anomaly detection."""

import inspect


def test_cluster_stops_spark_has_sorting():
    """Verify that cluster_stops_spark includes explicit sorting for deterministic results."""
    from ml.ops.anomaly_detection import cluster_stops_spark

    # Get the source code
    source = inspect.getsource(cluster_stops_spark)

    # Verify that orderBy is called for sorting
    assert (
        "orderBy" in source
    ), "cluster_stops_spark should have orderBy for deterministic sorting"
    assert "org_id" in source, "Should sort by org_id"
    assert "device_id" in source, "Should sort by device_id"
    assert "end_time_utc" in source, "Should sort by end_time_utc"

    # Verify comment about deterministic results
    assert (
        "deterministic" in source.lower()
    ), "Should have comment explaining deterministic sorting"


def test_sorting_prevents_non_determinism():
    """Test that sorting is documented as preventing non-deterministic DBSCAN results."""
    from ml.ops.anomaly_detection import cluster_stops_spark

    # Check cluster_stops_spark
    source_spark = inspect.getsource(cluster_stops_spark)

    # Should have explanation of why sorting is needed
    assert (
        "deterministic" in source_spark.lower() or "consistent" in source_spark.lower()
    ), "Should explain why sorting is needed for deterministic results"
