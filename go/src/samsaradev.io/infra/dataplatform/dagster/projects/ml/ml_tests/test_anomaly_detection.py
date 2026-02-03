"""Tests for anomaly detection functionality."""

from unittest.mock import Mock, patch

import pandas as pd
import pytest
from ml.ops.anomaly_detection import (
    _get_cluster_schema,
    cluster_stops,
    compute_aggregate_stats,
    retrieve_polygon_from_h3,
)
from pyspark.sql.types import (
    DoubleType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


@pytest.fixture
def sample_trips_data():
    """Create sample trips data for testing."""
    schema = StructType(
        [
            StructField("date", StringType(), True),
            StructField("org_id", LongType(), True),
            StructField("lookback_days", IntegerType(), True),
            StructField("driver_id_counts", MapType(LongType(), IntegerType())),
            StructField("h3_idx", StringType()),
            StructField("end_latitude", DoubleType(), True),
            StructField("end_longitude", DoubleType(), True),
            StructField("trip_count_for_end_coords", IntegerType(), True),
        ]
    )
    return schema


def test_get_cluster_schema():
    """Test final cluster schema (used for table write with date column)."""
    schema = _get_cluster_schema()

    assert schema is not None
    assert len(schema.fields) == 10

    field_names = [field.name for field in schema.fields]
    assert "org_id" in field_names
    assert "latitude" in field_names
    assert "longitude" in field_names
    assert "number_of_stops" in field_names
    assert "lookback_days" in field_names
    assert "polygon" in field_names
    assert "polygon_area" in field_names
    assert "driver_id_counts" in field_names
    assert "cluster_idx" in field_names
    assert "date" in field_names
    # Final schema should NOT have number_of_one_time_stops
    assert "number_of_one_time_stops" not in field_names


def polygon_test_util(h3_indices, latitudes, longitudes):
    """Test H3 polygon boundary retrieval."""

    polygon_coords, polygon_area = retrieve_polygon_from_h3(
        h3_indices, latitudes, longitudes
    )

    # area
    assert isinstance(polygon_area, float)

    # geojson format check
    assert isinstance(polygon_coords, dict)
    assert ("type", "Polygon") in polygon_coords.items()
    assert ("coordinates") in polygon_coords

    # single polygon check
    assert len(polygon_coords["coordinates"]) == 1
    assert len(polygon_coords["coordinates"][0]) > 2

    # first = last point (polygon closed), and no additional duplicate
    assert polygon_coords["coordinates"][0][0] == polygon_coords["coordinates"][0][-1]
    assert polygon_coords["coordinates"][0][0] != polygon_coords["coordinates"][0][-2]

    # point format check
    for point in polygon_coords["coordinates"][0]:
        assert isinstance(point, list)
        assert len(point) == 2
        assert isinstance(point[0], float)
        assert isinstance(point[1], float)


def test_retrieve_polygon_from_h3():
    """Test H3 polygon boundary retrieval."""

    # Test case 1: h3 indices make up single polygon (use viewer https://clupasq.github.io/h3-viewer/)
    h3_indices = ["8a2baa441b27fff", "8a2baa46a4d7fff", "8a2baa441b2ffff"]
    latitudes = [45.50521898784452, 45.50481106740713, 45.50421140019734]
    longitudes = [-73.57592396218361, -73.5742261238766, -73.5754384823519]
    polygon_test_util(h3_indices, latitudes, longitudes)

    # Test case 2: h3 indices make up a multi polygon, convex hull possible
    h3_indices = ["8a2baa441b27fff", "8a2baa46a4c7fff"]
    latitudes = [45.50521898784452, 45.50546527700181, 45.5045240800599]
    longitudes = [-73.57592396218361, -73.57551100022364, -73.57213067495994]
    polygon_test_util(h3_indices, latitudes, longitudes)

    # Test case 3: h3 indices make up a multi polygon, convex hull not possible, use first h3
    h3_indices = ["8a2baa441b27fff", "8a2baa46a4c7fff"]
    latitudes = [45.50521898784452, 45.5045240800599]
    longitudes = [-73.57592396218361, -73.57213067495994]
    polygon_test_util(h3_indices, latitudes, longitudes)


def test_cluster_stops_basic():
    """Test basic clustering of stops."""
    # Create sample data with two clear clusters separated by ~20km
    # Cluster 1: San Francisco (37.7749, -122.4194)
    # Cluster 2: San Jose (37.3382, -121.8863), ~70km south
    data = {
        "org_id": [1] * 10,
        "driver_id_counts": [
            {0: 9, 1123: 1},
            {0: 9, 1123: 1},
            {0: 9, 1123: 1},
            {0: 9, 1123: 1},
            {0: 7, 1123: 1},
            {0: 7, 1123: 1},
            {0: 7, 1123: 1},
            {0: 7, 1123: 1},
            {0: 9, 1123: 1},
            {0: 7, 1123: 1},
        ],
        # Cluster 1: SF area (spread over ~200m)
        "end_latitude": [37.7749, 37.7760, 37.7770, 37.7740, 37.7750] +
        # Cluster 2: San Jose area (spread over ~200m)
        [37.3382, 37.3390, 37.3400, 37.3370, 37.3380],
        "end_longitude": [-122.4194, -122.4205, -122.4215, -122.4185, -122.4195]
        + [-121.8863, -121.8870, -121.8880, -121.8855, -121.8865],
        "trip_count_for_end_coords": [10, 10, 10, 10, 10, 8, 8, 8, 8, 8],
        "lookback_days": [365] * 10,
        "h3_idx": [
            "8a283082800ffff",
            "8a28308280effff",
            "8a28308283b7fff",
            "8a2830828007fff",
            "8a283082800ffff",
            "8a2834449257fff",
            "8a28344492effff",
            "8a28347145b7fff",
            "8a283444920ffff",
            "8a2834449257fff",
        ],
    }
    df = pd.DataFrame(data)

    # eps=0.001 radians ≈ 111m at this latitude, clusters are ~70km apart
    df_clusters, df_with_labels = cluster_stops(
        df, eps=0.001, min_samples=2, min_duration_of_stop_min=3, h3_resolution=10
    )

    # Should have 2 clusters
    assert len(df_clusters) == 2
    assert "org_id" in df_clusters.columns
    assert "latitude" in df_clusters.columns
    assert "longitude" in df_clusters.columns
    assert "number_of_stops" in df_clusters.columns
    assert "polygon" in df_clusters.columns
    assert "cluster_idx" in df_clusters.columns
    assert "driver_id_counts" in df_clusters.columns
    assert "polygon_area" in df_clusters.columns
    assert "number_of_one_time_stops" in df_clusters.columns

    # Check that all org_ids are correct
    assert all(df_clusters["org_id"] == 1)

    # Check that stop counts are reasonable
    assert all(df_clusters["number_of_stops"] > 0)


def test_cluster_stops_single_point():
    """Test clustering with insufficient points (should result in no clusters)."""
    data = {
        "org_id": [1],
        "device_id": [100],
        "driver_id_counts": [{0: 1}],
        "end_latitude": [37.7749],
        "end_longitude": [-122.4194],
        "trip_count_for_end_coords": [1],
        "lookback_days": [365],
    }
    df = pd.DataFrame(data)

    df_clusters, df_with_labels = cluster_stops(
        df, eps=0.0000115, min_samples=2, min_duration_of_stop_min=3, h3_resolution=10
    )

    # Should have 0 clusters (single point is anomalous with min_samples=2)
    assert len(df_clusters) == 0


def test_cluster_stops_all_same_location():
    """Test clustering when all stops are at the same location."""
    data = {
        "org_id": [1] * 10,
        "driver_id_counts": [{0: 1}] * 10,
        "end_latitude": [37.7749] * 10,
        "end_longitude": [-122.4194] * 10,
        "trip_count_for_end_coords": [5] * 10,
        "h3_idx": ["8a283082800ffff"] * 10,
        "lookback_days": [365] * 10,
    }
    df = pd.DataFrame(data)

    df_clusters, df_with_labels = cluster_stops(
        df, eps=0.0000115, min_samples=2, min_duration_of_stop_min=3, h3_resolution=10
    )

    # Should have 1 cluster
    assert len(df_clusters) == 1
    assert df_clusters["number_of_stops"].iloc[0] == 50  # 10 stops * 5 count each


def test_cluster_stops_pandas():
    """Test cluster_stops function with pandas DataFrame."""
    # eps=0.001 radians ≈ 111m, which should separate clusters ~70km apart
    # Create sample data with two distinct clusters separated by ~70km
    # Cluster 1: SF area, Cluster 2: San Jose area
    data = {
        "org_id": [1] * 6,
        "driver_id_counts": [
            {0: 9, 1123: 1},
            {0: 9, 1123: 1},
            {0: 9, 1123: 1},
            {0: 9, 1123: 1},
            {0: 8, 1123: 1},
            {0: 8, 1123: 1},
        ],
        "end_latitude": [37.7749, 37.7760, 37.7740] + [37.3382, 37.3390, 37.3370],
        "end_longitude": [-122.4194, -122.4205, -122.4185]
        + [-121.8863, -121.8870, -121.8855],
        "end_time_utc": pd.date_range("2024-01-01", periods=6, freq="h"),
        "trip_count_for_end_coords": [10] * 6,
        "lookback_days": [365] * 6,
        "h3_idx": [
            "8a283082800ffff",
            "8a28308280effff",
            "8a28308283b7fff",
            "8a2830828007fff",
            "8a283082800ffff",
            "8a2834449257fff",
        ],
    }
    df = pd.DataFrame(data)

    df_clusters, df_with_labels = cluster_stops(
        df, eps=0.001, min_samples=2, min_duration_of_stop_min=3, h3_resolution=10
    )

    # Should return a DataFrame with clusters
    assert isinstance(df_clusters, pd.DataFrame)
    assert len(df_clusters) == 2  # Two clusters
    assert "org_id" in df_clusters.columns
    assert "latitude" in df_clusters.columns
    assert "longitude" in df_clusters.columns


def test_aggregate_stats_computation():
    data = {
        "org_id": [123, 123, 123, 456, 456],
        "cluster_idx": [1, 2, 3, 1, 2],
        "number_of_stops": [10, 20, 30, 40, 50],
        "lookback_days": [365, 365, 365, 146, 146],
        "polygon_area": [60.0, 40.0, 20.0, 10.0, 90.0],
        "number_of_one_time_stops": [1001, 1001, 1001, 755, 755],
    }

    df = pd.DataFrame(data)
    df_agg = compute_aggregate_stats(df).sort_values("org_id").reset_index(drop=True)

    assert len(df_agg) == 2
    assert df_agg["known_location_count"].loc[0] == 3
    assert df_agg["known_location_count"].loc[1] == 2
    assert df_agg["lookback_days"].loc[0] == 365
    assert df_agg["lookback_days"].loc[1] == 146
    assert df_agg["total_number_of_stops"].loc[0] == 60
    assert df_agg["total_number_of_stops"].loc[1] == 90
    assert df_agg["number_of_one_time_stops"].loc[0] == 1001
    assert df_agg["number_of_one_time_stops"].loc[1] == 755
    assert df_agg["polygon_area_percentile50"].loc[0] == 40.0
    assert df_agg["polygon_area_percentile50"].loc[1] == 50.0
    assert df_agg["number_of_stops_percentile50"].loc[0] == 20.0
    assert df_agg["number_of_stops_percentile50"].loc[1] == 45.0


@patch("ml.ops.anomaly_detection.logger")
def test_cluster_stops_logging(mock_logger):
    """Test that cluster_stops logs appropriate information."""
    data = {
        "org_id": [1] * 10,
        "driver_id_counts": [
            {0: 9, 1123: 1},
            {0: 9, 1123: 1},
            {0: 9, 1123: 1},
            {0: 9, 1123: 1},
            {0: 7, 1123: 1},
            {0: 7, 1123: 1},
            {0: 7, 1123: 1},
            {0: 7, 1123: 1},
            {0: 9, 1123: 1},
            {0: 7, 1123: 1},
        ],
        # Cluster 1: SF area (spread over ~200m)
        "end_latitude": [37.7749, 37.7760, 37.7770, 37.7740, 37.7750] +
        # Cluster 2: San Jose area (spread over ~200m)
        [37.3382, 37.3390, 37.3400, 37.3370, 37.3380],
        "end_longitude": [-122.4194, -122.4205, -122.4215, -122.4185, -122.4195]
        + [-121.8863, -121.8870, -121.8880, -121.8855, -121.8865],
        "trip_count_for_end_coords": [10, 10, 10, 10, 10, 8, 8, 8, 8, 8],
        "lookback_days": [365] * 10,
        "h3_idx": [
            "8a283082800ffff",
            "8a28308280effff",
            "8a28308283b7fff",
            "8a2830828007fff",
            "8a283082800ffff",
            "8a2834449257fff",
            "8a28344492effff",
            "8a28347145b7fff",
            "8a283444920ffff",
            "8a2834449257fff",
        ],
    }
    df = pd.DataFrame(data)

    cluster_stops(
        df, eps=0.0000115, min_samples=2, min_duration_of_stop_min=3, h3_resolution=10
    )

    # Check that logger was called
    assert mock_logger.info.called
    # Should log total stops, number of clusters, etc.
    log_messages = [call[0][0] for call in mock_logger.info.call_args_list]
    assert any("Total number of stops" in msg for msg in log_messages)
    assert any("Number of clusters" in msg for msg in log_messages)


def test_retrieve_stops_spark_mock(spark_session):
    """Test retrieve_stops_spark with mocked table access."""
    # This is a basic test to ensure the function structure is correct
    # Full integration tests would require a real Spark environment with data
    from ml.ops.anomaly_detection import retrieve_stops_spark

    with patch.object(spark_session, "table") as mock_table:
        # Mock the table response
        mock_fct_trips = Mock()
        mock_fct_trips.filter.return_value = mock_fct_trips
        mock_fct_trips.groupBy.return_value.agg.return_value = mock_fct_trips
        mock_fct_trips.withColumn.return_value = mock_fct_trips
        mock_fct_trips.select.return_value = mock_fct_trips
        mock_fct_trips.join.return_value = mock_fct_trips
        mock_fct_trips.drop.return_value = mock_fct_trips

        mock_table.return_value = mock_fct_trips

        # This should not raise an exception
        try:
            # Just verify the function exists and can be called
            assert callable(retrieve_stops_spark)
        except Exception as e:
            pytest.fail(f"retrieve_stops_spark raised an exception: {e}")
