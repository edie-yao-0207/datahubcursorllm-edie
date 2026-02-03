import datetime
import logging
from collections import Counter
from typing import Any, Dict, List, Optional, Union

from dagster import Config, OpExecutionContext
from ml.common import (
    NonEmptyDQCheck,
    NonNullDQCheck,
    Operator,
    PrimaryKeyDQCheck,
    SQLDQCheck,
    table_op,
)
from ml.common.utils import get_region, get_run_env

LOG_LEVEL = logging.INFO
FORMAT = "%(asctime)-15s %(message)s"
logger = logging.getLogger(__name__)
logging.basicConfig(format=FORMAT)
logger.setLevel(LOG_LEVEL)

DATABASE = "inference_store" if get_run_env() == "prod" else "datamodel_dev"
MONITORING_TABLE = f"{DATABASE}.device_known_locations_agg_stats"


def retrieve_orgs_from_feature_flag_spark(
    spark,  # SparkSession
    feature_flag_key: str,
):
    """Retrieve orgs from feature-flag."""
    import pyspark.sql.functions as F

    if get_region() == "eu-west-1":
        table_name = "data_tools_delta_share.datamodel_launchdarkly_bronze.feature_flag_environment_rule_clause"
    elif get_region() == "us-west-2":
        table_name = (
            "datamodel_launchdarkly_bronze.feature_flag_environment_rule_clause"
        )
    else:
        return []

    max_date = (
        spark.table(table_name).select("date").agg({"date": "max"}).collect()[0][0]
    )
    feature_flag_orgs = (
        spark.table(table_name)
        .filter(F.col("date") == F.lit(max_date))
        .select("feature_flag_key", "attribute", "values")
    )
    feature_flag_orgs = (
        feature_flag_orgs.filter(F.col("feature_flag_key") == F.lit(feature_flag_key))
        .filter(F.col("attribute") == F.lit("orgId"))
        .drop("feature_flag_key")
        .drop("attribute")
    )
    feature_flag_orgs = feature_flag_orgs.select(
        F.from_json(F.col("values"), "array<bigint>").alias("values")
    )
    feature_flag_orgs_exploded = feature_flag_orgs.select(
        "*", F.explode("values").alias("org_id")
    )

    max_date = (
        spark.table("datamodel_core.dim_organizations")
        .select("date")
        .agg({"date": "max"})
        .collect()[0][0]
    )
    dim_orgs = (
        spark.table("datamodel_core.dim_organizations")
        .filter(F.col("date") == F.lit(max_date))
        .filter(F.col("internal_type") == F.lit(0))
        .select("org_id")
        .distinct()
    )
    all_orgs = (
        feature_flag_orgs_exploded.join(dim_orgs, on="org_id")
        .distinct()
        .toPandas()["org_id"]
        .values.tolist()
    )

    return all_orgs


def retrieve_stops_spark(
    spark,  # SparkSession
    orgs: Optional[List[int]],
    time_between_trips_minutes: int = 1,
    min_trip_distance_metres: int = 10,
    max_count_per_rounded: int = 2,
    h3_resolution: int = 10,
    filter_orgs_by_states: Optional[List[Dict[str, Any]]] = None,
):
    """Retrieve and filter trip stops using PySpark DataFrame operations."""
    import h3
    import pyspark.sql.functions as F
    from pyspark.sql import Window
    from pyspark.sql.functions import udf
    from pyspark.sql.types import DoubleType, StringType

    # Create H3 UDF functions
    @udf(returnType=StringType())
    def get_h3_cell_udf(lat, lng, res):
        return h3.latlng_to_cell(lat, lng, res)

    @udf(returnType=DoubleType())
    def get_h3_cell_center_lat_udf(h3_idx):
        return h3.cell_to_latlng(h3_idx)[0]

    @udf(returnType=DoubleType())
    def get_h3_cell_center_lng_udf(h3_idx):
        return h3.cell_to_latlng(h3_idx)[1]

    max_date = datetime.date.today().strftime("%Y-%m-%d")
    min_date = (datetime.date.today() - datetime.timedelta(days=365)).strftime(
        "%Y-%m-%d"
    )

    # Pull raw trips using DataFrame operations
    window_spec_trip = Window.partitionBy("org_id", "device_id").orderBy(
        "start_time_utc"
    )

    fct_trips = spark.table("datamodel_telematics.fct_trips")
    trips_raw = fct_trips.filter(
        (F.col("date") >= F.lit(min_date))
        & (F.col("date") <= F.lit(max_date))
        & (~F.col("trip_end_reason").isin(["TripTooLong"]))
    ).select(
        "date",
        "org_id",
        "device_id",
        "driver_id",
        "start_latitude",
        "end_latitude",
        "start_longitude",
        "end_longitude",
        "trip_end_reason",
        (F.col("distance_meters") / 1000).alias("distance_km"),
        "start_time_utc",
        "end_time_utc",
        "start_time_ms",
        "end_time_ms",
        "end_state",
        F.lag("trip_end_reason", 1)
        .over(window_spec_trip)
        .alias("prev_trip_end_reason"),
        F.lag("start_time_utc", -1).over(window_spec_trip).alias("next_start_time_utc"),
    )

    if orgs is not None:
        trips_raw = trips_raw.filter(F.col("org_id").isin(orgs))

    # Filter short trips, short stops
    filtered_trips = trips_raw.withColumn(
        "duration_of_stop_min",
        F.expr("datediff(minute, end_time_utc, next_start_time_utc)"),
    )

    filtered_trips = filtered_trips.filter(
        F.col("distance_km") > min_trip_distance_metres / 1000
    )
    filtered_trips = filtered_trips.filter(
        F.expr("datediff(minute, end_time_utc, next_start_time_utc)")
        >= time_between_trips_minutes
    )

    # add h3 cell
    filtered_trips = filtered_trips.withColumn(
        "h3_idx",
        get_h3_cell_udf(
            F.col("end_latitude"), F.col("end_longitude"), F.lit(h3_resolution)
        ),
    )
    window_spec = Window.partitionBy(["org_id", "h3_idx"]).orderBy(
        F.desc("end_time_utc")
    )
    filtered_trips = filtered_trips.withColumn("rnk", F.row_number().over(window_spec))

    # aggregation for driver id, times
    driver_id_counts = filtered_trips.groupby(["org_id", "h3_idx", "driver_id"]).count()
    driver_id_counts = driver_id_counts.groupby(["org_id", "h3_idx"]).agg(
        F.map_from_entries(F.collect_list(F.struct("driver_id", "count"))).alias(
            "driver_id_counts"
        )
    )

    trips_agg = filtered_trips.groupBy(["org_id", "h3_idx"]).agg(
        F.count("*").alias("trip_count_for_end_coords"),
        F.mean(F.col("duration_of_stop_min")).alias("stop_duration_minutes_mean"),
    )

    # Remove duplicates
    trips = (
        filtered_trips.withColumn("lookback_days", F.lit(365))
        .select("org_id", "h3_idx", "date", "lookback_days", "rnk", "end_state")
        .filter(F.col("rnk") <= max_count_per_rounded)
        .join(trips_agg, on=["org_id", "h3_idx"])
        .join(driver_id_counts, on=["org_id", "h3_idx"])
        # need to correct this to account for the fact that we keep max_count_per_rounded duplicates
        .withColumn(
            "trip_count_for_end_coords",
            F.when(F.col("trip_count_for_end_coords") == 1, F.lit(1)).otherwise(
                F.col("trip_count_for_end_coords") / F.lit(max_count_per_rounded)
            ),
        )
        .withColumn(
            "trip_count_for_end_coords",
            F.col("trip_count_for_end_coords").cast("integer"),
        )
        .drop("rnk")
    )

    # temporary handling of Sysco
    if filter_orgs_by_states is not None:
        for filter_org in filter_orgs_by_states:
            org_id = filter_org["org_id"]
            states = filter_org["states"]
            trips = trips.filter(
                ~((F.col("org_id") == org_id) & (~F.col("end_state").isin(states)))
            )
    # get center lat/lng
    trips = (
        trips.withColumn("end_latitude", get_h3_cell_center_lat_udf(F.col("h3_idx")))
        .withColumn("end_longitude", get_h3_cell_center_lng_udf(F.col("h3_idx")))
        .drop("end_state")
    )

    return trips


def _get_cluster_schema():
    """Schema for final table output (with date column, without number_of_one_time_stops)."""
    import pyspark.sql.types as T

    cluster_structs = [
        T.StructField("org_id", T.LongType()),
        T.StructField("latitude", T.DoubleType()),
        T.StructField("longitude", T.DoubleType()),
        T.StructField("number_of_stops", T.IntegerType()),
        T.StructField("lookback_days", T.IntegerType()),
        T.StructField(
            "polygon",
            T.StructType(
                [
                    T.StructField("type", T.StringType()),
                    T.StructField(
                        "coordinates",
                        T.ArrayType(T.ArrayType(T.ArrayType(T.DoubleType()))),
                    ),
                ]
            ),
        ),
        T.StructField("polygon_area", T.DoubleType()),
        T.StructField("driver_id_counts", T.MapType(T.LongType(), T.IntegerType())),
        T.StructField("cluster_idx", T.IntegerType()),
        T.StructField("date", T.StringType()),  # Partition column
    ]
    return T.StructType(cluster_structs)


def retrieve_polygon_from_h3(
    h3_indices: List[str], latitudes: List[float], longitudes: List[float]
) -> Union[Dict[str, str | List[List[List[float]]]], float]:
    """Retrieve polygon boundary from H3 indices."""
    import h3
    import numpy as np
    import shapely
    from pyproj import Geod
    from scipy.spatial import ConvexHull, QhullError
    from shapely.geometry import MultiPolygon, Polygon
    from shapely.ops import orient

    polygons = []
    for idx in h3_indices:
        coords = [(lng, lat) for lat, lng in h3.cell_to_boundary(idx)]
        polygons.append(Polygon(coords))
    combined_boundary = shapely.union_all(MultiPolygon(polygons))

    if combined_boundary.geom_type == "MultiPolygon":
        lng_lat_points = np.array(
            [[lng, lat] for lng, lat in zip(longitudes, latitudes)]
        )
        try:
            convex_hull = ConvexHull(lng_lat_points, qhull_options="QJ")
            boundary_points = lng_lat_points[convex_hull.vertices].tolist()
            # close polygon
            boundary_points.append(boundary_points[0])
        except (QhullError, ValueError):
            # Fallback: use H3 boundary of the first cell if ConvexHull fails
            boundary_points = [
                [lng, lat] for lat, lng in h3.cell_to_boundary(h3_indices[0])
            ]
            # close polygon
            boundary_points.append(boundary_points[0])
    else:
        coords = combined_boundary.exterior.coords.xy
        boundary_points = [
            [lng, lat] for lng, lat in zip(coords[0].tolist(), coords[1].tolist())
        ]

    # compute area
    geod = Geod(ellps="WGS84")
    area, _ = geod.geometry_area_perimeter(orient(Polygon(boundary_points)))

    # geojson format (expects longitude first)
    geojson_polygon = {"type": "Polygon", "coordinates": [boundary_points]}
    return geojson_polygon, abs(area)


def _perform_clustering_core(
    df, eps, min_samples, h3_resolution, retrieve_polygon_func
):
    """Core clustering logic shared between cluster_stops and cluster_stops_spark.

    This function contains the DBSCAN + H3 clustering algorithm.

    Args:
        df: pandas DataFrame with stop data
        eps: DBSCAN epsilon parameter
        min_samples: DBSCAN min_samples parameter
        h3_resolution: H3 hexagon resolution
        retrieve_polygon_func: Function to retrieve polygon from H3 indices

    Returns:
        pandas DataFrame with cluster results
    """
    from collections import Counter
    from math import radians

    import h3
    import pandas as pd
    from sklearn.cluster import DBSCAN

    org_id = df["org_id"].iloc[0]
    lookback_days = df["lookback_days"].iloc[0]

    df["end_lat_radian"] = df["end_latitude"].map(radians)
    df["end_lng_radian"] = df["end_longitude"].map(radians)

    # 2. DBSCAN clustering
    cluster_model = DBSCAN(
        eps=eps,
        min_samples=min_samples,
        metric="haversine",
        algorithm="auto",
        leaf_size=30,
    )
    df["cluster_idx"] = cluster_model.fit_predict(
        df[["end_lat_radian", "end_lng_radian"]]
    )

    # 3. Collect cluster information and retrieve boundary polygons
    cluster_indices = []
    polygons = []
    polygon_areas = []
    latitudes = []
    longitudes = []
    stop_counts = []
    driver_id_counts = []

    for cluster_idx in sorted([c for c in df["cluster_idx"].unique() if c != -1]):
        df_idx = df[df["cluster_idx"] == cluster_idx]
        cluster_indices.append(cluster_idx)
        latitudes.append(df_idx["end_latitude"].median())
        longitudes.append(df_idx["end_longitude"].median())
        stop_counts.append(df_idx["trip_count_for_end_coords"].sum())
        h3_unique = sorted(df_idx["h3_idx"].unique().tolist())
        driver_counts = Counter()
        for counts in df_idx["driver_id_counts"]:
            driver_counts += counts
        driver_counts = {k: int(v / min_samples) for k, v in driver_counts.items()}
        driver_id_counts.append(driver_counts)
        polygon_coords, polygon_area = retrieve_polygon_func(
            h3_unique,
            df_idx["end_latitude"].values.tolist(),
            df_idx["end_longitude"].values.tolist(),
        )
        polygons.append(polygon_coords)
        polygon_areas.append(polygon_area)

    df_clusters = pd.DataFrame(
        {
            "org_id": len(latitudes) * [org_id],
            "latitude": latitudes,
            "longitude": longitudes,
            "number_of_stops": stop_counts,
            "lookback_days": len(latitudes) * [lookback_days],
            "polygon": polygons,
            "polygon_area": polygon_areas,
            "driver_id_counts": driver_id_counts,
            "cluster_idx": cluster_indices,
            "number_of_one_time_stops": len(latitudes)
            * [len(df[df["cluster_idx"] == -1])],
        }
    )

    return df_clusters, df


def cluster_stops(
    df,  # pandas DataFrame
    eps: float = 0.000025,
    min_samples: float = 2,
    min_duration_of_stop_min: int = 4,
    h3_resolution: int = 10,
):
    """Cluster stops using DBSCAN and H3 spatial indexing.

    This function is used for local/testing with pandas DataFrames.
    For distributed Spark processing, use cluster_stops_spark.
    """
    df_clusters, df_with_labels = _perform_clustering_core(
        df=df,
        eps=eps,
        min_samples=min_samples,
        h3_resolution=h3_resolution,
        retrieve_polygon_func=retrieve_polygon_from_h3,
    )

    df_anomalous_stops = df_with_labels[df_with_labels["cluster_idx"] == -1]
    logger.info(f"Total number of stops: {len(df_with_labels)}")
    logger.info(f"Number of clusters {len(df_clusters)}")
    logger.info(
        f"Number of anomalous stops (>{min_duration_of_stop_min} duration): {len(df_anomalous_stops)}"
    )

    # Conditional logging for driver_id
    if "driver_id" in df_with_labels.columns:
        logger.info(
            f"Number of distinct driver_id: {len(df_with_labels['driver_id'].unique())}"
        )
        logger.info(
            f"Number of distinct driver_id with anomalous stops: {len(df_anomalous_stops['driver_id'].unique())}"
        )

    return df_clusters, df_with_labels


def compute_aggregate_stats(df_clusters):
    def q50(x):
        return x.quantile(0.5)

    def q90(x):
        return x.quantile(0.9)

    def q10(x):
        return x.quantile(0.1)

    df_cluster_stats = df_clusters.groupby("org_id").agg(
        known_location_count=("cluster_idx", "count"),
        total_number_of_stops=("number_of_stops", "sum"),
        lookback_days=("lookback_days", "first"),
        number_of_one_time_stops=("number_of_one_time_stops", "first"),
        polygon_area_percentile50=("polygon_area", q50),
        polygon_area_percentile10=("polygon_area", q10),
        polygon_area_percentile90=("polygon_area", q90),
        number_of_stops_percentile50=("number_of_stops", q50),
        number_of_stops_percentile10=("number_of_stops", q10),
        number_of_stops_percentile90=("number_of_stops", q90),
    )

    return df_cluster_stats.reset_index()


def cluster_stops_spark(
    spark,
    stops,  # pyspark DataFrame
    eps: float = 0.000025,
    min_samples: float = 2,
    h3_resolution: int = 10,
):
    """Cluster stops using Spark's applyInPandas for distributed processing.

    Sorting is applied for deterministic and consistent results across runs.
    """
    import pyspark.sql.functions as F
    import pyspark.sql.types as T

    # Schema for clustering UDF output (includes number_of_one_time_stops,
    # excludes date which is added later)
    cluster_schema = T.StructType(
        [
            T.StructField("org_id", T.LongType()),
            T.StructField("latitude", T.DoubleType()),
            T.StructField("longitude", T.DoubleType()),
            T.StructField("number_of_stops", T.IntegerType()),
            T.StructField("lookback_days", T.IntegerType()),
            T.StructField(
                "polygon",
                T.StructType(
                    [
                        T.StructField("type", T.StringType()),
                        T.StructField(
                            "coordinates",
                            T.ArrayType(T.ArrayType(T.ArrayType(T.DoubleType()))),
                        ),
                    ]
                ),
            ),
            T.StructField("polygon_area", T.DoubleType()),
            T.StructField(
                "driver_id_counts",
                T.MapType(T.LongType(), T.IntegerType()),
            ),
            T.StructField("cluster_idx", T.IntegerType()),
            T.StructField("number_of_one_time_stops", T.IntegerType()),
        ]
    )

    # Define clustering function inline with all dependencies.
    # NOTE: This function must be self-contained for Spark serialization.
    # It mirrors the logic in _perform_clustering_core() - keep them in sync!

    def clustering_function(df):  # pandas DataFrame
        # All imports must be inside the function for Spark workers
        from collections import Counter
        from math import radians

        import h3
        import numpy as np
        import pandas as pd
        import shapely
        from pyproj import Geod
        from scipy.spatial import ConvexHull, QhullError
        from shapely.geometry import MultiPolygon, Polygon
        from shapely.ops import orient
        from sklearn.cluster import DBSCAN

        # Inline retrieve_polygon_from_h3 (mirrors retrieve_polygon_from_h3)
        def retrieve_polygon_from_h3_inline(h3_indices, latitudes, longitudes):

            polygons = []
            for idx in h3_indices:
                coords = [(lng, lat) for lat, lng in h3.cell_to_boundary(idx)]
                polygons.append(Polygon(coords))
            combined_boundary = shapely.union_all(MultiPolygon(polygons))

            if combined_boundary.geom_type == "MultiPolygon":
                lng_lat_points = np.array(
                    [[lng, lat] for lng, lat in zip(longitudes, latitudes)]
                )
                try:
                    convex_hull = ConvexHull(lng_lat_points, qhull_options="QJ")
                    boundary_points = lng_lat_points[convex_hull.vertices].tolist()
                    # close polygon
                    boundary_points.append(boundary_points[0])
                except (QhullError, ValueError):
                    # Fallback: use H3 boundary of the first cell if ConvexHull fails
                    boundary_points = [
                        [lng, lat] for lat, lng in h3.cell_to_boundary(h3_indices[0])
                    ]
                    # close polygon
                    boundary_points.append(boundary_points[0])
            else:
                coords = combined_boundary.exterior.coords.xy
                boundary_points = [
                    [lng, lat]
                    for lng, lat in zip(coords[0].tolist(), coords[1].tolist())
                ]

            # compute area
            geod = Geod(ellps="WGS84")
            area, _ = geod.geometry_area_perimeter(orient(Polygon(boundary_points)))

            # geojson format (expects longitude first)
            geojson_polygon = {"type": "Polygon", "coordinates": [boundary_points]}
            return geojson_polygon, abs(area)

        # Core clustering logic (mirrors _perform_clustering_core)
        org_id = df["org_id"].iloc[0]
        lookback_days = df["lookback_days"].iloc[0]

        # 1. Compute radians and h3 cell
        df["end_lat_radian"] = df["end_latitude"].map(radians)
        df["end_lng_radian"] = df["end_longitude"].map(radians)

        # Sort for deterministic results - device_id and end_time_utc ensure consistent ordering
        sort_cols = (
            ["device_id", "end_time_utc"]
            if "device_id" in df.columns and "end_time_utc" in df.columns
            else ["end_latitude", "end_longitude"]
        )
        df = df.sort_values(by=sort_cols).reset_index(drop=True)

        # 2. DBSCAN clustering
        cluster_model = DBSCAN(
            eps=eps,
            min_samples=min_samples,
            metric="haversine",
            algorithm="auto",
            leaf_size=30,
        )
        df["cluster_idx"] = cluster_model.fit_predict(
            df[["end_lat_radian", "end_lng_radian"]]
        )

        # 3. Collect cluster information and retrieve boundary polygons
        cluster_indices = []
        polygons = []
        polygon_areas = []
        latitudes = []
        longitudes = []
        stop_counts = []
        driver_id_counts = []

        for cluster_idx in sorted([c for c in df["cluster_idx"].unique() if c != -1]):
            df_idx = df[df["cluster_idx"] == cluster_idx]
            cluster_indices.append(cluster_idx)
            latitudes.append(df_idx["end_latitude"].median())
            longitudes.append(df_idx["end_longitude"].median())
            stop_counts.append(df_idx["trip_count_for_end_coords"].sum())
            h3_unique = sorted(df_idx["h3_idx"].unique().tolist())
            driver_counts = Counter()
            for counts in df_idx["driver_id_counts"]:
                driver_counts += counts
            driver_counts = {k: int(v / min_samples) for k, v in driver_counts.items()}
            driver_id_counts.append(driver_counts)
            polygon_coords, polygon_area = retrieve_polygon_from_h3_inline(
                h3_unique,
                df_idx["end_latitude"].values.tolist(),
                df_idx["end_longitude"].values.tolist(),
            )
            polygons.append(polygon_coords)
            polygon_areas.append(polygon_area)

        df_clusters = pd.DataFrame(
            {
                "org_id": len(latitudes) * [org_id],
                "latitude": latitudes,
                "longitude": longitudes,
                "number_of_stops": stop_counts,
                "lookback_days": len(latitudes) * [lookback_days],
                "polygon": polygons,
                "polygon_area": polygon_areas,
                "driver_id_counts": driver_id_counts,
                "cluster_idx": cluster_indices,
                "number_of_one_time_stops": len(latitudes)
                * [len(df[df["cluster_idx"] == -1])],
            }
        )

        return df_clusters

    today_date = datetime.date.today().strftime("%Y-%m-%d")
    clusters = (
        stops.groupBy("org_id")
        .applyInPandas(clustering_function, schema=cluster_schema)
        .orderBy("org_id", "cluster_idx")
    )

    cluster_stats = (
        clusters.groupBy("org_id")
        .agg(
            F.count(F.col("cluster_idx")).alias("known_location_count"),
            F.sum(F.col("number_of_stops")).alias("total_number_of_stops"),
            F.percentile(F.col("number_of_stops"), 0.5).alias(
                "number_of_stops_percentile50"
            ),
            F.percentile(F.col("number_of_stops"), 0.1).alias(
                "number_of_stops_percentile10"
            ),
            F.percentile(F.col("number_of_stops"), 0.9).alias(
                "number_of_stops_percentile90"
            ),
            F.first(F.col("lookback_days")).alias("lookback_days"),
            F.first(F.col("number_of_one_time_stops")).alias(
                "number_of_one_time_stops"
            ),
            F.percentile(F.col("polygon_area"), 0.5).alias("polygon_area_percentile50"),
            F.percentile(F.col("polygon_area"), 0.1).alias("polygon_area_percentile10"),
            F.percentile(F.col("polygon_area"), 0.9).alias("polygon_area_percentile90"),
            F.percentile(F.col("polygon_area"), 0.99).alias(
                "polygon_area_percentile99"
            ),
        )
        .withColumn("date", F.lit(today_date))
    )

    clusters = clusters.drop("number_of_one_time_stops").withColumn(
        "date", F.lit(today_date)
    )

    # Return both dataframes
    return {"clusters": clusters, "cluster_stats": cluster_stats}


def run_anomaly_detection(
    context,  # OpExecutionContext
    spark,  # SparkSession
    orgs: List[int] = [],
    filter_orgs_by_states: List[Dict[str, Any]] = [],
    min_trip_distance_metres: int = 10,
    min_duration_of_stop_minutes: int = 4,
    eps: float = 0.000025,
    min_samples: int = 2,
    h3_resolution: int = 10,
    feature_flag_key: str = "anomalousstopsworker-enabled",
):
    """Main function to run anomaly detection on trip stops."""
    import pyspark.sql.functions as F

    # Retrieve orgs
    feature_flag_orgs = retrieve_orgs_from_feature_flag_spark(
        spark=spark, feature_flag_key=feature_flag_key
    )
    logger.info(f"Feature flag orgs {feature_flag_orgs}")
    # Combine with manually set orgs (to be removed)
    if len(orgs) > 0:
        orgs = list(set(orgs + feature_flag_orgs))
    else:
        orgs = feature_flag_orgs
    logger.info(f"All orgs {orgs}")

    # Retrieve stops
    stops = retrieve_stops_spark(
        spark=spark,
        orgs=orgs if len(orgs) > 0 else None,
        filter_orgs_by_states=filter_orgs_by_states,
        time_between_trips_minutes=min_duration_of_stop_minutes,
        min_trip_distance_metres=min_trip_distance_metres,
        max_count_per_rounded=min_samples,
        h3_resolution=h3_resolution,
    )
    logger.info("Retrieved stops")

    # Clustering - returns dict with clusters and cluster_stats
    results = cluster_stops_spark(
        spark=spark,
        stops=stops,
        eps=eps,
        min_samples=min_samples,
        h3_resolution=h3_resolution,
    )
    logger.info("Clustered stops")

    clusters = results["clusters"]
    cluster_stats = results["cluster_stats"]

    # Temporary safeguard
    orgs_to_remove = (
        cluster_stats.filter(F.col("known_location_count") > 50000)
        .select("org_id")
        .toPandas()["org_id"]
        .values.tolist()
    )
    logger.info(f"Removing orgs with too many known locations: {orgs_to_remove}")
    clusters = clusters.filter(~F.col("org_id").isin(orgs_to_remove))
    cluster_stats = cluster_stats.filter(~F.col("org_id").isin(orgs_to_remove))

    # Write cluster_stats to monitoring table using dynamic partition overwrite
    logger.info(f"Writing cluster_stats to table: {MONITORING_TABLE}")
    cluster_stats.write.partitionBy("date").mode("overwrite").option(
        "partitionOverwriteMode", "dynamic"
    ).option("mergeSchema", "true").saveAsTable(MONITORING_TABLE)
    logger.info(
        f"Wrote cluster_stats to {MONITORING_TABLE} "
        f"using dynamic partition overwrite"
    )

    # Return clusters DataFrame for DQ checks and table write via decorator
    return clusters


class AnomalyDetectionConfig(Config):
    """Configuration for anomaly detection op."""

    orgs: List[int] = []
    filter_orgs_by_states: Optional[List[Dict[str, Any]]] = (
        [{"org_id": 45402, "states": ["TX"]}] if get_region() == "us-west-2" else None
    )

    min_duration_of_stop_minutes: int = 4
    min_trip_distance_metres: int = 50
    eps: float = 0.000025
    min_samples: int = 2
    h3_resolution: int = 10
    feature_flag_key: str = "anomalousstopsworker-enabled"


@table_op(
    dq_checks=[
        NonEmptyDQCheck(name="device_known_locations_nonempty"),
        NonNullDQCheck(
            name="device_known_locations_nonnull",
            columns=["org_id", "date"],
        ),
        PrimaryKeyDQCheck(
            name="device_known_locations_primary_key",
            columns=["org_id", "date", "cluster_idx"],
        ),
        SQLDQCheck(
            name="polygon_closed",
            sql=(
                "SELECT SUM(CASE WHEN ELEMENT_AT(polygon.coordinates[0], 1) = ELEMENT_AT(polygon.coordinates[0], -1) THEN 1 ELSE 0 END) / COUNT(*) AS observed_value FROM df"
            ),
            expected_value=1.0,
            operator=Operator.eq,
        ),
        SQLDQCheck(
            name="polygon_closed_on_last_only",
            sql=(
                "SELECT SUM(CASE WHEN ELEMENT_AT(polygon.coordinates[0], 1) <> ELEMENT_AT(polygon.coordinates[0], -2) THEN 1 ELSE 0 END) / COUNT(*) AS observed_value FROM df"
            ),
            expected_value=1.0,
            operator=Operator.eq,
        ),
        SQLDQCheck(
            name="latitude_longitude_valid",
            sql=(
                "SELECT SUM(CASE WHEN latitude >=-90 AND latitude <= 90 AND longitude >=-180 AND longitude <= 180 THEN 1 ELSE 0 END) / COUNT(*) AS observed_value FROM df"
            ),
            expected_value=1.0,
            operator=Operator.eq,
        ),
        SQLDQCheck(
            name="polygon_lat_lng_valid",
            sql=(
                "SELECT COUNT(CASE WHEN FORALL(polygon.coordinates[0], x -> x[0] >= -180 AND x[0] <=180 AND x[1] >= -90 AND x[1] <= 90) THEN 1 END) / COUNT(*) AS observed_value FROM df"
            ),
            expected_value=1.0,
            operator=Operator.eq,
        ),
    ],
    database=DATABASE,
    table="device_known_locations",
    owner="DataTools",
    schema=_get_cluster_schema(),
    mode="overwrite",
    partition_by=["date"],
    required_resource_keys={"databricks_pyspark_step_launcher"},
)
def anomaly_detection_op(
    context: OpExecutionContext, config: AnomalyDetectionConfig, start_after=None
):
    """Dagster op to run anomaly detection on trip stops.

    This op retrieves trip stops from Spark SQL, performs DBSCAN clustering
    to identify known locations, and detects anomalous stops.

    This op runs on Databricks using the pyspark_step_launcher resource.

    Args:
        context: Dagster execution context
        config: Configuration for anomaly detection
        start_after: Optional input to create dependency (value is ignored)

    Note: This op requires Databricks and cannot run locally. If you see
    errors about missing tables or catalogs, the job is not running on Databricks.
    This job must be run from a deployed Dagster instance.
    """
    import os

    from pyspark.sql import SparkSession

    context.log.info(f"[anomaly_detection_op] Starting anomaly detection")
    context.log.info(f"[anomaly_detection_op] ENV={os.getenv('ENV', 'not_set')}")
    context.log.info(f"[anomaly_detection_op] MODE={os.getenv('MODE', 'not_set')}")
    context.log.info(
        f"[anomaly_detection_op] DATABRICKS_TOKEN={'SET' if os.getenv('DATABRICKS_TOKEN') else 'NOT SET'}"
    )

    # Check if step launcher resource is available
    if hasattr(context.resources, "databricks_pyspark_step_launcher"):
        context.log.info(
            f"[anomaly_detection_op] databricks_pyspark_step_launcher resource: {type(context.resources.databricks_pyspark_step_launcher)}"
        )
    else:
        context.log.warning(
            "[anomaly_detection_op] databricks_pyspark_step_launcher resource NOT FOUND!"
        )

    # Check if we're in a test/mock mode
    if os.getenv("MODE") == "MOCK_LOCAL_RUN":
        context.log.warning("Running in MOCK mode - skipping actual execution")
        # Return empty DataFrame for mock mode
        import pyspark.sql.types as T

        spark = SparkSession.builder.enableHiveSupport().getOrCreate()
        schema = T.StructType(
            [
                T.StructField("org_id", T.LongType()),
                T.StructField("latitude", T.DoubleType()),
                T.StructField("longitude", T.DoubleType()),
                T.StructField("date", T.StringType()),
            ]
        )
        return spark.createDataFrame([], schema)

    # Get or create Spark session with Hive support
    # Note: When running via Databricks step launcher, this executes on Databricks
    # The catalog is already configured via spark_conf_overrides in the resource
    context.log.info("[anomaly_detection_op] Creating Spark session...")
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()

    context.log.info(
        f"[anomaly_detection_op] Current database: {spark.catalog.currentDatabase()}"
    )
    # Note: sparkContext.master not available in Spark Connect mode on Databricks

    # Run anomaly detection - returns clusters DataFrame
    clusters_df = run_anomaly_detection(
        context=context,
        spark=spark,
        orgs=config.orgs,
        filter_orgs_by_states=config.filter_orgs_by_states,
        min_trip_distance_metres=config.min_trip_distance_metres,
        eps=config.eps,
        min_samples=config.min_samples,
        min_duration_of_stop_minutes=config.min_duration_of_stop_minutes,
        h3_resolution=config.h3_resolution,
        feature_flag_key=config.feature_flag_key,
    )

    return clusters_df
