# Databricks notebook source
# MAGIC %md
# MAGIC # Dataset <> OSM Map matching
# MAGIC
# MAGIC 2 helpers to use
# MAGIC - do_interpolate_and_map_match
# MAGIC - do_map_match

# COMMAND ----------

# MAGIC %pip install h3==4.2.1
# MAGIC %pip install shapely
# MAGIC %pip install geopy

# COMMAND ----------

# MAGIC %run /backend/safety/speed_limits/utils_calling_conventions

# COMMAND ----------

# MAGIC %sql
# MAGIC SET
# MAGIC   spark.sql.broadcastTimeout = 100000;
# MAGIC SET
# MAGIC   spark.sql.autoBroadcastJoinThreshold = -1;

# COMMAND ----------

# DBTITLE 1,UDFs
from typing import List, Tuple
from datetime import datetime
import h3
import numpy as np
from pyspark.sql.functions import (
    udf,
    col,
    size,
    avg,
    max,
    sum,
    when,
    least,
    lit,
    first,
    explode,
    collect_list,
    regexp_extract,
    current_timestamp,
)
from pyspark.sql import DataFrame
from pyspark.sql.types import ArrayType, StringType
import shapely
from shapely import (
    geometry,
    wkt,
)


def process_osm(osm_table_name: str) -> DataFrame:
    # Pre-processing: Only include ways that are vehicle roads. (Note this excludes link roads since they're a source of false positives)
    # See https://wiki.openstreetmap.org/wiki/Key:highway for more details
    SPEED_LIMIT_HIGHWAY_TAGS = [
        "motorway",
        "trunk",
        "primary",
        "secondary",
        "tertiary",
        "unclassified",
        "residential",
        "service",
        "living_street",
    ]
    return spark.table(osm_table_name).filter(
        col("highway").isin(SPEED_LIMIT_HIGHWAY_TAGS)
    )


def get_osm_line_string(node_list: List) -> str:
    """get_osm_line_string takes a list of lat/lon nodes and outputs a wkt linestring."""
    if not node_list:
        return None
    node_list_sorted = sorted(node_list, key=lambda node: node["ind"])
    point_list = [(node["longitude"], node["latitude"]) for node in node_list_sorted]
    line = shapely.geometry.LineString(point_list)
    return line.wkt


get_osm_line_string_udf = udf(get_osm_line_string, StringType())

assert (
    get_osm_line_string(
        [
            {"ind": 2, "longitude": -93.6089, "latitude": 41.6006},
            {"ind": 1, "longitude": -93.6091, "latitude": 41.6005},
            {"ind": 3, "longitude": -93.6087, "latitude": 41.6007},
        ]
    )
    == "LINESTRING (-93.6091 41.6005, -93.6089 41.6006, -93.6087 41.6007)"
)


def get_dataset_line_string(node_list: List) -> str:
    if not node_list:
        return None
    point_list = [(pt[0], pt[1]) for pt in node_list]
    line = shapely.geometry.LineString(point_list)
    return line.wkt


get_dataset_line_string_udf = udf(get_dataset_line_string, StringType())

assert (
    get_dataset_line_string(
        [[-93.6091, 41.6005], [-93.6089, 41.6006], [-93.6087, 41.6007]]
    )
    == "LINESTRING (-93.6091 41.6005, -93.6089 41.6006, -93.6087 41.6007)"
)


def get_dataset_point(node_point: List) -> str:
    if not node_point:
        return None
    point = shapely.geometry.Point(node_point)
    return point.wkt


get_dataset_point_udf = udf(get_dataset_point, StringType())

assert (
    get_dataset_point([-108.72070312499997, 34.99400375757577])
    == "POINT (-108.72070312499997 34.99400375757577)"
)


def line_string_linear_interp(points_wkt: str, step: float = 0.00008) -> str:
    """Linear interpolates points within a line string, adding a new
    point at increments specified by step. This works by linearly
    interpolating both the x and y coordinates for each pair of points.
    We use this to ensure that sparse linestrings are able to nicely map
    into a sequence of h3 buckets.
    The default step of 0.00008 roughly corresponds to 8.88m when working with
    lat/lng coordinates. With an h3 resolution of 13, this is slightly
    more than double the hexagon edge length.
    Linear interpolation error increases closer to the poles since straight
    lines don't align well with more curved areas of the earth. However,
    since the distances between two adjacent point is so small interpolation
    error is negligible.
    """
    geom = shapely.wkt.loads(points_wkt)
    x = np.array(geom.xy[0])
    y = np.array(geom.xy[1])

    xvals = []
    yvals = []

    for i in range(len(x) - 1):
        # Determine how many additional points to add. We want at least one
        # point per incremental step. To meet this requirement, take the larger
        # of the number of points interpolation is required in either x or y.
        point_count = int(
            np.max(
                [
                    np.round(np.abs((x[i + 1] - x[i])) / step),
                    np.round(np.abs((y[i + 1] - y[i])) / step),
                ]
            )
        )
        if point_count <= 1:
            point_count = 2

        # Exclude the endpoint since it will be added in the next iteration
        # due to the pairwise loop.
        seg_x_vals = np.linspace(x[i], x[i + 1], point_count, endpoint=False)
        seg_y_vals = np.linspace(y[i], y[i + 1], point_count, endpoint=False)
        xvals.extend(seg_x_vals)
        yvals.extend(seg_y_vals)

    # Add the very last point since it was excluded.
    if len(x) > 0:
        xvals.append(x[-1])
    if len(y) > 0:
        yvals.append(y[-1])

    points = np.array((xvals, yvals)).T
    return str(shapely.geometry.LineString(points))


line_string_linear_interp_udf = udf(line_string_linear_interp, StringType())


def line_string_to_h3_str(geom: str, res: int) -> List[str]:
    """Converts points in a linestring to a series of h3 buckets for map-matching."""
    h3_indices = set()
    geom = shapely.wkt.loads(geom)
    for (lng, lat) in geom.coords[:]:
        h3_idx = h3.latlng_to_cell(lat, lng, res)
        h3_indices.add(h3_idx)
    return list(h3_indices)


line_string_to_h3_str_udf = udf(
    lambda geom, res: line_string_to_h3_str(geom, res), ArrayType(StringType())
)

assert line_string_to_h3_str(
    "LINESTRING (-93.6091 41.6005, -93.6089 41.6006, -93.6087 41.6007)", 13
) == ["8d260d8747101bf", "8d260d874715cbf", "8d260d87471097f"]


def point_to_h3_str(geom: str, res: int) -> List[str]:
    """Converts a point to a h3 bucket for map-matching."""
    geom = shapely.wkt.loads(geom)
    h3_idx = h3.latlng_to_cell(geom.y, geom.x, res)
    return [h3_idx]


point_to_h3_str_udf = udf(
    lambda geom, res: point_to_h3_str(geom, res), ArrayType(StringType())
)


assert point_to_h3_str("POINT (-93.6091 41.6005)", 13) == ["8d260d8747101bf"]


def find_latest_full_ways_table_by_region(region: str) -> str:
    """Given a region, return a *_full_ways table with the latest date. e.g. osm_20240619__usa__full_ways"""
    tables_df = spark.sql("SHOW TABLES IN safety_map_data")
    pattern = f"osm_\\d{{8}}__{region}__full_ways"
    filtered_tables_df = tables_df.filter(tables_df.tableName.rlike(pattern))

    pattern = f"osm_(\\d{{8}})__{region}__full_ways"
    latest_table_df = (
        filtered_tables_df.withColumn(
            "date", regexp_extract(col("tableName"), pattern, 1)
        )
        .orderBy(col("date").desc())
        .limit(1)
    )

    table_name = latest_table_df.collect()[0]["tableName"]
    return f"safety_map_data.{table_name}"


def get_osm_nodes_by_regions(regions: List[str]) -> DataFrame:
    """Get the merged the osm nodes from regions"""
    for r in regions:
        if r not in set(["usa", "can", "mex", "eur"]):
            raise ValueError(
                "Invalid regions, available regions are 'usa', 'can', 'mex', 'eur'"
            )
    osm_way_nodes = None
    for r in regions:
        region_full_ways_table = find_latest_full_ways_table_by_region(r)
        region_df = process_osm(region_full_ways_table)
        if not osm_way_nodes:
            osm_way_nodes = region_df
        else:
            osm_way_nodes = osm_way_nodes.union(region_df)
    return osm_way_nodes


# COMMAND ----------

# DBTITLE 1,Interpolation and map matching


def interpolate_osm_ways(regions: List, res: int = 13) -> DataFrame:
    """Necessary fields: way_id, nodes_lat_lng"""
    osm_way_nodes = get_osm_nodes_by_regions(regions)
    osm_way_nodes = (
        osm_way_nodes.filter(size("nodes_lat_lng") > 1)
        .withColumn("geometry", get_osm_line_string_udf(col("nodes_lat_lng")))
        .select("way_id", "geometry")
    )

    osm_way_nodes_interpolated = osm_way_nodes.withColumn(
        "interpolated_wkt", line_string_linear_interp_udf(col("geometry"))
    )
    osm_way_nodes_interpolated_h3 = osm_way_nodes_interpolated.withColumn(
        "interpolated_h3", line_string_to_h3_str_udf(col("interpolated_wkt"), lit(res))
    ).withColumn("interpolated_h3_size", size("interpolated_h3"))
    osm_way_nodes_interpolated_h3 = osm_way_nodes_interpolated_h3.select(
        col("way_id").alias("osm_way_id"), "interpolated_h3", "interpolated_h3_size"
    )
    return osm_way_nodes_interpolated_h3


def interpolate_dataset_ways(dataset_df: DataFrame, res: int = 13) -> DataFrame:
    """Necessary fields in speed limit dataset: dataset_way_id, coordinates"""

    # note that the wkt in tomotm is already in linestring, so don't need process
    # therefore, need to skip this get_dataset_line_string_udf for tomtom
    if "coordinates" in dataset_df.columns:
        dataset_ways = (
            dataset_df.filter(size("coordinates") > 1)
            .withColumn("coordinates", get_dataset_line_string_udf(col("coordinates")))
            .select("dataset_way_id", "coordinates")
        )
    elif "wkt" in dataset_df.columns:
        dataset_ways = dataset_df.select(
            "dataset_way_id", col("wkt").alias("coordinates")
        )
    else:
        raise ValueError("Dataset must contain either a 'coordinates' or 'wkt' column")

    dataset_ways_interpolated = dataset_ways.withColumn(
        "interpolated_wkt", line_string_linear_interp_udf(col("coordinates"))
    )
    dataset_ways_interpolated_h3 = dataset_ways_interpolated.withColumn(
        "interpolated_h3", line_string_to_h3_str_udf(col("interpolated_wkt"), lit(res))
    ).withColumn("interpolated_h3_size", size("interpolated_h3"))
    dataset_ways_interpolated_h3 = dataset_ways_interpolated_h3.select(
        "dataset_way_id", "interpolated_h3", "interpolated_h3_size"
    )
    return dataset_ways_interpolated_h3


def process_single_dataset_ways(dataset_df: DataFrame, res: int = 11) -> DataFrame:
    """Necessary fields in speed limit dataset: dataset_way_id, coordinates"""
    dataset_ways = dataset_df.withColumn(
        "interpolated_wkt", get_dataset_point_udf(col("coordinate"))
    ).select("dataset_way_id", "interpolated_wkt")
    dataset_ways_interpolated_h3 = dataset_ways.withColumn(
        "interpolated_h3", point_to_h3_str_udf(col("interpolated_wkt"), lit(res))
    ).withColumn("interpolated_h3_size", size("interpolated_h3"))
    dataset_ways_interpolated_h3 = dataset_ways_interpolated_h3.select(
        "dataset_way_id", "interpolated_h3", "interpolated_h3_size"
    )
    return dataset_ways_interpolated_h3


def find_map_matches(
    osm_way_nodes_interpolated_h3: DataFrame, dataset_ways_interpolated_h3: DataFrame
) -> DataFrame:
    """
    Takes a dataset extracted table, osm extracted table, and the name of the match table.
    Writes match data (osm_way_id, dataset_way_id, osm_match_count, dataset_match_count) to a table given by match_table_name.
    The match_table can potentially contain duplicate dataset_way_ids for each osm_way_id. Further processing should be done
    on match_table by calling extract_best_match.
    """
    # Explode h3 columns for both OSM and external dataset
    osm_way_nodes_exploded = osm_way_nodes_interpolated_h3.withColumn(
        "osm_h3", explode("interpolated_h3")
    ).select("osm_way_id", "osm_h3")
    dataset_way_nodes_exploded = dataset_ways_interpolated_h3.withColumn(
        "dataset_h3", explode("interpolated_h3")
    ).select("dataset_way_id", "dataset_h3")

    # Perform an full join on the exploded h3 columns
    dataset_matching_ways_full = osm_way_nodes_exploded.join(
        dataset_way_nodes_exploded,
        (osm_way_nodes_exploded.osm_h3 == dataset_way_nodes_exploded.dataset_h3),
        how="full_outer",
    )
    # Cross join
    matched_ways = dataset_matching_ways_full.groupBy(
        ["osm_way_id", "dataset_way_id"]
    ).agg(collect_list(col("dataset_h3")).alias("matched_h3"))
    matched_ways = (
        matched_ways.alias("matching_orig")
        .join(
            osm_way_nodes_interpolated_h3.alias("osm"),
            col("osm.osm_way_id") == col("matching_orig.osm_way_id"),
            how="left",
        )
        .join(
            dataset_ways_interpolated_h3.alias("dataset"),
            col("dataset.dataset_way_id") == col("matching_orig.dataset_way_id"),
            how="left",
        )
        .select(
            "matching_orig.*",
            col("osm.interpolated_h3_size").alias("osm_interp_h3_size"),
            col("dataset.interpolated_h3_size").alias("dataset_interp_h3_size"),
        )
    )
    return matched_ways


def extract_dataset_best_match(
    dataset_df: DataFrame, matched_ways: DataFrame
) -> DataFrame:
    """
    The general strategy for fetching the best match is as follows:
    We want to trade off reducing false positives for increasing false negatives.
    This is a strictly binary tradeoff since multiple dataset ways can map to a single OSM way.
    1. Average the dataset h3 size grouped by osm way id.
    2. Take the highest speed limit out of all matches >= the average.
    This ensures that we select the highest limit out of most probable matches.
    """
    # Exclude unmatched ways
    reduced_matched_ways = matched_ways.filter(
        "osm_way_id is not null and dataset_way_id is not null"
    )
    # count the size of the match array
    match_size_sdf = reduced_matched_ways.withColumn(
        "matched_h3_size", size(col("matched_h3"))
    ).drop("matched_h3")

    # choose between the maxspeed(tomtom) and the speed_limit_milliknots(regulatory, ml-cv)
    if "speed_limit_milliknots" in dataset_df.columns:
        speed_expr = col("dataset.speed_limit_milliknots").alias("speed_limit")
    else:
        speed_expr = col("dataset.maxspeed").alias("speed_limit")

    # Join the raw dataset to get the speed limit
    match_size_sdf = (
        match_size_sdf.alias("match")
        .join(
            dataset_df.alias("dataset"),
            (col("match.dataset_way_id") == col("dataset.dataset_way_id")),
        )
        .select("match.*", speed_expr)
    )
    # For each osm_way_id, fetch the average size of matches.
    df_50p = (
        (
            match_size_sdf.alias("a")
            .groupBy("osm_way_id")
            .agg(avg(col("matched_h3_size")).alias("avg_matched_h3_size"))
        )
        .join(match_size_sdf.alias("b"), col("a.osm_way_id") == col("b.osm_way_id"))
        .select("b.*", "avg_matched_h3_size")
    )
    # Select the greatest speed limit corresponding to ways that have a match size
    # greater than the average of matches for that way.
    df_top_50p = (
        df_50p.filter("matched_h3_size >= avg_matched_h3_size")
        .groupBy("osm_way_id")
        .agg(max(col("speed_limit")).alias("maxspeed"))
        .select("osm_way_id", "maxspeed")
    )
    # Get all ways that have the highest speed limit
    max_match_sdf = (
        match_size_sdf.alias("match")
        .join(
            df_top_50p.alias("top"),
            (col("match.osm_way_id") == col("top.osm_way_id"))
            & (col("match.speed_limit") == col("top.maxspeed")),
        )
        .join(
            df_50p.alias("matches_with_avg"),
            (col("matches_with_avg.osm_way_id") == col("match.osm_way_id"))
            & (col("matches_with_avg.dataset_way_id") == col("match.dataset_way_id")),
        )
        .select(
            "match.osm_way_id",
            "match.dataset_way_id",
            "match.matched_h3_size",
            "match.osm_interp_h3_size",
            "match.dataset_interp_h3_size",
            "matches_with_avg.avg_matched_h3_size",
        )
    )
    # Filter for ways that meet the minimum match size and which meet the minimum way overlap.

    # Require all matches for the speed limit to meet a minimum overlap percent
    # with the osm way.
    MIN_WAY_CUMULATIVE_COVERAGE_THRESHOLD_PCT = 0.5

    sum_matched_ways = (
        max_match_sdf.filter("osm_way_id IS NOT NULL AND dataset_way_id IS NOT NULL")
        .groupBy("osm_way_id", "osm_interp_h3_size")
        .agg(sum("matched_h3_size").alias("sum_matched_h3_size"))
        .select("osm_way_id", "osm_interp_h3_size", "sum_matched_h3_size")
    )
    cumulative_pct_sum_way = sum_matched_ways.select(
        "osm_way_id",
        least(
            col("sum_matched_h3_size") / col("osm_interp_h3_size") * lit(100), lit(100)
        ).alias("way_cumulative_coverage_pct"),
    )
    max_match_sdf = (
        max_match_sdf.alias("match")
        .join(
            cumulative_pct_sum_way.alias("sum_matched"),
            col("match.osm_way_id") == col("sum_matched.osm_way_id"),
            "left",
        )
        .filter(
            f"way_cumulative_coverage_pct >= {MIN_WAY_CUMULATIVE_COVERAGE_THRESHOLD_PCT}"
        )
        .filter("match.matched_h3_size >= match.avg_matched_h3_size")
        .select("match.*", "sum_matched.way_cumulative_coverage_pct")
    )
    # Out of all matches, get the longest match size.
    df_longest_match = (
        max_match_sdf.groupBy("osm_way_id")
        .agg(max(col("matched_h3_size")).alias("length"))
        .select("osm_way_id", "length")
    )
    # Select the match with the longest match size.
    max_match_sdf = (
        max_match_sdf.alias("match")
        .join(
            df_longest_match.alias("longest"),
            (col("match.osm_way_id") == col("longest.osm_way_id"))
            & (col("match.matched_h3_size") == col("longest.length")),
        )
        .select(
            "match.osm_way_id",
            "match.dataset_way_id",
            "match.matched_h3_size",
            "match.osm_interp_h3_size",
            "match.dataset_interp_h3_size",
            "match.way_cumulative_coverage_pct",
        )
    )
    # If two dataset ways tie for the same max length, select one at random.
    dataset_best_matches = max_match_sdf.groupBy("osm_way_id").agg(
        first(col("dataset_way_id")).alias("dataset_way_id"),
        first(col("matched_h3_size")).alias("matched_h3_size"),
        first(col("osm_interp_h3_size")).alias("osm_interp_h3_size"),
        first(col("dataset_interp_h3_size")).alias("dataset_interp_h3_size"),
        first(col("way_cumulative_coverage_pct")).alias("way_cumulative_coverage_pct"),
    )
    return dataset_best_matches


def join_map_matched_result(
    dataset_df: DataFrame, dataset_best_matches: DataFrame, regions: List[str]
) -> DataFrame:
    map_match_and_dataset = dataset_best_matches.alias("match").join(
        dataset_df.alias("dataset"),
        col("dataset.dataset_way_id") == col("match.dataset_way_id"),
    )

    # Add OSM
    osm_way_nodes = get_osm_nodes_by_regions(regions)
    joined_result = map_match_and_dataset.join(
        osm_way_nodes.alias("osm"),
        col("osm.way_id") == col("match.osm_way_id"),
        how="inner",
    )

    # for the optional columns
    raw_speed_limit_col = (
        col("dataset.raw_speed_limit").alias("raw_speed_limit")
        if "raw_speed_limit" in dataset_df.columns
        else lit(None).alias("raw_speed_limit")
    )

    raw_speed_limit_unit_col = (
        col("dataset.raw_speed_limit_unit").alias("raw_speed_limit_unit")
        if "raw_speed_limit_unit" in dataset_df.columns
        else lit(None).alias("raw_speed_limit_unit")
    )

    # Fixed columns to select
    cadence_refresh_at_ts = current_timestamp()
    columns_to_select = [
        col("osm.way_id").alias("osm_way_id"),
        col("osm.highway").alias("osm_highway"),
    ]

    optional_columns_regulatory = [
        col("dataset.dataset_way_id").alias("dataset_way_id"),
        col("dataset.speed_limit_milliknots").alias("speed_limit_milliknots"),
        raw_speed_limit_col,
        raw_speed_limit_unit_col,
        lit(cadence_refresh_at_ts).alias("created_at"),
        col("match.way_cumulative_coverage_pct").alias("way_cumulative_coverage_pct"),
    ]

    optional_columns_tomtom = [
        # regions list in tomtom only contains one region
        osm_passenger_limit_unit_fixer_udf(lit(regions[0]), col("osm.maxspeed")).alias(
            "osm_maxspeed"
        ),
        col("dataset.dataset_way_id").alias("tomtom_way_id"),
        col("osm.maxspeed").alias("osm_raw_maxspeed"),
        col("dataset.maxspeed").alias("tomtom_maxspeed"),
        col("dataset.vehicle_type_speed_map").alias(
            "tomtom_commercial_vehicle_speed_map"
        ),
        col("dataset.maxspeed_unit").alias("tomtom_maxspeed_unit"),
        col("dataset.country_code").alias("tomtom_country_code"),
        col("dataset.state_code").alias("tomtom_state_code"),
        col("match.matched_h3_size").alias("matched_h3_size"),
        col("match.osm_interp_h3_size").alias("osm_interp_h3_size"),
        col("match.dataset_interp_h3_size").alias("tomtom_interp_h3_size"),
        col("match.way_cumulative_coverage_pct").alias("way_cumulative_coverage_pct"),
    ]

    # TODO: make this configurable instead of simply hardcoding by column name
    add_optional_tomtom_columns = (
        True if "vehicle_type_speed_map" in dataset_df.columns else False
    )
    if add_optional_tomtom_columns:
        columns_to_select.extend(optional_columns_tomtom)

    add_optional_regulatory_columns = (
        True if "speed_limit_milliknots" in dataset_df.columns else False
    )
    if add_optional_regulatory_columns:
        columns_to_select.extend(optional_columns_regulatory)

    # Grab only columns we care about
    dataset_osm_matching_result = joined_result.select(*columns_to_select)
    return dataset_osm_matching_result


def do_interpolate_and_map_match(
    dataset_df: DataFrame, regions: List[str]
) -> DataFrame:
    """
    This func is for the datasets that have a list of coordinates per road
    """
    osm_h3 = interpolate_osm_ways(regions)
    dataset_h3 = interpolate_dataset_ways(dataset_df)
    matched_ways = find_map_matches(osm_h3, dataset_h3)
    dataset_best_matches = extract_dataset_best_match(dataset_df, matched_ways)
    dataset_osm_matching_result = join_map_matched_result(
        dataset_df, dataset_best_matches, regions
    )
    return dataset_osm_matching_result, matched_ways


def do_map_match(dataset_df: DataFrame, regions: List[str], res: int = 11) -> DataFrame:
    """
    This func is for the datasets that have only a coordinate per road

    For this type of datasets, the coordinates might not always lie on a road
    therefore not enough osm ways h3 cells to match, so we will need a lower resolution,
    however if too low there will be a precisoin problem. So I make this configurable, as it might vary across datasets

    H3 examples of a different resolution from the same speed limit sign coordinate from Utah dataset:
    - res = 13, https://h3geo.org/#hex=8d2693131b8e4bf
    - res = 12, https://h3geo.org/#hex=8c2693131b8e5ff
    - res = 11, https://h3geo.org/#hex=8b2693131b9dfff
    """
    osm_h3 = interpolate_osm_ways(regions, res)
    dataset_h3 = process_single_dataset_ways(dataset_df, res)
    matched_ways = find_map_matches(osm_h3, dataset_h3)
    dataset_best_matches = extract_dataset_best_match(dataset_df, matched_ways)
    dataset_osm_matching_result = join_map_matched_result(
        dataset_df, dataset_best_matches, regions
    )
    return dataset_osm_matching_result, matched_ways
