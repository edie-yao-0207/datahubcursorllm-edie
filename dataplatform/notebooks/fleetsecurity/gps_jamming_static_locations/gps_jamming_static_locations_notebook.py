# Databricks notebook source
# MAGIC %md
# MAGIC This notebook is persisted in our monorepo and scheduled to run every day. If you'd like to make a change, follow instructions here: [backend](https://github.com/samsara-dev/backend/tree/master/dataplatform/notebooks/fleetsecurity/gps_jamming_static_locations/README.md)

# COMMAND ----------

import copy
import json
import math
import os
import shutil
from datetime import datetime

import boto3
import numpy as np
import pandas as pd
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from sklearn.cluster import DBSCAN
from sklearn.preprocessing import StandardScaler

# COMMAND ----------

# DBTITLE 1,-------- CONFIG --------
# TODO: FILTER_MAX_DISTANCE_BETWEEN_ON_OFF_METERS is working far from ideal as osDJammingDetection sometimes has stale location data
#   see here for more details: https://samsara-dev-us-west-2.cloud.databricks.com/editor/notebooks/82285110011641?o=5924096274798303#command/82285110011644
#   thread: https://samsara-rd.slack.com/archives/C06MRH2GK8Q/p1743687655602079

FILTER_START_DATE_OFFSET_MONTHS = 1
FILTER_START_DATE = (
    datetime.now() - pd.offsets.DateOffset(months=FILTER_START_DATE_OFFSET_MONTHS)
).strftime(
    "%Y-%m-%d %H:%M:%S"
)  # example: 2025-03-15 08:00:00
FILTER_END_DATE = datetime.now().strftime(
    "%Y-%m-%d %H:%M:%S"
)  # example: 2025-04-15 08:00:00

FILTER_ORG_LOCALE = "mx"  # use empty string to disable filtering

FILTER_MIN_DURATION_BETWEEN_JAMMING_ON_OFF_SECONDS = 0  # use 0 to disable filtering
FILTER_MAX_TRANSITIONS_WITHIN_A_WEEK = 0  # use 0 to disable filtering
FILTER_MAX_GPS_FIX_CHANGES_WITHIN_A_WEEK = 2000  # use 0 to disable filtering
FILTER_MAX_DISTANCE_BETWEEN_ON_OFF_METERS = 2000  # use 0 to disable filtering

CLUSTER_EPS = 0.0007  # max radius to form a cluster
CLUSTER_MIN_SAMPLES = 5  # clusters with fewer than this number of events are dropped
CLUSTER_MIN_DAYS_WITHIN_CLUSTER = 2
CLUSTER_MIN_NUM_OF_VEHICLES = 2

GEOJSON_TARGET_DIRS = [
    "/Volumes/s3/databricks-workspace/fleetsec/gps_jamming_heatmap",
]

SCORING_CRITERIA = {
    "data_freshness": {
        "weight": 30,
        "to_achieve_max_score": {"days_since_jamming": 0},
        "to_achieve_min_score": {"days_since_jamming": 7},
    },
    "percentage_of_days_with_jamming": {
        "weight": 20,
        "to_achieve_max_score": {"min_percentage": 80},
    },
    "number_of_vehicles_affected": {
        "weight": 10,
        "to_achieve_max_score": {"min_unique_vehicles": 5},
    },
    "number_of_organizations_affected": {
        "weight": 5,
        "to_achieve_max_score": {"min_unique_orgs": 2},
    },
}


# COMMAND ----------

# MAGIC %run /backend/fleetsecurity/gps_jamming_static_locations/config_overrides

# COMMAND ----------

# COMMAND ----------

# MAGIC %md
# MAGIC # Shared

# COMMAND ----------

# DBTITLE 1,Common
def find_unique_object_ids(df: DataFrame) -> list[int]:
    return [row["object_id"] for row in df.select("object_id").distinct().collect()]


def render_object_ids(object_ids: list[int]):
    if len(object_ids) == 0:
        return "object ids: ALL"
    if len(object_ids) > 100:
        return f"object ids ({len(object_ids)})"
    return f"object ids ({len(object_ids)}): {','.join(map(str, object_ids))}"


def list_top_level_dirs(path):
    print(f"{path}:")
    try:
        with os.scandir(path) as entries:
            for entry in entries:
                if entry.is_dir():
                    print(entry.name)
    except Exception as e:
        print(f"Error: {str(e)}")


def backup_file(dir, file):
    file_path = os.path.join(dir, file)
    if not os.path.exists(file_path):
        return

    backup_dir = os.path.join(dir, "backup")
    os.makedirs(backup_dir, exist_ok=True)

    backup_file_path = os.path.join(
        backup_dir, datetime.now().strftime("%Y_%m_%d.%H_%M_%S") + "." + file
    )
    shutil.copy(file_path, backup_file_path)


# COMMAND ----------

# DBTITLE 1,Jamming events
SOURCE_GPS = 1

STATE_NOT_JAMMED = 1
STATE_JAMMED = 2

DEBUG_LINK_BEFORE_MS = 30 * 60 * 1000  # 30 minutes
DEBUG_LINK_AFTER_MS = 30 * 60 * 1000  # 30 minutes
CAMERA_BEFORE_MS = 30 * 1000  # 30 seconds
CAMERA_AFTER_MS = 30 * 1000  # 30 seconds


def fetch_jamming_events(
    start_time_ms: int,
    end_time_ms: int,
    org_locale: str,
    object_ids: list[int] = [],
    deduplicate=True,
    log_prefix="",
) -> DataFrame:
    start_date = datetime.fromtimestamp(start_time_ms / 1000).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    end_date = datetime.fromtimestamp(end_time_ms / 1000).strftime("%Y-%m-%d %H:%M:%S")
    str_time_range = f"time range: {start_date} - {end_date}"
    str_locale = f"locale: {org_locale}" if org_locale != "" else "locale: ALL"
    print(
        f"{log_prefix}Fetching osDJammingDetection | {str_time_range} | {str_locale} | {render_object_ids(object_ids)}"
    )

    query = f"""
      SELECT
        j.time,
        from_unixtime(CAST(j.time/1000 as BIGINT)) as datetime,
        j.org_id,
        j.object_id,
        state_source.state as state,
        j.value.proto_value.jamming_detection.last_location.latitude_nano_degrees / 1e9 AS latitude,
        j.value.proto_value.jamming_detection.last_location.longitude_nano_degrees / 1e9 AS longitude,
        j.value.proto_value.jamming_detection.last_location.accuracy_mm / 1000 as accuracy_meters
      FROM
        kinesisstats.osdjammingdetection j
      LEFT JOIN clouddb.organizations o ON o.id = j.org_id
      LATERAL VIEW EXPLODE(j.value.proto_value.jamming_detection.source_states) AS state_source
      WHERE
        j.value.proto_value IS NOT NULL AND
        j.value.proto_value.jamming_detection.last_location.latitude_nano_degrees IS NOT NULL AND
        j.value.proto_value.jamming_detection.last_location.longitude_nano_degrees IS NOT NULL AND
        state_source.source = {SOURCE_GPS} AND
        j.time >= "{start_time_ms}" AND j.time <= "{end_time_ms}"
        {"" if org_locale == "" else f"AND o.locale = '{org_locale}'"}
        {"" if not object_ids else f"AND j.object_id IN ({','.join(map(str, object_ids))})"}
      ;
    """
    df = spark.sql(query)

    if deduplicate:
        # there's a dropDuplicates spark function, but it may rows in non-deterministic order
        df = df.withColumn(
            "rank",
            F.row_number().over(
                Window.partitionBy(
                    "org_id", "object_id", "state", "latitude", "longitude"
                ).orderBy(F.col("time").asc())
            ),
        )
        df = df.filter(F.col("rank") == 1).drop("rank")

    return df.orderBy(df.time.desc())


def find_on_off_transitions(df: DataFrame, min_duration_seconds: int) -> DataFrame:
    # create a window specification to access the previous row for each device_id, ordered by timestamp
    window_spec = Window.partitionBy("object_id").orderBy("time")
    df = (
        df.withColumn("prev_time", F.lag("time").over(window_spec))
        .withColumn("prev_datetime", F.lag("datetime").over(window_spec))
        .withColumn("prev_state", F.lag("state").over(window_spec))
        .withColumn("prev_latitude", F.lag("latitude").over(window_spec))
        .withColumn("prev_longitude", F.lag("longitude").over(window_spec))
        .withColumn("duration_seconds", (F.col("time") - F.col("prev_time")) / 1000)
        .withColumn("duration_minutes", (F.col("duration_seconds")) / 60)
        .withColumn(
            "debug_link",
            F.concat(
                F.lit("https://cloud.samsara.com/o/"),
                F.col("org_id").cast("string"),
                F.lit("/devices/"),
                F.col("object_id").cast("string"),
                F.lit("/show?end_ms="),
                (F.col("time") + F.lit(DEBUG_LINK_AFTER_MS)).cast("string"),
                F.lit("&duration="),
                (
                    F.col("duration_seconds")
                    + F.lit((DEBUG_LINK_BEFORE_MS + DEBUG_LINK_AFTER_MS) / 1000)
                ).cast("string"),
            ),
        )
        .withColumn(
            "camera_link",
            F.concat(
                F.lit("https://cloud.samsara.com/o/"),
                F.col("org_id").cast("string"),
                F.lit("/fleet/safety/camera/"),
                F.col("object_id").cast("string"),
                F.lit("/trip?start_ms="),
                (F.col("prev_time") - F.lit(CAMERA_BEFORE_MS)).cast("string"),
                F.lit("&end_ms="),
                (F.col("time") + F.lit(CAMERA_AFTER_MS)).cast("string"),
            ),
        )
    )

    if min_duration_seconds > 0:
        df = df.filter(
            (F.col("prev_state") == STATE_JAMMED)
            & (F.col("duration_seconds") >= F.lit(min_duration_seconds))
            & (F.col("state") == STATE_NOT_JAMMED)
        )
    else:
        df = df.filter(
            (F.col("prev_state") == STATE_JAMMED) & (F.col("state") == STATE_NOT_JAMMED)
        )

    df = df.select(
        "*",
        F.col("latitude").alias("end_latitude"),
        F.col("longitude").alias("end_longitude"),
        F.col("time").alias("end_time"),
    )
    # drop not useful columns from the STATE_NOT_JAMMED state
    df = df.drop("time", "datetime", "latitude", "longitude")

    # rename `prev_` columns
    df = df.select(
        F.col("prev_time").alias("time"),
        F.col("prev_datetime").alias("datetime"),
        "*",
        F.col("prev_latitude").alias("latitude"),
        F.col("prev_longitude").alias("longitude"),
    ).drop("prev_time", "prev_datetime", "prev_latitude", "prev_longitude")

    # drop not useful columns
    df = df.drop("duration_minutes", "state", "prev_state")

    df = df.withColumn(
        "distance_meters",
        111320
        * F.sqrt(
            F.pow(F.col("latitude") - F.col("end_latitude"), 2)
            + F.pow(
                (F.col("longitude") - F.col("end_longitude"))
                * F.cos(F.radians(F.col("latitude"))),
                2,
            )
        ),
    )

    return df


def aggregate_jamming_events_to_per_week_stats(df: DataFrame) -> DataFrame:
    window_spec = Window.partitionBy("object_id").orderBy("time")
    df = df.withColumn("prev_state", F.lag("state").over(window_spec))
    df = df.filter(
        (F.col("prev_state") == STATE_NOT_JAMMED) & (F.col("state") == STATE_JAMMED)
    )
    df = df.groupBy(
        F.window(F.col("datetime"), "1 week").alias("week"),
        F.col("object_id"),
        F.col("org_id"),
    ).agg(F.count("state").alias("num_transitions"))
    return df


def filter_out_vehicles_with_at_least_n_transitions(
    events: DataFrame, transition_stats_per_week: DataFrame, n: int
) -> DataFrame:
    if n <= 0:
        return events
    vehicles_to_remove = (
        transition_stats_per_week.filter(F.col("num_transitions") >= n)
        .select("object_id")
        .distinct()
    )
    filtered_events = events.join(vehicles_to_remove, on="object_id", how="left_anti")
    return filtered_events


# COMMAND ----------

# DBTITLE 1,GPS State
GPS_FIX = 1
GPS_NO_FIX = 2


def fetch_gps_state(
    start_time_ms: int, end_time_ms: int, object_ids: list[int] = [], log_prefix=""
):
    start_date = datetime.fromtimestamp(start_time_ms / 1000).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    end_date = datetime.fromtimestamp(end_time_ms / 1000).strftime("%Y-%m-%d %H:%M:%S")
    str_time_range = f"time range: {start_date} - {end_date}"
    print(
        f"{log_prefix}Fetching osDGpsHealth | {str_time_range} | {render_object_ids(object_ids)}"
    )

    query = f"""
      SELECT
        g.time,
        from_unixtime(CAST(g.time/1000 as BIGINT)) as datetime,
        g.org_id,
        g.object_id,
        period.gps_state as state
      FROM
        kinesisstats.osdgpshealth g
      LATERAL VIEW explode(g.value.proto_value.gps_health.periods) as period
      WHERE
        g.value.proto_value IS NOT NULL AND
        g.value.proto_value.gps_health.periods IS NOT NULL AND
        g.time >= "{start_time_ms}" AND g.time <= "{end_time_ms}"
        {"" if not object_ids else f"AND g.object_id IN ({','.join(map(str, object_ids))})"}
      ;
    """
    df = spark.sql(query)
    return df


def aggregate_gps_state_to_per_week_stats(df: DataFrame) -> DataFrame:
    window_spec = Window.partitionBy("object_id").orderBy("datetime")

    df = df.withColumn("prev_state", F.lag("state").over(window_spec))
    df = df.withColumn(
        "state_changed", F.when(F.col("state") != F.col("prev_state"), 1).otherwise(0)
    )

    df = df.groupBy(
        F.window(F.col("datetime"), "1 week").alias("week"),
        F.col("object_id"),
        F.col("org_id"),
    ).agg(
        F.sum("state_changed").alias("num_state_changes"),
        F.sum(F.when(F.col("state") == GPS_FIX, 1).otherwise(0)).alias("num_fix"),
        F.sum(F.when(F.col("state") == GPS_NO_FIX, 1).otherwise(0)).alias("num_no_fix"),
    )
    return df


def filter_out_vehicles_with_at_least_n_gps_state_changes(
    events: DataFrame, transition_stats_per_week: DataFrame, n: int
) -> DataFrame:
    if n <= 0:
        return events
    vehicles_to_remove = (
        transition_stats_per_week.filter(F.col("num_state_changes") >= n)
        .select("object_id")
        .distinct()
    )
    filtered_events = events.join(vehicles_to_remove, on="object_id", how="left_anti")
    return filtered_events


# COMMAND ----------

# DBTITLE 1,Location
# Haversine formula to calculate distance between two lat/lon points
def haversine(lat1, lon1, lat2, lon2):
    # Radius of Earth in kilometers
    R = 6371.0

    # Convert latitude and longitude from degrees to radians
    lat1_rad, lon1_rad = np.radians(lat1), np.radians(lon1)
    lat2_rad, lon2_rad = np.radians(lat2), np.radians(lon2)

    # Differences in coordinates
    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad

    # Haversine formula
    a = (
        np.sin(dlat / 2) ** 2
        + np.cos(lat1_rad) * np.cos(lat2_rad) * np.sin(dlon / 2) ** 2
    )
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))

    # Distance in kilometers
    distance = R * c
    return distance * 1000  # in meters


def fetch_vehicles_in_proximity(
    lat: int, long: int, start_time_ms: int, end_time_ms: int, radius_meters: float
) -> DataFrame:
    start_date = datetime.fromtimestamp(start_time_ms / 1000).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    end_date = datetime.fromtimestamp(end_time_ms / 1000).strftime("%Y-%m-%d %H:%M:%S")
    display(
        f"Fetching vehicles in close proximity. Time range: {start_date} - {end_date} | radius: {radius_meters}m"
    )

    # Approximate distance calculation using Pythagorean theorem
    query = f"""
      SELECT
        l.time,
        from_unixtime(CAST(l.time/1000 as BIGINT)) as datetime,
        l.org_id,
        l.device_id AS object_id,
        l.value.latitude AS latitude,
        l.value.longitude AS longitude,
        111320 * SQRT(
            POW(l.value.latitude - {lat}, 2) +
            POW((l.value.longitude - {long}) * COS(RADIANS({lat})), 2)
        ) AS distance_to_centroid_meters
      FROM
        kinesisstats.location l
      WHERE
        l.value.latitude IS NOT NULL AND
        l.value.longitude IS NOT NULL AND
        l.time >= {start_time_ms} AND
        l.time <= {end_time_ms}
    """
    df = spark.sql(query)

    # Filter vehicles within the specified radius
    df_filtered = df.filter(
        F.col("distance_to_centroid_meters") <= radius_meters
    ).limit(1000)

    return df_filtered


def fetch_vehicle_location(
    object_id: int, start_time_ms: int, end_time_ms: int
) -> DataFrame:
    start_date = datetime.fromtimestamp(start_time_ms / 1000).strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    end_date = datetime.fromtimestamp(end_time_ms / 1000).strftime("%Y-%m-%d %H:%M:%S")
    display(
        f"Fetching LOCATION. Time range: {start_date} - {end_date} | object id: {object_id}"
    )

    query = f"""
      SELECT
        l.time,
        from_unixtime(CAST(l.time/1000 as BIGINT)) as datetime,
        l.org_id,
        l.device_id AS object_id,
        l.value.latitude AS latitude,
        l.value.longitude AS longitude
      FROM
        kinesisstats.location l
      WHERE
        l.value.latitude IS NOT NULL AND
        l.value.longitude IS NOT NULL AND
        l.time >= {start_time_ms} AND
        l.time <= {end_time_ms} AND
        l.device_id = {object_id}
    """
    df = spark.sql(query)
    df_filtered = df.limit(5000)

    return df_filtered


# COMMAND ----------

# DBTITLE 1,ClusterMetadata
class ClusterMetadata:
    def __init__(self, cluster: pd.DataFrame):
        self.cluster = cluster

        # Calculate the total number of days between the start and end dates
        self._start_date = datetime.strptime(FILTER_START_DATE, "%Y-%m-%d  %H:%M:%S")
        self._end_date = datetime.strptime(FILTER_END_DATE, "%Y-%m-%d %H:%M:%S")
        self._total_days = (self._end_date - self._start_date).days

        self.first_event = cluster.iloc[0]
        self.cluster_id = int(self.first_event["cluster"])

        self.centroid_lat = float(cluster["latitude"].mean())
        self.centroid_long = float(cluster["longitude"].mean())
        self.centroid_latlong = f"{self.centroid_lat},{self.centroid_long}"

        self.unique_days = cluster["datetime"].dt.date.nunique()
        self.unique_vehicles = cluster["object_id"].nunique()
        self.unique_orgs = cluster["org_id"].nunique()

        self.latest_jamming_date = self.cluster["datetime"].max().date()
        self.days_since_latest_jamming = (
            self._end_date.date() - self.latest_jamming_date
        ).days

        self.all_scores = self._calc_all_scores()
        self.score = self._score()

    def _calc_all_scores(self):
        def scale_to_weight(value, max_value, criteria):
            return min(float(value) / float(max_value), 1) * float(criteria["weight"])

        def score_data_freshness():
            criteria = SCORING_CRITERIA["data_freshness"]
            max_days_to_max_score = criteria["to_achieve_max_score"][
                "days_since_jamming"
            ]
            min_days_to_min_score = criteria["to_achieve_min_score"][
                "days_since_jamming"
            ]
            days_since = self.days_since_latest_jamming

            # example:
            # end date: 2025-01-29
            #
            # max_days_to_max_score: 3
            # max score date: 2025-01-26
            #
            # min_days_to_min_score: 7
            # min score date: 2025-01-22
            #
            # days since: 2 = 100%
            # days since: 3 = 100%
            # days since: 4 = 75%
            # days since: 5 = 50%
            # days since: 6 = 25%
            # days since: 7 = 0%
            if days_since <= max_days_to_max_score:
                return criteria["weight"]

            if days_since >= min_days_to_min_score:
                return 0

            max_value = min_days_to_min_score - max_days_to_max_score
            days_since_offset = max(days_since - max_days_to_max_score, 0)
            value = max_value - days_since_offset
            return scale_to_weight(value, max_value, criteria)

        def score_percentage_of_days_with_jamming():
            criteria = SCORING_CRITERIA["percentage_of_days_with_jamming"]
            min_to_max_score = (
                self._total_days
                * criteria["to_achieve_max_score"]["min_percentage"]
                / 100
            )
            return scale_to_weight(self.unique_days, min_to_max_score, criteria)

        def score_number_of_vehicles_affected():
            criteria = SCORING_CRITERIA["number_of_vehicles_affected"]
            min_to_max_score = criteria["to_achieve_max_score"]["min_unique_vehicles"]
            return scale_to_weight(self.unique_vehicles, min_to_max_score, criteria)

        def score_number_of_organizations_affected():
            criteria = SCORING_CRITERIA["number_of_organizations_affected"]
            min_to_max_score = criteria["to_achieve_max_score"]["min_unique_orgs"]
            return scale_to_weight(self.unique_orgs, min_to_max_score, criteria)

        scores = {
            "data_freshness": score_data_freshness(),
            "percentage_of_days_with_jamming": score_percentage_of_days_with_jamming(),
            "number_of_vehicles_affected": score_number_of_vehicles_affected(),
            "number_of_organizations_affected": score_number_of_organizations_affected(),
        }
        return scores

    def _score(self):
        # Scale the score to 100
        max_score = sum(criteria["weight"] for criteria in SCORING_CRITERIA.values())
        final_score = sum(self.all_scores.values())
        final_score = (final_score / max_score) * 100

        return math.floor(final_score)


# COMMAND ----------

# DBTITLE 1,Distance
def filter_events_by_distance(df: DataFrame, max_distance_meters: int) -> DataFrame:
    return df.filter(F.col("distance_meters") <= max_distance_meters)


def fetch_location_data_for_event(time, object_id):
    delta_ms = 30 * 60 * 1000  # 30 minutes
    query = f"""
      SELECT
        l.time,
        l.device_id AS object_id,
        l.value.latitude AS latitude,
        l.value.longitude AS longitude
      FROM
        kinesisstats.location l
      WHERE
        l.time >= {time - delta_ms} AND
        l.time <= {time + delta_ms} AND
        l.device_id = {object_id} AND
        l.value.latitude IS NOT NULL AND
        l.value.longitude IS NOT NULL
    """
    location_df = spark.sql(query)
    return location_df


def enrich_distance(events_df: DataFrame) -> DataFrame:
    def fetch_and_enrich(event_row):
        if event_row["distance_meters"] != 0:
            return event_row

        start_location_df = fetch_location_data_for_event(
            event_row["time"], event_row["object_id"]
        ).cache()
        end_location_df = fetch_location_data_for_event(
            event_row["end_time"], event_row["object_id"]
        ).cache()

        if start_location_df.count() == 0:
            return event_row

        if end_location_df.count() == 0:
            return event_row

        start_location = start_location_df.orderBy(
            F.abs(F.col("time") - event_row["time"])
        ).collect()[0]
        end_location = end_location_df.orderBy(
            F.abs(F.col("time") - event_row["end_time"])
        ).collect()[0]

        new_distance_meters = haversine(
            start_location["latitude"],
            start_location["longitude"],
            end_location["latitude"],
            end_location["longitude"],
        )

        event_row["distance_meters"] = new_distance_meters

        return event_row

    def fetch_and_enrich_udf(events_pd):
        enriched_events_pd = events_pd.apply(fetch_and_enrich, axis=1)
        return enriched_events_pd

    enriched_events_df = events_df.groupBy("org_id").applyInPandas(
        fetch_and_enrich_udf, schema=events_df.schema
    )
    return enriched_events_df


# COMMAND ----------

# MAGIC %md
# MAGIC # Algo

# COMMAND ----------

# DBTITLE 1,basic fetching and filtering
start_time_ms = int(
    datetime.strptime(FILTER_START_DATE, "%Y-%m-%d %H:%M:%S").timestamp() * 1000
)
end_time_ms = int(
    datetime.strptime(FILTER_END_DATE, "%Y-%m-%d %H:%M:%S").timestamp() * 1000
)

events = fetch_jamming_events(start_time_ms, end_time_ms, FILTER_ORG_LOCALE).cache()
print(f"  Number of osDJammingDetection entries (deduplicated): {events.count()}")

if FILTER_MIN_DURATION_BETWEEN_JAMMING_ON_OFF_SECONDS > 0:
    print(
        f"Finding transitions with at least {FILTER_MIN_DURATION_BETWEEN_JAMMING_ON_OFF_SECONDS} seconds between jamming on->off"
    )
else:
    print(f"Finding on-off transitions")

events = find_on_off_transitions(
    events, FILTER_MIN_DURATION_BETWEEN_JAMMING_ON_OFF_SECONDS
).cache()
print(f"  Number of on->off transitions: {events.count()}")

unique_object_ids = find_unique_object_ids(events)
print(f"  Number of vehicles: {len(unique_object_ids)}")

if FILTER_MAX_TRANSITIONS_WITHIN_A_WEEK > 0:
    print(
        f"Filtering to vehicles with at most {FILTER_MAX_TRANSITIONS_WITHIN_A_WEEK} jamming on->off transitions"
    )

    per_week_stats = aggregate_jamming_events_to_per_week_stats(events).cache()
    print("  Jamming events per week stats:")
    print(per_week_stats)

    events = filter_out_vehicles_with_at_least_n_transitions(
        events, per_week_stats, FILTER_MAX_TRANSITIONS_WITHIN_A_WEEK
    ).cache()
    print(f"  Number of filtered osDJammingDetection entries: {events.count()}")

    unique_object_ids = find_unique_object_ids(events)
    print(f"  Number of vehicles after filtering: {len(unique_object_ids)}")

if FILTER_MAX_GPS_FIX_CHANGES_WITHIN_A_WEEK > 0:
    print(
        f"Filtering to vehicles with at most {FILTER_MAX_GPS_FIX_CHANGES_WITHIN_A_WEEK} gps state changes within a single week"
    )

    gps_state = fetch_gps_state(
        start_time_ms, end_time_ms, object_ids=unique_object_ids, log_prefix="  "
    ).cache()
    print(f"  Total number of gps state entries: {gps_state.count()}")

    gps_state_per_week_stats = aggregate_gps_state_to_per_week_stats(gps_state)
    # print("GPS State per week stats:")
    # display(gps_state_per_week_stats)

    events = filter_out_vehicles_with_at_least_n_gps_state_changes(
        events, gps_state_per_week_stats, FILTER_MAX_GPS_FIX_CHANGES_WITHIN_A_WEEK
    ).cache()
    print(f"  Number of filtered jamming transitions:: {events.count()}")

    unique_object_ids = find_unique_object_ids(events)
    print(f"  Number of vehicles after filtering: {len(unique_object_ids)}")

if FILTER_MAX_DISTANCE_BETWEEN_ON_OFF_METERS > 0:
    print(
        f"Filtering to transition events with at most {FILTER_MAX_DISTANCE_BETWEEN_ON_OFF_METERS} meters between jamming on->off"
    )
    events = filter_events_by_distance(
        events, FILTER_MAX_DISTANCE_BETWEEN_ON_OFF_METERS
    ).cache()
    print(f"  Number of filtered jamming transitions: {events.count()}")

    unique_object_ids = find_unique_object_ids(events)
    print(f"  Number of vehicles after filtering: {len(unique_object_ids)}")

# BE CAREFUL! This is costly to run, as location data is huge
# print("Enriching distance between jamming on->off with location object stat")
# events = enrich_distance(events)

# BE CAREFUL! Too many events may kill the browser during map rendering. And if the browser is not working, it is impossible to edit the code either
# The only way to recover is to clone a notebook
display(events)


# COMMAND ----------

# DBTITLE 1,clustering
def cluster(df: DataFrame, eps: int, min_samples: int) -> pd.DataFrame:
    # convert to Pandas DataFrame for DBSCAN
    result = df.toPandas()

    # apply DBSCAN clustering
    dbscan = DBSCAN(eps=eps, min_samples=min_samples)
    result["cluster"] = dbscan.fit_predict(result[["latitude", "longitude"]])

    # filter out noise
    result = result[result["cluster"] != -1]

    # convert back to Spark DataFrame
    return spark.createDataFrame(result)


def recalculate_cluster_column(df: DataFrame) -> pd.DataFrame:
    # explicitly partition by a constant so that Spark knows it should use one partition
    window_spec = Window.partitionBy(F.lit(1)).orderBy("cluster")
    df = (
        df.withColumn("prev_cluster", F.lag("cluster").over(window_spec))
        .withColumn(
            "is_new_cluster",
            F.when(F.col("prev_cluster") == F.col("cluster"), 0).otherwise(1),
        )
        .withColumn("proper_cluster", F.sum("is_new_cluster").over(window_spec) - 1)
    )

    return df.drop("prev_cluster", "is_new_cluster", "cluster").withColumnRenamed(
        "proper_cluster", "cluster"
    )


def filter_by_cluster_duration(df: DataFrame, min_days: int) -> pd.DataFrame:
    if min_days <= 1:
        return df

    millis_in_day = 86_400_000
    duration_df = (
        df.groupBy("cluster")
        .agg(
            F.min("time").alias("start_time_ms"),
            F.max("time").alias("end_time_ms"),
        )
        .withColumn(
            "duration_days",
            (F.col("end_time_ms") - F.col("start_time_ms")) / millis_in_day,
        )
    )

    # Filter clusters with duration greater than one day
    long_duration_clusters = duration_df.filter(F.col("duration_days") >= min_days)

    # Join back to get the rows for these clusters
    filtered_df = df.join(
        long_duration_clusters.select("cluster"), on="cluster", how="inner"
    )

    return filtered_df


def filter_by_vehicles_in_cluster(df: DataFrame, min_vehicles: int) -> pd.DataFrame:
    if min_vehicles <= 1:
        return df

    vehicle_count_df = (
        df.groupBy("cluster")
        .agg(F.countDistinct("object_id").alias("num_vehicles"))
        .filter(F.col("num_vehicles") >= min_vehicles)
    )

    # Join back to get the rows for these clusters
    filtered_df = df.join(vehicle_count_df.select("cluster"), on="cluster", how="inner")

    return filtered_df


print("Calculating clusters")
clustered_events = cluster(events, eps=CLUSTER_EPS, min_samples=CLUSTER_MIN_SAMPLES)
total_clusters = clustered_events.select("cluster").distinct().count()
print(f"  Clusters: {total_clusters}")
print(f"  Events: {clustered_events.count()}")
# display(clustered_events) # uncomment to see the progress on filtering

print("Filtering clusters by min number of days within a cluster")
clustered_events = filter_by_cluster_duration(
    clustered_events, min_days=CLUSTER_MIN_DAYS_WITHIN_CLUSTER
)
total_clusters = clustered_events.select("cluster").distinct().count()
print(f"  Clusters: {total_clusters}")
print(f"  Events: {clustered_events.count()}")
# display(clustered_events) # uncomment to see the progress on filtering

print("Filtering clusters by min number of vehicles within a cluster")
clustered_events = filter_by_vehicles_in_cluster(
    clustered_events, min_vehicles=CLUSTER_MIN_NUM_OF_VEHICLES
)
total_clusters = clustered_events.select("cluster").distinct().count()
print(f"  Clusters: {total_clusters}")
print(f"  Events: {clustered_events.count()}")

clustered_events = recalculate_cluster_column(clustered_events).cache()
display(clustered_events)

# COMMAND ----------

# MAGIC %md
# MAGIC # Display

# COMMAND ----------

# DBTITLE 1,move to FIRST cluster
# move to first cluster
current_cluster = -1

# COMMAND ----------

# DBTITLE 1,move to PREVIOUS cluster
# move to a previous cluster
current_cluster = max(current_cluster - 2, -1)

# COMMAND ----------

# DBTITLE 1,DISPLAY next
current_cluster = min(current_cluster + 1, total_clusters - 1)


def display_cluster(cluster_id: int):
    if cluster_id >= total_clusters:
        print(
            f"Cluster ID {cluster_id} out of range (total clusters: {total_clusters})"
        )
        return

    print(f"Cluster ID: {cluster_id} ({cluster_id + 1}/{total_clusters})")

    # Filter the cluster and convert to Pandas DataFrame early (faster to run later)
    cluster = clustered_events.filter(F.col("cluster") == cluster_id).toPandas()

    # Convert 'datetime' column to datetime format
    cluster["datetime"] = pd.to_datetime(cluster["datetime"])

    meta = ClusterMetadata(cluster)

    print(f"Google maps: https://www.google.com/maps/place/{meta.centroid_latlong}")
    print(
        f"Samsara map (Julia's dashboard): https://cloud.samsara.com/o/7005746/fleet/list?bounds={meta.centroid_lat}%2C{meta.centroid_long}%2C{meta.centroid_lat}%2C{meta.centroid_long}&z=17&fc-overrides=%7B%22show-gps-jamming-overlay%22%3A%22true%22%7D&enabledOverlays=gpsJamming"
    )
    print(
        f'Samsara map (org from first event): https://cloud.samsara.com/o/{meta.first_event["org_id"]}/fleet/list?bounds={meta.centroid_lat}%2C{meta.centroid_long}%2C{meta.centroid_lat}%2C{meta.centroid_long}&z=17&fc-overrides=%7B%22show-gps-jamming-overlay%22%3A%22true%22%7D&enabledOverlays=gpsJamming'
    )
    print(f"Days: {meta.unique_days}")
    print(f"Vehicles: {meta.unique_vehicles}")
    print(f"Organizations: {meta.unique_orgs}")
    # print(f"Latest jamming date: {meta.latest_jamming_date}")
    # print(f"Days since latest jamming: {meta.days_since_latest_jamming}")
    print(f"All scores: {meta.all_scores}")
    print(f"Score: {meta.score}/100")

    display(cluster)


display_cluster(current_cluster)

# COMMAND ----------

# DBTITLE 1,DISPLAY specific cluster
SPECIFIC_CLUSTER = 99
display_cluster(SPECIFIC_CLUSTER)

# COMMAND ----------

# DBTITLE 1,PRINT cluster to score
# case studies: https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/5695420/GPS+jamming+case+studies
analyzed_clusters = [
    {
        "location": [20.655445, -103.3493973],
        "false_positive": "unknown",
        "name": "armored vehicles",
        "link": "https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/4572248/Armored+vehicles+-+Grupo+Armstrong",
    },
    {
        "location": [18.11516821198077, -94.40994944469232],
        "false_positive": "yes",
        "name": "orgs:4006357",
        "link": "https://cloud.samsara.com/o/4006357/devices/281474993319616/show?end_ms=1742434404837&duration=4773.971",
    },
    {
        "location": [29.0246307, -110.9068343],
        "false_positive": "no",
        "name": "orgs:9002647,11006168,48549",
        "link": "https://cloud.samsara.com/o/61854/devices/281474977272121/show?end_ms=1742439342270&duration=3603.996",
    },
    {
        "location": [17.998762874785715, -93.50750438985717],
        "false_positive": "yes",
        "name": "orgs:4006357",
        "link": "https://cloud.samsara.com/o/4006357/devices/281474993319616/show?end_ms=1742929291091&duration=4364.247",
    },
    {
        "location": [19.206815230315794, -96.14574301981581],
        "false_positive": "no",
        "name": "orgs:4006357",
        "link": "https://cloud.samsara.com/o/4006357/devices/281474994983295/show?end_ms=1742814561598&duration=3603.997",
    },
    {
        "location": [19.660708890000002, -99.19520422429233],
        "false_positive": "no",
        "name": "orgs:7004054",
        "link": "https://cloud.samsara.com/o/7004054/devices/281474987722781/show?end_ms=1743788378283&duration=3781.412",
    },
    {
        "location": [30.1450758028, -109.7790665764],
        "false_positive": "no",
        "name": "orgs:11006168",
        "link": "https://cloud.samsara.com/o/11006168/devices/281474990647751/show?end_ms=1742626345217&duration=4737.33",
    },
    {
        "location": [29.03026860513793, -110.93121113153447],
        "false_positive": "yes",
        "name": "orgs:9002647",
        "link": "https://cloud.samsara.com/o/9002647/devices/281474990920711/show?end_ms=1743871703072&duration=3983.808",
    },
    {
        "location": [18.03048060640625, -93.81304720546876],
        "false_positive": "yes",
        "name": "orgs:4006357,6004646",
        "link": "https://cloud.samsara.com/o/4006357/devices/281474993326189/show?end_ms=1744372891113&duration=4610.184",
    },
    {
        "location": [20.953081185333335, -101.42600545260001],
        "false_positive": "no",
        "name": "orgs:7001070",
        "link": "https://cloud.samsara.com/o/7001070/devices/281474993384561/show?end_ms=1742142314864&duration=3607.954",
    },
    {
        "location": [21.078813664125, -101.72789140681249],
        "false_positive": "no",
        "name": "orgs:61854,78700,7007337,67437,10003122,81016,4007044,57844,63200,5004562,63464,9001353",
        "link": "https://cloud.samsara.com/o/61854/devices/281474977272121/show?end_ms=1742439342270&duration=3603.996",
    },
    {
        "location": [19.103190188694445, -104.26838228152776],
        "false_positive": "no",
        "name": "orgs:8004415",
        "link": "https://cloud.samsara.com/o/8004415/devices/281474987090994/show?end_ms=1742466746125&duration=3675.947",
    },
    {
        "location": [19.741369456666664, -98.94625944983333],
        "false_positive": "yes",
        "name": "orgs:6007035",
        "link": "https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/5915053/Jammer+in+the+vehicle+activated+mid+trip",
    },
    {"location": [20.052739914777778, -99.33925854933334], "false_positive": "no"},
    {"location": [25.764482487486486, -100.23013478939188], "false_positive": "no"},
    {"location": [17.99877003725, -93.51391702033334], "false_positive": "yes"},
]


# TODO: remove false positives from results?


def main():
    threshold_meters = 800  # threshold to match the cluster

    def process_cluster(events: pd.DataFrame):
        events["datetime"] = pd.to_datetime(events["datetime"])
        meta = ClusterMetadata(events)

        name = ""
        link = ""
        is_analyzed = False
        is_false_positive = ""
        for analyzed in analyzed_clusters:
            distance = haversine(
                meta.centroid_lat,
                meta.centroid_long,
                analyzed["location"][0],
                analyzed["location"][1],
            )
            if distance <= threshold_meters:
                is_analyzed = True
                is_false_positive = analyzed["false_positive"]
                name = analyzed.get(
                    "name", f'orgs:{",".join(map(str, events["org_id"].unique()))}'
                )
                link = analyzed.get("link", meta.first_event.get("debug_link", ""))
                break

        result = {
            "cluster_id": events["cluster"].iloc[0],
            "cluster_lat": meta.centroid_lat,
            "cluster_long": meta.centroid_long,
            "score": meta.score,
            "vehicles": meta.unique_vehicles,
            "orgs": meta.unique_orgs,
            "days": meta.unique_days,
            "events": len(events),
            "analyzed": is_analyzed,
            "false_positive": is_false_positive,
            "name": name,
            "link": link,
        }
        return pd.DataFrame([result])

    result_schema = "cluster_id INT, score INT,  analyzed BOOLEAN, false_positive STRING, vehicles INT, orgs INT, days INT, events INT, cluster_lat FLOAT, cluster_long FLOAT, name STRING, link STRING"
    results_spark = clustered_events.groupBy("cluster").applyInPandas(
        process_cluster, schema=result_schema
    )
    results_df = results_spark.toPandas().sort_values(by="score", ascending=False)

    display(results_df)

    analyzed_clusters_with_metadata = copy.deepcopy(analyzed_clusters)
    for row in results_df.itertuples():
        for analyzed in analyzed_clusters_with_metadata:
            distance = haversine(
                row.cluster_lat,
                row.cluster_long,
                analyzed["location"][0],
                analyzed["location"][1],
            )
            if distance <= threshold_meters:
                analyzed["is_present"] = True
                break

    clusters_no_longer_present = [
        cluster
        for cluster in analyzed_clusters_with_metadata
        if not cluster.get("is_present", False)
    ]
    print("Clusters that are no longer present (possibly filtered out):")
    display(clusters_no_longer_present)


main()


# COMMAND ----------

# MAGIC %md
# MAGIC # Other

# COMMAND ----------

# DBTITLE 1,GEOJSON
# Custom serialization function for JSON
def json_serial(obj):
    if isinstance(obj, (pd.Timestamp,)):
        return obj.isoformat()
    raise TypeError(f"Type {obj.__class__.__name__} not serializable")


def write_geojson(geojson, dir):
    backup_file(dir, "data.geojson")

    file_path = os.path.join(dir, "data.geojson")
    print(f"Writing GEOJSON file to {file_path}")
    with open(file_path, "w") as f:
        json.dump(geojson, f, default=json_serial)


def main():
    all_clusters = clustered_events.select("cluster").distinct().collect()
    features = []
    for row in all_clusters:
        cluster_id = row["cluster"]
        cluster = clustered_events.filter(F.col("cluster") == cluster_id).toPandas()
        cluster["datetime"] = pd.to_datetime(cluster["datetime"])

        meta = ClusterMetadata(cluster)
        events = (
            cluster[["latitude", "longitude", "datetime"]]
            .rename(columns={"latitude": "lat", "longitude": "long"})
            .to_dict(orient="records")
        )
        features.append(
            {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [meta.centroid_long, meta.centroid_lat],
                },
                "properties": {
                    "clusterId": meta.cluster_id,
                    "score": meta.score,
                    "uniqueDays": meta.unique_days,
                    "uniqueVehicles": meta.unique_vehicles,
                    "uniqueOrgs": meta.unique_orgs,
                    "daysSinceLatestJamming": meta.days_since_latest_jamming,
                    "latestJammingDate": meta.latest_jamming_date.isoformat(),
                    "events": events,
                },
            }
        )

    geojson = {
        "type": "FeatureCollection",
        "features": features,
        "properties": {
            "startDate": datetime.fromtimestamp(start_time_ms / 1000).isoformat(),
            "endDate": datetime.fromtimestamp(end_time_ms / 1000).isoformat(),
        },
    }
    # geojson_str = json.dumps(geojson, default=json_serial) # indent=2
    # print(geojson_str)

    for dir in GEOJSON_TARGET_DIRS:
        write_geojson(geojson, dir)


main()

# COMMAND ----------

# DBTITLE 1,DEBUG single vehicle
# MAGIC %%script echo skipping
# MAGIC
# MAGIC def main():
# MAGIC   TEST_START_DATE = "2025-03-07 03:00:00"
# MAGIC   TEST_END_DATE = "2025-03-08 11:00:00"
# MAGIC   FILTER_ORG_LOCALE = "mx"
# MAGIC   OBJECT_ID = 281474987107988
# MAGIC
# MAGIC   start_time_ms = int(datetime.strptime(TEST_START_DATE, "%Y-%m-%d %H:%M:%S").timestamp() * 1000)
# MAGIC   end_time_ms = int(datetime.strptime(TEST_END_DATE, "%Y-%m-%d %H:%M:%S").timestamp() * 1000)
# MAGIC
# MAGIC   events = fetch_jamming_events(start_time_ms, end_time_ms, FILTER_ORG_LOCALE, deduplicate=False)
# MAGIC   events = events.filter(events.object_id == OBJECT_ID)
# MAGIC   events.cache()
# MAGIC
# MAGIC   deduplicated_events = fetch_jamming_events(start_time_ms, end_time_ms, FILTER_ORG_LOCALE, deduplicate=True)
# MAGIC   deduplicated_events = deduplicated_events.filter(deduplicated_events.object_id == OBJECT_ID)
# MAGIC   deduplicated_events.cache()
# MAGIC
# MAGIC   print(f"Object ID: {OBJECT_ID}")
# MAGIC
# MAGIC   print(f"All events")
# MAGIC   display(events)
# MAGIC
# MAGIC   print(f"Deduplicated events")
# MAGIC   display(deduplicated_events)
# MAGIC
# MAGIC   transitions = find_on_off_transitions(deduplicated_events, min_duration_seconds=0).cache()
# MAGIC   print(f"Num of transitions: {transitions.count()}")
# MAGIC
# MAGIC   enriched = enrich_distance(transitions)
# MAGIC   print(f"Enriched events")
# MAGIC   display(enriched)
# MAGIC
# MAGIC   # locations = fetch_vehicle_location(OBJECT_ID, start_time_ms, end_time_ms)
# MAGIC   # print(f"Vehicle locations")
# MAGIC   # display(locations)
# MAGIC
# MAGIC main()
