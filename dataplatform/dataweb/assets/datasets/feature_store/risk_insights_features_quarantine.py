from dagster import AssetExecutionContext, DailyPartitionsDefinition
from dataweb import NonEmptyDQCheck, NonNullDQCheck, PrimaryKeyDQCheck, SQLDQCheck, table
from dataweb._core.dq_utils import Operator
from dataweb.userpkgs.constants import (
    DATAENGINEERING,
    AWSRegion,
    ColumnDescription,
    Database,
    InstanceType,
    TableType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.query import create_run_config_overrides
from dataweb.userpkgs.utils import (
    build_table_description,
    partition_key_ranges_from_context,
)
from pyspark.sql import SparkSession
from pyspark.sql.types import *

PRIMARY_KEYS = [
    "date",
    "org_id",
    "device_id",
    "driver_id",
    "start_time_ms",
    "end_time_ms"
]

NON_NULL_COLUMNS = [
    "date",
    "org_id",
    "device_id",
    "driver_id",
    "start_time_ms",
    "end_time_ms",
    "duration_mins",
    "distance_miles",
    "max_speed_kmph",
    "over_speed_limit_mins",
    "speeding_ratio_over_limit",
    "light_speeding_mins",
    "speeding_ratio_light",
    "moderate_speeding_mins",
    "speeding_ratio_moderate",
    "heavy_speeding_mins",
    "speeding_ratio_heavy",
    "severe_speeding_mins",
    "speeding_ratio_severe",
    "has_dual_facing_camera",
    "has_camera",
    "driver_history_crash_30d",
    "driver_history_speeding_30d",
    "driver_history_following_distance_30d",
    "driver_history_mobile_usage_30d",
    "driver_history_braking_30d",
    "driver_history_harsh_turn_30d",
    "driver_history_no_seatbelt_30d",
    "driver_history_distracted_driving_30d",
    "driver_history_rolling_stop_30d",
    "driver_history_acceleration_30d",
    "driver_history_drowsy_30d",
    "driver_history_lane_departure_30d",
    "total_driver_history_events_30d",
    "total_driver_history_events_1d",
    "total_driver_history_events_120d",
    "any_driver_history_events_1d",
    "any_driver_history_events_30d",
    "any_driver_history_events_120d",
    "org_history_crash_30d",
    "org_history_harsh_turn_30d",
    "org_history_no_seatbelt_30d",
    "org_history_distracted_driving_30d",
    "org_history_drowsy_30d",
    "org_history_lane_departure_30d",
    "total_org_history_events_30d",
    "start_below_freezing",
    "start_over_35",
    "is_eld_relevant_d",
    "ended_at_night_d",
    "ended_in_morning_d",
    "total_duration_mins_prev_30_days",
    "total_trips_past_24h"
]

SCHEMA = [
    {
        "name": "date",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": ColumnDescription.DATE
        },
    },
    {
        "name": "org_id",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": ColumnDescription.ORG_ID
        },
    },
    {
        "name": "device_id",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": ColumnDescription.DEVICE_ID
        },
    },
    {
        "name": "driver_id",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": ColumnDescription.DRIVER_ID
        },
    },
    {
        "name": "start_time_ms",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Start time of trip"
        },
    },
    {
        "name": "end_time_ms",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "End time of trip"
        },
    },
    {
        "name": "duration_mins",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment": "The duration of the trip in minutes"
        },
    },
    {
        "name": "distance_miles",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment": "The distance in miles covered during the trip"
        },
    },
    {
        "name": "max_speed_kmph",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment": "The maximum speed in kmph during the course of this trip"
        },
    },
    {
        "name": "over_speed_limit_mins",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment": "Total duration the vehicle exceeded the speed limit during the trip, in minutes"
        },
    },
    {
        "name": "speeding_ratio_over_limit",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment": "Ratio of time spent speeding compared to total trip time"
        },
    },
    {
        "name": "light_speeding_mins",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment": "Total duration the vehicle was lightly speeding according to the org`s settings, in minutes"
        },
    },
    {
        "name": "speeding_ratio_light",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment": "Ratio of time spent light speeding compared to total trip time"
        },
    },
    {
        "name": "moderate_speeding_mins",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment": "Total duration the vehicle was moderately speeding according to the org`s settings, in minutes"
        },
    },
    {
        "name": "speeding_ratio_moderate",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment": "Ratio of time spent moderate speeding compared to total trip time"
        },
    },
    {
        "name": "heavy_speeding_mins",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment": "Total duration the vehicle was heavily speeding according to the org`s settings, in minutes"
        },
    },
    {
        "name": "speeding_ratio_heavy",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment": "Ratio of time spent heavy speeding compared to total trip time"
        },
    },
    {
        "name": "severe_speeding_mins",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment": "Total duration the vehicle was severely speeding according to the org`s settings, in minutes"
        },
    },
    {
        "name": "speeding_ratio_severe",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment": "Ratio of time spent severe speeding compared to total trip time"
        },
    },
    {
        "name": "end_state",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "The state where the trip ended in"
        },
    },
    {
        "name": "end_time_utc",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "The end timestamp of the trip in UTC"
        },
    },
    {
        "name": "has_dual_facing_camera",
        "type": "integer",
        "nullable": False,
        "metadata": {
            "comment": "Whether the vehicle has a dual-facing camera or not"
        },
    },
    {
        "name": "has_camera",
        "type": "integer",
        "nullable": False,
        "metadata": {
            "comment": "Whether the vehicle has a camera or not"
        },
    },
    {
        "name": "account_cs_tier",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Queried from edw.silver.dim_customer. Account tier - Premier, Plus, Starter, Elite"
        },
    },
    {
        "name": "account_size_segment",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Queried from edw.silver.dim_customer. Classifies accounts by size - Small Business, Mid Market, Enterprise"
        },
    },
    {
        "name": "account_arr_segment",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": """ARR from the previously publicly reporting quarter in the buckets:
            <0
            0 - 100k
            100k - 500K,
            500K - 1M
            1M+

            Note - ARR information is calculated at the account level, not the SAM Number level
            An account and have multiple SAMs. Additionally a SAM can have multiple accounts, resulting in a single SAM having multiple account_arr_segments
            """
        },
    },
    {
        "name": "driver_history_crash_30d",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of crash events for the driver in the last 30 days"
        },
    },
    {
        "name": "driver_history_speeding_30d",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of speeding events for the driver in the last 30 days"
        },
    },
    {
        "name": "driver_history_following_distance_30d",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of following distance events for the driver in the last 30 days"
        },
    },
    {
        "name": "driver_history_mobile_usage_30d",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of mobile usage events for the driver in the last 30 days"
        },
    },
    {
        "name": "driver_history_braking_30d",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of harsh braking events for the driver in the last 30 days"
        },
    },
    {
        "name": "driver_history_harsh_turn_30d",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of harsh turn events for the driver in the last 30 days"
        },
    },
    {
        "name": "driver_history_no_seatbelt_30d",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of no seatbelt events for the driver in the last 30 days"
        },
    },
    {
        "name": "driver_history_distracted_driving_30d",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of distracted driving events for the driver in the last 30 days"
        },
    },
    {
        "name": "driver_history_rolling_stop_30d",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of rolling stop events for the driver in the last 30 days"
        },
    },
    {
        "name": "driver_history_acceleration_30d",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of harsh acceleration events for the driver in the last 30 days"
        },
    },
    {
        "name": "driver_history_drowsy_30d",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of drowsiness events for the driver in the last 30 days"
        },
    },
    {
        "name": "driver_history_lane_departure_30d",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of lane departure events for the driver in the last 30 days"
        },
    },
    {
        "name": "total_driver_history_events_30d",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of safety events for the driver in the last 30 days"
        },
    },
    {
        "name": "total_driver_history_events_1d",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of safety events for the driver in the last day"
        },
    },
    {
        "name": "total_driver_history_events_120d",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of safety events for the driver in the last 120 days"
        },
    },
    {
        "name": "any_driver_history_events_1d",
        "type": "integer",
        "nullable": False,
        "metadata": {
            "comment": "Whether the driver has any safety events in the last day or not"
        },
    },
    {
        "name": "any_driver_history_events_30d",
        "type": "integer",
        "nullable": False,
        "metadata": {
            "comment": "Whether the driver has any safety events in the last 30 days or not"
        },
    },
    {
        "name": "any_driver_history_events_120d",
        "type": "integer",
        "nullable": False,
        "metadata": {
            "comment": "Whether the driver has any safety events in the last 120 days or not"
        },
    },
    {
        "name": "org_history_crash_30d",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of harsh crash events for the org in the last 30 days"
        },
    },
    {
        "name": "org_history_harsh_turn_30d",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of sharp turn events for the org in the last 30 days"
        },
    },
    {
        "name": "org_history_no_seatbelt_30d",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of no seatbelt events for the org in the last 30 days"
        },
    },
    {
        "name": "org_history_distracted_driving_30d",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of distracted driving events for the org in the last 30 days"
        },
    },
    {
        "name": "org_history_drowsy_30d",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of drowsiness events for the org in the last 30 days"
        },
    },
    {
        "name": "org_history_lane_departure_30d",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of lane departure events for the org in the last 30 days"
        },
    },
    {
        "name": "total_org_history_events_30d",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of safety events for the org in the last 30 days"
        },
    },
    {
        "name": "start_below_freezing",
        "type": "integer",
        "nullable": False,
        "metadata": {
            "comment": "Whether air temp at start of trip was below 0 or not"
        },
    },
    {
        "name": "start_over_35",
        "type": "integer",
        "nullable": False,
        "metadata": {
            "comment": "Whether air temp at start of trip was over 35 or not"
        },
    },
    {
        "name": "is_eld_relevant_d",
        "type": "integer",
        "nullable": False,
        "metadata": {
            "comment": "Whether the org has ELD-relevant devices or not"
        },
    },
    {
        "name": "ended_at_night_d",
        "type": "integer",
        "nullable": False,
        "metadata": {
            "comment": "Whether the trip ended during the night or not"
        },
    },
    {
        "name": "ended_in_morning_d",
        "type": "integer",
        "nullable": False,
        "metadata": {
            "comment": "Whether the trip ended in the morning or not"
        },
    },
    {
        "name": "total_duration_mins_prev_30_days",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment": "Total number of minutes driven by the driver in the last 30 days"
        },
    },
    {
        "name": "total_trips_past_24h",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total number of trips taken by the driver in past 24 hours"
        },
    },
]

QUERY = """
WITH combined_licensed_orgs AS (
  SELECT
      o.org_id,
      o.account_cs_tier,
      o.account_size_segment,
      o.account_arr_segment
  FROM
      datamodel_core.dim_organizations o
  WHERE
      o.date = (SELECT MAX(date) FROM datamodel_core.dim_organizations)
),
-- Filter devices based on product IDs
filtered_devices AS (
  SELECT DISTINCT
    device_id,
    associated_devices.camera_product_id AS camera_product_id
  FROM
    datamodel_core.dim_devices
  WHERE
    product_id IN (7, 17, 24, 35, 53, 89, 90, 178) -- VG product IDs
    AND date = (SELECT MAX(date) FROM datamodel_core.dim_devices)
),
-- Get trips for filtered devices and filtered orgs
eligible_trips AS (
    SELECT DISTINCT
        trips.date,
        trips.org_id,
        trips.device_id,
        trips.driver_id,
        trips.start_time_ms,
        trips.end_time_ms,
        trips.duration_mins,
        trips.distance_miles,
        trips.max_speed_kmph,
        trips.over_speed_limit_mins,
        trips.over_speed_limit_mins / trips.duration_mins AS speeding_ratio_over_limit,
        trips.light_speeding_mins,
        trips.light_speeding_mins / trips.duration_mins AS speeding_ratio_light,
        trips.moderate_speeding_mins,
        trips.moderate_speeding_mins / trips.duration_mins AS speeding_ratio_moderate,
        trips.heavy_speeding_mins,
        trips.heavy_speeding_mins / trips.duration_mins AS speeding_ratio_heavy,
        trips.severe_speeding_mins,
        trips.severe_speeding_mins / trips.duration_mins AS speeding_ratio_severe,
        trips.end_state,
        trips.end_time_utc,
        CASE WHEN p.name IN ('CM32', 'CM34') THEN 1 ELSE 0 END AS has_dual_facing_camera,
        CASE WHEN p.name IS NOT NULL THEN 1 ELSE 0 END AS has_camera,
        lo.account_cs_tier,
        lo.account_size_segment,
        lo.account_arr_segment
    FROM
        datamodel_telematics.fct_trips trips
    JOIN filtered_devices fd ON fd.device_id = trips.device_id
    JOIN combined_licensed_orgs lo ON lo.org_id = trips.org_id
    LEFT JOIN definitions.products p ON fd.camera_product_id = p.product_id
    WHERE
        trips.date BETWEEN DATE_SUB('{PARTITION_START}', 1) AND '{PARTITION_START}' -- get two days since we need to compute trips over last 24h
        AND trips.driver_id IS NOT NULL
        AND trips.driver_id != 0
        AND trips.trip_type = 'location_based'
),
extended_eligible_trips AS (
  SELECT
    trips.device_id,
    trips.driver_id,
    trips.org_id,
    trips.date,
    trips.duration_mins
  FROM
    datamodel_telematics.fct_trips trips
    JOIN filtered_devices fd ON fd.device_id = trips.device_id
    JOIN combined_licensed_orgs lo ON lo.org_id = trips.org_id
  WHERE
    trips.date BETWEEN DATE_SUB('{PARTITION_START}', 30) AND '{PARTITION_START}' -- Look back previous 31 days (to focus on 30 preceding days)
    AND trips.driver_id != 0
    AND trips.driver_id IS NOT NULL
    AND trips.trip_type = 'location_based'
),
-- Calculate the average trip duration for the prior thirty days, using extended trip data
-- First, aggregate trip durations by day for each driver
daily_driver_trips AS (
  SELECT
    et.org_id,
    et.driver_id,
    et.date,
    SUM(et.duration_mins) AS total_daily_duration
  FROM
    extended_eligible_trips et
  GROUP BY
    et.org_id,
    et.driver_id,
    et.date
),
-- Then, calculate the rolling sum of these daily totals over the past 30 days
rolling_duration_avg AS (
  SELECT
    dd.org_id,
    dd.driver_id,
    dd.date,
    SUM(dd.total_daily_duration) OVER (
      PARTITION BY dd.org_id, dd.driver_id
      ORDER BY dd.date
      ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING
    ) AS total_duration_mins_prev_30_days
  FROM
    daily_driver_trips dd
),
-- Use score report to get safety events per driver
daily_safety_events AS (
  SELECT
    e.org_id,
    e.object_id as driver_id,
    e.date,
    SUM(e.crash_count) AS crashes,
    SUM(e.speeding_count) + SUM(e.heavy_speeding_count) + SUM(e.severe_speeding_count) AS speeding,
    SUM(e.following_distance_count) AS following_distance,
    SUM(e.mobile_usage_count) + SUM(e.edge_mobile_usage_count) AS mobile_usage_violations,
    SUM(e.braking_count) AS harsh_braking,
    SUM(e.harsh_turn_count) AS harsh_turns,
    SUM(e.no_seatbelt_count) AS no_seatbelt_violations,
    SUM(e.generic_distraction_count) + SUM(e.edge_distracted_driving_count) AS distracted_driving,
    SUM(e.rolling_stop_count) AS rolling_stop,
    SUM(e.acceleration_count) AS harsh_accelerations,
    SUM(e.drowsy_count) AS drowsiness,
    SUM(e.lane_departure_count) AS lane_departure,
    (
        SUM(e.crash_count) + SUM(e.speeding_count) + SUM(e.heavy_speeding_count) + SUM(e.severe_speeding_count) +
        SUM(e.following_distance_count) + SUM(e.mobile_usage_count) + SUM(e.edge_mobile_usage_count) +
        SUM(e.braking_count) + SUM(e.harsh_turn_count) + SUM(e.no_seatbelt_count) + SUM(e.generic_distraction_count) +
        SUM(e.edge_distracted_driving_count) + SUM(e.rolling_stop_count) + SUM(e.acceleration_count) +
        SUM(e.drowsy_count) + SUM(e.lane_departure_count)
    ) AS total_events
  FROM
    safety_report.score_event_report_v4 e
  WHERE e.date BETWEEN DATE_SUB('{PARTITION_START}', 120) AND '{PARTITION_START}' -- Look back 121 days (to focus on 120 preceding days)
  AND e.object_id != 0
  AND e.object_id IS NOT NULL
  AND e.object_type = 5 -- driver
  GROUP BY
    e.org_id,
    e.object_id,
    e.date
),
-- Calculate rolling _driver_ averages based on daily aggregates
rolling_driver_safety_events AS (
  SELECT
    d.org_id,
    d.driver_id,
    d.date,

    -- 30 day history
    SUM(d.crashes) OVER (PARTITION BY d.driver_id, d.org_id ORDER BY d.date ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING) AS driver_history_crash_30d,
    SUM(d.speeding) OVER (PARTITION BY d.driver_id, d.org_id ORDER BY d.date ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING) AS driver_history_speeding_30d,
    SUM(d.following_distance) OVER (PARTITION BY d.driver_id, d.org_id ORDER BY d.date ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING) AS driver_history_following_distance_30d,
    SUM(d.mobile_usage_violations) OVER (PARTITION BY d.driver_id, d.org_id ORDER BY d.date ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING) AS driver_history_mobile_usage_30d,
    SUM(d.harsh_braking) OVER (PARTITION BY d.driver_id, d.org_id ORDER BY d.date ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING) AS driver_history_braking_30d,
    SUM(d.harsh_turns) OVER (PARTITION BY d.driver_id, d.org_id ORDER BY d.date ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING) AS driver_history_harsh_turn_30d,
    SUM(d.no_seatbelt_violations) OVER (PARTITION BY d.driver_id, d.org_id ORDER BY d.date ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING) AS driver_history_no_seatbelt_30d,
    SUM(d.distracted_driving) OVER (PARTITION BY d.driver_id, d.org_id ORDER BY d.date ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING) AS driver_history_distracted_driving_30d,
    SUM(d.rolling_stop) OVER (PARTITION BY d.driver_id, d.org_id ORDER BY d.date ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING) AS driver_history_rolling_stop_30d,
    SUM(d.harsh_accelerations) OVER (PARTITION BY d.driver_id, d.org_id ORDER BY d.date ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING) AS driver_history_acceleration_30d,
    SUM(d.lane_departure) OVER (PARTITION BY d.driver_id, d.org_id ORDER BY d.date ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING) AS driver_history_lane_departure_30d,
    SUM(d.drowsiness) OVER (PARTITION BY d.driver_id, d.org_id ORDER BY d.date ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING) AS driver_history_drowsy_30d,
    SUM(d.total_events) OVER (PARTITION BY d.driver_id, d.org_id ORDER BY d.date ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING) AS total_driver_history_events_30d,

    -- More daily history
    SUM(d.total_events) OVER (PARTITION BY d.driver_id, d.org_id ORDER BY d.date ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS total_driver_history_events_1d,
    SUM(d.total_events) OVER (PARTITION BY d.driver_id, d.org_id ORDER BY d.date ROWS BETWEEN 120 PRECEDING AND 1 PRECEDING) AS total_driver_history_events_120d

  FROM
    daily_safety_events d
),
daily_org_safety_events AS (
  SELECT
    dse.org_id,
    dse.date,
    SUM(dse.crashes) AS crashes,
    SUM(dse.harsh_turns) AS harsh_turns,
    SUM(dse.no_seatbelt_violations) AS no_seatbelt_violations,
    SUM(dse.distracted_driving) AS distracted_driving,
    SUM(dse.lane_departure) AS lane_departure,
    SUM(dse.drowsiness) AS drowsiness_detections,
    SUM(dse.total_events) AS total_events
  FROM daily_safety_events dse
  GROUP BY dse.org_id, dse.date
),
-- Apply the rolling sum using daily_org_safety_events
rolling_org_safety_events AS (
  SELECT
    doe.org_id,
    doe.date,
    -- 30 day history
    SUM(doe.crashes) OVER (PARTITION BY doe.org_id ORDER BY doe.date ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING) AS org_history_crash_30d,
    SUM(doe.harsh_turns) OVER (PARTITION BY doe.org_id ORDER BY doe.date ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING) AS org_history_harsh_turn_30d,
    SUM(doe.no_seatbelt_violations) OVER (PARTITION BY doe.org_id ORDER BY doe.date ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING) AS org_history_no_seatbelt_30d,
    SUM(doe.distracted_driving) OVER (PARTITION BY doe.org_id ORDER BY doe.date ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING) AS org_history_distracted_driving_30d,
    SUM(doe.drowsiness_detections) OVER (PARTITION BY doe.org_id ORDER BY doe.date ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING) AS org_history_drowsy_30d,
    SUM(doe.lane_departure) OVER (PARTITION BY doe.org_id ORDER BY doe.date ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING) AS org_history_lane_departure_30d,
    SUM(doe.total_events) OVER (PARTITION BY doe.org_id ORDER BY doe.date ROWS BETWEEN 30 PRECEDING AND 1 PRECEDING) AS total_org_history_events_30d

  FROM
    daily_org_safety_events doe
),
driving_hours_past_24h AS (
    SELECT
        t.org_id,
        t.driver_id,
        t.device_id,
        t.start_time_ms,
        t.duration_mins,
        t.date,
        COALESCE(COUNT(*) OVER (
            PARTITION BY t.org_id, t.driver_id, t.device_id
            ORDER BY t.start_time_ms
            RANGE BETWEEN 86400000 PRECEDING AND 1 PRECEDING
        ), 0) AS total_trips_past_24h  -- Default to 0 if NULL, there is no previous trip
    FROM
        eligible_trips t
),
eld_relevance AS (
  SELECT DISTINCT
    deld.org_id,
    deld.date
  FROM
    datamodel_telematics.dim_eld_relevant_devices deld
  INNER JOIN combined_licensed_orgs clo ON deld.org_id = clo.org_id
  WHERE
    deld.is_eld_relevant = TRUE
    AND deld.date = '{PARTITION_START}'
),
timezone_adjustments AS (
  SELECT
    et.org_id,
    et.device_id,
    et.start_time_ms,
    et.end_time_ms,
    CASE
        WHEN et.end_state IN ('ME', 'NH', 'MA', 'RI', 'CT', 'NY', 'NJ', 'PA', 'DE',
                                'MD', 'DC', 'VA', 'WV', 'NC', 'SC', 'GA', 'FL', 'VT',
                                'OH', 'MI', 'IN', 'KY', 'ON', 'QC', 'ROO') THEN 'Eastern'
        WHEN et.end_state IN ('WI', 'IL', 'MN', 'IA', 'MO', 'AR', 'ND', 'SD', 'NE', 'KS',
                                'OK', 'TX', 'TN', 'AL', 'MS', 'LA',
                                'MB', 'SK',
                                'MEX', 'NLE', 'TAM', 'JAL', 'GUA', 'CHH', 'DIF', 'VER',
                                'QUE', 'SLP', 'COA', 'HID', 'PUE', 'MIC', 'COL', 'YUC',
                                'AGU', 'TLA', 'TAB', 'ZAC', 'CHP', 'DUR', 'MOR', 'OAX',
                                'CAM', 'GRO') THEN 'Central'
        WHEN et.end_state IN ('MT', 'WY', 'CO', 'NM', 'ID', 'UT', 'AZ', 'AB', 'SON', 'SIN', 'Nayarit', 'BCS', 'NT', 'YT') THEN 'Mountain'
        WHEN et.end_state IN ('CA', 'OR', 'WA', 'NV', 'AK', 'BC', 'BCN', 'NU') THEN 'Pacific'
        WHEN et.end_state IN ('NB', 'NS', 'NL', 'PE', 'Puerto Rico') THEN 'Atlantic'
        WHEN et.end_state IN ('HI') THEN 'Hawaii'
        WHEN et.end_state IN ('Guam') THEN 'Chamorro'
    END AS timezone,
    et.end_time_utc
  FROM eligible_trips et
  WHERE et.date = '{PARTITION_START}'
),
nighttime_and_morning_determination AS (
  SELECT
    ta.org_id,
    ta.device_id,
    ta.start_time_ms,
    CASE
      WHEN (
        (ta.timezone = 'Chamorro' AND (HOUR(from_unixtime(ta.end_time_ms / 1000 + (10 * 3600))) >= 21 OR HOUR(from_unixtime(ta.end_time_ms / 1000 + (10 * 3600))) < 5)) OR
        (ta.timezone = 'Atlantic' AND (HOUR(from_unixtime(ta.end_time_ms / 1000 + (-4 * 3600))) >= 21 OR HOUR(from_unixtime(ta.end_time_ms / 1000 + (-4 * 3600))) < 5)) OR
        (ta.timezone = 'Eastern' AND (HOUR(from_unixtime(ta.end_time_ms / 1000 + (-5 * 3600))) >= 21 OR HOUR(from_unixtime(ta.end_time_ms / 1000 + (-5 * 3600))) < 5)) OR
        (ta.timezone = 'Central' AND (HOUR(from_unixtime(ta.end_time_ms / 1000 + (-6 * 3600))) >= 21 OR HOUR(from_unixtime(ta.end_time_ms / 1000 + (-6 * 3600))) < 5)) OR
        (ta.timezone = 'Mountain' AND (HOUR(from_unixtime(ta.end_time_ms / 1000 + (-7 * 3600))) >= 21 OR HOUR(from_unixtime(ta.end_time_ms / 1000 + (-7 * 3600))) < 5)) OR
        (ta.timezone = 'Pacific' AND (HOUR(from_unixtime(ta.end_time_ms / 1000 + (-8 * 3600))) >= 21 OR HOUR(from_unixtime(ta.end_time_ms / 1000 + (-8 * 3600))) < 5)) OR
        (ta.timezone = 'Hawaii' AND (HOUR(from_unixtime(ta.end_time_ms / 1000 + (-10 * 3600))) >= 21 OR HOUR(from_unixtime(ta.end_time_ms / 1000 + (-10 * 3600))) < 5))
      ) THEN 1 ELSE 0
    END AS ended_at_night_d,
    CASE
      WHEN (
        (ta.timezone = 'Chamorro' AND (HOUR(from_unixtime(ta.end_time_ms / 1000 + (10 * 3600))) >= 4 AND HOUR(from_unixtime(ta.end_time_ms / 1000 + (10 * 3600))) < 9)) OR
        (ta.timezone = 'Atlantic' AND (HOUR(from_unixtime(ta.end_time_ms / 1000 + (-4 * 3600))) >= 4 AND HOUR(from_unixtime(ta.end_time_ms / 1000 + (-4 * 3600))) < 9)) OR
        (ta.timezone = 'Eastern' AND (HOUR(from_unixtime(ta.end_time_ms / 1000 + (-5 * 3600))) >= 4 AND HOUR(from_unixtime(ta.end_time_ms / 1000 + (-5 * 3600))) < 9)) OR
        (ta.timezone = 'Central' AND (HOUR(from_unixtime(ta.end_time_ms / 1000 + (-6 * 3600))) >= 4 AND HOUR(from_unixtime(ta.end_time_ms / 1000 + (-6 * 3600))) < 9)) OR
        (ta.timezone = 'Mountain' AND (HOUR(from_unixtime(ta.end_time_ms / 1000 + (-7 * 3600))) >= 4 AND HOUR(from_unixtime(ta.end_time_ms / 1000 + (-7 * 3600))) < 9)) OR
        (ta.timezone = 'Pacific' AND (HOUR(from_unixtime(ta.end_time_ms / 1000 + (-8 * 3600))) >= 4 AND HOUR(from_unixtime(ta.end_time_ms / 1000 + (-8 * 3600))) < 9)) OR
        (ta.timezone = 'Hawaii' AND (HOUR(from_unixtime(ta.end_time_ms / 1000 + (-10 * 3600))) >= 4 AND HOUR(from_unixtime(ta.end_time_ms / 1000 + (-10 * 3600))) < 9))
      ) THEN 1 ELSE 0
    END AS ended_in_morning_d
  FROM timezone_adjustments ta
),
start_air_temp AS (
  SELECT
    b.org_id,
    b.device_id,
    b.date,
    b.start_time_ms,
    MIN(k.value.proto_value.engine_gauge_event.air_temp_milli_c / 1000) AS air_temp_c_start
  FROM eligible_trips b
  JOIN kinesisstats_history.osdenginegauge k
    ON k.org_id = b.org_id
    AND k.object_id = b.device_id
    AND k.time BETWEEN (b.start_time_ms - 60000) AND (b.start_time_ms + 60000)
  WHERE k.value.is_databreak = 'false'
    AND k.value.is_end = 'false'
    AND b.date = '{PARTITION_START}'
    AND k.date BETWEEN DATE_SUB('{PARTITION_START}', 1) AND DATE_ADD('{PARTITION_START}', 1) -- expand window since we look back/forward a minute
  GROUP BY b.org_id, b.device_id, b.date, b.start_time_ms
)
SELECT
  et.*,

  -- driver history - 30 day
  COALESCE (rdse.driver_history_crash_30d, 0) AS driver_history_crash_30d,
  COALESCE (rdse.driver_history_speeding_30d, 0) AS driver_history_speeding_30d,
  COALESCE (rdse.driver_history_following_distance_30d, 0) AS driver_history_following_distance_30d,
  COALESCE (rdse.driver_history_mobile_usage_30d, 0) AS driver_history_mobile_usage_30d,
  COALESCE (rdse.driver_history_braking_30d, 0) AS driver_history_braking_30d,
  COALESCE (rdse.driver_history_harsh_turn_30d, 0) AS driver_history_harsh_turn_30d,
  COALESCE (rdse.driver_history_no_seatbelt_30d, 0) AS driver_history_no_seatbelt_30d,
  COALESCE (rdse.driver_history_distracted_driving_30d, 0) AS driver_history_distracted_driving_30d,
  COALESCE (rdse.driver_history_rolling_stop_30d, 0) AS driver_history_rolling_stop_30d,
  COALESCE (rdse.driver_history_acceleration_30d, 0) AS driver_history_acceleration_30d,
  COALESCE (rdse.driver_history_drowsy_30d, 0) AS driver_history_drowsy_30d,
  COALESCE (rdse.driver_history_lane_departure_30d, 0) AS driver_history_lane_departure_30d,
  COALESCE (rdse.total_driver_history_events_30d, 0) AS total_driver_history_events_30d,
  COALESCE (rdse.total_driver_history_events_1d, 0) AS total_driver_history_events_1d,
  COALESCE (rdse.total_driver_history_events_120d, 0) AS total_driver_history_events_120d,

  CASE WHEN total_driver_history_events_1d > 0 THEN 1 ELSE 0 END AS any_driver_history_events_1d,
  CASE WHEN total_driver_history_events_30d > 0 THEN 1 ELSE 0 END AS any_driver_history_events_30d,
  CASE WHEN total_driver_history_events_120d > 0 THEN 1 ELSE 0 END AS any_driver_history_events_120d,

  -- org history
  COALESCE (rose.org_history_crash_30d, 0) AS org_history_crash_30d,
  COALESCE (rose.org_history_harsh_turn_30d, 0) AS org_history_harsh_turn_30d,
  COALESCE (rose.org_history_no_seatbelt_30d, 0) AS org_history_no_seatbelt_30d,
  COALESCE (rose.org_history_distracted_driving_30d, 0) AS org_history_distracted_driving_30d,
  COALESCE (rose.org_history_drowsy_30d, 0) AS org_history_drowsy_30d,
  COALESCE (rose.org_history_lane_departure_30d, 0) AS org_history_lane_departure_30d,
  COALESCE (rose.total_org_history_events_30d, 0) AS total_org_history_events_30d,

  -- Temperatures
  CASE WHEN sat.air_temp_c_start < 0 THEN 1 ELSE 0 END AS start_below_freezing,
  CASE WHEN sat.air_temp_c_start > 35 THEN 1 ELSE 0 END AS start_over_35,

  CASE
      WHEN er.org_id IS NOT NULL THEN 1
      ELSE 0
      END AS is_eld_relevant_d,
  COALESCE(nd.ended_at_night_d, 0) AS ended_at_night_d,
  COALESCE(nd.ended_in_morning_d, 0) AS ended_in_morning_d,
  COALESCE(rda.total_duration_mins_prev_30_days, 0) AS  total_duration_mins_prev_30_days, -- This is for normalizing other variables that are 30 day rolling
  COALESCE(dhp.total_trips_past_24h, 0) AS total_trips_past_24h

FROM eligible_trips et
LEFT JOIN rolling_driver_safety_events rdse
    ON et.org_id = rdse.org_id
    AND et.driver_id = rdse.driver_id
    AND et.date = rdse.date
LEFT JOIN rolling_org_safety_events rose
    ON et.org_id = rose.org_id
    AND et.date = rose.date
LEFT JOIN eld_relevance er
    ON et.org_id = er.org_id
    AND et.date = er.date
LEFT JOIN nighttime_and_morning_determination nd
    ON et.org_id = nd.org_id
    AND et.device_id = nd.device_id
    AND et.start_time_ms = nd.start_time_ms
LEFT JOIN rolling_duration_avg rda
    ON et.driver_id = rda.driver_id
    AND et.org_id = rda.org_id
    AND et.date = rda.date
LEFT JOIN driving_hours_past_24h dhp
    ON et.org_id = dhp.org_id
    AND et.device_id = dhp.device_id
    AND et.driver_id = dhp.driver_id
    AND et.start_time_ms = dhp.start_time_ms
LEFT JOIN start_air_temp sat
    ON et.org_id = sat.org_id
    AND et.device_id = sat.device_id
    AND et.date = sat.date
    AND et.start_time_ms = sat.start_time_ms
WHERE et.date = '{PARTITION_START}'
"""


@table(
    database=Database.FEATURE_STORE,
    description=build_table_description(
        table_desc="""A dataset containing aggregated metric details to be used in crash/risk insights. This is a quarantine table, not to be used for actual use.""",
        row_meaning="""Each row represents a trip with its associated stats""",
        related_table_info={},
        table_type=TableType.STAGING,
        freshness_slo_updated_by="9pm PST",
    ),
    regions=[AWSRegion.US_WEST_2],
    owners=[DATAENGINEERING],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DailyPartitionsDefinition(start_date="2024-01-01"),
    write_mode=WarehouseWriteMode.OVERWRITE,
    max_retries=3,
    dq_checks=[
        NonEmptyDQCheck(name="dq_non_empty_risk_insights_features_quarantine"),
        PrimaryKeyDQCheck(
            name="dq_pk_risk_insights_features_quarantine",
            primary_keys=PRIMARY_KEYS,
            block_before_write=True,
        ),
        NonNullDQCheck(
            name="dq_non_null_risk_insights_features_quarantine",
            non_null_columns=NON_NULL_COLUMNS,
            block_before_write=True,
        ),
        SQLDQCheck(
            name="dq_sql_risk_insights_features_max_total_driver_history_events_30d",
            sql_query="""
                select max(total_driver_history_events_30d) as observed_value from df
            """,
            expected_value=43200,
            operator=Operator.lte,
            block_before_write=False,
        ),
        SQLDQCheck(
            name="dq_sql_risk_insights_features_max_driver_history_distracted_driving_30d",
            sql_query="""
                select max(driver_history_distracted_driving_30d) as observed_value from df
            """,
            expected_value=43200,
            operator=Operator.lte,
            block_before_write=False,
        ),
        SQLDQCheck(
            name="dq_sql_risk_insights_features_max_driver_history_drowsy_30d",
            sql_query="""
                select max(driver_history_drowsy_30d) as observed_value from df
            """,
            expected_value=43200,
            operator=Operator.lte,
            block_before_write=False,
        ),
        SQLDQCheck(
            name="dq_sql_risk_insights_features_max_driver_history_mobile_usage_30d",
            sql_query="""
                select max(driver_history_mobile_usage_30d) as observed_value from df
            """,
            expected_value=43200,
            operator=Operator.lte,
            block_before_write=False,
        ),
        SQLDQCheck(
            name="dq_sql_risk_insights_features_max_driver_history_no_seatbelt_30d",
            sql_query="""
                select max(driver_history_no_seatbelt_30d) as observed_value from df
            """,
            expected_value=43200,
            operator=Operator.lte,
            block_before_write=False,
        ),
        SQLDQCheck(
            name="dq_sql_risk_insights_features_max_total_trips_past_24h",
            sql_query="""
                select max(total_trips_past_24h) as observed_value from df
            """,
            expected_value=288,
            operator=Operator.lte,
            block_before_write=False,
        ),
        SQLDQCheck(
            name="dq_sql_risk_insights_features_max_max_speed_kmph",
            sql_query="""
                select max(max_speed_kmph) as observed_value from df
            """,
            expected_value=320,
            operator=Operator.lte,
            block_before_write=False,
        ),
        SQLDQCheck(
            name="dq_sql_risk_insights_features_max_over_speed_limit_mins",
            sql_query="""
                select max(over_speed_limit_mins) as observed_value from df
            """,
            expected_value=1440,
            operator=Operator.lte,
            block_before_write=False,
        ),
    ],
    upstreams=[
        "us-west-2:datamodel_core.dim_organizations",
        "us-west-2:datamodel_telematics.dim_eld_relevant_devices",
        "us-west-2:datamodel_telematics.fct_trips",
        "us-west-2:datamodel_core.dim_devices",
        "us-west-2:safety_report.score_event_report_v4",
        "us-west-2:kinesisstats_history.osdenginegauge",
    ],
    run_config_overrides=create_run_config_overrides(
        driver_instance_type=InstanceType.RD_FLEET_2XLARGE,
        worker_instance_type=InstanceType.RD_FLEET_4XLARGE,
        max_workers=8,
    ),
)
def risk_insights_features_quarantine(context: AssetExecutionContext) -> str:
    context.log.info("Updating risk_insights_features_quarantine")
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    partition_keys = partition_key_ranges_from_context(context)[0]
    PARTITION_START = partition_keys[0]

    query = QUERY.format(
        PARTITION_START=PARTITION_START,
    )
    return query
