from dagster import AssetExecutionContext, DailyPartitionsDefinition
from dataweb import NonEmptyDQCheck, NonNullDQCheck, PrimaryKeyDQCheck, table
from dataweb.userpkgs.constants import (
    DATAENGINEERING,
    AWSRegion,
    ColumnDescription,
    Database,
    TableType,
    WarehouseWriteMode,
)
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
SELECT *
FROM feature_store.risk_insights_features_quarantine
WHERE date = '{PARTITION_START}'
AND total_driver_history_events_30d <= 43200
AND driver_history_distracted_driving_30d <= 43200
AND driver_history_drowsy_30d <= 43200
AND driver_history_mobile_usage_30d <= 43200
AND driver_history_no_seatbelt_30d <= 43200
AND total_trips_past_24h <= 288
AND max_speed_kmph <= 320
AND over_speed_limit_mins <= 1440
"""


@table(
    database=Database.FEATURE_STORE,
    description=build_table_description(
        table_desc="""A dataset containing aggregated metric details to be used in crash/risk insights""",
        row_meaning="""Each row represents a trip with its associated stats""",
        related_table_info={},
        table_type=TableType.TRANSACTIONAL_FACT,
        freshness_slo_updated_by="9pm PST",
    ),
    regions=[AWSRegion.US_WEST_2],
    owners=[DATAENGINEERING],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DailyPartitionsDefinition(start_date="2024-01-01"),
    write_mode=WarehouseWriteMode.OVERWRITE,
    dq_checks=[
        NonEmptyDQCheck(name="dq_non_empty_risk_insights_features"),
        PrimaryKeyDQCheck(
            name="dq_pk_risk_insights_features",
            primary_keys=PRIMARY_KEYS,
            block_before_write=True,
        ),
        NonNullDQCheck(
            name="dq_non_null_risk_insights_features",
            non_null_columns=NON_NULL_COLUMNS,
            block_before_write=True,
        ),
    ],
    upstreams=[
        "us-west-2:feature_store.risk_insights_features_quarantine",
    ],
)
def risk_insights_features(context: AssetExecutionContext) -> str:
    context.log.info("Updating risk_insights_features")
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    partition_keys = partition_key_ranges_from_context(context)[0]
    PARTITION_START = partition_keys[0]

    query = QUERY.format(
        PARTITION_START=PARTITION_START,
    )
    return query
