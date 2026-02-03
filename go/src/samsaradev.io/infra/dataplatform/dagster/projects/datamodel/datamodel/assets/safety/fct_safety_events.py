from dagster import (
    AssetIn,
    AssetKey,
    BackfillPolicy,
    Backoff,
    DailyPartitionsDefinition,
    Jitter,
    RetryPolicy,
)
from pyspark.sql import DataFrame, SparkSession

from ...common.constants import (
    SLACK_ALERTS_CHANNEL_DATA_ENGINEERING,
    device_id_default_description,
    driver_id_default_description,
    org_id_default_description,
)
from ...common.utils import (
    AWSRegions,
    Database,
    DQGroup,
    JoinableDQCheck,
    NonEmptyDQCheck,
    PrimaryKeyDQCheck,
    TableType,
    TrendDQCheck,
    WarehouseWriteMode,
    apply_db_overrides,
    build_assets_from_sql,
    build_table_description,
    get_all_regions,
)

key_prefix = "datamodel_safety"
databases = {
    "database_bronze": Database.DATAMODEL_SAFETY_BRONZE,
    "database_silver": Database.DATAMODEL_SAFETY_SILVER,
    "database_gold": Database.DATAMODEL_SAFETY,
}
database_dev_overrides = {
    "database_bronze_dev": Database.DATAMODEL_DEV,
    "database_silver_dev": Database.DATAMODEL_DEV,
    "database_gold_dev": Database.DATAMODEL_DEV,
}
databases = apply_db_overrides(databases, database_dev_overrides)
pipeline_group_name = "fct_safety_events"
daily_partition_def = DailyPartitionsDefinition(start_date="2020-01-01", end_offset=1)
BACKFILL_POLICY = BackfillPolicy.multi_run(max_partitions_per_run=7)
SLACK_ALERTS_CHANNEL = SLACK_ALERTS_CHANNEL_DATA_ENGINEERING
dqs = DQGroup(
    group_name=pipeline_group_name,
    partition_def=daily_partition_def,
    slack_alerts_channel=SLACK_ALERTS_CHANNEL,
    regions=get_all_regions(),
    backfill_policy=BACKFILL_POLICY,
)

# A RetryPolicy is specified for these assets because of the high number of partitions these assets may materialize.
fct_safety_events_retry_policy = RetryPolicy(
    max_retries=0,  # Maximum number of retries
    delay=180,  # Initial delay in seconds
    backoff=Backoff.EXPONENTIAL,  # Delay multiplier for each retry
    jitter=Jitter.FULL,  # Apply randomness to delay intervals
)

stg_safety_events_schema = [
    {
        "name": "date",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "The field which partitions the table, in 'YYYY-mm-dd' format. `date` is derived from `event_ms`."
        },
    },
    {
        "name": "org_id",
        "type": "long",
        "nullable": True,
        "metadata": {"comment": org_id_default_description},
    },
    {
        "name": "device_id",
        "type": "long",
        "nullable": True,
        "metadata": {"comment": device_id_default_description},
    },
    {
        "name": "event_ms",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "Unixtime (in milliseconds) that the event was triggered. A specific safety event is identified by joining on (org_id, device_id, event_ms). event_id may not be unique, due to int overflow issues in the current safety events framework."
        },
    },
    {
        "name": "accel_type",
        "type": "integer",
        "nullable": True,
        "metadata": {
            "comment": "The type of event that was triggered by the device. Possible values can be retrieved from definitions.harsh_accel_type_enums"
        },
    },
    {
        "name": "harsh_event_type",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "The type of the event that was triggered by the device. Possible values can be retrieved from definitions.harsh_accel_type_enums"
        },
    },
    {
        "name": "event_id",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "The ID of the event that was triggered by the device. May not be unique, due to int overflow issues in the current safety events framework."
        },
    },
    {
        "name": "additional_labels",
        "type": {
            "type": "struct",
            "fields": [
                {
                    "name": "additional_labels",
                    "type": {
                        "type": "array",
                        "elementType": {
                            "type": "struct",
                            "fields": [
                                {
                                    "name": "label_type",
                                    "type": "integer",
                                    "nullable": True,
                                    "metadata": {
                                        "comment": "The type of label that was triggered by the device"
                                    },
                                },
                                {
                                    "name": "label_source",
                                    "type": "integer",
                                    "nullable": True,
                                    "metadata": {
                                        "comment": "Enum representing the source of the label; i.e., system or user"
                                    },
                                },
                                {
                                    "name": "last_added_ms",
                                    "type": "long",
                                    "nullable": True,
                                    "metadata": {
                                        "comment": "The time the label was added"
                                    },
                                },
                                {
                                    "name": "user_id",
                                    "type": "long",
                                    "nullable": True,
                                    "metadata": {
                                        "comment": "The user ID of the user who added the label"
                                    },
                                },
                            ],
                        },
                        "containsNull": True,
                    },
                    "nullable": True,
                    "metadata": {
                        "comment": "Behavior labels that were applied to the safety event"
                    },
                }
            ],
        },
        "nullable": True,
        "metadata": {
            "comment": "Proto blob that contains a list of labels that have been applied (either automatically by the system or by a user) to this event. The labels are represented by an enum. Possible values can be found in definitions.behavior_label_type_enums"
        },
    },
    {
        "name": "driver_id",
        "type": "long",
        "nullable": True,
        "metadata": {"comment": driver_id_default_description},
    },
    {
        "name": "release_stage",
        "type": "short",
        "nullable": True,
        # TODO: add metadatahelpers.EnumDescription entry once feature is available.
        # safetyproto.SafetyEventReleaseStage_name
        "metadata": {
            "comment": "The release stage of the safety event. Controls visibility to end users."
        },
    },
    {
        "name": "trip_start_ms",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "Unixtime (in milliseconds) of the start of the trip that the event occurred on. 0 if the event happened off-trip"
        },
    },
    {
        "name": "trip_start_speed_milliknots",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "GPS speed in milliknots reported by the device at the start of the event. Used to determine whether the event is considered low speed (speeds under 4344.8812095 milliknots for events that are not crashes or rolling stops are filtered out)."
        },
    },
    {
        "name": "harsh_event_surveys",
        "type": {
            "type": "array",
            "elementType": {
                "type": "struct",
                "fields": [
                    {
                        "name": "created_at",
                        "type": "long",
                        "nullable": True,
                        "metadata": {
                            "comment": "Unix timestamp at which survey was created"
                        },
                    },
                    {
                        "name": "created_at_datetime",
                        "type": "string",
                        "nullable": True,
                        "metadata": {"comment": "Datetime at which survey was created"},
                    },
                    {
                        "name": "user_id",
                        "type": "long",
                        "nullable": True,
                        "metadata": {"comment": "User ID associated with survey"},
                    },
                    {
                        "name": "is_useful",
                        "type": "boolean",
                        "nullable": True,
                        "metadata": {"comment": "Whether survey is useful or not"},
                    },
                    {
                        "name": "not_useful_reason",
                        "type": "string",
                        "nullable": True,
                        "metadata": {"comment": "The reason the survey was not useful"},
                    },
                    {
                        "name": "not_useful_reason_enum",
                        "type": "integer",
                        "nullable": True,
                        "metadata": {
                            "comment": "Enum value for why survey was not useful"
                        },
                    },
                    {
                        "name": "other_reason",
                        "type": "string",
                        "nullable": True,
                        "metadata": {
                            "comment": "Other reason (if specified) why survey was not useful"
                        },
                    },
                    {
                        "name": "is_dismissed_through_survey",
                        "type": "boolean",
                        "nullable": True,
                        "metadata": {
                            "comment": "Whether review was dismissed through survey or not"
                        },
                    },
                ],
            },
            "containsNull": False,
        },
        "nullable": True,
        "metadata": {"comment": "Metadata on harsh event surveys"},
    },
]

stg_safety_events_query = """--sql
WITH surveys AS (
    WITH not_useful_reasons AS (
        SELECT
            `date`,
            org_id,
            device_id,
            event_ms,
            harsh_accel_type,
            created_at,
            user_id,
            detail_proto.is_useful,
            CASE detail_proto.not_useful_reason
                WHEN 0 THEN 'InvalidReason'
                WHEN 1 THEN 'NoSpecifiedReason'
                WHEN 2 THEN 'NoHarshEvent'
                WHEN 3 THEN 'LabeledIncorrectly'
                WHEN 4 THEN 'MinorIncident'
                WHEN 5 THEN 'Other'
                -- For policy violations
                --
                -- Driver was holding something else (for mobile usage and smoking events)
                WHEN 6 THEN 'HoldingOtherObject'
                -- For all policy violation event types
                WHEN 7 THEN 'ObjectInVehicle'
                -- Unable to see what triggered the event (for policy violations and FD)
                WHEN 8 THEN 'NoVisualIndicator'
                -- Driver was drinking, not eating (for eating events)
                WHEN 9 THEN 'DrinkingNotEating'
                -- Driver was eating, not drinking (for drinking events)
                WHEN 10 THEN 'EatingNotDrinking'
                -- Driver was doing something other than eating or drinking
                -- (for eating and drinking events)
                WHEN 11 THEN 'NotEatingOrDrinking'
                -- Driver was wearing a seatbelt (for no seatbelt events)
                WHEN 12 THEN 'SeatbeltOn'
                -- Driver was wearing a mask (for no mask events)
                WHEN 13 THEN 'MaskOn'
                -- Camera was not obstructed (for camera obstructed events)
                WHEN 14 THEN 'CameraNotObstructed'
                -- For ADAS events
                --
                -- For DD events
                --
                -- The driver is looking down but not distracted
                WHEN 15 THEN 'NotDistracted'
                -- The box is not around the driver's head; the AI is instead
                -- tracking the steering wheel, arm, passenger, etc.
                WHEN 16 THEN 'BoundingBoxIssue'
                -- The box is around the driver's head, but the driver
                -- is not looking down (for DD events)
                WHEN 17 THEN 'NotLookingDown'
                -- Driver's face is obstructed
                WHEN 18 THEN 'FaceObstructed'
                -- For FD events, the driver doesn't appear to be following too closely
                WHEN 19 THEN 'NoUnsafeFollowingDistance'
                -- For FCW events
                --
                -- The vehicle is turning or going around a curved road
                WHEN 20 THEN 'VehicleTurning'
                -- A vehicle nearby is turning
                WHEN 21 THEN 'OtherVehicleTurning'
                -- An object other than a vehicle
                WHEN 22 THEN 'ObjectOtherThanVehicle'
                -- For Rolling Stop Events, a vehicle is marked as rolling a stop sign but the stop sign does not exist
                WHEN 23 THEN 'NoStopSign'
                -- A vehicle is marked as rolling a railroad crossing but the railroad crossing does not exist
                WHEN 24 THEN 'NoRailroadCrossing'
                -- The driver slowed below threshold for the rolling stop but was still marked as rolling a stop sign.
                WHEN 25 THEN 'DriverSlowedBelowThreshold'
            END AS not_useful_reason,
            detail_proto.not_useful_reason AS not_useful_reason_enum,
            detail_proto.other_reason,
            detail_proto.is_dismissed_through_survey
        FROM safetydb_shards.harsh_event_surveys
        -- Fetch the surveys starting from the last two days of the partition ranage.
        -- Surveys will be on Safety Events up to the date of the survey.
        WHERE `date` >= '{PARTITION_KEY_RANGE_START}'
    )
    , surveys_struct AS (
        SELECT
            `date`,
            org_id,
            device_id,
            event_ms,
            NAMED_STRUCT(
                'created_at', created_at,
                'created_at_datetime', FROM_UNIXTIME(created_at/1000, 'yyyy-MM-dd HH:mm:ss'),
                'user_id', user_id,
                'is_useful', is_useful,
                'not_useful_reason', not_useful_reason,
                'not_useful_reason_enum', not_useful_reason_enum,
                'other_reason', other_reason,
                'is_dismissed_through_survey', is_dismissed_through_survey
            ) AS harsh_event_survey
        FROM not_useful_reasons
    )
    -- Collect all surveys for an event into an array since the grain of the table
    -- stg_safety_events is per event; i.e., multiple surveys per event would attempt to
    -- merge multiple entries matching the same condition.
    SELECT
        CAST(DATE(FROM_UNIXTIME(event_ms/1000, 'yyyy-MM-dd HH:mm:ss')) AS STRING) AS event_date,
        org_id, device_id, event_ms,
        COLLECT_SET(harsh_event_survey) AS harsh_event_surveys
    FROM surveys_struct
    GROUP BY 1,2,3,4
)
SELECT
    CAST(se.`date` AS string) AS `date`,
    se.org_id,
    se.device_id,
    se.event_ms,
    se.detail_proto.accel_type,
    ha.event_type AS harsh_event_type,
    se.detail_proto.event_id,
    se.additional_labels,
    se.driver_id,
    se.release_stage,
    se.trip_start_ms,
    se.detail_proto.start.speed_milliknots AS trip_start_speed_milliknots,
    sv.harsh_event_surveys
FROM safetydb_shards.safety_events se
LEFT JOIN definitions.harsh_accel_type_enums ha ON se.detail_proto.accel_type = ha.enum
LEFT JOIN surveys sv ON se.`date` = sv.event_date
    AND se.org_id = sv.org_id
    AND se.device_id = sv.device_id
    AND se.event_ms = sv.event_ms
WHERE {PARTITION_FILTERS}
--endsql"""

stg_safety_events_assets = build_assets_from_sql(
    name="stg_safety_events",
    schema=stg_safety_events_schema,
    description="""From its source, safetydb.safety_events, 1:(0 or 1) to its output. Any events that occured in the last 35 days in safetydb.safety_events are kept up-to-date in this table.""",
    sql_query=stg_safety_events_query,
    primary_keys=[
        "date",
        "org_id",
        "device_id",
        "event_ms",
    ],
    upstreams=[
        AssetKey(["safetydb_shards", "safety_events"]),
    ],
    regions=get_all_regions(),
    group_name=pipeline_group_name,
    database=databases["database_silver_dev"],
    databases=databases,
    write_mode=WarehouseWriteMode.merge,
    partitions_def=daily_partition_def,
    backfill_policy=BACKFILL_POLICY,
    retry_policy=fct_safety_events_retry_policy,
)

stg_safety_events_us = stg_safety_events_assets[AWSRegions.US_WEST_2.value]
stg_safety_events_eu = stg_safety_events_assets[AWSRegions.EU_WEST_1.value]
stg_safety_events_ca = stg_safety_events_assets[AWSRegions.CA_CENTRAL_1.value]

# dqs["stg_safety_events"].append(
#     PrimaryKeyDQCheck(
#         name="dq_pk_stg_safety_events",
#         table="stg_safety_events",
#         primary_keys=["org_id", "device_id", "event_ms"],
#         blocking=True,
#         database=databases["database_silver_dev"],
#     )
# )

# dqs["stg_safety_events"].append(
#    TrendDQCheck(
#        name="dq_trend_stg_safety_events",
#        database=databases["database_silver_dev"],
#        table="stg_safety_events",
#        blocking=False,
#        lookback_days=7,
#        dimension="harsh_event_type",
#        min_percent_share=0.1,
#        tolerance=0.5,
#    )
# )

stg_activity_events_schema = [
    {
        "name": "date",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "The field which partitions the table, in 'YYYY-mm-dd' format. `date` is derived from `event_ms`."
        },
    },
    {
        "name": "org_id",
        "type": "long",
        "nullable": True,
        "metadata": {"comment": org_id_default_description},
    },
    {
        "name": "device_id",
        "type": "long",
        "nullable": True,
        "metadata": {"comment": device_id_default_description},
    },
    {
        "name": "event_ms",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "Unixtime (in milliseconds) that the event was triggered. A specific safety event is identified by joining on (org_id, device_id, event_ms). event_id may not be unique, due to int overflow issues in the current safety events framework."
        },
    },
    {
        "name": "created_at",
        "type": "timestamp",
        "nullable": True,
        "metadata": {"comment": "datetime that the activity occured"},
    },
    {
        "name": "activity_enum",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "enum representing the type of activity that occurred for this safety event. Possible values can be found in definitions.safety_activity_type_enums"
        },
    },
    {
        "name": "activity_name",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "enum representing the type of activity that occurred for this safety event. Possible values can be found in definitions.safety_activity_type_enums"
        },
    },
    {
        "name": "coaching_state_name",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "The current coaching state of the safety event. Possible values can be found in definitions.coaching_state_enums"
        },
    },
    {
        "name": "behavior_label_name",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "The behavior label that was applied to the safety event. Possible values can be found in definitions.behavior_label_type_enums"
        },
    },
    {
        "name": "behavior_label_addition",
        "type": "boolean",
        "nullable": True,
        "metadata": {
            "comment": "Indicates whether or not a behavior label activity is adding a behavior label (true) or removing a behavior label (false)"
        },
    },
    {
        "name": "detail_proto",
        "type": {
            "type": "struct",
            "fields": [
                {
                    "name": "behavior_label_content",
                    "type": {
                        "type": "struct",
                        "fields": [
                            {"name": "algorithm_version", "type": "integer"},
                            {"name": "label_type", "type": "integer"},
                            {"name": "new_value", "type": "boolean"},
                        ],
                    },
                },
                {
                    "name": "coaching_content",
                    "type": {
                        "type": "struct",
                        "fields": [
                            {
                                "name": "main_event_identifier",
                                "type": {
                                    "type": "struct",
                                    "fields": [
                                        {"name": "device_id", "type": "long"},
                                        {"name": "event_ms", "type": "long"},
                                    ],
                                },
                            },
                            {"name": "manager_user_id", "type": "long"},
                            {"name": "new_state", "type": "integer"},
                            {"name": "reason", "type": "integer"},
                        ],
                    },
                },
                {
                    "name": "comment_content",
                    "type": {
                        "type": "struct",
                        "fields": [
                            {"name": "comment_text", "type": "string"},
                            {"name": "url", "type": "string"},
                        ],
                    },
                },
                {
                    "name": "dismissal_with_reason_content",
                    "type": {
                        "type": "struct",
                        "fields": [
                            {"name": "comment_text", "type": "string"},
                            {"name": "dismissal_reason", "type": "integer"},
                        ],
                    },
                },
                {
                    "name": "triage_label_content",
                    "type": {
                        "type": "struct",
                        "fields": [
                            {"name": "label_id", "type": "string"},
                            {"name": "label_name", "type": "string"},
                            {"name": "new_value", "type": "boolean"},
                        ],
                    },
                },
                {
                    "name": "driver_assignment_content",
                    "type": {
                        "type": "struct",
                        "fields": [
                            {"name": "driver_id", "type": "long"},
                            {"name": "prev_driver_id", "type": "long"},
                        ],
                    },
                },
                {
                    "name": "driver_comment_content",
                    "type": {
                        "type": "struct",
                        "fields": [{"name": "comment_text", "type": "string"}],
                    },
                },
                {
                    "name": "harsh_event_label_content",
                    "type": {
                        "type": "struct",
                        "fields": [{"name": "new_user_label", "type": "integer"}],
                    },
                },
                {
                    "name": "inbox_content",
                    "type": {
                        "type": "struct",
                        "fields": [{"name": "new_state", "type": "integer"}],
                    },
                },
                {
                    "name": "manager_assignment_content",
                    "type": {
                        "type": "struct",
                        "fields": [
                            {"name": "manager_user_id", "type": "long"},
                            {"name": "prev_manager_user_id", "type": "long"},
                        ],
                    },
                },
                {
                    "name": "trigger_label_content",
                    "type": {
                        "type": "struct",
                        "fields": [
                            {"name": "algorithm_version", "type": "integer"},
                            {"name": "severity_level", "type": "integer"},
                            {"name": "trigger_reason", "type": "integer"},
                        ],
                    },
                },
                {"name": "user_id", "type": "long"},
                {"name": "user_type", "type": "integer"},
            ],
        },
    },
]
# "nullable": True,
#         # TODO: add metadatahelpers.EnumDescription entry once feature is available.
#         # go/src/samsaradev.io/safety/safetyproto/safetyactivity.proto
#         "metadata": {
#             "comment": "Proto blob containing details of the activity that occurred for this safety event"
#         },

stg_activity_events_query = """--sql
SELECT
    CAST(`date` AS string) AS `date`,
    ae.org_id,
    ae.device_id,
    ae.event_ms,
    ae.created_at,
    ae.activity_type AS activity_enum,
    es.activity_type AS activity_name,
    ec.coaching_type AS coaching_state_name,
    eb.behavior_label_type AS behavior_label_name,
    ae.detail_proto.behavior_label_content.new_value AS behavior_label_addition,
    ae.detail_proto
FROM safetydb_shards.activity_events ae
LEFT JOIN definitions.safety_activity_type_enums es ON ae.activity_type = es.enum
LEFT JOIN definitions.coaching_state_enums ec ON ae.detail_proto.coaching_content.new_state = ec.enum
LEFT JOIN definitions.behavior_label_type_enums eb ON ae.detail_proto.behavior_label_content.label_type = eb.enum
WHERE {PARTITION_FILTERS}
--endsql"""

stg_activity_events_assets = build_assets_from_sql(
    name="stg_activity_events",
    schema=stg_activity_events_schema,
    description="""Copy from its source, safetydb.activity_events, with some added data. For any events that occured in the last 365 days in safetydb.safety_events, the corresponding activites, from safetydb.activity_events, are kept up-to-date in this table.""",
    sql_query=stg_activity_events_query,
    primary_keys=[
        "date",
        "org_id",
        "device_id",
        "event_ms",
        "created_at",
        "activity_enum",
    ],
    upstreams=[
        AssetKey(["safetydb_shards", "activity_events"]),
    ],
    regions=get_all_regions(),
    group_name=pipeline_group_name,
    database=databases["database_silver_dev"],
    databases=databases,
    write_mode=WarehouseWriteMode.merge,
    partitions_def=daily_partition_def,
    backfill_policy=BACKFILL_POLICY,
    retry_policy=fct_safety_events_retry_policy,
)

stg_activity_events_us = stg_activity_events_assets[AWSRegions.US_WEST_2.value]
stg_activity_events_eu = stg_activity_events_assets[AWSRegions.EU_WEST_1.value]
stg_activity_events_ca = stg_activity_events_assets[AWSRegions.CA_CENTRAL_1.value]

dqs["stg_activity_events"].append(
    PrimaryKeyDQCheck(
        name="dq_pk_stg_activity_events",
        table="stg_activity_events",
        primary_keys=[
            "date",
            "org_id",
            "device_id",
            "event_ms",
            "created_at",
            "activity_enum",
        ],
        blocking=True,
        database=databases["database_silver_dev"],
    )
)


fct_safety_events_schema = [
    {
        "name": "date",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "The field which partitions the table, in 'YYYY-mm-dd' format. `date` is derived from `event_ms`."
        },
    },
    {
        "name": "org_id",
        "type": "long",
        "nullable": True,
        "metadata": {"comment": org_id_default_description},
    },
    {
        "name": "device_id",
        "type": "long",
        "nullable": True,
        "metadata": {"comment": device_id_default_description},
    },
    {
        "name": "event_ms",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "Unixtime (in milliseconds) that the event was triggered"
        },
    },
    {
        "name": "event_id",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "ID of the event that was triggered by the device. May not be unique, due to int overflow issues in the current safety events framework."
        },
    },
    {
        "name": "activity_names_array",
        "type": {
            "type": "array",
            "elementType": "string",
            "containsNull": True,
        },
        "nullable": True,
        "metadata": {
            "comment": "Array of activity names that occurred for this safety event"
        },
    },
    {
        "name": "first_activity_time",
        "type": "timestamp",
        "nullable": True,
        "metadata": {
            "comment": "datetime of the first activity that occurred for this safety event"
        },
    },
    {
        "name": "latest_activity_time",
        "type": "timestamp",
        "nullable": True,
        "metadata": {
            "comment": "datetime of the latest activity that occurred for this safety event"
        },
    },
    {
        "name": "total_activity_count",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "Total number of activities that occurred for this safety event"
        },
    },
    {
        "name": "detection_label",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "The detection label that was applied to the safety event"
        },
    },
    {
        "name": "behavior_label_names_array",
        "type": {
            "type": "array",
            "elementType": "string",
            "containsNull": True,
        },
        "nullable": True,
        "metadata": {
            "comment": "Array of behavior labels that were applied to the safety event"
        },
    },
    {
        "name": "coaching_state_names_array",
        "type": {
            "type": "array",
            "elementType": "string",
            "containsNull": True,
        },
        "nullable": True,
        "metadata": {
            "comment": "Array of coaching states that occurred for this safety event"
        },
    },
    {
        "name": "coaching_state_details_array",
        "type": {
            "type": "array",
            "elementType": {
                "type": "struct",
                "fields": [
                    {
                        "name": "coaching_state_name",
                        "type": "string",
                        "nullable": True,
                        "metadata": {
                            "comment": "The name of coaching state for this activity"
                        },
                    },
                    {
                        "name": "created_at",
                        "type": "timestamp",
                        "nullable": True,
                        "metadata": {"comment": "datetime that the activity occured"},
                    },
                    {
                        "name": "user_id",
                        "type": "long",
                        "nullable": True,
                        "metadata": {
                            "comment": "The user ID of the user that triggered the coaching state activity"
                        },
                    },
                ],
            },
            "containsNull": False,
        },
        "nullable": True,
        "metadata": {
            "comment": "Array containing details of the coaching activities that occurred for this safety event"
        },
    },
    {
        "name": "was_event_coached",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Indicates whether or not this safety event was coached"
        },
    },
    {
        "name": "driver_id",
        "type": "long",
        "nullable": True,
        "metadata": {"comment": driver_id_default_description},
    },
    {
        "name": "release_stage",
        "type": "short",
        "nullable": True,
        # TODO: add metadatahelpers.EnumDescription entry once feature is available.
        # safetyproto.SafetyEventReleaseStage_name
        "metadata": {
            "comment": "The release stage of the safety event. Controls visibility to end users."
        },
    },
    {
        "name": "trip_start_ms",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "Unixtime (in milliseconds) of the start of the trip that the event occurred on. 0 if the event happened off-trip"
        },
    },
    {
        "name": "trip_start_speed_milliknots",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "GPS speed in milliknots reported by the device at the start of the event. Used to determine whether the event is considered low speed (speeds under 4344.8812095 milliknots for events that are not crashes or rolling stops are filtered out)."
        },
    },
    {
        "name": "harsh_event_surveys",
        "type": {
            "type": "array",
            "elementType": {
                "type": "struct",
                "fields": [
                    {
                        "name": "created_at",
                        "type": "long",
                        "nullable": True,
                        "metadata": {
                            "comment": "The unix timestamp of when the survey was created"
                        },
                    },
                    {
                        "name": "created_at_datetime",
                        "type": "string",
                        "nullable": True,
                        "metadata": {
                            "comment": "Human readable datetime of when the survey was created"
                        },
                    },
                    {
                        "name": "user_id",
                        "type": "long",
                        "nullable": True,
                        "metadata": {
                            "comment": "The user ID of the user who created the survey"
                        },
                    },
                    {
                        "name": "is_useful",
                        "type": "boolean",
                        "nullable": True,
                        "metadata": {
                            "comment": "Whether the video was marked as useful (thumbs up or thumbs down)"
                        },
                    },
                    {
                        "name": "not_useful_reason",
                        "type": "string",
                        "nullable": True,
                        "metadata": {
                            "comment": "The reason the video was marked as not useful"
                        },
                    },
                    {
                        "name": "not_useful_reason_enum",
                        "type": "integer",
                        "nullable": True,
                        "metadata": {
                            "comment": "The enum value of the reason the video was marked as not useful"
                        },
                    },
                    {
                        "name": "other_reason",
                        "type": "string",
                        "nullable": True,
                        "metadata": {
                            "comment": "Populated if the reason the video was marked as not useful is not one of the pre-selected reasons"
                        },
                    },
                    {
                        "name": "is_dismissed_through_survey",
                        "type": "boolean",
                        "nullable": True,
                        "metadata": {
                            "comment": "Whether the event was dismissed through the survey"
                        },
                    },
                ],
            },
            "containsNull": True,
        },
        "nullable": True,
        "metadata": {"comment": "Data from survey prompt after a video is reviewed"},
    },
]

fct_safety_events_query = """--sql
WITH behavior_label_activities AS (
    SELECT
        org_id,
        device_id,
        event_ms,
        created_at,
        activity_name,
        behavior_label_name,

        COLLECT_SET(behavior_label_name) OVER (PARTITION BY (org_id, device_id, event_ms)) behavior_label_names,
        FIRST(created_at) OVER (PARTITION BY (org_id, device_id, event_ms, behavior_label_name) ORDER BY created_at DESC) last_behavior_label_time,
        FIRST(behavior_label_addition) OVER (PARTITION BY (org_id, device_id, event_ms, behavior_label_name) ORDER BY created_at DESC) last_behavior_label_addition,
        COUNT(*) OVER (PARTITION BY (org_id, device_id, event_ms)) total_behavior_activities

    FROM {database_silver_dev}.stg_activity_events
    WHERE {PARTITION_FILTERS}
        AND activity_name = "BehaviorLabelActivityType"
),
behavior_label_activities_grouped AS (
    SELECT
        org_id,
        device_id,
        event_ms,
        activity_name,
        COLLECT_SET(CASE WHEN last_behavior_label_addition THEN behavior_label_name END) AS valid_behavior_label_names
    FROM behavior_label_activities
    GROUP BY 1, 2, 3, 4
),
activities_by_type AS (
    SELECT
        org_id,
        device_id,
        event_ms,
        activity_name,
        MIN(created_at) AS first_activity_time,
        MAX(created_at) AS latest_activity_time,
        COUNT(*) AS activity_count,

        -- user_id=0 correlates to Samsara system user adding a label. Find the first one.
        MIN_BY(CASE WHEN detail_proto.user_id=0 THEN behavior_label_name END, created_at) AS detection_label,

        COLLECT_SET(coaching_state_name) AS coaching_state_names,
        ARRAY_AGG(
            CASE WHEN coaching_state_name IS NOT NULL THEN STRUCT(coaching_state_name, created_at, detail_proto.user_id AS user_id) END
        ) AS coaching_state_details
    FROM {database_silver_dev}.stg_activity_events
    WHERE {PARTITION_FILTERS}
    GROUP BY 1, 2, 3, 4
), activities_by_event AS (
    SELECT
        CAST(DATE(from_unixtime(a.event_ms/1000, 'yyyy-MM-dd HH:mm:ss')) AS string) AS `date`,
        a.org_id,
        a.device_id,
        a.event_ms,

        COLLECT_SET(a.activity_name) AS activity_names_array,
        MIN(first_activity_time) AS first_activity_time,
        MAX(latest_activity_time) AS latest_activity_time,
        SUM(activity_count) AS total_activity_count,

        MIN(detection_label) AS detection_label,
        MAX(
            CASE WHEN CARDINALITY(bg.valid_behavior_label_names) = 0 THEN NULL ELSE bg.valid_behavior_label_names END
        ) AS behavior_label_names_array,

        MAX(
            CASE WHEN CARDINALITY(coaching_state_names) = 0 THEN NULL ELSE coaching_state_names END
        ) AS coaching_state_names_array,
        MAX(
            CASE WHEN CARDINALITY(coaching_state_details) = 0 THEN NULL ELSE coaching_state_details END
        ) AS coaching_state_details_array
    FROM activities_by_type a
    LEFT JOIN behavior_label_activities_grouped bg ON a.org_id = bg.org_id
        AND a.device_id = bg.device_id
        AND a.event_ms = bg.event_ms
        AND a.activity_name = bg.activity_name -- bg only has BehaviorLabelActivityType activities
    GROUP BY 2, 3, 4
)

SELECT
    a.`date` AS `date`,
    a.org_id,
    a.device_id,
    a.event_ms,
    se.event_id,

    activity_names_array,
    first_activity_time,
    latest_activity_time,
    total_activity_count,

    -- TODO: troubleshoot why detection_label from previous CTE returns null for some events
    se.harsh_event_type AS detection_label,
    behavior_label_names_array,

    coaching_state_names_array,
    coaching_state_details_array,
    COALESCE(ARRAY_CONTAINS(coaching_state_names_array, 'Coached'), FALSE) was_event_coached,

    se.driver_id,
    se.release_stage, -- release_stage >= 2 for inbox
    se.trip_start_ms,  -- trip_start > 0 for inbox
    se.trip_start_speed_milliknots, -- trip_start_speed_milliknots > 0 for inbox
    se.harsh_event_surveys
FROM activities_by_event a
-- Safety FS is aware of an issue where activities for an <org_id, device_id, event_ms> combination
-- may be present in safetydb.activity_events (and thus in fct_safety_events) but not present in safetydb.safety_events.
-- Therefore, make this an inner join between stg_safety_events and fct_safety_events, as opposed to LEFT JOIN.
INNER JOIN {database_silver_dev}.stg_safety_events se ON a.`date` = se.`date`
    AND a.org_id = se.org_id
    AND a.device_id = se.device_id
    AND a.event_ms = se.event_ms
--endsql"""

fct_safety_events_assets = build_assets_from_sql(
    name="fct_safety_events",
    step_launcher="databricks_pyspark_step_launcher",
    schema=fct_safety_events_schema,
    description=build_table_description(
        table_desc="""Warning: This table is not intended to replicate what users see on the Safety Inbox cloud dashboard page; use product_analytics.fct_safety_triage_events for that purpose. Instead, this table is 'upstream' of the inbox and can be used to see which events made it into the backend. Further, this table can be used to understand the activities that have occurred on a safety event. It is a summary of the various activities that have occured on a safety event; e.g., coaching activities, video views, adding/removing behavior labels, etc.""",
        row_meaning="""Each row corresponds to a safety event and contains fields aggregating the various activities that have occurred on it; e.g., coaching_state_details_array contains the timestamp, user_id, and coaching state name for each coaching activity that has occurred on the safety event.""",
        table_type=TableType.ACCUMULATING_SNAPSHOT_FACT,
        freshness_slo_updated_by="9am PST",
    ),
    sql_query=fct_safety_events_query,
    primary_keys=[
        "date",
        "org_id",
        "device_id",
        "event_ms",
        "event_id",
    ],
    upstreams=[
        AssetKey([databases["database_silver_dev"], "dq_stg_safety_events"]),
        AssetKey([databases["database_silver_dev"], "dq_stg_activity_events"]),
    ],
    regions=get_all_regions(),
    group_name=pipeline_group_name,
    database=databases["database_gold_dev"],
    databases=databases,
    write_mode=WarehouseWriteMode.merge,
    partitions_def=daily_partition_def,
    backfill_policy=BACKFILL_POLICY,
    retry_policy=fct_safety_events_retry_policy,
)

fct_safety_events_us = fct_safety_events_assets[AWSRegions.US_WEST_2.value]
fct_safety_events_eu = fct_safety_events_assets[AWSRegions.EU_WEST_1.value]
fct_safety_events_ca = fct_safety_events_assets[AWSRegions.CA_CENTRAL_1.value]

dqs["fct_safety_events"].append(
    PrimaryKeyDQCheck(
        name="dq_pk_fct_safety_events",
        table="fct_safety_events",
        primary_keys=[
            "date",
            "org_id",
            "device_id",
            "event_ms",
            "event_id",
        ],
        blocking=True,
        database=databases["database_gold_dev"],
    )
)

dqs["fct_safety_events"].append(
    NonEmptyDQCheck(
        name="dq_empty_fct_safety_events",
        table="fct_safety_events",
        blocking=True,
        database=databases["database_gold_dev"],
    )
)

dqs["fct_safety_events"].append(
    JoinableDQCheck(
        name="dq_join_fct_safety_events",
        database="safetydb_shards",
        database_2=databases["database_gold_dev"],
        input_asset_1="safety_events",
        input_asset_2="fct_safety_events",
        join_keys=[
            ("date", "date"),
            ("org_id", "org_id"),
            ("device_id", "device_id"),
            ("event_ms", "event_ms"),
        ],
        blocking=False,
        # TODO: edge cases may result in a small number of events associated with activities
        # in safetydb.activity_events not being present in safetydb.safety_events.
        # This number is the guaranteed accuracy of the pipeline.
        # After the current day, the share of events in safetydb_shards.safety_events and
        # that are not in fct_safety_events drops off geometrically. The threshold is set
        # such that the current_date-1 and current_date-2 do not fail constantly.
        null_right_table_rows_ratio=0.02,
    )
)


dq_assets = dqs.generate()
