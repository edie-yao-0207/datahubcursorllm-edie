from dagster import (
    AssetKey,
    BackfillPolicy,
    Backoff,
    DailyPartitionsDefinition,
    Jitter,
    RetryPolicy,
)

from ...common.constants import (
    SLACK_ALERTS_CHANNEL_DATA_ENGINEERING,
    device_id_default_description,
    model_registry_key_default_description,
    org_id_default_description,
    run_id_default_description,
)
from ...common.utils import (
    AWSRegions,
    Database,
    DQGroup,
    JoinableDQCheck,
    NonEmptyDQCheck,
    PrimaryKeyDQCheck,
    TableType,
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

pipeline_group_name = "fct_firmware_events"
# end_offset=1 ensures that the pipeline runs for the current day
# start_date is based on the earliest data available in upstream dependency dim_devices
daily_partition_def = DailyPartitionsDefinition(start_date="2023-08-20", end_offset=1)
BACKFILL_POLICY = BackfillPolicy.multi_run(max_partitions_per_run=7)
fct_firmware_events_retry_policy = RetryPolicy(
    max_retries=3,  # Maximum number of retries
    delay=180,  # Initial delay in seconds
    backoff=Backoff.EXPONENTIAL,  # Delay multiplier for each retry
    jitter=Jitter.FULL,  # Apply randomness to delay intervals
)

dqs = DQGroup(
    group_name=pipeline_group_name,
    partition_def=daily_partition_def,
    slack_alerts_channel=SLACK_ALERTS_CHANNEL_DATA_ENGINEERING,
    regions=get_all_regions(),
    backfill_policy=BACKFILL_POLICY,
)

stg_osdaccelerometer_schema = [
    {
        "name": "date",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "The date the event was logged (yyyy-mm-dd)."},
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
        "name": "event_id",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "The ID of the event that was triggered by the device. May not be unique, due to int overflow issues in the current safety events framework."
        },
    },
    {
        "name": "harsh_accel_type",
        "type": "integer",
        "nullable": True,
        "metadata": {
            "comment": "The type of event that was triggered by the device. Possible values can be retrieved from definitions.harsh_accel_type_enums"
        },
    },
    {
        "name": "harsh_accel_name",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "The name of the event that was triggered by the device. Possible values can be retrieved from definitions.harsh_accel_type_enums"
        },
    },
    {
        "name": "run_id",
        "type": "long",
        "nullable": True,
        "metadata": {"comment": run_id_default_description},
    },
    {
        "name": "product_id",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "Joins to definitions.products to allow for human-readable product names and Product Group ID"
        },
    },
    {
        "name": "device_type",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Attribute defining the device type (e.g. VG, CM, AG, ...)"
        },
    },
    {
        "name": "associated_devices",
        "type": {
            "type": "map",
            "keyType": "string",
            "valueType": "string",
            "valueContainsNull": True,
        },
        "nullable": True,
        "metadata": {
            "comment": "Map describing devices associated with the device_id. For example the VG associated with the CM or vice versa"
        },
    },
    {
        "name": "dim_devices_date",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "The date of the dim_devices entry that was used to update this event (yyyy-mm-dd)."
        },
    },
    {
        "name": "updated_date",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "The date this event was last updated in this table (yyyy-mm-dd)."
        },
    },
]

stg_osdaccelerometer_query = """--sql
    -- This query adds features to the events in osdaccelerometer, such as the product_id, device_type, and associated_devices.
    WITH ka AS (
        SELECT
            CAST(ka.`date` AS string) AS `date`,
            ka.org_id,
            ka.object_id AS device_id,
            ka.time AS event_ms,
            ka.value.proto_value.accelerometer_event.event_id AS event_id,
            ka.value.proto_value.accelerometer_event.harsh_accel_type AS harsh_accel_type,
            ha.event_type AS harsh_accel_name,
            ka.value.proto_value.accelerometer_event.ml_run_tag.run_id
        FROM kinesisstats.osdaccelerometer ka
        LEFT JOIN definitions.harsh_accel_type_enums ha
            ON ka.value.proto_value.accelerometer_event.harsh_accel_type = ha.enum
        WHERE {PARTITION_FILTERS}
    )
    -- dim_devices and safety_events are joined in separate CTEs because PARTITION_FILTERS generates a WHERE clause
    -- that would have an ambiguous column reference to `date`.
    , dv AS (
        SELECT
            `date`,
            device_id,
            product_id,
            device_type,
            associated_devices
        FROM datamodel_core.dim_devices
        WHERE ({PARTITION_FILTERS})
            OR `date` >= (SELECT MAX(`date`) FROM datamodel_core.dim_devices)
    )
    -- Calculate max date here to avoid SparkException NotSerializableException: org.apache.spark.sql.catalyst.expressions.ExpressionSet
    -- org.apache.spark.SparkException: Job aborted due to stage failure: Task not serializable: java.io.NotSerializableException: org.apache.spark.sql.catalyst.expressions.ExpressionSet
    , dv_max AS (
      SELECT MAX(`date`) AS max_date FROM datamodel_core.dim_devices
    )
    -- Join to either the dim_devices entry that corresponds to the event date
    -- or the latest entry in dim_devices.
    -- This is intended to generate valide device data for a run that has occurred prior to
    -- dim_devices generating data for that day. E.g., If this query is running for the
    -- 2024-03-26 partition but dim_devices only has device data up to 2024-03-25.
    SELECT
        ka.*,
        dv.product_id,
        dv.device_type,
        dv.associated_devices, -- will be used to find associated VG/CM devices
        dv.`date` AS dim_devices_date,
        CAST(CURRENT_DATE() AS string) AS updated_date
    FROM ka
    LEFT JOIN dv_max ON ka.`date` > dv_max.max_date
    LEFT JOIN dv ON ka.device_id = dv.device_id
        AND LEAST(ka.`date`, (dv_max.max_date)) = dv.`date`
--endsql"""

stg_osdaccelerometer_assets = build_assets_from_sql(
    name="stg_osdaccelerometer",
    schema=stg_osdaccelerometer_schema,
    description="""Staging table that contains additional elements added to osdaccelerometer""",
    sql_query=stg_osdaccelerometer_query,
    primary_keys=[
        "date",
        "org_id",
        "device_id",
        "event_ms",
    ],
    upstreams=[
        AssetKey(["kinesisstats", "osdaccelerometer"]),
        AssetKey([Database.DATAMODEL_CORE, "dq_dim_devices"]),
        AssetKey(["definitions", "harsh_accel_type_enums"]),
    ],
    regions=get_all_regions(),
    group_name=pipeline_group_name,
    database=databases["database_silver_dev"],
    databases=databases,
    write_mode=WarehouseWriteMode.merge,
    partitions_def=daily_partition_def,
    backfill_policy=BACKFILL_POLICY,
    retry_policy=fct_firmware_events_retry_policy,
)

stg_osdaccelerometer_us = stg_osdaccelerometer_assets[AWSRegions.US_WEST_2.value]
stg_osdaccelerometer_eu = stg_osdaccelerometer_assets[AWSRegions.EU_WEST_1.value]
stg_osdaccelerometer_ca = stg_osdaccelerometer_assets[AWSRegions.CA_CENTRAL_1.value]

dqs["stg_osdaccelerometer"].append(
    PrimaryKeyDQCheck(
        name="dq_pk_stg_osdaccelerometer",
        table="stg_osdaccelerometer",
        primary_keys=["date", "org_id", "device_id", "event_ms"],
        blocking=True,
        database=databases["database_silver_dev"],
    )
)


fct_firmware_events_schema = [
    {
        "name": "date",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "The date the event was logged (yyyy-mm-dd)."},
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
        "name": "event_id",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "The ID of the event that was triggered by the device. May not be unique, due to int overflow issues in the current safety events framework."
        },
    },
    {
        "name": "harsh_accel_type",
        "type": "integer",
        "nullable": True,
        "metadata": {
            "comment": "The type of event that was triggered by the device. Possible values can be retrieved from definitions.harsh_accel_type_enums"
        },
    },
    {
        "name": "harsh_accel_name",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "The name of the event that was triggered by the device. Possible values can be retrieved from definitions.harsh_accel_type_enums"
        },
    },
    {
        "name": "device_type",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Attribute defining the device type (e.g. VG, CM, AG, ...)"
        },
    },
    {
        "name": "product_id",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "Joins to definitions.products to allow for human-readable product names and Product Group ID"
        },
    },
    {
        "name": "device_build",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "The build the device is running"},
    },
    {
        "name": "associated_device_id",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "The device_id of the associated device. For example, the VG associated with the CM or vice versa."
        },
    },
    {
        "name": "associated_device_type",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "The device_type of the associated device"},
    },
    {
        "name": "associated_product_id",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "The product_id of the associated device"},
    },
    {
        "name": "associated_device_build",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "The build of the associated device"},
    },
    {
        "name": "in_backend",
        "type": "boolean",
        "nullable": True,
        "metadata": {
            "comment": "Whether or not the device is in the safetydb.safety_events table"
        },
    },
    {
        "name": "safety_events_device_id",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "The device_id of the safety event in the safetydb.safety_events table. If NULL, the event is not in safetydb.safety_events."
        },
    },
    {
        "name": "safety_events_device_type",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "The device_type of the safety event in the safetydb.safety_events table. If NULL, the event is not in safetydb.safety_events."
        },
    },
    {
        "name": "run_id",
        "type": "long",
        "nullable": True,
        "metadata": {"comment": run_id_default_description},
    },
    {
        "name": "ml_run_id",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "The unique identifier of the ML run ID instance for the device in the associated mlapprunevent table. If NULL, the event is not in the mlapprunevent table."
        },
    },
    {
        "name": "model_registry_key",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": model_registry_key_default_description},
    },
    {
        "name": "updated_date",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "The date this event was last updated in this table (yyyy-mm-dd)."
        },
    },
]

# TODO: Add heartbeat_builds to dim_devices, which would avoid the need for the heartbeat_builds CTE in the query.
fct_firmware_events_query = """--sql
    -- Get daily device build data
    WITH heartbeat_builds AS (
      SELECT
        date,
        org_id,
        object_id AS device_id,
        MAX_BY(
          value.proto_value.hub_server_device_heartbeat.connection.device_hello.build, time
        ) AS build,
      MAX(value.time) AS latest_stat_timesamp
      FROM kinesisstats_history.osdhubserverdeviceheartbeat
      WHERE {PARTITION_FILTERS}
        AND NOT value.is_end
        AND NOT value.is_databreak
        AND org_id NOT IN (0, 1)
      GROUP BY 1, 2, 3
    )
    -- Get firmware events from osdaccelerometer separately to avoid ambiguous column reference in PARTITION_FILTERS.
    , base_firmware AS (
        SELECT * FROM {database_silver_dev}.stg_osdaccelerometer WHERE {PARTITION_FILTERS}
    )
    -- Calculate max date here to avoid SparkException NotSerializableException: org.apache.spark.sql.catalyst.expressions.ExpressionSet
    -- org.apache.spark.SparkException: Job aborted due to stage failure: Task not serializable: java.io.NotSerializableException: org.apache.spark.sql.catalyst.expressions.ExpressionSet
    , hb_max AS (
        SELECT MAX(`date`) AS max_date FROM heartbeat_builds
    )
    -- Get events from VG devices. Extract connected CM data from associated_devices.
    , gateways AS (
        SELECT
            f.`date`,
            f.org_id,
            f.device_id,
            f.event_ms,
            f.event_id,
            f.harsh_accel_type,
            f.harsh_accel_name,
            f.device_type,
            f.product_id,
            hbv.build AS device_build,
            -- Only VG devices will have these camera fields in associated_devices
            f.associated_devices.camera_device_id AS associated_device_id,
            f.associated_devices.camera_device_type AS associated_device_type,
            f.associated_devices.camera_product_id AS associated_product_id,
            hbc.build AS associated_device_build,
            -- Omit associated_devices at this point, because map type fields cannot be unioned (next step).
            f.run_id
        FROM base_firmware f
        LEFT JOIN hb_max ON f.`date` > hb_max.max_date
        -- Join to either the heartbeat_builds entry that corresponds to the event date
        -- or the latest entry in heartbeat_builds.
        LEFT JOIN heartbeat_builds hbv ON f.org_id = hbv.org_id
            AND f.device_id = hbv.device_id
            AND LEAST(f.`date`, (hb_max.max_date)) = hbv.`date`
        LEFT JOIN heartbeat_builds hbc ON f.org_id = hbc.org_id
            AND f.associated_devices.camera_device_id = hbc.device_id
            AND LEAST(f.`date`, (hb_max.max_date)) = hbc.`date`
        WHERE f.device_type = 'VG - Vehicle Gateway'
    )
    -- Get events from CM devices. Extract connected VG data from associated_devices.
    , cameras AS (
        SELECT
            f.`date`,
            f.org_id,
            f.device_id,
            f.event_ms,
            f.event_id,
            f.harsh_accel_type,
            f.harsh_accel_name,
            f.device_type,
            f.product_id,
            hbc.build AS device_build,
            -- Only CM devices will have these VG fields in associated_devices
            f.associated_devices.vg_device_id AS associated_device_id,
            f.associated_devices.vg_device_type AS associated_device_type,
            f.associated_devices.vg_product_id AS associated_product_id,
            hbv.build AS associated_device_build,
            -- Omit associated_devices at this point, because map type fields cannot be unioned (next step).
            f.run_id
        FROM base_firmware f
        -- Get events from CM devices. Extract connected VG data from associated_devices.
        -- Join to either the heartbeat_builds entry that corresponds to the event date
        -- or the latest entry in heartbeat_builds.
        LEFT JOIN hb_max ON f.`date` > hb_max.max_date
        LEFT JOIN heartbeat_builds hbc ON f.org_id = hbc.org_id
            AND f.device_id = hbc.device_id
            AND LEAST(f.`date`, (hb_max.max_date)) = hbc.`date`
        LEFT JOIN heartbeat_builds hbv ON f.org_id = hbv.org_id
            AND f.associated_devices.vg_device_id = hbv.device_id
            AND LEAST(f.`date`, (hb_max.max_date)) = hbv.`date`
        WHERE f.device_type = 'CM - AI Dash Cam'
    )
    -- Combine VG and CM events
    , combined_devices AS (
        WITH all_devices AS (
            SELECT * FROM gateways
            UNION
            SELECT * FROM cameras
        ),
        -- Join all devices based on osdaccelerometer device_id first.
        join_safety AS (
            SELECT
                f.`date`,
                f.org_id,
                f.device_id,
                f.event_ms,
                f.event_id,
                f.harsh_accel_type,
                f.harsh_accel_name,
                f.device_type,
                f.product_id,
                f.device_build,
                f.associated_device_id,
                f.associated_device_type,
                f.associated_product_id,
                f.associated_device_build,
                IFNULL(se.device_id IS NOT NULL, FALSE) AS in_backend,
                se.device_id AS safety_events_device_id,
                dv.device_type AS safety_events_device_type,
                f.run_id
            FROM all_devices f
            -- Join on osdaccelerometer device_id first. Will be joined on associated_device later.
            LEFT JOIN safetydb_shards.safety_events se ON f.`date` = se.`date`
                AND f.org_id = se.org_id
                AND f.device_id = se.device_id
                AND f.event_ms = se.event_ms
            LEFT JOIN datamodel_core.dim_devices dv ON se.`date` = dv.`date`
                AND se.device_id = dv.device_id
        ),
        -- Get all devices that successfully joined on osdaccelerometer device_id
        in_safety AS (
            SELECT * FROM join_safety WHERE in_backend IS TRUE
        ),
        -- Select devices that didn't join based on device_id
        not_in_safety AS (
            SELECT
                f.`date`,
                f.org_id,
                f.device_id,
                f.event_ms,
                f.event_id,
                f.harsh_accel_type,
                f.harsh_accel_name,
                f.device_type,
                f.product_id,
                f.device_build,
                f.associated_device_id,
                f.associated_device_type,
                f.associated_product_id,
                f.associated_device_build,
                IFNULL(se.device_id IS NOT NULL, FALSE) AS in_backend,
                se.device_id AS safety_events_device_id,
                dv.device_type AS safety_events_device_type,
                f.run_id
            FROM (SELECT * FROM join_safety WHERE in_backend IS FALSE) AS f
            -- For devices that didn't join to safetydb.safety_events with their device_id,
            -- try joining with their associated_device_id.
            LEFT JOIN safetydb_shards.safety_events se ON f.`date` = se.`date`
                AND f.org_id = se.org_id
                AND f.associated_device_id = se.device_id
                AND f.event_ms = se.event_ms
            LEFT JOIN datamodel_core.dim_devices dv ON se.`date` = dv.`date`
                AND se.device_id = dv.device_id
        )
        SELECT * FROM in_safety
        UNION
        SELECT * FROM not_in_safety
    )
    -- Join events to their corresponding mlapprunevent tables.
    -- Add events that don't run on the mlapprunevent framework at the end.
    , join_mlapps AS (
        WITH a AS (
            SELECT f.*,
                ml_run_id,
                model_registry_key
            FROM combined_devices f
            LEFT JOIN (
                SELECT
                    value.proto_value.mlapp_run_event.run_tag.run_id AS ml_run_id,
                    value.proto_value.mlapp_run_event.model_file_state.model_registry_key
                FROM kinesisstats.osdmlappruneventforwardcollisionwarning
                WHERE {PARTITION_FILTERS}
                    AND value.proto_value.mlapp_run_event.run_event_type = 1 -- corresponding to START
            ) ml ON f.run_id = ml.ml_run_id
            WHERE harsh_accel_type = 12 -- haNearCollision aka forward collision warning
        ),
        b AS (
            SELECT f.*,
                ml_run_id,
                model_registry_key
            FROM combined_devices f
            LEFT JOIN (
                SELECT
                    value.proto_value.mlapp_run_event.run_tag.run_id AS ml_run_id,
                    value.proto_value.mlapp_run_event.model_file_state.model_registry_key
                FROM kinesisstats.osdmlappruneventinwardsafety
                WHERE {PARTITION_FILTERS}
                    AND value.proto_value.mlapp_run_event.run_event_type = 1 -- corresponding to START
            ) ml ON f.run_id = ml.ml_run_id
            WHERE harsh_accel_type IN (13,19,23) -- haDistractedDriving,haPhonePolicy,haSeatbeltPolicy
        ),
        c AS (
            SELECT f.*,
                ml_run_id,
                model_registry_key
            FROM combined_devices f
            LEFT JOIN (
                SELECT
                    value.proto_value.mlapp_run_event.run_tag.run_id AS ml_run_id,
                    value.proto_value.mlapp_run_event.model_file_state.model_registry_key
                FROM kinesisstats.osdmlappruneventfollowingdistance
                WHERE {PARTITION_FILTERS}
                    AND value.proto_value.mlapp_run_event.run_event_type = 1 -- corresponding to START
            ) ml ON f.run_id = ml.ml_run_id
            WHERE harsh_accel_type = 14 -- haTailgating aka following distance
                -- Only Brigid devices use FDv1.5 (osdmlappruneventfollowingdistance).
                -- Future CM devices will use FDv2 (see outwardsafety below)
                AND f.product_id IN (155, 167)
        ),
        d AS (
            SELECT f.*,
                ml_run_id,
                model_registry_key
            FROM combined_devices f
            LEFT JOIN (
                SELECT
                    value.proto_value.mlapp_run_event.run_tag.run_id AS ml_run_id,
                    value.proto_value.mlapp_run_event.model_file_state.model_registry_key
                FROM kinesisstats.osdmlappruneventinwardobstruction
                WHERE {PARTITION_FILTERS}
                    AND value.proto_value.mlapp_run_event.run_event_type = 1 -- corresponding to START
            ) ml ON f.run_id = ml.ml_run_id
            WHERE harsh_accel_type = 25 -- haDriverObstructionPolicy aka inward obstruction
        ),
        e AS (
            SELECT f.*,
                ml_run_id,
                model_registry_key
            FROM combined_devices f
            LEFT JOIN (
                SELECT
                    value.proto_value.mlapp_run_event.run_tag.run_id AS ml_run_id,
                    value.proto_value.mlapp_run_event.model_file_state.model_registry_key
                FROM kinesisstats.osdmlappruneventoutwardsafety
                WHERE {PARTITION_FILTERS}
                    AND value.proto_value.mlapp_run_event.run_event_type = 1 -- corresponding to START
            ) ml ON f.run_id = ml.ml_run_id
            WHERE harsh_accel_type = 32 -- haLaneDeparture aka outward safety aka ldw
                -- CM devices after Brigid will use FDv2
                OR (harsh_accel_type = 14 AND f.product_id NOT IN (155, 167))
        ),
        g AS (
            SELECT f.*,
                ml_run_id,
                model_registry_key
            FROM combined_devices f
            LEFT JOIN (
                SELECT
                    value.proto_value.mlapp_run_event.run_tag.run_id AS ml_run_id,
                    value.proto_value.mlapp_run_event.model_file_state.model_registry_key
                FROM kinesisstats.osdmlappruneventdrowsiness
                WHERE {PARTITION_FILTERS}
                    AND value.proto_value.mlapp_run_event.run_event_type = 1 -- corresponding to START
            ) ml ON f.run_id = ml.ml_run_id
            WHERE harsh_accel_type = 33 -- haDrowsinessDetection
        ),
        -- All other event types that don't yet have an mlapp
        h AS (
            SELECT f.*,
                NULL AS ml_run_id,
                NULL AS model_registry_key
            FROM combined_devices f
            WHERE harsh_accel_type NOT IN (12,13,14,19,23,25,32,33) -- all other types
        )
        SELECT * FROM a
        UNION
        SELECT * FROM b
        UNION
        SELECT * FROM c
        UNION
        SELECT * FROM d
        UNION
        SELECT * FROM e
        UNION
        SELECT * FROM g
        UNION
        SELECT * FROM h
    )
    SELECT *, CAST(CURRENT_DATE() AS string) AS updated_date FROM join_mlapps
--endsql"""

fct_firmware_events_assets = build_assets_from_sql(
    name="fct_firmware_events",
    step_launcher="databricks_pyspark_step_launcher_firmware_events",
    schema=fct_firmware_events_schema,
    description=build_table_description(
        table_desc="""Logs events triggered by the VG or CM firmware. This table can be used to determine whether or not an event triggered in the firmware ultimately lands in the backend (in_backend field tracks presence of the event in safetydb.safety_events). This table can also be used to correlate an event instance with the corresponding ML model that triggered this event (ml_run_id and model_registry_key fields).""",
        row_meaning="""An event, that is triggered by a VG or CM when either detect a driver behavior that we want to surface to the user as important. Sent up both for accelerometer-detected events (e.g. crash) and AI-detected events (e.g. seatbelt).""",
        table_type=TableType.TRANSACTIONAL_FACT,
        freshness_slo_updated_by="9am PST",
    ),
    sql_query=fct_firmware_events_query,
    primary_keys=["date", "org_id", "device_id", "event_ms"],
    upstreams=[
        AssetKey([databases["database_silver_dev"], "dq_stg_osdaccelerometer"]),
        AssetKey([Database.DATAMODEL_CORE, "dq_dim_devices"]),
        AssetKey(["safetydb_shards", "safety_events"]),
    ],
    regions=get_all_regions(),
    group_name=pipeline_group_name,
    database=databases["database_gold_dev"],
    databases=databases,
    write_mode=WarehouseWriteMode.merge,
    partitions_def=daily_partition_def,
    backfill_policy=BACKFILL_POLICY,
    retry_policy=fct_firmware_events_retry_policy,
)

fct_firmware_events_us = fct_firmware_events_assets[AWSRegions.US_WEST_2.value]
fct_firmware_events_eu = fct_firmware_events_assets[AWSRegions.EU_WEST_1.value]
fct_firmware_events_ca = fct_firmware_events_assets[AWSRegions.CA_CENTRAL_1.value]

dqs["fct_firmware_events"].append(
    PrimaryKeyDQCheck(
        name="dq_pk_fct_firmware_events",
        table="fct_firmware_events",
        primary_keys=["date", "org_id", "device_id", "event_ms"],
        blocking=True,
        database=databases["database_gold_dev"],
    )
)

dqs["fct_firmware_events"].append(
    NonEmptyDQCheck(
        name="dq_empty_fct_firmware_events",
        table="fct_firmware_events",
        blocking=True,
        database=databases["database_gold_dev"],
    )
)

dqs["fct_firmware_events"].append(
    JoinableDQCheck(
        name="dq_join_fct_firmware_events",
        database=databases["database_silver_dev"],
        database_2=databases["database_gold_dev"],
        input_asset_1="stg_osdaccelerometer",  # stg_osdaccelerometer is 1:1 with kinesisstats.osdaccelerometer
        input_asset_2="fct_firmware_events",
        join_keys=[
            ("date", "date"),
            ("org_id", "org_id"),
            ("device_id", "device_id"),
            ("event_ms", "event_ms"),
        ],
        blocking=False,
        # A small number of osdaccelerometer events may not have corresponding events in fct_firmware_events
        # due to: 1. only VG and CM devices are included; 2. some events don't have corresponding device data
        # in dim_devices.
        null_right_table_rows_ratio=0.001,
    )
)


dq_assets = dqs.generate()
