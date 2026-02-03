from dagster import (
    AssetKey,
    AutoMaterializePolicy,
    AutoMaterializeRule,
    DailyPartitionsDefinition,
)

from ...common.constants import (
    SLACK_ALERTS_CHANNEL_DATA_ENGINEERING,
    device_id_default_description,
    org_id_default_description,
)
from ...common.lifetime import Lifetime, generate_lifetime_schema
from ...common.utils import (
    AWSRegions,
    Database,
    DQGroup,
    NonEmptyDQCheck,
    PrimaryKeyDQCheck,
    SQLDQCheck,
    TableType,
    TrendDQCheck,
    WarehouseWriteMode,
    apply_db_overrides,
    build_assets_from_sql,
    build_table_description,
    get_all_regions,
)

key_prefix = "datamodel_core"

databases = {
    "database_bronze": Database.DATAMODEL_CORE_BRONZE,
    "database_silver": Database.DATAMODEL_CORE_SILVER,
    "database_gold": Database.DATAMODEL_CORE,
    "database_telematics_silver": Database.DATAMODEL_TELEMATICS_SILVER,
    "database_telematics_gold": Database.DATAMODEL_TELEMATICS,
}

database_dev_overrides = {
    "database_bronze_dev": Database.DATAMODEL_DEV,
    "database_silver_dev": Database.DATAMODEL_DEV,
    "database_gold_dev": Database.DATAMODEL_DEV,
    "database_telematics_silver_dev": Database.DATAPLATFORM_DEV,
    "database_telematics_gold_dev": Database.DATAMODEL_DEV,
}

databases = apply_db_overrides(databases, database_dev_overrides)

daily_partition_def = DailyPartitionsDefinition(start_date="2015-06-01")
pipeline_group_name = "active_devices"

dqs = DQGroup(
    group_name=pipeline_group_name,
    partition_def=daily_partition_def,
    slack_alerts_channel=SLACK_ALERTS_CHANNEL_DATA_ENGINEERING,
    regions=get_all_regions(),
)

stg_device_activity_daily_schema = [
    {
        "name": "date",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "The field which partitions the table, in 'YYYY-mm-dd' format."
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
        "name": "device_type",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Attribute defining the device type (e.g. VG, CM, AG, ...)"
        },
    },
    {
        "name": "product_name",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "The product name of the device (e.g. AHD4, AIM4, VG34, etc.)"
        },
    },
    {
        "name": "has_heartbeat",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Has the device triggered a heartbeat on a day. For AT (Asset Trackers), this field represents whether the device has a valid location rather than a heartbeat."
        },
    },
    {
        "name": "trip_count",
        "type": "long",
        "nullable": False,
        "metadata": {"comment": "Daily trips connected to the device"},
    },
    {
        "name": "has_dashcam_target_state",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Has the device triggered a osddashcamtargetstate signal on a day, with a non-null recording_reason. This measures whether or not the Recording State Manager (RSM) sent a signal to start recording on a given day."
        },
    },
    {
        "name": "is_active",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "If VG or CM, requires a daily trip and heartbeat to be considered active. If AHD4/AIM4, requires a non-null recording_reason. If AT, requires a valid location and an assigned name. For other devices, requires ONLY a heartbeat."
        },
    },
]

# TODO: Why does heartbeats exclude org_id 0 and 1? Other signals don't filter these out.
# Additionally, if we did want to filter out internal orgs, we should be using the
# internal_type field in dim_organizations.
stg_device_activity_daily_query = """
--sql

WITH
    dim_device AS (
        SELECT
            org_id,
            device_id,
            device_type,
            device_name,
            product_name,
            associated_devices['vg_device_id'] AS vg_device_id
        FROM datamodel_core.dim_devices
        WHERE date = GREATEST('{DATEID}', (SELECT MIN(date) FROM datamodel_core.dim_devices))
    ),
    heartbeats AS (
        SELECT
            org_id,
            object_id AS device_id
        FROM kinesisstats_history.osdhubserverdeviceheartbeat
        WHERE date = '{DATEID}'
            AND NOT value.is_end
            AND NOT value.is_databreak
            AND org_id NOT IN (0, 1)
        GROUP BY 1, 2
    ),
    at11_locations AS (
        SELECT
            org_id,
            device_id
        FROM kinesisstats_history.location
        WHERE date = '{DATEID}'
            AND device_id IN (SELECT device_id FROM dim_device WHERE device_type = 'AT - Asset Tracker')
    ),
    -- The camera connector AHD4/AIM4 can record outside of trips.
    -- The updates that enabled AHD4/AIM4 to record outside of trips also created the signal
    -- osddashcamtargetstate, which will have a non-null recording_reason when the
    -- Recording State Manager (RSM) sends a signal that the device should be recording.
    dashcam_target_states AS (
        SELECT
            org_id,
            object_id as device_id
        FROM kinesisstats_history.osddashcamtargetstate
        WHERE date = '{DATEID}'
            AND NOT value.is_end
            AND NOT value.is_databreak
            AND value.proto_value.dashcam_target_state.recording_reason IS NOT NULL
        GROUP BY 1, 2
    ),
    trips AS (
        SELECT
            org_id,
            device_id,
            COUNT(*) AS trip_count
        FROM datamodel_telematics.fct_trips
        WHERE date = '{DATEID}'
        GROUP BY 1, 2
    ),
    heartbeat_join AS (
        SELECT
            COALESCE(dim_device.org_id, heartbeats.org_id) AS org_id,
            COALESCE(dim_device.device_id, heartbeats.device_id) AS device_id,
            COALESCE(dim_device.device_type, 'unknown') AS device_type,
            COALESCE(dim_device.product_name, 'unknown') AS product_name,
            heartbeats.device_id IS NOT NULL AS has_heartbeat,
            device_name IS NOT NULL AS is_named,
            COALESCE(dim_device.vg_device_id, -1) AS vg_device_id
        FROM dim_device
        FULL OUTER JOIN heartbeats
            ON dim_device.org_id = heartbeats.org_id
            AND dim_device.device_id = heartbeats.device_id
    ),
    heartbeat_and_trips AS (
        SELECT
            COALESCE(heartbeat_join.org_id, trips.org_id) AS org_id,
            COALESCE(heartbeat_join.device_id, trips.device_id) AS device_id,
            COALESCE(heartbeat_join.device_type, 'unknown') AS device_type,
            COALESCE(heartbeat_join.product_name, 'unknown') AS product_name,
            -- AT11 devices don't have cellular connectivity and thus no heartbeat, in the current
            -- sense of the term.
            CASE
                WHEN COALESCE(heartbeat_join.device_type, 'unknown') = 'AT - Asset Tracker'
                    THEN (heartbeat_join.org_id, heartbeat_join.device_id) IN (SELECT org_id, device_id FROM at11_locations)
                ELSE COALESCE(has_heartbeat, False)
            END AS has_heartbeat,
            COALESCE(trips.trip_count, 0) AS trip_count,
            COALESCE(is_named, False) AS is_named
        FROM heartbeat_join
        FULL OUTER JOIN trips
            ON heartbeat_join.org_id = trips.org_id
            AND CASE WHEN (device_type = 'CM - AI Dash Cam' OR device_type = 'Camera Connector') THEN vg_device_id
                ELSE heartbeat_join.device_id END = trips.device_id
    ),
    heartbeats_trips_and_dashcam_target_states AS (
        SELECT
            COALESCE(heartbeat_and_trips.org_id, dashcam_target_states.org_id) AS org_id,
            COALESCE(heartbeat_and_trips.device_id, dashcam_target_states.device_id) AS device_id,
            COALESCE(heartbeat_and_trips.device_type, 'unknown') AS device_type,
            COALESCE(heartbeat_and_trips.product_name, 'unknown') AS product_name,
            COALESCE(heartbeat_and_trips.has_heartbeat, False) AS has_heartbeat,
            COALESCE(heartbeat_and_trips.trip_count, 0) AS trip_count,
            COALESCE(heartbeat_and_trips.is_named, False) AS is_named,
            dashcam_target_states.device_id IS NOT NULL AS has_dashcam_target_state
        FROM heartbeat_and_trips
        FULL OUTER JOIN dashcam_target_states
            ON heartbeat_and_trips.org_id = dashcam_target_states.org_id
            AND heartbeat_and_trips.device_id = dashcam_target_states.device_id
    ),
    final AS (
        SELECT
            org_id,
            device_id,
            device_type,
            product_name,
            has_heartbeat,
            trip_count,
            has_dashcam_target_state,
            -- For VGs and CMs, the device is considered active if it has both a heartbeat and at least one trip.
            -- For AHD4/AIM4, the device is considered active if it has a non-null recording_reason.
            -- For ATs (Asset Trackers), the device is active if it has a valid location and is named.
            -- For other devices, the device is active if it has a heartbeat.
            -- The query uses full outer joins to combine devices, trips, and heartbeats, ensuring all relevant activity signals are included.
            CASE
                WHEN device_type IN ('VG - Vehicle Gateway', 'CM - AI Dash Cam') THEN (has_heartbeat AND trip_count > 0)
                WHEN (product_name = 'AHD4' OR product_name = 'AIM4') THEN has_dashcam_target_state
                WHEN device_type = 'AT - Asset Tracker' THEN has_heartbeat AND is_named
                ELSE has_heartbeat
            END AS is_active
        FROM heartbeats_trips_and_dashcam_target_states
    )
SELECT
    '{DATEID}' AS date,
    org_id,
    device_id,
    device_type,
    product_name,
    has_heartbeat,
    trip_count,
    has_dashcam_target_state,
    is_active
FROM final

--endsql
"""


lifetime_device_activity_schema = [
    {
        "name": "org_id",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "Unique identifier for organizations. An account has multiple organizations under it. Joins to datamodel_core.dim_organizations"
        },
    },
    {
        "name": "device_id",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "Unique identifier for devices. Joins to datamodel_core.dim_devices"
        },
    },
    {
        "name": "device_type",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Attribute defining the device type (e.g. VG, CM, AG, ...)"
        },
    },
]

lifetime_device_activity_schema.extend(generate_lifetime_schema())


lifetime_device_online_schema = [
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
        "name": "device_type",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Attribute defining the device type (e.g. VG, CM, AG, ...)"
        },
    },
]

lifetime_device_online_schema.extend(generate_lifetime_schema())


assets = build_assets_from_sql(
    name="stg_device_activity_daily",
    schema=stg_device_activity_daily_schema,
    sql_query=stg_device_activity_daily_query,
    primary_keys=["date", "org_id", "device_id"],
    upstreams=[
        AssetKey([databases["database_gold_dev"], "dim_devices"]),
        AssetKey([databases["database_telematics_gold_dev"], "fct_trips"]),
    ],
    regions=get_all_regions(),
    group_name=pipeline_group_name,
    database=databases["database_silver_dev"],
    databases=databases,
    write_mode=WarehouseWriteMode.overwrite,
    partitions_def=daily_partition_def,
    auto_materialize_policy=AutoMaterializePolicy.eager().with_rules(
        AutoMaterializeRule.skip_on_not_all_parents_updated()
    ),
)
stg1 = assets[AWSRegions.US_WEST_2.value]
stg2 = assets[AWSRegions.EU_WEST_1.value]
stg3 = assets[AWSRegions.CA_CENTRAL_1.value]


dqs["stg_device_activity_daily"].append(
    NonEmptyDQCheck(
        name="dq_empty_stg_device_activity_daily",
        table="stg_device_activity_daily",
        blocking=True,
        database=databases["database_silver_dev"],
    )
)

dqs["stg_device_activity_daily"].append(
    SQLDQCheck(
        name="dq_active_vgs_stg_device_activity_daily",
        database=databases["database_silver_dev"],
        sql_query="""SELECT MAX(CASE WHEN is_active THEN 1 ELSE 0 END) AS observed_value
                     FROM {database_silver_dev}.stg_device_activity_daily
                     WHERE device_type = 'VG - Vehicle Gateway'
                     """.format(
            DATEID="{DATEID}", database_silver_dev=databases["database_silver_dev"]
        ),
        expected_value=1,
        blocking=True,
    )
)

lt_active = Lifetime(
    name="lifetime_device_activity",
    description=build_table_description(
        table_desc="""This table provides an historical look at active devices for the entire lifetime of a device.
A device is considered active based on the following logic:

- For VG (Vehicle Gateway) and CM (Camera Module) device types, the device is considered active if it has both a trip and a heartbeat on the given date.

- For AHD4 (AI Dash Cam) and AIM4 (AI Motion Camera) device types, the device is considered active if it has a non-null recording_reason on the given date.

- For AT (Asset Tracker) device types, the device is considered active if it has a valid location and an assigned device name.

- For all other device types, the device is considered active if it has a heartbeat on the given date.""",
        row_meaning="""Each row represents the entire active device history of a device starting on the devices first activity date""",
        related_table_info={
            "datamodel_core.lifetime_device_online": """Similar table to lifetime_device_activity, however looks at online devices rather than active devices"""
        },
        table_type=TableType.LIFETIME,
        freshness_slo_updated_by="9am PST",
    ),
    schema=lifetime_device_activity_schema,
    primary_keys=["org_id", "device_id"],
    metric="CASE WHEN is_active THEN 1 ELSE 0 END",
    attribute_columns=["device_type"],
    upstreams=[
        AssetKey([databases["database_silver_dev"], "dq_stg_device_activity_daily"])
    ],
    group_name=pipeline_group_name,
    source_database=databases["database_silver"],
    source_table="stg_device_activity_daily",
    source_filter="is_active",
    database=databases["database_gold_dev"],
    databases=databases,
    partitions_def=daily_partition_def,
)


assets2 = lt_active.generate_asset()
lt_active1 = assets2[AWSRegions.US_WEST_2.value]
lt_active2 = assets2[AWSRegions.EU_WEST_1.value]
lt_active3 = assets2[AWSRegions.CA_CENTRAL_1.value]

dqs["lifetime_device_activity"].append(
    PrimaryKeyDQCheck(
        name="dq_pk_lifetime_device_activity",
        table="lifetime_device_activity",
        database=databases["database_gold_dev"],
        primary_keys=["date", "org_id", "device_id"],
        blocking=True,
    )
)

dqs["lifetime_device_activity"].append(
    NonEmptyDQCheck(
        name="dq_non_empty_lifetime_device_activity",
        database=databases["database_gold_dev"],
        table="lifetime_device_activity",
        blocking=True,
    )
)

dqs["lifetime_device_activity"].append(
    TrendDQCheck(
        name="lifetime_device_activity_check_day_over_day_row_count",
        database=databases["database_gold_dev"],
        table="lifetime_device_activity",
        tolerance=0.04,
        blocking=True,
    )
)

lt_online = Lifetime(
    name="lifetime_device_online",
    description=build_table_description(
        table_desc="""This table provides an historical look at online devices for the entire lifetime of a device.
A device is considered online if the device has a heartbeat on a given date.""",
        row_meaning="""Each row represents the entire online device history of a device starting on the devices first online date""",
        related_table_info={
            "datamodel_core.lifetime_device_activity": """Similar table to lifetime_device_activity, however looks at active devices rather than online devices"""
        },
        table_type=TableType.LIFETIME,
        freshness_slo_updated_by="9am PST",
    ),
    schema=lifetime_device_online_schema,
    primary_keys=["org_id", "device_id"],
    metric="CAST(CASE WHEN has_heartbeat THEN 1 ELSE 0 END AS LONG)",
    attribute_columns=["device_type"],
    upstreams=[
        AssetKey([databases["database_silver_dev"], "dq_stg_device_activity_daily"])
    ],
    group_name=pipeline_group_name,
    source_database=databases["database_silver"],
    source_table="stg_device_activity_daily",
    source_filter="has_heartbeat",
    database=databases["database_gold_dev"],
    databases=databases,
    partitions_def=daily_partition_def,
)

assets3 = lt_online.generate_asset()
lt_online1 = assets3[AWSRegions.US_WEST_2.value]
lt_online2 = assets3[AWSRegions.EU_WEST_1.value]
lt_online3 = assets3[AWSRegions.CA_CENTRAL_1.value]

dqs["lifetime_device_online"].append(
    PrimaryKeyDQCheck(
        name="dq_pk_lifetime_device_online",
        table="lifetime_device_online",
        database=databases["database_gold_dev"],
        primary_keys=["date", "org_id", "device_id"],
        blocking=True,
    )
)

dqs["lifetime_device_online"].append(
    NonEmptyDQCheck(
        name="dq_non_empty_lifetime_device_online",
        database=databases["database_gold_dev"],
        table="lifetime_device_online",
        blocking=True,
    )
)

dqs["lifetime_device_online"].append(
    TrendDQCheck(
        name="lifetime_device_online_check_day_over_day_row_count",
        database=databases["database_gold_dev"],
        table="lifetime_device_online",
        tolerance=0.04,
        blocking=True,
    )
)

dq_assets = dqs.generate()
