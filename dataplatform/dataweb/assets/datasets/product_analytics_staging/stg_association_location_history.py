from dagster import AssetExecutionContext, DailyPartitionsDefinition
from dataweb import NonEmptyDQCheck, NonNullDQCheck, PrimaryKeyDQCheck, table
from dataweb.userpkgs.constants import (
    ALL_COMPUTE_REGIONS,
    DATAENGINEERING,
    FRESHNESS_SLO_9AM_PST,
    ColumnDescription,
    Database,
    InstanceType,
    TableType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.query import create_run_config_overrides
from dataweb.userpkgs.utils import (
    build_table_description,
    get_run_env,
    partition_key_ranges_from_context,
)
from pyspark.sql import SparkSession

PRIMARY_KEYS = [
    "date",
    "org_id",
    "central_device_id",
    "peripheral_device_id",
    "start_ms",
    "end_ms",
    "loc_update_ms",
]

SCHEMA = [
    {
        "name": "date",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": ColumnDescription.DATE,
        },
    },
    {
        "name": "org_id",
        "type": "long",
        "nullable": False,
        "metadata": {"comment": "The Internal ID for the customer`s Samsara org."},
    },
    {
        "name": "peripheral_device_id",
        "type": "long",
        "nullable": False,
        "metadata": {"comment": "Device ID of the peripheral device"},
    },
    {
        "name": "peripheral_product_name",
        "type": "string",
        "nullable": False,
        "metadata": {"comment": "Product name of the peripheral device"},
    },
    {
        "name": "central_device_id",
        "type": "long",
        "nullable": False,
        "metadata": {"comment": "Device ID of the central device"},
    },
    {
        "name": "central_product_name",
        "type": "string",
        "nullable": False,
        "metadata": {"comment": "Product name of the central device"},
    },
    {
        "name": "start_ms",
        "type": "long",
        "nullable": False,
        "metadata": {"comment": "Start time of association"},
    },
    {
        "name": "end_ms",
        "type": "long",
        "nullable": False,
        "metadata": {"comment": "End time of association"},
    },
    {
        "name": "separation_distance_meters",
        "type": "double",
        "nullable": False,
        "metadata": {"comment": "Separation distance between devices"},
    },
    {
        "name": "loc_update_ms",
        "type": "long",
        "nullable": False,
        "metadata": {"comment": "Time at which location was updated"},
    },
    {
        "name": "loc_time_to_end_minutes",
        "type": "double",
        "nullable": False,
        "metadata": {"comment": "Time between location update and end of association"},
    },
    {
        "name": "central_latitude",
        "type": "double",
        "nullable": False,
        "metadata": {"comment": "Latitute of central device"},
    },
    {
        "name": "central_longitude",
        "type": "double",
        "nullable": False,
        "metadata": {"comment": "Longitude of central device"},
    },
    {
        "name": "peripheral_latitude",
        "type": "double",
        "nullable": False,
        "metadata": {"comment": "Latitute of peripheral device"},
    },
    {
        "name": "peripheral_longitude",
        "type": "double",
        "nullable": False,
        "metadata": {"comment": "Longitude of peripheral device"},
    },
    {
        "name": "central_last_location_ms",
        "type": "long",
        "nullable": False,
        "metadata": {"comment": "Last observed time of central device"},
    },
    {
        "name": "peripheral_last_location_ms",
        "type": "long",
        "nullable": False,
        "metadata": {"comment": "Last observed time of peripheral device"},
    },
    {
        "name": "central_location_age_sec",
        "type": "double",
        "nullable": False,
        "metadata": {"comment": "Age of central device location based on loc_update_ms"},
    },
    {
        "name": "peripheral_location_age_sec",
        "type": "double",
        "nullable": False,
        "metadata": {"comment": "Age of peripheral device location based on loc_update_ms"},
    },
    {
        "name": "distance_meters",
        "type": "double",
        "nullable": True,
        "metadata": {"comment": "Distance between devices"},
    },
]

QUERY = """
    WITH base_assoc AS (
        SELECT
            date,
            org_id,
            peripheral_device_id,
            peripheral_product_name,
            central_device_id,
            central_product_name,
            start_ms,
            end_ms,
            separation_distance_meters
        FROM {source}.stg_association_stat
        WHERE date BETWEEN '{PARTITION_START}' AND '{PARTITION_END}'
    ),
    central_loc AS (
        SELECT
            ba.date,
            ba.org_id,
            ba.peripheral_device_id,
            ba.peripheral_product_name,
            ba.central_device_id,
            ba.central_product_name,
            ba.start_ms,
            ba.end_ms,
            ba.separation_distance_meters,
            loc.time AS loc_update_ms,
            CAST(NULL AS BIGINT) AS peripheral_time,
            CAST(NULL AS DOUBLE) AS peripheral_lat,
            CAST(NULL AS DOUBLE) AS peripheral_lon,
            loc.time AS central_time,
            loc.value.latitude AS central_lat,
            loc.value.longitude AS central_lon
        FROM base_assoc ba
        LEFT JOIN kinesisstats_history.location loc
            ON ba.central_device_id = loc.device_id
            AND loc.time BETWEEN greatest(ba.end_ms - (1000 * 60 * 60 * 3), (ba.start_ms - (1000 * 60 * 20))) AND ba.end_ms + (1000 * 60 * 15) -- locations should be logged within 3 hours based on https://samsara-dev-us-west-2.cloud.databricks.com/editor/notebooks/4462036561361814?o=5924096274798303#command/7863669101264125
            AND loc.date BETWEEN DATE_SUB(ba.date, 1) AND DATE_ADD(ba.date, 1)
            AND loc.date BETWEEN DATE_SUB('{PARTITION_START}', 1) AND DATE_ADD('{PARTITION_END}', 1)
    ),
    peripheral_loc AS (
        SELECT
            ba.date,
            ba.org_id,
            ba.peripheral_device_id,
            ba.peripheral_product_name,
            ba.central_device_id,
            ba.central_product_name,
            ba.start_ms,
            ba.end_ms,
            ba.separation_distance_meters,
            loc.time AS loc_update_ms,
            loc.time AS peripheral_time,
            loc.value.latitude AS peripheral_lat,
            loc.value.longitude AS peripheral_lon,
            CAST(NULL AS BIGINT) AS central_time,
            CAST(NULL AS DOUBLE) AS central_lat,
            CAST(NULL AS DOUBLE) AS central_lon
        FROM base_assoc ba
        LEFT JOIN kinesisstats_history.location loc
            ON ba.peripheral_device_id = loc.device_id
            AND loc.time BETWEEN greatest(ba.end_ms - (1000 * 60 * 60 * 3), (ba.start_ms-(1000 *60 * 20))) AND ba.end_ms + (1000 * 60 * 15) -- locations should be logged within 3 hours based on https://samsara-dev-us-west-2.cloud.databricks.com/editor/notebooks/4462036561361814?o=5924096274798303#command/7863669101264125
            AND loc.date BETWEEN DATE_SUB(ba.date, 1) AND DATE_ADD(ba.date, 1)
            AND loc.date BETWEEN DATE_SUB('{PARTITION_START}', 1) AND DATE_ADD('{PARTITION_END}', 1)
    ),
    all_locs AS (
        SELECT * FROM central_loc

        UNION

        SELECT * FROM peripheral_loc
    ),
    collapsed_locs AS (
        SELECT
            date,
            org_id,
            peripheral_device_id,
            peripheral_product_name,
            central_device_id,
            central_product_name,
            start_ms,
            end_ms,
            separation_distance_meters,
            loc_update_ms,
            -- if a peripheral update exists at this loc_update_ms, keep it
            MAX(peripheral_time) AS peripheral_time,
            MAX(peripheral_lat)  AS peripheral_lat,
            MAX(peripheral_lon)  AS peripheral_lon,
            -- if a central update exists at this loc_update_ms, keep it
            MAX(central_time) AS central_time,
            MAX(central_lat)  AS central_lat,
            MAX(central_lon)  AS central_lon
        FROM all_locs
        GROUP BY
            date, org_id, peripheral_device_id, peripheral_product_name,
            central_device_id, central_product_name,
            start_ms, end_ms, separation_distance_meters, loc_update_ms
    ),
    filled AS (
        SELECT
            date,
            org_id,
            peripheral_device_id,
            peripheral_product_name,
            central_device_id,
            central_product_name,
            start_ms,
            end_ms,
            separation_distance_meters,
            loc_update_ms,
            -- Forward-fill peripheral coordinates and their last-observed timestamp
            last_value(peripheral_lat,  TRUE) OVER (
                PARTITION BY date, org_id, peripheral_device_id, central_device_id, start_ms, end_ms
                ORDER BY loc_update_ms
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS peripheral_latitude,
            last_value(peripheral_lon,  TRUE) OVER (
                PARTITION BY date, org_id, peripheral_device_id, central_device_id, start_ms, end_ms
                ORDER BY loc_update_ms
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS peripheral_longitude,
            last_value(peripheral_time, TRUE) OVER (   -- <-- peripheral last known fix time
                PARTITION BY date, org_id, peripheral_device_id, central_device_id, start_ms, end_ms
                ORDER BY loc_update_ms
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS peripheral_last_location_ms,
            -- Forward-fill central coordinates and their last-observed timestamp
            last_value(central_lat,  TRUE) OVER (
                PARTITION BY date, org_id, peripheral_device_id, central_device_id, start_ms, end_ms
                ORDER BY loc_update_ms
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS central_latitude,
            last_value(central_lon,  TRUE) OVER (
                PARTITION BY date, org_id, peripheral_device_id, central_device_id, start_ms, end_ms
                ORDER BY loc_update_ms
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS central_longitude,
            last_value(central_time, TRUE) OVER (   -- <-- central last known fix time
                PARTITION BY date, org_id, peripheral_device_id, central_device_id, start_ms, end_ms
                ORDER BY loc_update_ms
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS central_last_location_ms
            FROM collapsed_locs
    )
    SELECT
        date,
        org_id,
        peripheral_device_id,
        peripheral_product_name,
        central_device_id,
        central_product_name,
        start_ms,
        end_ms,
        separation_distance_meters,
        loc_update_ms,
        (loc_update_ms - end_ms) / (1000 * 60) AS loc_time_to_end_minutes,
        -- forward-filled coordinates
        peripheral_latitude,
        peripheral_longitude,
        central_latitude,
        central_longitude,
        -- NEW: timestamps of the last known fixes for each device
        peripheral_last_location_ms,
        central_last_location_ms,
        -- Optional: staleness diagnostics (how old the fixes are at this timeline instant)
        (loc_update_ms - peripheral_last_location_ms) / 1000 AS peripheral_location_age_sec,
        (loc_update_ms - central_last_location_ms) / 1000 AS central_location_age_sec,
        -- distance only when both sides exist
        CASE
        WHEN peripheral_latitude IS NOT NULL AND peripheral_longitude IS NOT NULL
        AND central_latitude IS NOT NULL AND central_longitude IS NOT NULL THEN
        2 * 6371000 * ASIN(SQRT(
            POWER(SIN(RADIANS((central_latitude - peripheral_latitude) / 2)), 2) +
            COS(RADIANS(peripheral_latitude)) * COS(RADIANS(central_latitude)) *
            POWER(SIN(RADIANS((central_longitude - peripheral_longitude) / 2)), 2)
        ))
        ELSE NULL
        END AS distance_meters
    FROM filled
"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="""Location history of peripheral/central devices near the end of associations, used for gauging association accuracy""",
        row_meaning="""Each row contains a location update for the central or peripheral that is used to determine how far the devices within the association are separated.""",
        related_table_info={},
        table_type=TableType.STAGING,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[DATAENGINEERING],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DailyPartitionsDefinition(start_date="2025-07-01"),
    write_mode=WarehouseWriteMode.OVERWRITE,
    backfill_batch_size=5,
    upstreams=[
        "product_analytics_staging.stg_association_stat",
        "kinesisstats_history.location",
    ],
    run_config_overrides=create_run_config_overrides(
        driver_instance_type=InstanceType.RD_FLEET_2XLARGE,
        worker_instance_type=InstanceType.RD_FLEET_4XLARGE,
        max_workers=8,
    ),
    dq_checks=[
        NonEmptyDQCheck(name="dq_non_empty_stg_association_location_history", block_before_write=False), #keeping as non-blocking for now until we get more data in CA
        NonNullDQCheck(name="dq_non_null_stg_association_location_history", non_null_columns=["date", "org_id", "central_device_id", "peripheral_device_id", "start_ms", "end_ms"], block_before_write=False),
        PrimaryKeyDQCheck(name="dq_pk_stg_association_location_history", primary_keys=PRIMARY_KEYS, block_before_write=False)
    ]
)
def stg_association_location_history(context: AssetExecutionContext) -> str:
    context.log.info("Updating stg_association_location_history")
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    partition_keys = partition_key_ranges_from_context(context)[0]
    PARTITION_START = partition_keys[0]
    PARTITION_END = partition_keys[-1]
    source = "product_analytics_staging"
    if get_run_env() == "dev":
        source = "datamodel_dev"
    query = QUERY.format(
        source=source,
        PARTITION_START=PARTITION_START,
        PARTITION_END=PARTITION_END,
    )
    context.log.info(f"{query}")
    return query
