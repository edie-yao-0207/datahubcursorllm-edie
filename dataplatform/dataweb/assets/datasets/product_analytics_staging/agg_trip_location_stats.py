from dagster import AssetExecutionContext, DailyPartitionsDefinition
from dataweb import (
    NonEmptyDQCheck,
    NonNullDQCheck,
    PrimaryKeyDQCheck,
    TrendDQCheck,
    table,
)
from dataweb.userpkgs.constants import (
    DATAENGINEERING,
    FRESHNESS_SLO_9AM_PST,
    MEMORY_OPTIMIZED_INSTANCE_POOL_KEY,
    AWSRegion,
    ColumnDescription,
    Database,
    TableType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.query import create_run_config_overrides
from dataweb.userpkgs.utils import (
    build_table_description,
    partition_key_ranges_from_context,
)

PRIMARY_KEYS = ["date", "org_id", "device_id", "trip_start_time_ms", "trip_end_time_ms", "zip_code", "country"]

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
        "metadata": {
            "comment": ColumnDescription.ORG_ID,
        },
    },
    {
        "name": "device_id",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": ColumnDescription.DEVICE_ID,
        },
    },
    {
        "name": "driver_id",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": ColumnDescription.DRIVER_ID,
        },
    },
    {
        "name": "trip_start_time_ms",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Trip start time in epoch milliseconds."
        },
    },
    {
        "name": "trip_end_time_ms",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Trip end time in epoch milliseconds."
        },
    },
    {
        "name": "state",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "The state or province corresponding to the latitude / longitude of this device as determined by reverse geolocation"
        },
    },
    {
        "name": "city",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "The city corresponding to the latitude / longitude of this device as determined by reverse geolocation"
        },
    },
    {
        "name": "zip_code",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "The postcode corresponding to the latitude / longitude of this device as determined by reverse geolocation"
        },
    },
    {
        "name": "country",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "the country corresponding to the latitude / longitude of this device as determined by reverse geolocation"
        },
    },
    {
        "name": "total",
        "type": "long",
        "nullable": False,
        "metadata": {"comment": "Count of locations."},
    },
]

QUERY = """
WITH trips AS (
SELECT date,
       device_id,
       org_id,
       driver_id,
       start_time_ms,
       end_time_ms
FROM datamodel_telematics.fct_trips
WHERE date BETWEEN '{PARTITION_START}' AND '{PARTITION_END}')

SELECT loc.date,
       loc.org_id,
       loc.device_id,
       trips.driver_id,
       trips.start_time_ms AS trip_start_time_ms,
       trips.end_time_ms AS trip_end_time_ms,
       TRANSLATE(LOWER(regexp_replace(replace(loc.value.revgeo_city, '-', ' '), '[0-9]', '') ),'àáâãäåèéêëìíîïòóôõöùúûüñç','aaaaaaeeeeiiiiooooouuuunc') AS city,
       UPPER(loc.value.revgeo_state) AS state,
       CASE WHEN loc.value.revgeo_country = 'US'
            THEN split(loc.value.revgeo_postcode, '-')[0]
            ELSE  loc.value.revgeo_postcode END AS zip_code,
       UPPER(loc.value.revgeo_country) AS country,
       COUNT(*) AS total
FROM kinesisstats_history.location loc
INNER JOIN trips
  ON trips.org_id = loc.org_id
  AND trips.date = loc.date
  AND trips.device_id = loc.device_id
  AND loc.time BETWEEN trips.start_time_ms AND trips.end_time_ms
WHERE loc.date BETWEEN '{PARTITION_START}' AND '{PARTITION_END}'
AND loc.value.revgeo_postcode IS NOT NULL
AND loc.value.gps_speed_meters_per_second > 0
GROUP BY ALL
"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="""Breakdown of locations by trip to support primary driving enviroment calculation.""",
        row_meaning="""Each row represents the count of visits for a given device at a given zip/country""",
        related_table_info={},
        table_type=TableType.AGG,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=[AWSRegion.US_WEST_2],
    owners=[DATAENGINEERING],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DailyPartitionsDefinition(start_date="2023-09-01"),
    write_mode=WarehouseWriteMode.OVERWRITE,
    run_config_overrides=create_run_config_overrides(
        instance_pool_type=MEMORY_OPTIMIZED_INSTANCE_POOL_KEY,
        max_workers=8,
    ),
    dq_checks=[
        NonEmptyDQCheck(name="dq_non_empty_agg_trip_location_stats"),
        PrimaryKeyDQCheck(name="dq_pk_agg_trip_location_stats", primary_keys=PRIMARY_KEYS),
        NonNullDQCheck(
            name="dq_non_null_agg_trip_location_stats",
            non_null_columns=["device_id", "total", "date", "zip_code", "org_id", "trip_start_time_ms", "trip_end_time_ms"],
        ),
    ],
    backfill_batch_size=1,
    upstreams=["kinesisstats_history.location", "datamodel_telematics.fct_trips"],
)
def agg_trip_location_stats(context: AssetExecutionContext) -> str:
    context.log.info("Updating agg_trip_location_stats")
    partition_keys = partition_key_ranges_from_context(context)[0]
    PARTITION_START = partition_keys[0]
    PARTITION_END = partition_keys[-1]
    query = QUERY.format(
        PARTITION_START=PARTITION_START,
        PARTITION_END=PARTITION_END,
    )
    context.log.info(f"{query}")
    return query
