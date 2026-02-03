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

PRIMARY_KEYS = ["date", "org_id", "device_id", "zip_code", "country"]

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
        "metadata": {"comment": "Count for given combo"},
    },
]

QUERY = """
    select date,
       org_id,
       device_id,
       TRANSLATE(LOWER(regexp_replace(replace(value.revgeo_city, '-', ' '), '[0-9]', '') ),'àáâãäåèéêëìíîïòóôõöùúûüñç','aaaaaaeeeeiiiiooooouuuunc') AS city,
       UPPER(value.revgeo_state) AS state,
       CASE WHEN value.revgeo_country = 'US'
            THEN split(value.revgeo_postcode, '-')[0]
            ELSE  value.revgeo_postcode END as zip_code,
       UPPER(value.revgeo_country) as country,
       count(*) as total
from kinesisstats_history.location
where date between '{PARTITION_START}' and '{PARTITION_END}'
and value.revgeo_postcode is not null
and value.gps_speed_meters_per_second > 0
group by all
"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="""Breakdown of which locations devices have visited over time""",
        row_meaning="""Each row represents the count of visits for a given device at a given zip/country""",
        related_table_info={},
        table_type=TableType.AGG,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=[AWSRegion.US_WEST_2, AWSRegion.CA_CENTRAL_1],
    owners=[DATAENGINEERING],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DailyPartitionsDefinition(start_date="2024-06-01"),
    write_mode=WarehouseWriteMode.OVERWRITE,
    run_config_overrides=create_run_config_overrides(
        instance_pool_type=MEMORY_OPTIMIZED_INSTANCE_POOL_KEY,
        max_workers=8,
    ),
    dq_checks=[
        NonEmptyDQCheck(name="dq_non_empty_agg_location_stats"),
        PrimaryKeyDQCheck(name="dq_pk_agg_location_stats", primary_keys=PRIMARY_KEYS),
        NonNullDQCheck(
            name="dq_non_null_agg_location_stats",
            non_null_columns=["device_id", "total", "date", "zip_code", "org_id"],
        ),
        TrendDQCheck(
            name="dq_trend_agg_location_stats",
            tolerance=0.2,
            lookback_days=1,
            dimension="country",
            min_percent_share=0.1,
        ),
    ],
    backfill_batch_size=1,
    upstreams=["kinesisstats_history.location"],
)
def agg_location_stats(context: AssetExecutionContext) -> str:
    context.log.info("Updating agg_location_stats")
    partition_keys = partition_key_ranges_from_context(context)[0]
    PARTITION_START = partition_keys[0]
    PARTITION_END = partition_keys[-1]
    query = QUERY.format(
        PARTITION_START=PARTITION_START,
        PARTITION_END=PARTITION_END,
    )
    context.log.info(f"{query}")
    return query
