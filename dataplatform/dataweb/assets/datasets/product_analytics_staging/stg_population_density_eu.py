from dagster import AssetExecutionContext, DailyPartitionsDefinition
from dataweb import (
    NonEmptyDQCheck,
    NonNullDQCheck,
    PrimaryKeyDQCheck,
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
    get_run_env,
    partition_key_ranges_from_context,
)

PRIMARY_KEYS = ["date", "org_id", "device_id", "country", "pop_density_mapping"]

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
        "name": "country",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "the country corresponding to the latitude / longitude of this device as determined by reverse geolocation"
        },
    },
    {
        "name": "pop_density_mapping",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Population density mapping of urban, rural, or mixed sourced from Eurostat 2km grid population data."
        },
    },
    {
        "name": "total",
        "type": "long",
        "nullable": False,
        "metadata": {"comment": "Count of locations for combination."},
    },
]


QUERY = """
with lat_lng as (
  select *,
  case when population_per_mile >= 0 and population_per_mile < 500 then 'rural'
        when population_per_mile >= 500 and population_per_mile < 1000 then 'mixed'
        when population_per_mile >= 1000 then 'urban' end as pop_density_mapping
  from read_files(
          's3://samsara-eu-databricks-workspace/dataengineering/eu_2km_grid.csv',
          format => 'csv',
          header => true))
select
       date,
       locations.org_id,
       locations.device_id,
       lat_lng.pop_density_mapping as pop_density_mapping,
       locations.value.revgeo_country AS country,
       count(*) as total
from kinesisstats_history.location locations
left outer join lat_lng
  on (round(locations.value.latitude, 2) = lat_lng.latitude
  and round(locations.value.longitude,2) = lat_lng.longitude)
  or (round(locations.value.latitude, 1) = round(lat_lng.latitude,1)
  and round(locations.value.longitude,1) = round(lat_lng.longitude,1))
where locations.date between '{PARTITION_START}' and '{PARTITION_END}'
and locations.value.gps_speed_meters_per_second > 0
and locations.value.revgeo_country != 'US'
group by all
"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="""The table aggregates locations at the country, device, organization, and population density level to support matching organizations to primary driving environment characteristics""",
        row_meaning="""Each row represents the count of locations for the date, organization, device_id, country, and population density mapping.""",
        related_table_info={},
        table_type=TableType.STAGING,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=[AWSRegion.EU_WEST_1],
    owners=[DATAENGINEERING],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DailyPartitionsDefinition(start_date="2024-06-01"),
    write_mode=WarehouseWriteMode.OVERWRITE,
    run_config_overrides=create_run_config_overrides(
        instance_pool_type=MEMORY_OPTIMIZED_INSTANCE_POOL_KEY,
        max_workers=4,
    ),
    dq_checks=[
        NonEmptyDQCheck(name="dq_non_empty_stg_population_density_eu"),
        PrimaryKeyDQCheck(
            name="dq_pk_stg_population_density_eu", primary_keys=PRIMARY_KEYS
        ),
        NonNullDQCheck(
            name="dq_non_null_stg_population_density_eu",
            non_null_columns=["device_id", "total", "date", "org_id"],
        ),
    ],
    backfill_batch_size=5,
    upstreams=[],
)
def stg_population_density_eu(context: AssetExecutionContext) -> str:
    context.log.info("Updating stg_population_density_eu")
    partition_keys = partition_key_ranges_from_context(context)[0]
    PARTITION_START = partition_keys[0]
    PARTITION_END = partition_keys[-1]
    query = QUERY.format(
        PARTITION_START=PARTITION_START,
        PARTITION_END=PARTITION_END,
    )
    context.log.info(f"{query}")
    return query
