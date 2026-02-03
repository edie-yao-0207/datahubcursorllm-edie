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
            "comment": "Population density mapping of urban, rural, or mixed sourced from product_analytics_staging.map_population_density by referencing the location zip code for the United States, city and state for Canada, and state for Mexico."
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
(SELECT /*+ SHUFFLE_HASH(map_population_density) */
  locations.date,
  locations.org_id,
  locations.device_id,
  locations.country,
  map_population_density.pop_density_mapping,
  sum(locations.total) as total
FROM {source}.agg_location_stats locations
left outer join {source}.map_population_density map_population_density
  on map_population_density.country = 'US'
    AND map_population_density.zip = locations.zip_code
where locations.date BETWEEN '{PARTITION_START}' and '{PARTITION_END}'
      and locations.country = 'US'
group by all)
union all
(SELECT /*+ SHUFFLE_HASH(map_population_density) */
  locations.date,
  locations.org_id,
  locations.device_id,
  locations.country,
  map_population_density.pop_density_mapping,
  sum(locations.total) as total
FROM {source}.agg_location_stats locations
left outer join {source}.map_population_density map_population_density
  on (locations.country = "CA"
      AND map_population_density.state = locations.state
      AND map_population_density.city = locations.city)
    OR (locations.country = 'MX'
      AND map_population_density.state = locations.state)
where locations.date BETWEEN '{PARTITION_START}' and '{PARTITION_END}'
      and locations.country IN ('CA', 'MX')
group by all)
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
    regions=[AWSRegion.US_WEST_2, AWSRegion.CA_CENTRAL_1],
    owners=[DATAENGINEERING],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DailyPartitionsDefinition(start_date="2024-06-01"),
    write_mode=WarehouseWriteMode.OVERWRITE,
    run_config_overrides=create_run_config_overrides(
        instance_pool_type=MEMORY_OPTIMIZED_INSTANCE_POOL_KEY,
        max_workers=16,
    ),
    dq_checks=[
        NonEmptyDQCheck(name="dq_non_empty_stg_population_density"),
        PrimaryKeyDQCheck(
            name="dq_pk_stg_population_density", primary_keys=PRIMARY_KEYS
        ),
        NonNullDQCheck(
            name="dq_non_null_stg_population_density",
            non_null_columns=["device_id", "total", "date", "org_id"],
        ),
    ],
    backfill_batch_size=1,
    upstreams=["product_analytics_staging.agg_location_stats"],
)
def stg_population_density(context: AssetExecutionContext) -> str:
    context.log.info("Updating stg_population_density")
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
