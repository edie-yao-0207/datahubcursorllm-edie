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

PRIMARY_KEYS = ["date", "org_id", "device_id", "trip_start_time_ms", "trip_end_time_ms"]

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
        "name": "count_rural_locations",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Count of rural locations (approximate population density < 500 people per square mile) during trip sourced from map_population_density table."
        },
    },
    {
        "name": "count_urban_locations",
        "type": "long",
        "nullable": False,
            "metadata": {
                "comment": "Count of urban locations (approximate population density > 1000 people per square mile) during trip sourced from map_population_density table."
            },
    },
    {
        "name": "count_mixed_locations",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Count of mixed urbanization locations (approximate population density between 500 and 1000 people per square mile) during trip sourced from map_population_density table."
        },
    },
    {
        "name": "primary_driving_environment",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Primary driving environment type of rural, urban, or mixed sourced from maximum count of rural, urban, or mixed locations during trip."
        },
    },
    {
        "name": "total_locations",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total count of locations during trip."
        },
    },
]

QUERY = """
WITH us_query AS
(SELECT  /*+ SHUFFLE_HASH(map_population_density) */
       location_stats.date,
       location_stats.org_id,
       location_stats.device_id,
       location_stats.driver_id,
       location_stats.trip_start_time_ms,
       location_stats.trip_end_time_ms,
       SUM(CASE WHEN map_population_density.pop_density_mapping = 'rural' THEN location_stats.total ELSE 0 END) AS count_rural_locations,
       SUM(CASE WHEN map_population_density.pop_density_mapping = 'urban' THEN location_stats.total ELSE 0 END) AS count_urban_locations,
       SUM(CASE WHEN map_population_density.pop_density_mapping = 'mixed' THEN location_stats.total ELSE 0 END) AS count_mixed_locations,
       SUM(location_stats.total) AS total_locations
FROM product_analytics_staging.agg_trip_location_stats location_stats
LEFT OUTER JOIN product_analytics_staging.map_population_density map_population_density
  ON (location_stats.country = 'US'
      AND map_population_density.country = 'US'
      AND map_population_density.zip = location_stats.zip_code)
WHERE location_stats.date BETWEEN '{PARTITION_START}' AND '{PARTITION_END}'
AND location_stats.driver_id IS NOT NULL
AND location_stats.country = 'US'
AND len(location_stats.zip_code) = 5
GROUP BY ALL),
ca_query AS (
SELECT  /*+ SHUFFLE_HASH(map_population_density) */
       location_stats.date,
       location_stats.org_id,
       location_stats.device_id,
       location_stats.driver_id,
       location_stats.trip_start_time_ms,
       location_stats.trip_end_time_ms,
       SUM(CASE WHEN map_population_density.pop_density_mapping = 'rural' THEN location_stats.total ELSE 0 END) AS count_rural_locations,
       SUM(CASE WHEN map_population_density.pop_density_mapping = 'urban' THEN location_stats.total ELSE 0 END) AS count_urban_locations,
       SUM(CASE WHEN map_population_density.pop_density_mapping = 'mixed' THEN location_stats.total ELSE 0 END) AS count_mixed_locations,
       SUM(location_stats.total) AS total_locations
FROM product_analytics_staging.agg_trip_location_stats location_stats
LEFT OUTER JOIN product_analytics_staging.map_population_density map_population_density
  ON (location_stats.country = 'CA'
      AND map_population_density.country = 'CA'
      AND map_population_density.state = location_stats.state
      AND map_population_density.city = location_stats.city)
WHERE location_stats.date BETWEEN '{PARTITION_START}' AND '{PARTITION_END}'
AND location_stats.driver_id IS NOT NULL
AND location_stats.country = 'CA'
AND len(location_stats.zip_code) = 7
GROUP BY ALL),
mx_query AS (
SELECT  /*+ SHUFFLE_HASH(map_population_density) */
       location_stats.date,
       location_stats.org_id,
       location_stats.device_id,
       location_stats.driver_id,
       location_stats.trip_start_time_ms,
       location_stats.trip_end_time_ms,
       SUM(CASE WHEN map_population_density.pop_density_mapping = 'rural' THEN location_stats.total ELSE 0 END) AS count_rural_locations,
       SUM(CASE WHEN map_population_density.pop_density_mapping = 'urban' THEN location_stats.total ELSE 0 END) AS count_urban_locations,
       SUM(CASE WHEN map_population_density.pop_density_mapping = 'mixed' THEN location_stats.total ELSE 0 END) AS count_mixed_locations,
       SUM(location_stats.total) AS total_locations
FROM product_analytics_staging.agg_trip_location_stats location_stats
LEFT OUTER JOIN product_analytics_staging.map_population_density map_population_density
  ON (location_stats.country = 'MX'
       AND map_population_density.country = 'MX'
      AND map_population_density.state = location_stats.state)
WHERE location_stats.date BETWEEN '{PARTITION_START}' AND '{PARTITION_END}'
AND location_stats.driver_id IS NOT NULL
AND location_stats.country = 'MX'
AND len(location_stats.zip_code) = 5
GROUP BY ALL ),
unioned AS (
(SELECT * FROM us_query)
UNION ALL
(SELECT * FROM ca_query)
UNION ALL
(SELECT * FROM mx_query))

SELECT location_stats.date,
       location_stats.org_id,
       location_stats.device_id,
       location_stats.driver_id,
       location_stats.trip_start_time_ms,
       location_stats.trip_end_time_ms,
       SUM(count_rural_locations) AS count_rural_locations,
       SUM(count_urban_locations) AS count_urban_locations,
       SUM(count_mixed_locations) AS count_mixed_locations,
       CASE
          WHEN SUM(count_mixed_locations) = GREATEST(SUM(count_rural_locations), SUM(count_urban_locations),SUM(count_mixed_locations)) THEN 'mixed'
          WHEN SUM(count_urban_locations) = SUM(count_rural_locations)
               AND SUM(count_urban_locations) > 0
               AND SUM(count_rural_locations) > 0
          THEN 'mixed'
          WHEN SUM(count_rural_locations) = GREATEST(SUM(count_rural_locations), SUM(count_urban_locations),SUM
           (count_mixed_locations)) THEN 'rural'
           WHEN SUM(count_urban_locations) = GREATEST(SUM(count_rural_locations), SUM(count_urban_locations),SUM(count_mixed_locations)) THEN 'urban'
       END AS primary_driving_environment,
       SUM(total_locations) AS total_locations
FROM unioned location_stats
GROUP BY ALL
"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="""Breakdown of trips by locations per driving environment type of rural, urban, and mixed.""",
        row_meaning="""Each row represents the driving environment calculation for a trip.""",
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
        NonEmptyDQCheck(name="dq_non_empty_agg_trip_driving_environment"),
        PrimaryKeyDQCheck(name="dq_pk_agg_trip_driving_environment", primary_keys=PRIMARY_KEYS),
        NonNullDQCheck(
            name="dq_non_null_agg_trip_driving_environment",
            non_null_columns=["device_id", "date", "org_id", "trip_start_time_ms", "trip_end_time_ms"],
        ),
    ],
    backfill_batch_size=1,
    upstreams=["product_analytics_staging.agg_trip_location_stats",
               "product_analytics_staging.map_population_density"],
)
def agg_trip_driving_environment(context: AssetExecutionContext) -> str:
    context.log.info("Updating agg_trip_driving_environment")
    partition_keys = partition_key_ranges_from_context(context)[0]
    PARTITION_START = partition_keys[0]
    PARTITION_END = partition_keys[-1]
    query = QUERY.format(
        PARTITION_START=PARTITION_START,
        PARTITION_END=PARTITION_END,
    )
    context.log.info(f"{query}")
    return query
