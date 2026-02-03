from dataclasses import dataclass
from dagster import AssetExecutionContext
from functools import reduce
from pyspark.sql import DataFrame, SparkSession
from dataweb.userpkgs.firmware.constants import (
    DATAWEB_PARTITION_DATE,
    ProductArea,
    SourceLink,
)
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.firmware.metric import Metric, MetricEnum, StatMetadata, StrEnum
from dataweb.userpkgs.firmware.kinesisstats import KinesisStatsMetric
from dataweb import table, build_general_dq_checks
from dataweb.userpkgs.firmware.schema import (
    Column,
    ColumnType,
    DataType,
    Metadata,
    columns_to_schema,
)
from dataweb.userpkgs.firmware.table import (
    KinesisStats,
    ProductAnalyticsStaging,
)
from dataweb.userpkgs.constants import (
    ALL_COMPUTE_REGIONS,
    FIRMWAREVDP,
    FRESHNESS_SLO_9AM_PST,
    Database,
    TableType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.utils import (
    build_table_description,
    partition_key_ranges_from_context,
    schema_to_columns_with_property,
)


class TableDimension(StrEnum):
    GASEOUS_FUEL_CONSUMPTION = "gaseous_consumption"
    LIQUID_FUEL_CONSUMPTION = "liquid_consumption"


class TableMetric(MetricEnum):
    GASEOUS_CONSUMPTION_RATE = Metric(
        type=ProductAnalyticsStaging.FCT_HOURLY_FUEL_CONSUMPTION,
        field=TableDimension.GASEOUS_FUEL_CONSUMPTION,
        label="gaseous_fuel_consumption",
        metadata=StatMetadata(
            product_area=ProductArea.VEHICLE_DIAGNOSTICS,
            sub_product_area=ProductArea.FUEL_AND_ENERGY,
            signal_name="Hourly Fuel Consumption - Gaseous",
            default_priority=1,
            value_min=0.0,
            value_max=271000.0,  # 271 kg/hr, equivalent to 100 usg/hr
            documentation_link=SourceLink.FUEL_CONSUMPTION,
            is_applicable_ice=True,
            is_applicable_hydrogen=True,
            is_applicable_hybrid=True,
            is_applicable_phev=True,
            is_applicable_bev=False,
            is_applicable_unknown=False,
        ),
    )
    LIQUID_CONSUMPTION_RATE = Metric(
        type=ProductAnalyticsStaging.FCT_HOURLY_FUEL_CONSUMPTION,
        field=TableDimension.LIQUID_FUEL_CONSUMPTION,
        label="liquid_fuel_consumption",
        metadata=StatMetadata(
            product_area=ProductArea.VEHICLE_DIAGNOSTICS,
            sub_product_area=ProductArea.FUEL_AND_ENERGY,
            signal_name="Hourly Fuel Consumption - Liquid",
            default_priority=1,
            value_min=0.0,
            value_max=454609.0,  # approx. 120 usg
            documentation_link=SourceLink.FUEL_CONSUMPTION,
            is_applicable_ice=True,
            is_applicable_hydrogen=True,
            is_applicable_hybrid=True,
            is_applicable_phev=True,
            is_applicable_bev=False,
            is_applicable_unknown=False,
        ),
    )


SCHEMA = columns_to_schema(
    ColumnType.DATE,
    ColumnType.TIME,
    ColumnType.ORG_ID,
    ColumnType.DEVICE_ID,
    ColumnType.BUS_ID,
    ColumnType.REQUEST_ID,
    ColumnType.RESPONSE_ID,
    ColumnType.OBD_VALUE,
    Column(
        name=TableDimension.GASEOUS_FUEL_CONSUMPTION,
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(
            comment="Total gaseous fuel consumption over an hour interval in grams."
        ),
    ),
    Column(
        name=TableDimension.LIQUID_FUEL_CONSUMPTION,
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(
            comment="Total liquid fuel consumption over an hour interval in mL."
        ),
    ),
)
PRIMARY_KEYS = schema_to_columns_with_property(SCHEMA)
NON_NULL_COLUMNS = schema_to_columns_with_property(SCHEMA, "nullable")


@dataclass
class HourlyConsumptionSettings:
    source_metric: Metric
    field_name: str


STAT_SETTINGS = [
    HourlyConsumptionSettings(
        source_metric=KinesisStatsMetric.OSD_FUEL_CONSUMED_GASEOUS_INT_VALUE,
        field_name=TableDimension.GASEOUS_FUEL_CONSUMPTION,
    ),
    HourlyConsumptionSettings(
        source_metric=KinesisStatsMetric.DELTA_FUEL_CONSUMED_INT_VALUE,
        field_name=TableDimension.LIQUID_FUEL_CONSUMPTION,
    ),
]

QUERY = """
SELECT
  date,
  org_id,
  object_id AS device_id,
  UNIX_TIMESTAMP(DATE_TRUNC('hour', FROM_UNIXTIME(time / 1000))) * 1000 AS time,
  SUM({source_metric.field}) AS {field_name}
FROM {source_metric.type}
WHERE date BETWEEN "{date_start}" AND "{date_end}"
GROUP BY
  date,
  org_id,
  object_id,
  DATE_TRUNC('hour', FROM_UNIXTIME(time / 1000))
"""

MERGE_QUERY = """
SELECT
    date,
    org_id,
    device_id,
    time,
    CAST(NULL AS LONG) AS bus_id,
    CAST(NULL AS LONG) AS request_id,
    CAST(NULL AS LONG) AS response_id,
    CAST(NULL AS LONG) AS obd_value,
    {metrics_sql}
FROM mergedStats
"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Gaseous fuel consumption rates per device.",
        row_meaning="Gaseous fuel consumption rate in g/s for a single reported datapoint.",
        related_table_info={},
        table_type=TableType.STAGING,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DATAWEB_PARTITION_DATE,
    upstreams=[
        AnyUpstream(KinesisStats.OSD_FUEL_CONSUMED_GASEOUS),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    backfill_batch_size=5,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.FCT_HOURLY_FUEL_CONSUMPTION.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
)
def fct_hourlyfuelconsumption(context: AssetExecutionContext) -> DataFrame:
    partition_keys = partition_key_ranges_from_context(context)[0]

    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

    dfs = []
    for settings in STAT_SETTINGS:
        query = QUERY.format(
            source_metric=settings.source_metric,
            field_name=settings.field_name,
            date_start=partition_keys[0],
            date_end=partition_keys[-1],
        )

        context.log.info(query)
        dfs.append(spark.sql(query))

    # Outer join all the computed stats together - one stat should take up one column in this table
    reduce(
        lambda x, y: x.join(y, ["date", "org_id", "device_id", "time"], how="outer"),
        dfs,
    ).registerTempTable("mergedStats")

    merge_query = MERGE_QUERY.format(
        metrics_sql=",\n\t".join(
            [f"COALESCE({s.field_name}, 0) AS {s.field_name}" for s in STAT_SETTINGS]
        )
    )
    context.log.info(merge_query)

    return spark.sql(merge_query)
