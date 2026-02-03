from dataclasses import dataclass, field
from enum import Enum
from typing import Optional
from dagster import AssetExecutionContext
from dataweb import build_general_dq_checks, get_databases, table
from dataweb.assets.datasets.product_analytics_staging import fct_statechangeevents
from dataweb.userpkgs.constants import (
    ALL_COMPUTE_REGIONS,
    FIRMWAREVDP,
    FRESHNESS_SLO_9AM_PST,
    Database,
    TableType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.firmware.constants import (
    DATAWEB_PARTITION_DATE,
    ScalarAggregation,
)
from dataweb.userpkgs.firmware.kinesisstats import KinesisStatsMetric
from dataweb.userpkgs.firmware.metric import (
    Metric,
    MetricEnum,
    MetricValue,
    check_unique_metric_strings,
)
from dataweb.userpkgs.firmware.schema import (
    ColumnType,
    columns_to_names,
    columns_to_schema,
)
from dataweb.userpkgs.firmware.table import DatabaseTable, ProductAnalytics
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.utils import (
    build_table_description,
    merge_pyspark_dataframes,
    partition_key_ranges_from_context,
    schema_to_columns_with_property,
)
from dataweb.assets.datasets.product_analytics_staging import (
    fct_osdcommandschedulerstats
)
from pyspark.sql import DataFrame, SparkSession

SCHEMA = columns_to_schema(
    ColumnType.DATE,
    ColumnType.TYPE,
    ColumnType.ORG_ID,
    ColumnType.DEVICE_ID,
    ColumnType.BUS_ID,
    ColumnType.REQUEST_ID,
    ColumnType.RESPONSE_ID,
    ColumnType.OBD_VALUE,
    ColumnType.VALUE,
)

PRIMARY_KEYS = columns_to_names(
    ColumnType.DATE,
    ColumnType.TYPE,
    ColumnType.ORG_ID,
    ColumnType.DEVICE_ID,
    ColumnType.BUS_ID,
    ColumnType.REQUEST_ID,
    ColumnType.RESPONSE_ID,
    ColumnType.OBD_VALUE,
)

NON_NULL_KEYS = schema_to_columns_with_property(SCHEMA, "nullable")


class TableMetric(MetricEnum):
    ODOMETER_TO_GPS_DISTANCE = Metric(
        type=ProductAnalytics.AGG_DEVICE_STATS_SECONDARY_RATIO,
        field=MetricValue.VALUE,
        label="odometer_to_gps_distance",
    )
    ODOMETER_TO_ENGINE_SECONDS = Metric(
        type=ProductAnalytics.AGG_DEVICE_STATS_SECONDARY_RATIO,
        field=MetricValue.VALUE,
        label="odometer_to_engine_seconds",
    )
    SEATBELT_TOGGLES_TO_GPS_DISTANCE = Metric(
        type=ProductAnalytics.AGG_DEVICE_STATS_SECONDARY_RATIO,
        field=MetricValue.VALUE,
        label="seatbelt_toggles_to_gps_distance",
    )
    FUEL_CONSUMED_TO_GPS_DISTANCE = Metric(
        type=ProductAnalytics.AGG_DEVICE_STATS_SECONDARY_RATIO,
        field=MetricValue.VALUE,
        label="fuel_consumed_to_gps_distance",
    )
    COASTING_TIME_TO_GPS_DISTANCE = Metric(
        type=ProductAnalytics.AGG_DEVICE_STATS_SECONDARY_RATIO,
        field=MetricValue.VALUE,
        label="coasting_time_to_gps_distance"
    )
    BRAKE_EVENTS_TO_GPS_DISTANCE = Metric(
        type=ProductAnalytics.AGG_DEVICE_STATS_SECONDARY_RATIO,
        field=MetricValue.VALUE,
        label="brake_events_to_gps_distance"
    )
    ANTICIPATION_BRAKE_EVENTS_TO_GPS_DISTANCE = Metric(
        type=ProductAnalytics.AGG_DEVICE_STATS_SECONDARY_RATIO,
        field=MetricValue.VALUE,
        label="anticipation_brake_events_to_gps_distance"
    )
    COMMAND_SCHEDULER_STATS_TX_RX_RATE = Metric(
        type=ProductAnalytics.AGG_DEVICE_STATS_SECONDARY_RATIO,
        field=MetricValue.VALUE,
        label="command_scheduler_rx_tx_rate"
    )


class TableDimension(str, Enum):
    VALUE = "value"


check_unique_metric_strings(TableMetric)


@dataclass
class RatioSettings:
    numerator: KinesisStatsMetric
    """Pre-aggregated metric to use for numerator."""
    numerator_expression: str
    """Expression to use in the numerator, e.g. 'max - min'"""
    denominator: KinesisStatsMetric
    """Pre-aggregated metric to use for denominator."""
    denominator_expression: str
    """Expression to use in the denominator, e.g. 'max - min'"""
    output: TableMetric
    """Secondary field to be written to the table for this metric."""
    max_value: Optional[str] = field(default=None)
    """Maximum value for the ratio. If the calculated ratio exceeds the provided value, then max_value
    will be used as the value for the metric.
    """


STAT_SETTINGS = [
    RatioSettings(
        numerator=KinesisStatsMetric.ODOMETER_INT_VALUE,
        numerator_expression=f"{ScalarAggregation.MAX} - {ScalarAggregation.MIN}",
        denominator=KinesisStatsMetric.DELTA_GPS_DISTANCE_DOUBLE_VALUE,
        denominator_expression=ScalarAggregation.SUM,
        output=TableMetric.ODOMETER_TO_GPS_DISTANCE,
    ),
    RatioSettings(
        numerator=KinesisStatsMetric.ODOMETER_INT_VALUE,
        # Multiply by 3.6 to convert m/s to km/h
        numerator_expression=f"({ScalarAggregation.MAX} - {ScalarAggregation.MIN}) * 3.6",
        denominator=KinesisStatsMetric.ENGINE_SECONDS_INT_VALUE,
        denominator_expression=f"{ScalarAggregation.MAX} - {ScalarAggregation.MIN}",
        output=TableMetric.ODOMETER_TO_ENGINE_SECONDS,
    ),
    RatioSettings(
        numerator=fct_statechangeevents.TableMetric.SEATBELT_TOGGLES,
        numerator_expression=ScalarAggregation.SUM,
        denominator=KinesisStatsMetric.DELTA_GPS_DISTANCE_DOUBLE_VALUE,
        denominator_expression=f"{ScalarAggregation.SUM} * 0.001",
        output=TableMetric.SEATBELT_TOGGLES_TO_GPS_DISTANCE,
    ),
    RatioSettings(
        numerator=KinesisStatsMetric.DELTA_FUEL_CONSUMED_INT_VALUE,
        # Multiply by 100 to convert ml/m to L/100km
        numerator_expression=f"({ScalarAggregation.SUM}) * 100",
        denominator=KinesisStatsMetric.DELTA_GPS_DISTANCE_DOUBLE_VALUE,
        denominator_expression=ScalarAggregation.SUM,
        output=TableMetric.FUEL_CONSUMED_TO_GPS_DISTANCE
    ),
    RatioSettings(
        numerator=KinesisStatsMetric.DELTA_COASTING_TIME_MS_INT_VALUE,
        numerator_expression=ScalarAggregation.SUM,
        denominator=KinesisStatsMetric.DELTA_GPS_DISTANCE_DOUBLE_VALUE,
        denominator_expression=ScalarAggregation.SUM,
        output=TableMetric.COASTING_TIME_TO_GPS_DISTANCE,
    ),
    RatioSettings(
        numerator=KinesisStatsMetric.OSD_DELTA_BRAKE_EVENTS_INT_VALUE,
        numerator_expression=ScalarAggregation.SUM,
        denominator=KinesisStatsMetric.DELTA_GPS_DISTANCE_DOUBLE_VALUE,
        denominator_expression=ScalarAggregation.SUM,
        output=TableMetric.BRAKE_EVENTS_TO_GPS_DISTANCE,
    ),
    RatioSettings(
        numerator=KinesisStatsMetric.DELTA_ANTICIPATION_BRAKE_EVENTS_INT_VALUE,
        numerator_expression=ScalarAggregation.SUM,
        denominator=KinesisStatsMetric.DELTA_GPS_DISTANCE_DOUBLE_VALUE,
        denominator_expression=ScalarAggregation.SUM,
        output=TableMetric.ANTICIPATION_BRAKE_EVENTS_TO_GPS_DISTANCE,
    ),
    RatioSettings(
        numerator=fct_osdcommandschedulerstats.TableMetric.ANY_RESPONSE_COUNT,
        numerator_expression=ScalarAggregation.SUM,
        denominator=fct_osdcommandschedulerstats.TableMetric.REQUEST_COUNT,
        denominator_expression=ScalarAggregation.SUM,
        output=TableMetric.COMMAND_SCHEDULER_STATS_TX_RX_RATE,
        max_value=1,
    ),
]

METRICS = [setting.output for setting in STAT_SETTINGS]

QUERY = """

WITH numerator AS (
    SELECT
        date
        , type
        , org_id
        , device_id
        , bus_id
        , request_id
        , response_id
        , obd_value
        , {setting.numerator_expression} AS value

    FROM
        {product_analytics}.agg_device_stats_primary

    WHERE
        date BETWEEN "{date_start}" AND "{date_end}"
        AND type == "{setting.numerator}"
)

, denominator AS (
    SELECT
        date
        , type
        , org_id
        , device_id
        , bus_id
        , request_id
        , response_id
        , obd_value
        , {setting.denominator_expression} AS value

    FROM
        {product_analytics}.agg_device_stats_primary

    WHERE
        date BETWEEN "{date_start}" AND "{date_end}"
        AND type == "{setting.denominator}"
)

SELECT
    numerator.date
    , "{setting.output}" AS type
    , numerator.org_id
    , numerator.device_id
    , numerator.bus_id
    , numerator.request_id
    , numerator.response_id
    , numerator.obd_value
    , {value_expr} AS value

FROM
    numerator

JOIN
    denominator
    ON numerator.date == denominator.date
    AND numerator.org_id == denominator.org_id
    AND numerator.device_id == denominator.device_id
    AND COALESCE(numerator.bus_id, -1) == COALESCE(denominator.bus_id, -1)
    AND COALESCE(numerator.request_id, -1) == COALESCE(denominator.request_id, -1)
    AND COALESCE(numerator.response_id, -1) == COALESCE(denominator.response_id, -1)
    AND COALESCE(numerator.obd_value, -1) == COALESCE(denominator.obd_value, -1)

"""


@table(
    database=Database.PRODUCT_ANALYTICS,
    description=build_table_description(
        table_desc="Computes the ratio of two different VDP metrics",
        row_meaning="Computed ratio of different VDP metrics. The metrics used for the ratio are indicated in type.",
        table_type=TableType.AGG,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DATAWEB_PARTITION_DATE,
    upstreams=[
        AnyUpstream(ProductAnalytics.AGG_DEVICE_STATS_PRIMARY),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    backfill_batch_size=3,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalytics.AGG_DEVICE_STATS_SECONDARY_RATIO.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_KEYS,
        block_before_write=True,
    ),
    max_retries=5,
)
def agg_device_stats_secondary_ratio(context: AssetExecutionContext) -> DataFrame:
    partition_keys = partition_key_ranges_from_context(context)[0]
    databases = get_databases()

    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

    dfs = []
    for settings in STAT_SETTINGS:
        query = QUERY.format(
            date_start=partition_keys[0],
            date_end=partition_keys[-1],
            setting=settings,
            product_analytics=databases[Database.PRODUCT_ANALYTICS],
            value_expr=(
                f"array_min(array(numerator.value / denominator.value, {settings.max_value}))"
                if settings.max_value is not None else
                "numerator.value / denominator.value"
            )
        )

        context.log.info(query)

        dfs.append(spark.sql(query).repartition(1))

    return merge_pyspark_dataframes(dfs)
