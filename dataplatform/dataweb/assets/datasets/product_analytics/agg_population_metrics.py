from dataclasses import dataclass, field
from typing import List

from dagster import (
    AssetExecutionContext,
    MultiPartitionsDefinition,
    StaticPartitionsDefinition,
)
from dataweb import build_general_dq_checks, get_databases, table, JoinableDQCheck
from dataweb.assets.datasets.product_analytics import (
    agg_device_stats_secondary_consistency,
    agg_device_stats_secondary_coverage,
    agg_device_stats_secondary_ratio,
    agg_device_stats_secondary_response_ratio,
)
from dataweb.assets.datasets.product_analytics_staging import (
    agg_osdcommandschedulerstats,
    agg_stat_coverage_on_drive,
    fct_engine_segment_stats,
    fct_osdcanprotocolsdetected,
    fct_osdenginefault,
    fct_osdj1939claimedaddress,
    fct_osdobdvaluelockadded,
    fct_osdvin,
    fct_enginestateerrors,
    fct_enginestate_method,
    fct_hourlyfuelconsumption,
    fct_enginestate_comparison_stats,
)
from dataweb.userpkgs.constants import (
    ALL_COMPUTE_REGIONS,
    FIRMWAREVDP,
    FRESHNESS_SLO_9AM_PST,
    Database,
    DQCheckMode,
    InstanceType,
    RunEnvironment,
    TableType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.firmware.constants import (
    DATAWEB_PARTITION_DATE,
    ConcurrencyKey,
)
from dataweb.userpkgs.query import create_run_config_overrides
from dataweb.userpkgs.firmware.kinesisstats import KinesisStatsMetric
from dataweb.userpkgs.firmware.metric import Metric
from dataweb.userpkgs.firmware.populations import (
    DEVICE_COLUMNS,
    GROUPING_SETS,
    ColumnType,
    Population,
    population_id_value_expr,
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
    get_run_env,
    merge_pyspark_dataframes,
    partition_key_ranges_from_range_tags,
    schema_to_columns_with_property,
)
from pyspark.sql import DataFrame, SparkSession

SCHEMA = columns_to_schema(
    ColumnType.DATE,
    ColumnType.METRIC_NAME,
    ColumnType.POPULATION_ID,
    ColumnType.COUNT,
    ColumnType.SUM,
    ColumnType.MIN,
    ColumnType.MAX,
    ColumnType.AVG,
    ColumnType.VARIANCE,
    ColumnType.STDDEV,
    ColumnType.MEAN,
    ColumnType.MEDIAN,
    ColumnType.MODE,
    ColumnType.FIRST,
    ColumnType.LAST,
    ColumnType.KURTOSIS,
    ColumnType.PERCENTILE,
    ColumnType.HISTOGRAM,
    ColumnType.POPULATION_COUNT,
    ColumnType.HISTOGRAM_DISCRETE,
)

PRIMARY_KEYS = columns_to_names(
    ColumnType.DATE,
    ColumnType.METRIC_NAME,
    ColumnType.POPULATION_ID,
)

ANOMALY_COUNT_METRICS = [
    KinesisStatsMetric.FUEL_TYPE_INT_VALUE,
    KinesisStatsMetric.ODOMETER_INT_VALUE,
    KinesisStatsMetric.OBD_CABLE_ID_INT_VALUE,
    KinesisStatsMetric.ENGINE_STATE_INT_VALUE,
    KinesisStatsMetric.ENGINE_SECONDS_INT_VALUE,
    KinesisStatsMetric.ENGINE_GAUGE_ENGINE_RPM,
    KinesisStatsMetric.ENGINE_GAUGE_FUEL_LEVEL_PERCENT,
    KinesisStatsMetric.FUEL_TYPE_INT_VALUE,
    KinesisStatsMetric.DELTA_FUEL_CONSUMED_INT_VALUE,
    KinesisStatsMetric.DELTA_ACCEL_ENGINE_TORQUE_OVER_LIMIT_MS_INT_VALUE,
    KinesisStatsMetric.DELTA_ACCEL_ENGINE_TORQUE_OVER_LIMIT_WHILE_NOT_ON_CRUISE_CONTROL_MS_INT_VALUE,
    KinesisStatsMetric.DELTA_ACCELERATOR_PEDAL_TIME_GREATER_THAN_95_PERCENT_MS_INT_VALUE,
    KinesisStatsMetric.DELTA_ANTICIPATION_BRAKE_EVENTS_INT_VALUE,
    KinesisStatsMetric.OSD_DELTA_BRAKE_EVENTS_INT_VALUE,
    KinesisStatsMetric.DELTA_COASTING_TIME_MS_INT_VALUE,
    KinesisStatsMetric.DELTA_COASTING_TIME_TORQUE_BASED_WHILE_NOT_ON_CRUISE_CONTROL_MS_INT_VALUE,
    KinesisStatsMetric.DELTA_COASTING_TIME_WHILE_NOT_ON_CRUISE_CONTROL_MS_INT_VALUE,
    KinesisStatsMetric.DELTA_CRUISE_CONTROL_MS_INT_VALUE,
    KinesisStatsMetric.EV_CHARGER_MILLI_AMP_INT_VALUE,
    KinesisStatsMetric.EV_CHARGER_MILLI_VOLT_INT_VALUE,
    KinesisStatsMetric.EV_CHARGING_STATUS_INT_VALUE,
    # TODO VDP-4974: Add this value to anomaly counts.
    # KinesisStatsMetric.EV_CHARGING_TYPE_INT_VALUE,
    KinesisStatsMetric.EV_HIGH_CAPACITY_BATTERY_MILLI_AMP_INT_VALUE,
    KinesisStatsMetric.EV_HIGH_CAPACITY_BATTERY_MILLI_VOLT_INT_VALUE,
    KinesisStatsMetric.EV_HIGH_CAPACITY_BATTERY_STATE_OF_HEALTH_MILLI_PERCENT_INT_VALUE,
    KinesisStatsMetric.EV_USABLE_STATE_OF_CHARGE_MILLI_PERCENT_INT_VALUE,
    KinesisStatsMetric.DELTA_EV_DISTANCE_DRIVEN_ON_ELECTRIC_POWER_METERS_INT_VALUE,
    fct_hourlyfuelconsumption.TableMetric.GASEOUS_CONSUMPTION_RATE,
    fct_hourlyfuelconsumption.TableMetric.LIQUID_CONSUMPTION_RATE,
]

DEFAULT_PERCENTILES = [.01, .05, .1, .2, .5, .7, .8, .9, .95, .99]
ENGINE_STATE_STAT_PERCENTILES = [.85, .90, .95, .99, .999]

@dataclass
class OpsMetric:
    """
    Operation applied to a single field (column) from a source table. The output of the operation
    will be stored as a new metric in agg_population_metrics (the "destination" metric).
    """
    table: DatabaseTable
    metric: Metric
    """
    The operation to apply to the metric. If None, the metric field is used as the value
    """
    destination: str
    operation: str = None
    filter_condition: str = ""
    """
    Whether or not the metric has discrete values (e.g. engine state, power state).
    """
    is_discrete_metric: bool = False
    """
    List of percentiles to calculate for the metric.
    """
    percentiles: List[float] = field(default_factory=lambda: DEFAULT_PERCENTILES)


def ops_metric_to_dest(ops_metric: OpsMetric):
    """Gets a Metric referring to the output of an OpsMetric in agg_population_metrics."""
    return Metric(
        type=ProductAnalytics.AGG_POPULATION_METRICS,
        field=None,
        label=ops_metric.destination
    )

CONDITION_DIAGNOSTICS_CAPABLE: str = "AND dims.diagnostics_capable"

OPS: List[OpsMetric] = [
    OpsMetric(
        table=ProductAnalytics.AGG_DEVICE_STATS_PRIMARY,
        metric=fct_osdobdvaluelockadded.TableMetric.DELAY_MS,
        operation="count",
        destination="obd_value_lock_added_count",
    ),
    OpsMetric(
        table=ProductAnalytics.AGG_DEVICE_STATS_PRIMARY,
        metric=KinesisStatsMetric.OSD_HUB_SERVER_DEVICE_HEARTBEAT_BUILD,
        operation="count",
        destination="hub_server_device_heartbeat_build_count",
    ),
    OpsMetric(
        table=ProductAnalytics.AGG_DEVICE_STATS_PRIMARY,
        metric=agg_osdcommandschedulerstats.TableMetric.STOPPED,
        operation="max",
        destination="command_scheduler_stopped_max",
    ),
    OpsMetric(
        table=ProductAnalytics.AGG_DEVICE_STATS_PRIMARY,
        metric=agg_osdcommandschedulerstats.TableMetric.EXPIRED,
        operation="max",
        destination="command_scheduler_expired_max",
    ),
    OpsMetric(
        table=ProductAnalytics.AGG_DEVICE_STATS_PRIMARY,
        metric=fct_osdenginefault.TableMetric.J1939_FAULT_COUNT,
        operation="sum / count",
        destination="j1939_fault_count",
    ),
    OpsMetric(
        table=ProductAnalytics.AGG_DEVICE_STATS_PRIMARY,
        metric=fct_osdenginefault.TableMetric.PASSENGER_FAULT_COUNT,
        operation="sum / count",
        destination="passenger_fault_count",
    ),
    OpsMetric(
        table=ProductAnalytics.AGG_DEVICE_STATS_PRIMARY,
        metric=fct_osdj1939claimedaddress.TableMetric.NO_OPEN_ADDRESS_FOUND,
        operation="sum > 0",
        destination="j1939_no_open_address_found",
    ),
    OpsMetric(
        table=ProductAnalytics.AGG_DEVICE_STATS_PRIMARY,
        metric=fct_osdcanprotocolsdetected.TableMetric.DETECTED_PROTOCOL,
        operation="ARRAY_CONTAINS(histogram.x, 2)",
        destination="can_protocol_detected_protocol",
    ),
    OpsMetric(
        table=ProductAnalytics.AGG_DEVICE_STATS_PRIMARY,
        metric=KinesisStatsMetric.FUEL_TYPE_INT_VALUE,
        operation="count",
        destination="fuel_type_count"
    ),
    OpsMetric(
        table=ProductAnalytics.AGG_DEVICE_STATS_PRIMARY,
        metric=KinesisStatsMetric.ODOMETER_INT_VALUE,
        operation="count",
        destination="odometer_count",
    ),
    OpsMetric(
        table=ProductAnalytics.AGG_DEVICE_STATS_PRIMARY,
        metric=KinesisStatsMetric.OBD_CABLE_ID_INT_VALUE,
        operation="count",
        destination="obd_cable_id_count",
    ),
    OpsMetric(
        table=ProductAnalytics.AGG_DEVICE_STATS_PRIMARY,
        metric=KinesisStatsMetric.ENGINE_STATE_INT_VALUE,
        operation="count",
        destination="engine_state_count",
    ),
    OpsMetric(
        table=ProductAnalytics.AGG_DEVICE_STATS_PRIMARY,
        metric=KinesisStatsMetric.ENGINE_ACTIVITY_METHOD_INT_VALUE,
        operation="mode",
        destination="engine_activity_method_mode",
        is_discrete_metric=True
    ),
    OpsMetric(
        table=ProductAnalytics.AGG_DEVICE_STATS_PRIMARY,
        metric=KinesisStatsMetric.ENGINE_SECONDS_INT_VALUE,
        operation="count",
        destination="engine_seconds_count",
    ),
    OpsMetric(
        table=ProductAnalytics.AGG_DEVICE_STATS_PRIMARY,
        metric=KinesisStatsMetric.ENGINE_GAUGE_ENGINE_RPM,
        operation="count",
        destination="engine_rpm_count",
    ),
    OpsMetric(
        table=ProductAnalytics.AGG_DEVICE_STATS_PRIMARY,
        metric=KinesisStatsMetric.ENGINE_GAUGE_FUEL_LEVEL_PERCENT,
        operation="count",
        destination="fuel_level_percent_count",
    ),
    OpsMetric(
        table=ProductAnalytics.AGG_DEVICE_STATS_PRIMARY,
        metric=fct_osdvin.TableMetric.VIN_COUNT,
        operation="sum",  # total # of VIN events across all devices
        destination="vin_count",
    ),
    OpsMetric(
        table=ProductAnalytics.AGG_DEVICE_STATS_PRIMARY,
        metric=agg_stat_coverage_on_drive.TableMetric.GPS_CABLE_NON_ZERO_VOLTAGE_PERCENT,
        operation="avg",
        destination="on_drive_gps_cable_non_zero_voltage_percent",
        filter_condition=CONDITION_DIAGNOSTICS_CAPABLE,
    ),
    OpsMetric(
        table=ProductAnalytics.AGG_DEVICE_STATS_PRIMARY,
        metric=agg_stat_coverage_on_drive.TableMetric.GPS_CAN_CONNECTED_PERCENT,
        operation="avg",
        destination="on_drive_gps_can_connected_percent",
        filter_condition=CONDITION_DIAGNOSTICS_CAPABLE,
    ),
    OpsMetric(
        table=ProductAnalytics.AGG_DEVICE_STATS_PRIMARY,
        metric=agg_stat_coverage_on_drive.TableMetric.GPS_ENGINE_IDLE_PERCENT,
        operation="avg",
        destination="on_drive_gps_engine_idle_percent",
        filter_condition=CONDITION_DIAGNOSTICS_CAPABLE,
    ),
    OpsMetric(
        table=ProductAnalytics.AGG_DEVICE_STATS_PRIMARY,
        metric=agg_stat_coverage_on_drive.TableMetric.GPS_ENGINE_ON_PERCENT,
        operation="avg",
        destination="on_drive_gps_engine_on_percent",
        filter_condition=CONDITION_DIAGNOSTICS_CAPABLE,
    ),
    OpsMetric(
        table=ProductAnalytics.AGG_DEVICE_STATS_PRIMARY,
        metric=agg_stat_coverage_on_drive.TableMetric.GPS_ENGINE_OFF_PERCENT,
        operation="avg",
        destination="on_drive_gps_engine_off_percent",
        filter_condition=CONDITION_DIAGNOSTICS_CAPABLE,
    ),
    OpsMetric(
        table=ProductAnalytics.AGG_DEVICE_STATS_PRIMARY,
        metric=agg_stat_coverage_on_drive.TableMetric.HAS_ECU_SPEED_PERCENT,
        operation="avg",
        destination="on_drive_has_ecu_speed_percent",
        filter_condition=CONDITION_DIAGNOSTICS_CAPABLE,
    ),
    OpsMetric(
        table=ProductAnalytics.AGG_DEVICE_STATS_PRIMARY,
        metric=fct_enginestateerrors.TableMetric.COUNT_ENGINE_ON_ERRORS,
        operation="sum",
        destination="count_engine_state_on_errors",
    ),
    OpsMetric(
        table=ProductAnalytics.AGG_DEVICE_STATS_PRIMARY,
        metric=fct_enginestateerrors.TableMetric.COUNT_ENGINE_OFF_ERRORS,
        operation="sum",
        destination="count_engine_state_off_errors",
    ),
    OpsMetric(
        table=ProductAnalytics.AGG_DEVICE_STATS_PRIMARY,
        metric=fct_enginestateerrors.TableMetric.COUNT_ENGINE_IDLE_ERRORS,
        operation="sum",
        destination="count_engine_state_idle_errors"
    ),
    OpsMetric(
        table=ProductAnalytics.AGG_DEVICE_STATS_PRIMARY,
        metric=fct_enginestateerrors.TableMetric.COUNT_MISSED_IDLE_ERRORS,
        operation="sum",
        destination="count_engine_state_missed_idle_errors"
    ),
    OpsMetric(
        table=ProductAnalytics.AGG_DEVICE_STATS_PRIMARY,
        metric=fct_enginestateerrors.TableMetric.COUNT_ENGINE_ON_ERRORS_LOG_ONLY,
        operation="sum",
        destination="count_engine_state_on_errors_log_only",
    ),
    OpsMetric(
        table=ProductAnalytics.AGG_DEVICE_STATS_PRIMARY,
        metric=fct_enginestateerrors.TableMetric.COUNT_ENGINE_OFF_ERRORS_LOG_ONLY,
        operation="sum",
        destination="count_engine_state_off_errors_log_only",
    ),
    OpsMetric(
        table=ProductAnalytics.AGG_DEVICE_STATS_PRIMARY,
        metric=fct_enginestateerrors.TableMetric.COUNT_ENGINE_IDLE_ERRORS_LOG_ONLY,
        operation="sum",
        destination="count_engine_state_idle_errors_log_only"
    ),
    OpsMetric(
        table=ProductAnalytics.AGG_DEVICE_STATS_PRIMARY,
        metric=fct_enginestateerrors.TableMetric.COUNT_MISSED_IDLE_ERRORS_LOG_ONLY,
        operation="sum",
        destination="count_engine_state_missed_idle_errors_log_only"
    ),
    OpsMetric(
        table=ProductAnalytics.AGG_DEVICE_STATS_PRIMARY,
        metric=fct_enginestateerrors.TableMetric.PERCENT_ENGINE_OFF_ERRORS,
        operation="avg",
        destination="avg_percent_engine_state_off_errors",
        percentiles=ENGINE_STATE_STAT_PERCENTILES,
    ),
    OpsMetric(
        table=ProductAnalytics.AGG_DEVICE_STATS_PRIMARY,
        metric=fct_enginestateerrors.TableMetric.PERCENT_ENGINE_ON_ERRORS,
        operation="avg",
        destination="avg_percent_engine_state_on_errors",
        percentiles=ENGINE_STATE_STAT_PERCENTILES,
    ),
    OpsMetric(
        table=ProductAnalytics.AGG_DEVICE_STATS_PRIMARY,
        metric=fct_enginestateerrors.TableMetric.PERCENT_ENGINE_IDLE_ERRORS,
        operation="avg",
        destination="avg_percent_engine_state_idle_errors",
        percentiles=ENGINE_STATE_STAT_PERCENTILES,
    ),
    OpsMetric(
        table=ProductAnalytics.AGG_DEVICE_STATS_PRIMARY,
        metric=fct_enginestateerrors.TableMetric.PERCENT_MISSED_IDLE_ERRORS,
        operation="avg",
        destination="avg_percent_engine_state_missed_idle_errors",
        percentiles=ENGINE_STATE_STAT_PERCENTILES,
    ),
    OpsMetric(
        table=ProductAnalytics.AGG_DEVICE_STATS_PRIMARY,
        metric=fct_enginestateerrors.TableMetric.PERCENT_ENGINE_OFF_ERRORS_LOG_ONLY,
        operation="avg",
        destination="avg_percent_engine_state_off_errors_log_only",
        percentiles=ENGINE_STATE_STAT_PERCENTILES,
    ),
    OpsMetric(
        table=ProductAnalytics.AGG_DEVICE_STATS_PRIMARY,
        metric=fct_enginestateerrors.TableMetric.PERCENT_ENGINE_ON_ERRORS_LOG_ONLY,
        operation="avg",
        destination="avg_percent_engine_state_on_errors_log_only",
        percentiles=ENGINE_STATE_STAT_PERCENTILES,
    ),
    OpsMetric(
        table=ProductAnalytics.AGG_DEVICE_STATS_PRIMARY,
        metric=fct_enginestateerrors.TableMetric.PERCENT_ENGINE_IDLE_ERRORS_LOG_ONLY,
        operation="avg",
        destination="avg_percent_engine_state_idle_errors_log_only",
        percentiles=ENGINE_STATE_STAT_PERCENTILES,
    ),
    OpsMetric(
        table=ProductAnalytics.AGG_DEVICE_STATS_PRIMARY,
        metric=fct_enginestateerrors.TableMetric.PERCENT_MISSED_IDLE_ERRORS_LOG_ONLY,
        operation="avg",
        destination="avg_percent_engine_state_missed_idle_errors_log_only",
        percentiles=ENGINE_STATE_STAT_PERCENTILES,
    ),
    OpsMetric(
        table=ProductAnalytics.AGG_DEVICE_STATS_PRIMARY,
        metric=fct_enginestate_comparison_stats.TableMetric.PCT_ENGINE_STATE_DISAGREEMENT,
        operation="avg",
        destination="avg_pct_engine_state_disagreement"
    ),
    OpsMetric(
        table=ProductAnalytics.AGG_DEVICE_STATS_PRIMARY,
        metric=fct_enginestate_method.TableMetric.IS_LOG_ONLY_ESR,
        operation="sum",
        destination="sum_engine_state_method_is_log_only_esr"
    ),
    OpsMetric(
        table=ProductAnalytics.AGG_DEVICE_STATS_PRIMARY,
        metric=fct_enginestate_method.TableMetric.IS_PROD_ESR,
        operation="sum",
        destination="sum_engine_state_method_is_prod_esr"
    ),
    OpsMetric(
        table=ProductAnalytics.AGG_DEVICE_STATS_PRIMARY,
        metric=fct_engine_segment_stats.TableMetric.IDLE_DURATION_MS,
        operation="sum",
        destination="sum_idle_duration_ms"
    ),
]


@dataclass
class PopulationMetric:
    """Operation to be performed on multiple source metrics."""
    table: str
    operation: str
    metrics: List[Metric]
    prefix: str
    filter_condition: str = ""


SETTINGS = [
    PopulationMetric(
        table=ProductAnalytics.AGG_DEVICE_STATS_SECONDARY_CONSISTENCY,
        metrics=agg_device_stats_secondary_consistency.METRICS,
        operation=agg_device_stats_secondary_consistency.TableDimension.IS_CONSISTENT.value,
        prefix="consistency",
    ),
    PopulationMetric(
        table=ProductAnalytics.AGG_DEVICE_STATS_SECONDARY_RATIO,
        metrics=agg_device_stats_secondary_ratio.METRICS,
        operation=agg_device_stats_secondary_ratio.TableDimension.VALUE.value,
        prefix="ratio",
    ),
    PopulationMetric(
        table=ProductAnalytics.AGG_DEVICE_STATS_SECONDARY_RESPONSE_RATIO,
        metrics=agg_device_stats_secondary_response_ratio.METRICS,
        operation=agg_device_stats_secondary_response_ratio.TableDimension.VALUE.value,
        prefix="response_ratio",
    ),
    PopulationMetric(
        table=ProductAnalytics.AGG_DEVICE_STATS_SECONDARY_COVERAGE,
        metrics=agg_device_stats_secondary_coverage.METRICS,
        operation=agg_device_stats_secondary_coverage.TableDimension.IS_COVERED.value,
        prefix="coverage",
        filter_condition=CONDITION_DIAGNOSTICS_CAPABLE,
    ),
    PopulationMetric(
        table=ProductAnalytics.AGG_DEVICE_STATS_PRIMARY,
        metrics=ANOMALY_COUNT_METRICS,
        operation="""COALESCE(count_bound_anomaly, 0)
            + COALESCE(count_delta_anomaly, 0)
            + COALESCE(count_delta_over_time_anomaly, 0)""",
        prefix="anomaly_count",
    ),
    PopulationMetric(
        table=ProductAnalytics.AGG_DEVICE_STATS_PRIMARY,
        metrics=ANOMALY_COUNT_METRICS,
        operation="""CAST(
            COALESCE(count_bound_anomaly, 0)
            + COALESCE(count_delta_anomaly, 0)
            + COALESCE(count_delta_over_time_anomaly, 0)
        AS DOUBLE
        ) / count""",
        prefix="percent_invalid",
    ),
]


OPS = OPS + [
    OpsMetric(
        table=setting.table,
        metric=metric,
        operation=setting.operation,
        destination=f"{setting.prefix}_{metric}",
        filter_condition=setting.filter_condition,
    )
    for setting in SETTINGS
    for metric in setting.metrics
]

METRICS = [ops_metric_to_dest(setting) for setting in OPS]

# TODO VDP-5019: Add upstream dependency on agg_device_stats_primary
UPSTREAMS = [
    AnyUpstream(ProductAnalytics.DIM_DEVICE_DIMENSIONS),
    AnyUpstream(ProductAnalytics.DIM_POPULATIONS),
]

MAPPING = {str(metric.destination): metric for metric in OPS}
METRIC_KEYS = sorted(MAPPING.keys())

PARTITIONS = MultiPartitionsDefinition(
    {
        str(ColumnType.DATE): DATAWEB_PARTITION_DATE,
        str(ColumnType.METRIC_NAME): StaticPartitionsDefinition(
            partition_keys=METRIC_KEYS,
        ),
    }
)

NON_NULL_COLUMNS = schema_to_columns_with_property(SCHEMA, "nullable")

# Empty discrete groupings query for non-discrete metrics
EMPTY_DISCRETE_GROUPINGS_QUERY = f"""
    SELECT
      CAST(NULL AS DATE) AS date,
      CAST(NULL AS STRING) AS population_id,
      CAST(ARRAY() AS ARRAY<STRUCT<x: DOUBLE, y: DOUBLE>>) AS histogram_discrete
    WHERE FALSE
"""

DISCRETE_GROUPINGS_QUERY = """
    WITH discreteStatsPerValue AS (
        SELECT
            date,
            value,
            COUNT(*) AS count,
            {population_id_value_expr} AS population_id
        FROM data
        GROUP BY
        date
        , value
        , GROUPING SETS (
        {grouping_sets}
        )
    )
    SELECT
      date,
      population_id,
      COLLECT_LIST(named_struct('x', value, 'y', CAST(count AS DOUBLE))) AS histogram_discrete
    FROM discreteStatsPerValue
    GROUP BY
      date,
      population_id
"""

QUERY = """

WITH
    stats AS (
        SELECT
            date
            , org_id
            , device_id
            , CAST({metric.operation} AS DOUBLE) AS value

        FROM {source_table} AS stats

        WHERE
            date BETWEEN "{date_start}" AND "{date_end}"
            AND type = '{metric.metric}'
    )

    , data AS (
        SELECT
            dims.date
            , stats.value
            , stats.device_id IS NOT NULL AND stats.value IS NOT NULL as valid
            , {dim_dimension_columns}

        FROM {product_analytics}.dim_device_dimensions AS dims

        -- Left join to allow full counts of populations where a device may
        -- not have a metric. This implies that NULL values are ignored in the
        -- aggregate measures, unless you account for a value being NULL.
        LEFT JOIN stats USING (date, org_id, device_id)

        WHERE
            dims.date BETWEEN "{date_start}" AND "{date_end}"
            AND dims.include_in_agg_metrics
            {filter_condition}

        {subsample}
    )

    , discreteGroupings AS (
      {discrete_groupings_query}
    )

    , groupings AS (
        SELECT
            date
            , {population_id_value_expr} AS population_id
            , COUNT_IF(valid) AS count
            , SUM(value) AS sum
            , MIN(value) AS min
            , MAX(value) AS max
            , AVG(value) AS avg
            , VARIANCE(value) AS variance
            , STDDEV(value) AS stddev
            , MEAN(value) AS mean
            , MEDIAN(value) AS median
            , MODE(value) AS mode
            , FIRST(value) AS first
            , LAST(value) AS last
            , KURTOSIS(value) AS kurtosis
            , ZIP_WITH({pct}, PERCENTILE_APPROX(value, {pct}), (x, y) -> (DOUBLE(x) AS x, DOUBLE(y) AS y)) AS percentile
            , HISTOGRAM_NUMERIC(value, {histogram_buckets}) AS histogram
            , COUNT(1) AS population_count

        FROM
            data

        GROUP BY
            date
            , GROUPING SETS (
                {grouping_sets}
            )
        )

    SELECT
        a.date
        , a.population_id
        , "{metric.destination}" AS metric_name
        , a.count
        , a.sum
        , a.min
        , a.max
        , a.avg
        , a.variance
        , a.stddev
        , a.mean
        , a.median
        , a.mode
        , a.first
        , a.last
        , a.kurtosis
        , a.percentile
        , a.histogram
        , a.population_count
        , b.histogram_discrete

    FROM
        groupings a
    LEFT JOIN discreteGroupings b
        USING (date, population_id)

"""

databases = get_databases()


@table(
    database=Database.PRODUCT_ANALYTICS,
    description=build_table_description(
        table_desc="Aggregated metrics over populations of devices.",
        row_meaning="Aggregated stats for devices",
        table_type=TableType.AGG,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=PARTITIONS,
    upstreams=UPSTREAMS,
    write_mode=WarehouseWriteMode.OVERWRITE,
    concurrency_key=ConcurrencyKey.VDP,
    single_run_backfill=True,
    run_config_overrides=create_run_config_overrides(
        min_workers=1,
        max_workers=16,
        driver_instance_type=InstanceType.MD_FLEET_4XLARGE,
        worker_instance_type=InstanceType.MD_FLEET_4XLARGE,
    ),
    dq_checks=[
        JoinableDQCheck(
            name="dq_joinable_agg_population_metrics",
            database_2=databases[Database.PRODUCT_ANALYTICS],
            input_asset_2=ProductAnalytics.DIM_POPULATIONS.value,
            join_keys=[("date", "date"), ("population_id", "population_id")],
            block_before_write=True
        ),
        *build_general_dq_checks(
            asset_name=ProductAnalytics.AGG_POPULATION_METRICS,
            primary_keys=PRIMARY_KEYS,
            non_null_keys=NON_NULL_COLUMNS,
            block_before_write=True,
        ),
    ],
    dq_check_mode=DQCheckMode.WHOLE_RESULT,
    max_retries=5,
)
def agg_population_metrics(context: AssetExecutionContext) -> DataFrame:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

    partition_ranges = partition_key_ranges_from_range_tags(context)
    date_range = partition_ranges[0]
    context.log.info(f"Date range: {date_range}")

    metric_start, metric_end = partition_ranges[1]
    metric_range = METRIC_KEYS[METRIC_KEYS.index(metric_start):METRIC_KEYS.index(metric_end) + 1]
    context.log.info(f"Metric range: {metric_range}")

    def _table_name(stat: OpsMetric) -> str:
        dbtable = str(stat.table).split(".")
        return f"{databases[dbtable[0]]}.{dbtable[1]}"

    def _format_query(metric: OpsMetric) -> str:
        population_id_expr = population_id_value_expr("GROUPING_ID()", Population.MOST_GRANULAR)

        return QUERY.format(
            product_analytics=databases[Database.PRODUCT_ANALYTICS],
            source_table=_table_name(metric),
            metric=metric,
            date_start=date_range[0],
            date_end=date_range[-1],
            pct=f"array({', '.join(map(str, metric.percentiles))})",
            histogram_buckets=20,
            grouping_sets=GROUPING_SETS,
            dimension_columns=DEVICE_COLUMNS,
            population_id_value_expr=population_id_expr,
            dim_dimension_columns=", ".join(
                f"dims.{dimension}" for dimension in Population.MOST_GRANULAR.value
            ),
            device_level_match=" AND ".join(
                f"a.{dimension} <=> b.{dimension}"
                for dimension in Population.MOST_GRANULAR.value
            ),
            subsample="" if get_run_env() == RunEnvironment.PROD else "LIMIT 1000",
            filter_condition=metric.filter_condition,
            discrete_groupings_query=DISCRETE_GROUPINGS_QUERY.format(
                population_id_value_expr=population_id_expr,
                grouping_sets=GROUPING_SETS,
            ) if metric.is_discrete_metric else EMPTY_DISCRETE_GROUPINGS_QUERY,
        )

    # Calculating all DFs needs to happen in a loop to avoid
    # list comprehension lazy evaluation. Otherwise, the Spark
    # fails with a missing property error within reduce.
    def _calculate_dfs() -> List[DataFrame]:
        dfs = []
        for metric in metric_range:
            stat = MAPPING[metric]
            assert stat != None
            context.log.info(f"Source: {stat.table}")
            context.log.info(f"Metrics: {stat.metric}")
            query = _format_query(stat)
            context.log.info(f"{query}")

            # Reduce all subqueries down to one partition to prevent Spark from excessively serializing/deserializing partitions
            result = spark.sql(query).repartition(1, ColumnType.DATE.name, ColumnType.METRIC_NAME.name)
            dfs.append(result)

        return dfs

    return merge_pyspark_dataframes(_calculate_dfs())
