from typing import List

from dagster import (
    AssetExecutionContext,
    MultiPartitionsDefinition,
    StaticPartitionsDefinition,
)
from dataweb import build_general_dq_checks, table
from dataweb.assets.datasets.product_analytics_staging import (
    agg_osdcommandschedulerstats,
    agg_stat_coverage_on_drive,
    fct_engine_segment_stats,
    fct_enginestateerrors,
    fct_enginestate_method,
    fct_enginestate_comparison_stats,
    fct_osdcanbitratedetectionv2,
    fct_osdcanprotocolsdetected,
    fct_osdcommandschedulerstats,
    fct_osdenginefault,
    fct_hourlyfuelconsumption,
    fct_osdj1708can3busautodetectresult,
    fct_osdj1939claimedaddress,
    fct_osdobdvaluelockadded,
    fct_osdvin,
    fct_statechangeevents,
    stg_daily_faults,
)
from dataweb.userpkgs.constants import (
    ALL_COMPUTE_REGIONS,
    FIRMWAREVDP,
    FRESHNESS_SLO_9AM_PST,
    Database,
    DQCheckMode,
    InstanceType,
    TableType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.firmware.constants import (
    DATAWEB_PARTITION_DATE,
    ConcurrencyKey,
)
from dataweb.userpkgs.query import create_run_config_overrides
from dataweb.userpkgs.query import (
    format_date_partition_query,
)
from dataweb.userpkgs.firmware.kinesisstats import KinesisStatsMetric
from dataweb.userpkgs.firmware.metric import MetricEnum
from dataweb.userpkgs.firmware.schema import (
    ColumnType,
    columns_to_names,
    columns_to_schema,
)
from dataweb.userpkgs.firmware.table import ProductAnalytics, ProductAnalyticsStaging
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.utils import (
    build_table_description,
    schema_to_columns_with_property,
    get_partition_ranges_from_context,
    run_partitioned_queries,
)
from dataweb.userpkgs.extract_utils import select_extract_pattern
from pyspark.sql import DataFrame

SCHEMA = columns_to_schema(
    ColumnType.DATE,
    ColumnType.TYPE,
    ColumnType.ORG_ID,
    ColumnType.DEVICE_ID,
    ColumnType.BUS_ID,
    ColumnType.REQUEST_ID,
    ColumnType.RESPONSE_ID,
    ColumnType.OBD_VALUE,
    ColumnType.SUM,
    ColumnType.COUNT,
    ColumnType.AVG,
    ColumnType.STDDEV,
    ColumnType.VARIANCE,
    ColumnType.MEAN,
    ColumnType.MEDIAN,
    ColumnType.MODE,
    ColumnType.FIRST,
    ColumnType.LAST,
    ColumnType.KURTOSIS,
    ColumnType.PERCENTILE,
    ColumnType.HISTOGRAM,
    ColumnType.MIN,
    ColumnType.MAX,
    ColumnType.COUNT_BOUND_ANOMALY,
    ColumnType.COUNT_DELTA_ANOMALY,
    ColumnType.COUNT_DELTA_OVER_TIME_ANOMALY,
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

METRICS: List[MetricEnum] = [
    KinesisStatsMetric.ENGINE_GAUGE_AIR_TEMP_MILLI_C,
    KinesisStatsMetric.ENGINE_GAUGE_BATTERY_MILLI_V,
    KinesisStatsMetric.ENGINE_GAUGE_COOLANT_TEMP_MILLI_C,
    KinesisStatsMetric.ENGINE_GAUGE_ENGINE_LOAD_PERCENT,
    KinesisStatsMetric.ENGINE_GAUGE_ENGINE_RPM,
    KinesisStatsMetric.ENGINE_GAUGE_FUEL_LEVEL_PERCENT,
    KinesisStatsMetric.ENGINE_GAUGE_OIL_PRESSURE_K_PA,
    KinesisStatsMetric.ENGINE_GAUGE_TIRE_PRESSURE_BACK_LEFT_K_PA,
    KinesisStatsMetric.ENGINE_GAUGE_TIRE_PRESSURE_BACK_RIGHT_K_PA,
    KinesisStatsMetric.ENGINE_GAUGE_TIRE_PRESSURE_FRONT_LEFT_K_PA,
    KinesisStatsMetric.ENGINE_GAUGE_TIRE_PRESSURE_FRONT_RIGHT_K_PA,
    KinesisStatsMetric.OSD_HUB_SERVER_DEVICE_HEARTBEAT_BUILD,
    KinesisStatsMetric.AT1_DPF_SOOT_LOAD_PERCENT_INT_VALUE,
    KinesisStatsMetric.OSD_CAN_CONNECTED_INT_VALUE,
    KinesisStatsMetric.COASTING_TIME_TORQUE_BASED_IGNORING_BRAKE_MS_INT_VALUE,
    KinesisStatsMetric.DEF_TANK_LEVEL_MILLI_PERCENT_INT_VALUE,
    KinesisStatsMetric.DELTA_ACCEL_ENGINE_TORQUE_OVER_LIMIT_MS_INT_VALUE,
    KinesisStatsMetric.DELTA_ACCEL_ENGINE_TORQUE_OVER_LIMIT_WHILE_NOT_ON_CRUISE_CONTROL_MS_INT_VALUE,
    KinesisStatsMetric.DELTA_ACCELERATOR_PEDAL_TIME_GREATER_THAN_95_PERCENT_MS_INT_VALUE,
    KinesisStatsMetric.DELTA_ANTICIPATION_BRAKE_EVENTS_INT_VALUE,
    KinesisStatsMetric.OSD_DELTA_BRAKE_EVENTS_INT_VALUE,
    KinesisStatsMetric.DELTA_COASTING_TIME_MS_INT_VALUE,
    KinesisStatsMetric.DELTA_COASTING_TIME_TORQUE_BASED_WHILE_NOT_ON_CRUISE_CONTROL_MS_INT_VALUE,
    KinesisStatsMetric.DELTA_COASTING_TIME_WHILE_NOT_ON_CRUISE_CONTROL_MS_INT_VALUE,
    KinesisStatsMetric.DELTA_CRUISE_CONTROL_MS_INT_VALUE,
    KinesisStatsMetric.DELTA_EV_CHARGING_ENERGY_MICRO_WH_INT_VALUE,
    KinesisStatsMetric.DELTA_EV_DISTANCE_DRIVEN_ON_ELECTRIC_POWER_METERS_INT_VALUE,
    KinesisStatsMetric.DELTA_EV_ENERGY_CONSUMED_MICRO_WH_INT_VALUE,
    KinesisStatsMetric.DELTA_EV_TOTAL_ENERGY_REGENERATED_WHILE_DRIVING_MICRO_WH_INT_VALUE,
    KinesisStatsMetric.DELTA_FUEL_CONSUMED_INT_VALUE,
    KinesisStatsMetric.DELTA_GPS_DISTANCE_DOUBLE_VALUE,
    KinesisStatsMetric.DELTA_RPM_GREEN_BAND_MS_INT_VALUE,
    KinesisStatsMetric.DERIVED_ENGINE_SECONDS_FROM_IGNITION_CYCLE_INT_VALUE,
    KinesisStatsMetric.ENGINE_MILLI_KNOTS_INT_VALUE,
    KinesisStatsMetric.ENGINE_SECONDS_INT_VALUE,
    KinesisStatsMetric.ENGINE_STATE_INT_VALUE,
    KinesisStatsMetric.EV_AVERAGE_CELL_TEMPERATURE_MILLI_CELSIUS_INT_VALUE,
    KinesisStatsMetric.EV_CHARGER_MILLI_AMP_INT_VALUE,
    KinesisStatsMetric.EV_CHARGER_MILLI_VOLT_INT_VALUE,
    KinesisStatsMetric.EV_CHARGING_STATUS_INT_VALUE,
    KinesisStatsMetric.EV_HIGH_CAPACITY_BATTERY_MILLI_AMP_INT_VALUE,
    KinesisStatsMetric.EV_HIGH_CAPACITY_BATTERY_MILLI_VOLT_INT_VALUE,
    KinesisStatsMetric.EV_HIGH_CAPACITY_BATTERY_STATE_OF_HEALTH_MILLI_PERCENT_INT_VALUE,
    KinesisStatsMetric.EV_USABLE_STATE_OF_CHARGE_MILLI_PERCENT_INT_VALUE,
    KinesisStatsMetric.FUEL_TYPE_INT_VALUE,
    KinesisStatsMetric.J1939_CVW_GROSS_COMBINATION_VEHICLE_WEIGHT_KILOGRAMS_INT_VALUE,
    KinesisStatsMetric.OBD_CABLE_ID_INT_VALUE,
    KinesisStatsMetric.ODOMETER_INT_VALUE,
    KinesisStatsMetric.POWER_TAKE_OFF_INT_VALUE,
    KinesisStatsMetric.SEAT_BELT_DRIVER_INT_VALUE,
    KinesisStatsMetric.DPF_LAMP_STATUS_INT_VALUE,
    KinesisStatsMetric.DOOR_LOCK_STATUS_INT_VALUE,
    KinesisStatsMetric.DOOR_OPEN_STATUS_INT_VALUE,
    KinesisStatsMetric.VEHICLE_CURRENT_GEAR_INT_VALUE,
    KinesisStatsMetric.TELL_TALE_STATUS_INT_VALUE,
    KinesisStatsMetric.J1939_OEL_TURN_SIGNAL_SWITCH_STATE_INT_VALUE,
    KinesisStatsMetric.J1939_LCMD_LEFT_TURN_SIGNAL_LIGHTS_COMMAND_STATE_INT_VALUE,
    KinesisStatsMetric.J1939_LCMD_RIGHT_TURN_SIGNAL_LIGHTS_COMMAND_STATE_INT_VALUE,
    KinesisStatsMetric.J1939_ETC5_TRANSMISSION_REVERSE_DIRECTION_SWITCH_STATE_INT_VALUE,
    KinesisStatsMetric.J1939_TCO1_DIRECTION_INDICATOR_STATE_INT_VALUE,
    KinesisStatsMetric.J1939_VDC2_STEERING_WHEEL_ANGLE_PICO_RAD_INT_VALUE,
    KinesisStatsMetric.J1939_VDC2_YAW_RATE_FEMTO_RAD_PER_SECONDS_INT_VALUE,
    KinesisStatsMetric.FUEL_LEVEL_2_DECI_PERCENT_INT_VALUE,
    KinesisStatsMetric.ENGINE_ACTIVITY_METHOD_INT_VALUE,
    fct_osdj1708can3busautodetectresult.TableMetric.DETECTED,
    fct_osdj1939claimedaddress.TableMetric.SOURCE_ADDRESS_CLAIMED,
    fct_osdj1939claimedaddress.TableMetric.NO_OPEN_ADDRESS_FOUND,
    fct_osdobdvaluelockadded.TableMetric.DELAY_MS,
    stg_daily_faults.TableMetric.FAULT_COUNT,
    fct_osdvin.TableMetric.VIN_COUNT,
    fct_osdvin.TableMetric.UNIQUE_VIN_COUNT,
    fct_statechangeevents.TableMetric.SEATBELT_TOGGLES,
    agg_osdcommandschedulerstats.TableMetric.EXPIRED,
    agg_osdcommandschedulerstats.TableMetric.STOPPED,
    agg_stat_coverage_on_drive.TableMetric.GPS_CABLE_NON_ZERO_VOLTAGE_PERCENT,
    agg_stat_coverage_on_drive.TableMetric.GPS_CAN_CONNECTED_PERCENT,
    agg_stat_coverage_on_drive.TableMetric.GPS_ENGINE_IDLE_PERCENT,
    agg_stat_coverage_on_drive.TableMetric.GPS_ENGINE_OFF_PERCENT,
    agg_stat_coverage_on_drive.TableMetric.GPS_ENGINE_ON_PERCENT,
    agg_stat_coverage_on_drive.TableMetric.HAS_ECU_SPEED_PERCENT,
    agg_stat_coverage_on_drive.TableMetric.TOTAL_COUNT,
    fct_osdcanbitratedetectionv2.TableMetric.BITRATE,
    fct_osdcanprotocolsdetected.TableMetric.DETECTED_PROTOCOL,
    fct_osdcanprotocolsdetected.TableMetric.DETECTED_PROTOCOL_COUNT,
    fct_osdcanprotocolsdetected.TableMetric.PROTOCOL_SELECTED_TO_RUN,
    fct_osdcommandschedulerstats.TableMetric.IS_STOPPED,
    fct_osdcommandschedulerstats.TableMetric.EXPIRATION_OFFSET_MS,
    fct_osdcommandschedulerstats.TableMetric.DATA_IDENTIFIER,
    fct_osdcommandschedulerstats.TableMetric.REQUEST_COUNT,
    fct_osdcommandschedulerstats.TableMetric.ANY_RESPONSE_COUNT,
    fct_osdcommandschedulerstats.TableMetric.TOTAL_RESPONSE_COUNT,
    fct_osdenginefault.TableMetric.J1939_FAULT_COUNT,
    fct_osdenginefault.TableMetric.PASSENGER_FAULT_COUNT,
    fct_hourlyfuelconsumption.TableMetric.GASEOUS_CONSUMPTION_RATE,
    fct_hourlyfuelconsumption.TableMetric.LIQUID_CONSUMPTION_RATE,
    fct_enginestateerrors.TableMetric.COUNT_ENGINE_ON_ERRORS,
    fct_enginestateerrors.TableMetric.COUNT_ENGINE_OFF_ERRORS,
    fct_enginestateerrors.TableMetric.COUNT_ENGINE_IDLE_ERRORS,
    fct_enginestateerrors.TableMetric.COUNT_MISSED_IDLE_ERRORS,
    fct_enginestateerrors.TableMetric.COUNT_ENGINE_ON_ERRORS_LOG_ONLY,
    fct_enginestateerrors.TableMetric.COUNT_ENGINE_OFF_ERRORS_LOG_ONLY,
    fct_enginestateerrors.TableMetric.COUNT_ENGINE_IDLE_ERRORS_LOG_ONLY,
    fct_enginestateerrors.TableMetric.COUNT_MISSED_IDLE_ERRORS_LOG_ONLY,
    fct_enginestateerrors.TableMetric.PERCENT_ENGINE_OFF_ERRORS,
    fct_enginestateerrors.TableMetric.PERCENT_ENGINE_ON_ERRORS,
    fct_enginestateerrors.TableMetric.PERCENT_ENGINE_IDLE_ERRORS,
    fct_enginestateerrors.TableMetric.PERCENT_MISSED_IDLE_ERRORS,
    fct_enginestateerrors.TableMetric.PERCENT_ENGINE_OFF_ERRORS_LOG_ONLY,
    fct_enginestateerrors.TableMetric.PERCENT_ENGINE_ON_ERRORS_LOG_ONLY,
    fct_enginestateerrors.TableMetric.PERCENT_ENGINE_IDLE_ERRORS_LOG_ONLY,
    fct_enginestateerrors.TableMetric.PERCENT_MISSED_IDLE_ERRORS_LOG_ONLY,
    fct_enginestate_method.TableMetric.IS_LOG_ONLY_ESR,
    fct_enginestate_method.TableMetric.IS_PROD_ESR,
    fct_enginestate_comparison_stats.TableMetric.PCT_ENGINE_STATE_DISAGREEMENT,
    fct_engine_segment_stats.TableMetric.IDLE_DURATION_MS,
]

DATABASE_TABLES = sorted(list(map(lambda x: f"{x.type}", METRICS)))

UPSTREAMS = list(map(lambda x: AnyUpstream(x), DATABASE_TABLES))

MAPPING = {f"{metric}": metric for metric in METRICS}

METRIC_KEYS = sorted(map(lambda metric: str(metric), METRICS))

PARTITIONS = MultiPartitionsDefinition(
    {
        str(ColumnType.DATE): DATAWEB_PARTITION_DATE,
        str(ColumnType.TYPE): StaticPartitionsDefinition(
            partition_keys=METRIC_KEYS,
        ),
    }
)

# Custom extract patterns for agg_device_stats_primary with additional columns
KINESIS_STATS_EXTRACT_FULL = """

SELECT
    date
    , time
    , org_id
    , object_id AS device_id
    , CAST(NULL AS LONG) AS bus_id
    , CAST(NULL AS LONG) AS request_id
    , CAST(NULL AS LONG) AS response_id
    , CAST(NULL AS LONG) AS obd_value
    , CAST({source.field} AS DOUBLE) AS value

FROM
    {source_table}

WHERE
    date BETWEEN "{date_start}" AND "{date_end}"
    AND NOT value.is_end
    AND NOT value.is_databreak

"""

DATAWEB_EXTRACT_FULL = """

SELECT
    date
    , time
    , org_id
    , device_id
    , bus_id
    , request_id
    , response_id
    , obd_value
    , CAST({source.field} AS DOUBLE) AS value

FROM
    {source_table}

WHERE
    date BETWEEN "{date_start}" AND "{date_end}"

"""

# TODO this join is unncessary if we have access to each metrics ata in python
# Remove the join with fct_telematics_stat_ata when possible
QUERY = """

WITH
    data AS (
        {extract}
    )

    , stat_values_prev as (
        SELECT
            date
            , time
            , org_id
            , device_id
            , bus_id
            , request_id
            , response_id
            , obd_value
            , value
            , LAG((time, value)) OVER (PARTITION BY org_id, device_id ORDER BY time) AS prev
        FROM
            data
    )

    , grouped AS (
        SELECT
            date
            , org_id
            , device_id
            , bus_id
            , request_id
            , response_id
            , obd_value
            , SUM(value) AS sum
            , COUNT(*) AS count
            , AVG(value) AS avg
            , STDDEV(value) AS stddev
            , VARIANCE(value) AS variance
            , MEAN(value) AS mean
            , MEDIAN(value) AS median
            , MODE(value) AS mode
            , FIRST((time, value)).value AS first
            , LAST((time, value)).value AS last
            , KURTOSIS(value) AS kurtosis

            , ZIP_WITH(
                    {pct}
                    , PERCENTILE_APPROX(value, {pct})
                    , (x, y) -> (DOUBLE(x) AS x, DOUBLE(y) AS y)
            ) AS percentile

            , HISTOGRAM_NUMERIC(value, {histogram_buckets}) AS histogram
            , MIN(value) AS min
            , MAX(value) AS max

            , COUNT_IF(
                value < {metric.metadata.value_min_sql_str}
                OR value > {metric.metadata.value_max_sql_str}
            ) AS count_bound_anomaly

            , COUNT_IF(
                (value - prev.value) < {metric.metadata.delta_min_sql_str}
                OR (value - prev.value) > {metric.metadata.delta_max_sql_str}
            ) AS count_delta_anomaly

            , COUNT_IF(
                (value - prev.value) / (time - prev.time) < {metric.metadata.delta_velocity_min_sql_str}
                OR (value - prev.value) / (time - prev.time) > {metric.metadata.delta_velocity_max_sql_str}
            ) AS count_delta_over_time_anomaly

        FROM
            stat_values_prev stat

        GROUP BY ALL
)

SELECT
    date
    , '{type}' AS type
    , org_id
    , device_id
    , bus_id
    , request_id
    , response_id
    , obd_value
    , sum
    , count
    , avg
    , stddev
    , variance
    , mean
    , median
    , mode
    , first
    , last
    , kurtosis
    , percentile
    , histogram
    , min
    , max
    , count_bound_anomaly
    , count_delta_anomaly
    , count_delta_over_time_anomaly

FROM
    grouped

"""


@table(
    database=Database.PRODUCT_ANALYTICS,
    description=build_table_description(
        table_desc="Derive aggregate statistics over a single day per device. This is meant to be consumed upstream to generate more device level metrics based on simple aggregate measures.",
        row_meaning="Aggregated statistics for a given device",
        related_table_info={},
        table_type=TableType.MONTHLY_REPORTING_AGG,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=PARTITIONS,
    upstreams=[
        *UPSTREAMS,
        AnyUpstream(ProductAnalyticsStaging.FCT_TELEMATICS_STAT_METADATA),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    concurrency_key=ConcurrencyKey.VDP,
    single_run_backfill=True,
    dq_check_mode=DQCheckMode.WHOLE_RESULT,
    run_config_overrides=create_run_config_overrides(
        min_workers=1,
        max_workers=32,
        driver_instance_type=InstanceType.MD_FLEET_4XLARGE,
        worker_instance_type=InstanceType.MD_FLEET_4XLARGE,
    ),
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalytics.AGG_DEVICE_STATS_PRIMARY.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_KEYS,
        block_before_write=True,
    ),
    max_retries=5,
)
def agg_device_stats_primary(context: AssetExecutionContext) -> DataFrame:
    partitions = get_partition_ranges_from_context(context, full_secondary_keys=METRIC_KEYS)

    def build_query(metric_type: str) -> str:
        # Get the metric and select appropriate extract pattern
        metric = MAPPING[metric_type]

        # Use custom extract patterns for this asset since it needs additional columns
        extract = select_extract_pattern(
            metric=metric,
            kinesis_stats_extract=KINESIS_STATS_EXTRACT_FULL,
            dataweb_extract=DATAWEB_EXTRACT_FULL,
            date_start=partitions.date_start,
            date_end=partitions.date_end,
        )

        formatted_query = format_date_partition_query(
            QUERY,
            context,
            partitions=partitions,
            type=metric_type,
            extract=extract,
            metric=metric,
            pct="array(.01, .05, .1, .2, .5, .7, .8, .9, .95, .99)",
            histogram_buckets=20,
        )

        return formatted_query

    return run_partitioned_queries(
        context=context,
        query_builder=build_query,
        secondary_keys=partitions.selected_secondary_keys,
        repartition_cols=[str(ColumnType.DATE), str(ColumnType.TYPE)],
    )
