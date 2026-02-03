from enum import Enum
from dagster import AssetExecutionContext
from dataweb import build_general_dq_checks, table
from dataweb.assets.datasets.product_analytics_staging import (
    fct_hourlyfuelconsumption,
    fct_osdvin,
    stg_daily_faults,
)
from dataweb.userpkgs.constants import (
    ALL_COMPUTE_REGIONS,
    FIRMWAREVDP,
    FRESHNESS_SLO_9AM_PST,
    Database,
    TableType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.firmware.constants import DATAWEB_PARTITION_DATE
from dataweb.userpkgs.firmware.kinesisstats import KinesisStatsMetric
from dataweb.userpkgs.firmware.metric import metric_values_to_partition_map
from dataweb.userpkgs.firmware.schema import (
    Column,
    ColumnType,
    DataType,
    Metadata,
    columns_to_names,
    columns_to_schema,
)
from dataweb.userpkgs.firmware.table import ProductAnalytics
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.utils import (
    build_table_description,
    schema_to_columns_with_property,
)
from dataweb.userpkgs.query import format_date_partition_query

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
    Column(
        name="is_covered",
        type=DataType.BOOLEAN,
        nullable=False,
        metadata=Metadata(comment="Whether the data stream is covered by the data."),
    ),
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


METRICS = [
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
    KinesisStatsMetric.AT1_DPF_SOOT_LOAD_PERCENT_INT_VALUE,
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
    fct_osdvin.TableMetric.UNIQUE_VIN_COUNT,
    stg_daily_faults.TableMetric.FAULT_COUNT,
    KinesisStatsMetric.OSD_CAN_CONNECTED_INT_VALUE,
    fct_hourlyfuelconsumption.TableMetric.GASEOUS_CONSUMPTION_RATE,
]

class TableDimension(str, Enum):
    IS_COVERED = "is_covered"

STAT_SETTINGS = metric_values_to_partition_map(*METRICS)

QUERY = """

SELECT
  date
  , type
  , org_id
  , device_id
  , CAST(NULL AS LONG) AS bus_id
  , CAST(NULL AS LONG) AS request_id
  , CAST(NULL AS LONG) AS response_id
  , CAST(NULL AS LONG) AS obd_value
  , CAST(NULL AS DOUBLE) AS value
  , COALESCE(
        CASE
            -- Vehicles report OFF charging status pre-VG31. We can use this logic once this is fixed in VG-31
            WHEN type = "{charging_status}" THEN avg > 0 AND stddev > 0

            -- Fords have stuck engine hours at a non-zero value. Expect this to change
            WHEN type = "{engine_seconds}" THEN avg > 0 AND stddev > 0

            -- Faults/TTS are going to always be a static number, a vehicle is covered if it has a non-zero number
            WHEN type = "{tell_tale}" THEN count > 0

            -- Faults are considered covered if we've seen a PGN/PID/DID for faults. The contents are not judged, otherwise
            -- a fault would need to be active before we say it would be covered.
            WHEN type = "{faults_seen}" THEN count > 0

            -- CAN connected statuses for CAN or J1708 connection established. If we don't see this we don't have a vehicle
            -- connection.
            WHEN type = "{can_connected}" THEN min IN (2, 3) OR max IN (2, 3)

            WHEN type IN (
                "{air_temp_milli_c}",
                "{coolant_temp_milli_c}"
                "{steering_wheel_angle}",
                "{yaw_rate}",
                "{j1939_oel_turn_signal_switch_state}",
                "{left_turn_signal}",
                "{right_turn_signal}",
                "{vehicle_current_gear}",
                "{reverse_direction}",
                "{direction_indicator}",
                "{door_open_status}"
            ) THEN stddev > 0

            -- only count coverage if we've seen data and the standard deviation is
            -- non-zero. We expect data to change over the course of a drive. We also
            -- assume the input is consistently reporting over the length of a drive.
            -- Engine temperature, battery current, gear speed can go negative
            ELSE count > 0 AND (max != 0 OR min != 0)
        END
        , FALSE
    ) AS is_covered

FROM
    {product_analytics}.agg_device_stats_primary

WHERE
    date BETWEEN "{date_start}" AND "{date_end}"
    AND (
        {condition}
    )

"""


@table(
    database=Database.PRODUCT_ANALYTICS,
    description=build_table_description(
        table_desc="Extracts the coverage per data stream.",
        row_meaning="Per day, per device coverage.",
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
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalytics.AGG_DEVICE_STATS_SECONDARY_COVERAGE.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_KEYS,
        block_before_write=True,
    ),
    write_mode=WarehouseWriteMode.OVERWRITE,
    backfill_batch_size=5,
    max_retries=5,
)
def agg_device_stats_secondary_coverage(context: AssetExecutionContext) -> str:
    return format_date_partition_query(
        QUERY,
        context,
        condition="\n\tOR ".join([f'(type = "{metric}")' for metric in METRICS]),
        tell_tale=KinesisStatsMetric.TELL_TALE_STATUS_INT_VALUE,
        engine_seconds=KinesisStatsMetric.ENGINE_SECONDS_INT_VALUE,
        charging_status=KinesisStatsMetric.EV_CHARGING_STATUS_INT_VALUE,
        faults_seen=stg_daily_faults.TableMetric.FAULT_COUNT,
        can_connected=KinesisStatsMetric.OSD_CAN_CONNECTED_INT_VALUE,
        air_temp_milli_c=KinesisStatsMetric.ENGINE_GAUGE_AIR_TEMP_MILLI_C,
        coolant_temp_milli_c=KinesisStatsMetric.ENGINE_GAUGE_COOLANT_TEMP_MILLI_C,
        steering_wheel_angle=KinesisStatsMetric.J1939_VDC2_STEERING_WHEEL_ANGLE_PICO_RAD_INT_VALUE,
        yaw_rate=KinesisStatsMetric.J1939_VDC2_YAW_RATE_FEMTO_RAD_PER_SECONDS_INT_VALUE,
        j1939_oel_turn_signal_switch_state=KinesisStatsMetric.J1939_OEL_TURN_SIGNAL_SWITCH_STATE_INT_VALUE,
        left_turn_signal=KinesisStatsMetric.J1939_LCMD_LEFT_TURN_SIGNAL_LIGHTS_COMMAND_STATE_INT_VALUE,
        right_turn_signal=KinesisStatsMetric.J1939_LCMD_RIGHT_TURN_SIGNAL_LIGHTS_COMMAND_STATE_INT_VALUE,
        vehicle_current_gear=KinesisStatsMetric.VEHICLE_CURRENT_GEAR_INT_VALUE,
        reverse_direction=KinesisStatsMetric.J1939_ETC5_TRANSMISSION_REVERSE_DIRECTION_SWITCH_STATE_INT_VALUE,
        direction_indicator=KinesisStatsMetric.J1939_TCO1_DIRECTION_INDICATOR_STATE_INT_VALUE,
        door_open_status=KinesisStatsMetric.DOOR_OPEN_STATUS_INT_VALUE,
    )
