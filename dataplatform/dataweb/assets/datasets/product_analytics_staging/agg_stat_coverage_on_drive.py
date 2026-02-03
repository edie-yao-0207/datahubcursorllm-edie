from dagster import AssetExecutionContext
from dataweb import build_general_dq_checks, get_databases, table
from dataweb.userpkgs.constants import (
    ALL_COMPUTE_REGIONS,
    FIRMWAREVDP,
    FRESHNESS_SLO_9AM_PST,
    Database,
    TableType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.firmware.constants import DATAWEB_PARTITION_DATE
from dataweb.userpkgs.firmware.metric import (
    Metric,
    MetricEnum,
    StrEnum,
    check_unique_metric_strings,
)
from dataweb.userpkgs.firmware.schema import (
    Column,
    ColumnType,
    DataType,
    Metadata,
    columns_to_names,
    columns_to_schema,
)
from dataweb.userpkgs.firmware.table import ProductAnalyticsStaging
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.utils import (
    build_table_description,
    schema_to_columns_with_property,
)
from dataweb.userpkgs.query import (
    format_date_partition_query,
)


class TableDimension(StrEnum):
    TOTAL_COUNT = "total_count"
    GPS_CABLE_NON_ZERO_VOLTAGE_PERCENT = "gps_cable_non_zero_voltage_percent"
    GPS_CAN_CONNECTED_PERCENT = "gps_can_connected_percent"
    GPS_ENGINE_IDLE_PERCENT = "gps_engine_idle_percent"
    GPS_ENGINE_OFF_PERCENT = "gps_engine_off_percent"
    GPS_ENGINE_ON_PERCENT = "gps_engine_on_percent"
    HAS_ECU_SPEED_PERCENT = "has_ecu_speed_percent"


class TableMetric(MetricEnum):
    TOTAL_COUNT = Metric(
        type=ProductAnalyticsStaging.AGG_STAT_COVERAGE_ON_DRIVE,
        field=TableDimension.TOTAL_COUNT,
        label="coverage_on_drive_total_count",
    )
    GPS_CABLE_NON_ZERO_VOLTAGE_PERCENT = Metric(
        type=ProductAnalyticsStaging.AGG_STAT_COVERAGE_ON_DRIVE,
        field=TableDimension.GPS_CABLE_NON_ZERO_VOLTAGE_PERCENT,
        label="coverage_on_drive_gps_cable_non_zero_voltage_percent",
    )
    GPS_CAN_CONNECTED_PERCENT = Metric(
        type=ProductAnalyticsStaging.AGG_STAT_COVERAGE_ON_DRIVE,
        field=TableDimension.GPS_CAN_CONNECTED_PERCENT,
        label="coverage_on_drive_gps_can_connected_percent",
    )
    GPS_ENGINE_IDLE_PERCENT = Metric(
        type=ProductAnalyticsStaging.AGG_STAT_COVERAGE_ON_DRIVE,
        field=TableDimension.GPS_ENGINE_IDLE_PERCENT,
        label="coverage_on_drive_gps_engine_idle_percent",
    )
    GPS_ENGINE_OFF_PERCENT = Metric(
        type=ProductAnalyticsStaging.AGG_STAT_COVERAGE_ON_DRIVE,
        field=TableDimension.GPS_ENGINE_OFF_PERCENT,
        label="coverage_on_drive_gps_engine_off_percent",
    )
    GPS_ENGINE_ON_PERCENT = Metric(
        type=ProductAnalyticsStaging.AGG_STAT_COVERAGE_ON_DRIVE,
        field=TableDimension.GPS_ENGINE_ON_PERCENT,
        label="coverage_on_drive_gps_engine_on_percent",
    )
    HAS_ECU_SPEED_PERCENT = Metric(
        type=ProductAnalyticsStaging.AGG_STAT_COVERAGE_ON_DRIVE,
        field=TableDimension.HAS_ECU_SPEED_PERCENT,
        label="coverage_on_drive_has_ecu_speed_percent",
    )


check_unique_metric_strings(TableMetric)


SCHEMA = columns_to_schema(
    ColumnType.DATE,
    ColumnType.TIME,
    ColumnType.ORG_ID,
    ColumnType.DEVICE_ID,
    ColumnType.BUS_ID,
    ColumnType.REQUEST_ID,
    ColumnType.RESPONSE_ID,
    ColumnType.OBD_VALUE,
    ColumnType.VALUE,
    Column(
        name=TableDimension.TOTAL_COUNT,
        type=DataType.LONG,
        nullable=True,
        metadata=Metadata(
            comment="Total number of diagnostics recorded while the device is on a drive.",
        ),
    ),
    Column(
        name=TableDimension.GPS_CABLE_NON_ZERO_VOLTAGE_PERCENT,
        type=DataType.DOUBLE,
        nullable=True,
        metadata=Metadata(
            comment="Percentage of diagnostics where the GPS cable has non-zero voltage.",
        ),
    ),
    Column(
        name=TableDimension.GPS_CAN_CONNECTED_PERCENT,
        type=DataType.DOUBLE,
        nullable=True,
        metadata=Metadata(
            comment="Percentage of diagnostics where the GPS CAN is connected.",
        ),
    ),
    Column(
        name=TableDimension.GPS_ENGINE_IDLE_PERCENT,
        type=DataType.DOUBLE,
        nullable=True,
        metadata=Metadata(
            comment="Percentage of diagnostics where the engine is idle.",
        ),
    ),
    Column(
        name=TableDimension.GPS_ENGINE_OFF_PERCENT,
        type=DataType.DOUBLE,
        nullable=True,
        metadata=Metadata(
            comment="Percentage of diagnostics where the engine is off.",
        ),
    ),
    Column(
        name=TableDimension.GPS_ENGINE_ON_PERCENT,
        type=DataType.DOUBLE,
        nullable=True,
        metadata=Metadata(
            comment="Percentage of diagnostics where the engine is on.",
        ),
    ),
    Column(
        name=TableDimension.HAS_ECU_SPEED_PERCENT,
        type=DataType.DOUBLE,
        nullable=True,
        metadata=Metadata(
            comment="Percentage of diagnostics where the device has ECU speed."
        ),
    ),
)

PRIMARY_KEYS = columns_to_names(
    ColumnType.DATE,
    ColumnType.ORG_ID,
    ColumnType.DEVICE_ID,
)

NON_NULL_COLUMNS = schema_to_columns_with_property(SCHEMA, "nullable")

QUERY = """

WITH
    data AS (
        SELECT
            date
            , time
            , org_id
            , device_id
            , gps_cable_non_zero_voltage_count
            , gps_can_connected_count
            , gps_engine_idle_count
            , gps_engine_off_count
            , gps_engine_on_count
            , has_ecu_speed_count
            , total_count
        FROM
            {product_analytics_staging}.fct_derivedstatcoverageondrive
        WHERE
            date BETWEEN date_sub("{date_start}", {lookback_days}) AND "{date_end}"
    )

    , totals AS (
        SELECT
            -- The window clause does not include the day-of, so if the date is 2023-11-27 and the window is 7 days
            -- it will return data between 2023-11-19 and 2023-11-26, but label the date as 2023-11-27
            -- To resolve this grab the date just after the one we care about and decrement the date
            CAST(DATE_SUB(DATE(WINDOW(date, "{lookback_days} DAYS", "1 DAY").END), 1) AS STRING) AS date
            , org_id
            , device_id
            , MAX(time) as time
            , SUM(gps_cable_non_zero_voltage_count) AS gps_cable_non_zero_voltage_count
            , SUM(gps_can_connected_count) AS gps_can_connected_count
            , SUM(gps_engine_idle_count) AS gps_engine_idle_count
            , SUM(gps_engine_off_count) AS gps_engine_off_count
            , SUM(gps_engine_on_count) AS gps_engine_on_count
            , SUM(has_ecu_speed_count) AS has_ecu_speed_count
            , SUM(total_count) AS total_count

        FROM
            data

        GROUP BY
            WINDOW(date, "{lookback_days} DAYS", "1 DAY")
            , org_id
            , device_id
    )

    , percents AS (
        SELECT
            date
            , time
            , org_id
            , device_id
            , total_count
            , gps_cable_non_zero_voltage_count / total_count * 100 AS gps_cable_non_zero_voltage_percent
            , gps_can_connected_count / total_count * 100 AS gps_can_connected_percent
            , gps_engine_idle_count / total_count * 100 AS gps_engine_idle_percent
            , gps_engine_off_count / total_count * 100 AS gps_engine_off_percent
            , gps_engine_on_count / total_count * 100 AS gps_engine_on_percent
            , has_ecu_speed_count / total_count * 100 AS has_ecu_speed_percent
        FROM
            totals
    )

SELECT
    date
    , time
    , org_id
    , device_id
    , CAST(NULL AS LONG) AS bus_id
    , CAST(NULL AS LONG) AS request_id
    , CAST(NULL AS LONG) AS response_id
    , CAST(NULL AS LONG) AS obd_value
    , CAST(0 AS DOUBLE) AS value
    , total_count
    , gps_cable_non_zero_voltage_percent
    , gps_can_connected_percent
    , gps_engine_idle_percent
    , gps_engine_off_percent
    , gps_engine_on_percent
    , has_ecu_speed_percent

FROM
    percents

WHERE
    date BETWEEN "{date_start}" AND "{date_end}"

"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc=(
            "Generate coverage of key diagnostics while a device is on a drive."
            "Diagnostics are expected when the VG is connected onto the vehicle network and engine state is detected as on."
        ),
        row_meaning="Each row contains some stat for a given device_id",
        related_table_info={},
        table_type=TableType.MONTHLY_REPORTING_AGG,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DATAWEB_PARTITION_DATE,
    upstreams=[
        AnyUpstream(ProductAnalyticsStaging.FCT_DERIVED_STAT_COVERAGE_ON_DRIVE),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    backfill_batch_size=5,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.AGG_STAT_COVERAGE_ON_DRIVE.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
)
def agg_stat_coverage_on_drive(context: AssetExecutionContext) -> str:
    return format_date_partition_query(QUERY, context, lookback_days=7)
