from dagster import AssetExecutionContext
from dataweb import build_general_dq_checks, table
from dataweb.userpkgs.constants import (
    ALL_COMPUTE_REGIONS,
    FIRMWAREVDP,
    FRESHNESS_SLO_9AM_PST,
    MEMORY_OPTIMIZED_INSTANCE_POOL_KEY,
    Database,
    TableType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.firmware.constants import DATAWEB_PARTITION_DATE
from dataweb.userpkgs.firmware.schema import (
    Column,
    ColumnType,
    DataType,
    Metadata,
    columns_to_schema,
)
from dataweb.userpkgs.firmware.table import (
    CloudDb,
    KinesisStats,
    ProductAnalyticsStaging,
    ProductsDb,
)
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.query import create_run_config_overrides
from dataweb.userpkgs.utils import (
    build_table_description,
    partition_key_ranges_from_context,
    schema_to_columns_with_property,
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
    ColumnType.VALUE,
    Column(
        name="gps_can_connected_count",
        type=DataType.LONG,
        nullable=True,
        metadata=Metadata(
            comment="Number of diagnostics where the GPS CAN is connected.",
        ),
    ),
    Column(
        name="gps_cable_non_zero_voltage_count",
        type=DataType.LONG,
        nullable=True,
        metadata=Metadata(
            comment="Number of diagnostics where the GPS cable has non-zero voltage.",
        ),
    ),
    Column(
        name="has_ecu_speed_count",
        type=DataType.LONG,
        nullable=True,
        metadata=Metadata(
            comment="Number of diagnostics where the device has ECU speed.",
        ),
    ),
    Column(
        name="gps_engine_on_count",
        type=DataType.LONG,
        nullable=True,
        metadata=Metadata(
            comment="Number of diagnostics where the engine is on.",
        ),
    ),
    Column(
        name="gps_engine_off_count",
        type=DataType.LONG,
        nullable=True,
        metadata=Metadata(
            comment="Number of diagnostics where the engine is off.",
        ),
    ),
    Column(
        name="gps_engine_idle_count",
        type=DataType.LONG,
        nullable=True,
        metadata=Metadata(
            comment="Number of diagnostics where the engine is idle.",
        ),
    ),
    Column(
        name="total_count",
        type=DataType.LONG,
        nullable=True,
        metadata=Metadata(
            comment="Total number of diagnostics recorded while the device is on a drive.",
        ),
    ),
)

PRIMARY_KEYS = schema_to_columns_with_property(SCHEMA)
NON_NULL_COLUMNS = schema_to_columns_with_property(SCHEMA, "nullable")


QUERY = """

WITH
    can_connect AS (
        SELECT
            org_id
            , object_id
            , date
            , value.int_value AS can_connect_status
            , time AS can_connect_status_start_ms
            -- Can connected can go days without a state change, if this doesn't happen within date window.
            -- Ensure the end_ms is just the end date window to close interval
            , COALESCE(
                LEAD(time) OVER (PARTITION BY org_id, object_id ORDER BY time ASC)
                , UNIX_TIMESTAMP("{date_end}", 'yyyy-MM-dd') * 1000
            ) AS can_connect_status_end_ms

        FROM
            kinesisstats.osdcanconnected

        WHERE
            -- lag two days, and also capture 7 days of stats before start since can connected can go days without change
            date BETWEEN DATE_SUB("{date_start}", 9) AND "{date_end}"
            AND NOT value.is_databreak
            AND NOT value.is_end
            -- consider canbus connect or disconnect events only
            AND value.int_value IN (2, 3)
    )

    , engine_state AS (
        SELECT
          org_id
          , object_id
          , date
          , value.int_value AS engine_state
          , time AS engine_state_start_ms
          , LEAD(time) OVER (PARTITION BY org_id, object_id ORDER BY time ASC) AS engine_state_end_ms

        FROM
            kinesisstats.osdenginestate

        WHERE
            -- lag two days, and also capture 2 days of stats before start
            date BETWEEN DATE_SUB("{date_start}", 4) AND "{date_end}"
            AND NOT value.is_databreak
            AND NOT value.is_end
    )

    , cable_volt_tmp AS (
        SELECT
            c.org_id
            , c.object_id
            , c.date
            , c.time
            , c.value.is_databreak
            , c.value.is_end
            , c.value.int_value AS cur_voltage
            , LEAD(c.time) OVER (PARTITION BY c.org_id, c.object_id ORDER BY time ASC) AS next_voltage_start_ms

        FROM
          kinesisstats.osdcablevoltage AS c

        JOIN
          productsdb.devices AS d
          ON c.org_id = d.org_id
          AND c.object_id = d.id

        WHERE
            -- VG34s (24) & VG34EUs (35) & VG54s (53) & VG54EUs (89) & VG55s (178)
            -- lag two days, and also capture 2 days of stats before start
            date BETWEEN DATE_SUB("{date_start}", 4) AND "{date_end}"
            AND d.product_id IN (24, 35, 53, 89, 178)
      )

    , zero_cable_volt AS (
        SELECT
            org_id
            , object_id
            , date
            , cur_voltage AS zero_cable_voltage
            , time AS zero_cable_voltage_start_ms
            , next_voltage_start_ms AS zero_cable_voltage_end_ms

        FROM
          cable_volt_tmp

        WHERE
            cur_voltage = 0
            AND NOT is_databreak
            AND NOT is_end
      )

    , all_devices as (
        -- join devices and organization data, filter out extraneous calcs
        SELECT
            d.org_id
            , d.id as device_id

        FROM
            productsdb.devices AS d

        JOIN
            clouddb.organizations AS org
            ON d.org_id = org.id

        WHERE
            -- VG34s (24) & VG34EUs (35) & VG54s (53) & VG54EUs (89) & VG55s (178)
            -- lag two days, and also capture 2 days of stats before start
            d.product_id IN (24, 35, 53, 89, 178)
            -- include only customer orgs
            AND org.internal_type = 0
            -- filter out orgs that are late on payment as we do not collect object stats for those orgs
            AND org.quarantine_enabled <> 1
      )

    , speed_data AS (
        -- gps speed and ecu speed
        SELECT
            org_id
            , device_id
            , date
            , time
            , value.has_ecu_speed AS has_ecu_speed
            , value.ecu_speed_meters_per_second AS ecu_speed_meters_per_second

        FROM
            kinesisstats.location

        WHERE
            -- lag two days, we want to days of data past the end date so we can capture end transitions from stats
            date BETWEEN DATE_SUB("{date_start}", 2) AND DATE_SUB("{date_end}", 2)
            -- 25 mph minimum speed
            AND value.gps_speed_meters_per_second >= 11
            AND value.accuracy_millimeters IS NOT NULL
            AND value.accuracy_millimeters < 10000
      )

    , gps_data AS (
        SELECT
            l.org_id
            , l.device_id
            , l.date
            , SUM(CASE WHEN (l.has_ecu_speed AND l.ecu_speed_meters_per_second IS NOT NULL) THEN 1 ELSE 0 END) AS has_ecu_speed_count
            , SUM(CASE WHEN es.engine_state == 0 THEN 1 ELSE 0 END) AS gps_engine_off_count
            , SUM(CASE WHEN es.engine_state == 1 THEN 1 ELSE 0 END) AS gps_engine_on_count
            , SUM(CASE WHEN es.engine_state == 2 THEN 1 ELSE 0 END) AS gps_engine_idle_count
            , SUM(CASE WHEN cc.can_connect_status == 2 THEN 1 ELSE 0 END) AS gps_can_connected_count
            , SUM(CASE WHEN cv.zero_cable_voltage is null THEN 1 ELSE 0 END) AS gps_cable_non_zero_voltage_count
            , COUNT(*) AS total_count

        FROM
            speed_data l

        JOIN
            all_devices d
            ON l.org_id = d.org_id
            AND l.device_id = d.device_id

        LEFT JOIN
            can_connect cc
            ON l.org_id = cc.org_id
            AND l.device_id = cc.object_id
            AND (l.time BETWEEN cc.can_connect_status_start_ms AND cc.can_connect_status_end_ms)

        LEFT JOIN
            engine_state es
            ON l.org_id = es.org_id
            AND l.device_id = es.object_id
            AND l.time BETWEEN es.engine_state_start_ms AND es.engine_state_end_ms

        LEFT JOIN
            zero_cable_volt cv
            ON l.org_id = cv.org_id
            AND l.device_id = cv.object_id
            AND l.time BETWEEN cv.zero_cable_voltage_start_ms AND cv.zero_cable_voltage_end_ms

        GROUP BY ALL
    )

    , filtered AS (
        SELECT
            gps_data.*

        FROM
            gps_data

        INNER JOIN
            all_devices
            ON gps_data.org_id = all_devices.org_id
            AND gps_data.device_id = all_devices.device_id

        WHERE
            -- filter out devices with statistically insignificant counts
            total_count > 100
    )

SELECT
    -- This metric lags by two days to get an accurate picture of
    -- data. Increment the date in the event by two days so it lines
    -- up with the materialization date. Without this, rows are
    -- dropped since they lie outside the partition definition
    CAST(DATE_ADD(date, 2) AS STRING) AS date
    , CAST(NULL AS LONG) AS time
    , org_id
    , device_id
    , CAST(NULL AS LONG) AS bus_id
    , CAST(NULL AS LONG) AS request_id
    , CAST(NULL AS LONG) AS response_id
    , CAST(NULL AS LONG) AS obd_value
    , CAST(NULL AS DOUBLE) AS value
    , gps_can_connected_count
    , gps_cable_non_zero_voltage_count
    , has_ecu_speed_count
    , gps_engine_on_count
    , gps_engine_off_count
    , gps_engine_idle_count
    , total_count

FROM
    filtered

"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Find coverage of signals while a device is expected to have diagnostics.",
        row_meaning="Device signal coverage",
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
        AnyUpstream(KinesisStats.OSD_CAN_CONNECTED),
        AnyUpstream(KinesisStats.OSD_ENGINE_STATE),
        AnyUpstream(KinesisStats.OSD_CABLE_VOLTAGE),
        AnyUpstream(ProductsDb.DEVICES),
        AnyUpstream(CloudDb.ORGANIZATIONS),
        AnyUpstream(KinesisStats.LOCATION),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    backfill_batch_size=5,
    run_config_overrides=create_run_config_overrides(
        instance_pool_type=MEMORY_OPTIMIZED_INSTANCE_POOL_KEY,
    ),
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.FCT_DERIVED_STAT_COVERAGE_ON_DRIVE.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
)
def fct_derivedstatcoverageondrive(context: AssetExecutionContext) -> str:
    partition_keys = partition_key_ranges_from_context(context)[0]

    return QUERY.format(
        date_start=partition_keys[0],
        date_end=partition_keys[-1],
    )
