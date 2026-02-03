from dagster import AssetExecutionContext
from dataweb import build_general_dq_checks, table
from dataweb.userpkgs.constants import (
    FIRMWAREVDP,
    FRESHNESS_SLO_9AM_PST,
    AWSRegion,
    Database,
    DQCheckMode,
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
    get_non_null_columns,
    get_primary_keys,
)
from dataweb.userpkgs.firmware.table import KinesisStatsHistory, ProductAnalyticsStaging
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.query import format_date_partition_query
from dataweb.userpkgs.utils import build_table_description

# Reusable column types for CAN recording windows (can be imported by other tables)
START_TIME_COLUMN = Column(
    name="start_time",
    type=DataType.LONG,
    nullable=False,
    primary_key=True,
    metadata=Metadata(comment="Start time in milliseconds since epoch."),
)

END_TIME_COLUMN = Column(
    name="end_time",
    type=DataType.LONG,
    nullable=False,
    primary_key=True,
    metadata=Metadata(comment="End time in milliseconds since epoch."),
)

CAPTURE_DURATION_COLUMN = Column(
    name="capture_duration",
    type=DataType.LONG,
    nullable=False,
    metadata=Metadata(comment="Duration in milliseconds."),
)

CAN_0_NAME_COLUMN = Column(
    name="can_0_name",
    type=DataType.STRING,
    metadata=Metadata(comment="The name of the CAN 0 interface."),
)

CAN_1_NAME_COLUMN = Column(
    name="can_1_name",
    type=DataType.STRING,
    nullable=True,
    metadata=Metadata(comment="The name of the CAN 1 interface."),
)

QUERY = """
WITH filtered_records AS (
    SELECT
        c.date,
        c.org_id,
        c.object_id as device_id,
        c.value.time,
        c.value.proto_value.can_recorder_info.interfaces[0].name as can_0_name,
        c.value.proto_value.can_recorder_info.interfaces[0].state as can_0_state,
        c.value.proto_value.can_recorder_info.interfaces[1].name as can_1_name,
        c.value.proto_value.can_recorder_info.interfaces[1].state as can_1_state
    FROM kinesisstats_history.osdcanrecorderinfo c
    -- Include data from day before date_start to capture cross-midnight windows
    WHERE c.date BETWEEN DATE_SUB(TO_DATE("{date_start}"), 1) AND TO_DATE("{date_end}")
        AND NOT c.value.is_databreak
        AND NOT c.value.is_end
),
state_transitions AS (
    SELECT
        a.date,
        a.org_id,
        a.device_id,
        a.can_0_name,
        a.time,
        a.can_0_state,
        LEAD(a.time) OVER (PARTITION BY a.org_id, a.device_id ORDER BY a.time) as next_time,
        LEAD(a.can_0_state) OVER (PARTITION BY a.org_id, a.device_id ORDER BY a.time) as next_can_0_state,
        LEAD(a.date) OVER (PARTITION BY a.org_id, a.device_id ORDER BY a.time) as next_date,
        a.can_1_name,
        a.can_1_state
    FROM filtered_records a
),
recording_windows AS (
    SELECT
        -- Use the date of the end event (when recording stopped)
        c.next_date as date,
        c.org_id,
        c.device_id,
        c.time as start_time,
        c.next_time as end_time,
        c.next_time - c.time as capture_duration,
        c.can_0_name,
        c.can_1_name
    FROM state_transitions c
    WHERE c.can_0_state = 1  -- State 1: Recording started
        AND c.next_can_0_state = 2  -- State 2: Recording stopped
        AND c.next_time - c.time > 0  -- Ensure positive duration
)
SELECT
    rw.date,
    rw.org_id,
    rw.device_id,
    rw.start_time,
    rw.end_time,
    rw.capture_duration,
    rw.can_0_name,
    rw.can_1_name
FROM recording_windows rw
-- Only include windows that end in the target date range
WHERE rw.date BETWEEN "{date_start}" AND "{date_end}"
"""

COLUMNS = [
    ColumnType.DATE,
    ColumnType.ORG_ID,
    ColumnType.DEVICE_ID,
    START_TIME_COLUMN,
    END_TIME_COLUMN,
    CAPTURE_DURATION_COLUMN,
    CAN_0_NAME_COLUMN,
    CAN_1_NAME_COLUMN,
]

SCHEMA = columns_to_schema(*COLUMNS)
PRIMARY_KEYS = get_primary_keys(COLUMNS)
NON_NULL_COLUMNS = get_non_null_columns(COLUMNS)


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="This table contains CAN recording windows. These are are defined "
            "as periods where CAN recording is enabled on a device. Since recording CAN frames "
            "is resource intensive, recording is enabled for short periods that are described by this table.",
        row_meaning="Each row represents a window where CAN recording is active for a device with a start and end time.",
        table_type=TableType.TRANSACTIONAL_FACT,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=[AWSRegion.US_WEST_2],  # CAN recording only enabled in the US currently
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DATAWEB_PARTITION_DATE,
    upstreams=[AnyUpstream(KinesisStatsHistory.OSD_CAN_RECORDER_INFO),],
    write_mode=WarehouseWriteMode.OVERWRITE,
    single_run_backfill=True,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.FCT_CAN_RECORDING_WINDOWS.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
    dq_check_mode=DQCheckMode.WHOLE_RESULT,
)
def fct_can_recording_windows(context: AssetExecutionContext) -> str:
    return format_date_partition_query(QUERY, context)
