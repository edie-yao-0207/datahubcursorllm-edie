"""
Eligible Traces Base

Single source of truth for all CAN traces eligible for collection.
Consolidates common filtering logic:
- Joins fct_eligible_can_recording_windows with dim_device_vehicle_properties
- Excludes traces already collected (in fct_can_trace_status)
- Excludes internal orgs via dim_organizations.internal_type = 0
- Includes MMYEF vehicle properties for population-based filtering

Downstream query-specific tables (representative, training, inference) should read from this
base table and apply their own additional filtering criteria (duration, on_trip, quotas).

Key Features:
- Pre-filtered eligible recording windows with vehicle properties
- Already-collected traces excluded via anti-join
- External orgs only
- is_valid_mmy flag for optional downstream filtering
- All timing details preserved for downstream duration filtering
"""

from dagster import AssetExecutionContext
from dataweb import build_general_dq_checks, table
from dataweb.userpkgs.constants import (
    AWSRegion,
    Database,
    DQCheckMode,
    FIRMWAREVDP,
    FRESHNESS_SLO_9AM_PST,
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
from dataweb.userpkgs.firmware.table import (
    DataModelCore,
    ProductAnalyticsStaging,
)
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.query import format_date_partition_query
from dataweb.userpkgs.utils import build_table_description

from .dim_device_vehicle_properties import MMYEF_ID
from .fct_can_recording_windows import (
    START_TIME_COLUMN,
    END_TIME_COLUMN,
    CAPTURE_DURATION_COLUMN,
    CAN_0_NAME_COLUMN,
    CAN_1_NAME_COLUMN,
)


QUERY = """
WITH
-- Get eligible recording windows (already filters for VG devices, SFDC accounts, CM3/4, etc.)
eligible_windows AS (
    SELECT
        erw.date,
        erw.org_id,
        erw.device_id,
        erw.start_time,
        erw.end_time,
        erw.capture_duration,
        erw.on_trip,
        erw.can_0_name,
        erw.can_1_name
    FROM product_analytics_staging.fct_eligible_can_recording_windows erw
    WHERE erw.date BETWEEN '{date_start}' AND '{date_end}'
),

-- Join with vehicle properties to get MMYEF
windows_with_vehicle_props AS (
    SELECT
        ew.date,
        ew.org_id,
        ew.device_id,
        ew.start_time,
        ew.end_time,
        ew.capture_duration,
        ew.on_trip,
        ew.can_0_name,
        ew.can_1_name,
        dvp.mmyef_id,
        dvp.make,
        dvp.model,
        dvp.year,
        dvp.engine_model,
        dvp.powertrain,
        dvp.fuel_group,
        dvp.trim,
        -- Flag for downstream filtering: TRUE if make/model/year are all non-null
        (dvp.make IS NOT NULL AND dvp.model IS NOT NULL AND dvp.year IS NOT NULL) AS is_valid_mmy
    FROM eligible_windows ew
    JOIN product_analytics_staging.dim_device_vehicle_properties dvp
        USING (date, org_id, device_id)
),

-- Exclude internal orgs (only include external/customer orgs)
external_orgs_only AS (
    SELECT
        wvp.*
    FROM windows_with_vehicle_props wvp
    JOIN datamodel_core.dim_organizations orgs
        USING (date, org_id)
    WHERE orgs.internal_type = 0
),

-- Exclude traces that have already been collected (available in library)
-- Anti-join against fct_can_trace_status to filter out already-pulled traces
not_yet_collected AS (
    SELECT
        eoo.*
    FROM external_orgs_only eoo
    LEFT JOIN product_analytics_staging.fct_can_trace_status cts
        ON eoo.org_id = cts.org_id
        AND eoo.device_id = cts.device_id
        AND eoo.start_time = cts.start_time
    WHERE cts.trace_uuid IS NULL
)

SELECT
    date,
    org_id,
    device_id,
    start_time,
    end_time,
    capture_duration,
    on_trip,
    can_0_name,
    can_1_name,
    mmyef_id,
    make,
    model,
    year,
    engine_model,
    powertrain,
    fuel_group,
    trim,
    is_valid_mmy
FROM not_yet_collected
"""


COLUMNS = [
    # Primary keys and identifiers
    ColumnType.DATE,
    ColumnType.ORG_ID,
    ColumnType.DEVICE_ID,
    # Trace timing details
    START_TIME_COLUMN,
    END_TIME_COLUMN,
    CAPTURE_DURATION_COLUMN,
    # Trip status
    Column(
        name="on_trip",
        type=DataType.BOOLEAN,
        nullable=False,
        metadata=Metadata(
            comment="Whether the CAN recording window occurred during an active trip."
        ),
    ),
    # CAN bus info
    CAN_0_NAME_COLUMN,
    CAN_1_NAME_COLUMN,
    # Vehicle properties
    MMYEF_ID,
    ColumnType.MAKE,
    ColumnType.MODEL,
    ColumnType.YEAR,
    ColumnType.ENGINE_MODEL,
    ColumnType.POWERTRAIN,
    ColumnType.FUEL_GROUP,
    ColumnType.TRIM,
    # Validity flag
    Column(
        name="is_valid_mmy",
        type=DataType.BOOLEAN,
        nullable=False,
        metadata=Metadata(
            comment="TRUE if make, model, and year are all non-null. "
            "Downstream queries requiring MMY can filter on this flag."
        ),
    ),
]

SCHEMA = columns_to_schema(*COLUMNS)
PRIMARY_KEYS = get_primary_keys(COLUMNS)
NON_NULL_COLUMNS = get_non_null_columns(COLUMNS)


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Single source of truth for CAN traces eligible for collection. "
        "Consolidates common filtering logic: joins eligible recording windows with vehicle properties, "
        "excludes already-collected traces (via fct_can_trace_status anti-join), and filters to external orgs only. "
        "Downstream query-specific tables (representative, training, inference) should read from this base table "
        "and apply their own additional filtering criteria (duration, on_trip requirements, quotas). "
        "Includes is_valid_mmy flag for optional MMY enforcement.",
        row_meaning="A CAN recording window eligible for collection with vehicle properties and collection status verified",
        related_table_info={},
        table_type=TableType.TRANSACTIONAL_FACT,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=[AWSRegion.US_WEST_2],  # CAN recording only enabled in the US currently
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DATAWEB_PARTITION_DATE,
    upstreams=[
        AnyUpstream(ProductAnalyticsStaging.FCT_ELIGIBLE_CAN_RECORDING_WINDOWS),
        AnyUpstream(ProductAnalyticsStaging.DIM_DEVICE_VEHICLE_PROPERTIES),
        AnyUpstream(ProductAnalyticsStaging.FCT_CAN_TRACE_STATUS),
        AnyUpstream(DataModelCore.DIM_ORGANIZATIONS),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    single_run_backfill=True,
    dq_check_mode=DQCheckMode.WHOLE_RESULT,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.FCT_ELIGIBLE_TRACES_BASE.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
)
def fct_eligible_traces_base(context: AssetExecutionContext) -> str:
    """
    Provide the base set of eligible CAN traces for collection.

    This asset provides a single upstream source for all downstream trace selection queries,
    consolidating common filtering logic.

    Key filtering applied:
    - Only eligible recording windows (VG devices, SFDC accounts, CM3/4 cameras)
    - Vehicle properties joined for MMYEF-based population filtering
    - Internal orgs excluded (only external/customer orgs)
    - Already-collected traces excluded via anti-join

    Downstream queries should apply their own:
    - Duration requirements (min/max capture_duration)
    - on_trip filtering
    - Population quota enforcement
    - Query-specific ranking

    Args:
        context: Dagster asset execution context with partition information

    Returns:
        Formatted SQL query string for execution
    """
    return format_date_partition_query(
        QUERY,
        context,
        datamodel_core=Database.DATAMODEL_CORE,
    )

