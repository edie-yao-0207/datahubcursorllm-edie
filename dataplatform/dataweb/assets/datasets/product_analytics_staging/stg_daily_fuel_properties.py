"""
stg_daily_fuel_properties

Daily staging asset for fuel properties, converting the non-date partitioned
dataprep.fuel_properties table to a date-partitioned format. Uses the latest
fuel property values for each device as of each date.
"""

from dagster import AssetExecutionContext
from dataweb import build_general_dq_checks, table
from dataweb.userpkgs.constants import (
    ALL_COMPUTE_REGIONS,
    FIRMWAREVDP,
    FRESHNESS_SLO_9AM_PST,
    PRIMARY_KEYS_DATE_ORG_DEVICE,
    Database,
    TableType,
    WarehouseWriteMode,
    DQCheckMode,
)
from dataweb.userpkgs.firmware.constants import DATAWEB_PARTITION_DATE
from dataclasses import replace
from dataweb.userpkgs.firmware.schema import (
    ColumnType,
    columns_to_schema,
)
from dataweb.userpkgs.firmware.table import ProductAnalyticsStaging, DataPrep
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.utils import (
    build_table_description,
    schema_to_columns_with_property,
)
from dataweb.userpkgs.query import format_date_partition_query

COLUMNS = [
    ColumnType.DATE,
    ColumnType.ORG_ID,
    ColumnType.DEVICE_ID,
    replace(ColumnType.POWERTRAIN.value, primary_key=False),
    replace(ColumnType.FUEL_GROUP.value, primary_key=False),
]

SCHEMA = columns_to_schema(*COLUMNS)
NON_NULL_KEYS = schema_to_columns_with_property(SCHEMA, "nullable")

QUERY = """
WITH date_sequence AS (
    SELECT
        EXPLODE(SEQUENCE(DATE("{date_start}"), DATE("{date_end}"))) AS date
),

data AS (
    SELECT
        ds.date,
        fp.org_id,
        fp.device_id,
        fp.power_train_id,
        fp.fuel_group_id,
        ROW_NUMBER() OVER (
            PARTITION BY ds.date, fp.org_id, fp.device_id 
            ORDER BY fp.created_at DESC
        ) as rn
    FROM date_sequence ds
    CROSS JOIN dataprep.fuel_properties fp
    -- The system didn't exist until 2023. Records before this date are invalid.
    -- Set to 2022 to be safe around records included
    -- Only consider fuel properties that existed on or before the snapshot date
    WHERE DATE(fp.created_at) BETWEEN DATE('2022-01-01') AND ds.date
)

SELECT
    CAST(date AS STRING) AS date,
    org_id,
    device_id,
    CAST(power_train_id AS INTEGER) AS powertrain,
    CAST(fuel_group_id AS INTEGER) AS fuel_group
FROM data
WHERE rn = 1
"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Daily fuel properties staging data converted from non-partitioned source. "
        "Provides the latest fuel group and powertrain information for each device as of each date, "
        "enabling time-based analysis of vehicle fuel characteristics.",
        row_meaning="Each row represents the most recent fuel properties (powertrain, fuel_group) "
        "for a device as of a specific date.",
        table_type=TableType.STAGING,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS_DATE_ORG_DEVICE,
    partitioning=DATAWEB_PARTITION_DATE,
    upstreams=[
        AnyUpstream(DataPrep.FUEL_PROPERTIES),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    single_run_backfill=True,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.STG_DAILY_FUEL_PROPERTIES.value,
        primary_keys=PRIMARY_KEYS_DATE_ORG_DEVICE,
        non_null_keys=NON_NULL_KEYS,
        block_before_write=True,
    ),
    priority=5,
    dq_check_mode=DQCheckMode.WHOLE_RESULT,
)
def stg_daily_fuel_properties(context: AssetExecutionContext) -> str:
    return format_date_partition_query(
        QUERY,
        context,
    )
