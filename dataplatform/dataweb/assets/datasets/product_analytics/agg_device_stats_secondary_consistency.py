from enum import Enum
from typing import List

from dagster import AssetExecutionContext
from dataweb import build_general_dq_checks, get_databases, table
from dataweb.assets.datasets.product_analytics_staging import (
    agg_osdcommandschedulerstats,
    fct_osdcanbitratedetectionv2,
    fct_osdcanprotocolsdetected,
    fct_osdj1708can3busautodetectresult,
    fct_osdj1939claimedaddress,
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
from dataweb.userpkgs.firmware.metric import Metric, StrEnum
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
    partition_key_ranges_from_context,
    schema_to_columns_with_property,
)

METRICS: List[Metric] = [
    fct_osdcanbitratedetectionv2.TableMetric.BITRATE,
    fct_osdcanprotocolsdetected.TableMetric.DETECTED_PROTOCOL_COUNT,
    fct_osdcanprotocolsdetected.TableMetric.PROTOCOL_SELECTED_TO_RUN,
    fct_osdj1939claimedaddress.TableMetric.SOURCE_ADDRESS_CLAIMED,
    fct_osdj1708can3busautodetectresult.TableMetric.DETECTED,
    agg_osdcommandschedulerstats.TableMetric.STOPPED
]


class TableDimension(StrEnum):
    DAYS_CONSISTENT = "days_consistent"
    IS_CONSISTENT = "is_consistent"


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
        name=TableDimension.IS_CONSISTENT,
        type=DataType.BOOLEAN,
        nullable=False,
        metadata=Metadata(
            comment="Whether the data is consistent. A value of 1 indicates that the data is consistent.",
        ),
    ),
    Column(
        name=TableDimension.DAYS_CONSISTENT,
        type=DataType.DOUBLE,
        nullable=False,
        metadata=Metadata(
            comment="Percentage of days in the past 3 days where the standard deviation of the data is 0 and the minimum value is greater than 0.",
        ),
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

QUERY = """

WITH
    data AS (
        SELECT
            -- This will aggregate to a rate of devices receiving this failure per day
            -- Subtract one day since the end of a window function is exclusive.  This shifts the window of
            -- interest to the expected dates.
            DATE_SUB(DATE(WINDOW(date, "{window_days} DAYS", "1 DAY").END), 1) AS date
            , type
            , org_id
            , device_id
            , bus_id
            , request_id
            , response_id
            , obd_value
            -- Coalesce the stddev since the stddev statistic reports null if there is only a single
            -- value.  In this case, we want to treat a single reported value as a pass.
            -- Sometimes we see very tiny numbers for stddev such as 4e-11 even when a group of
            -- all the same values is reported, so check against some small epsilon rather than
            -- for equality to zero.
            , SUM(CAST(COALESCE(stddev, 0) < 0.001 AND min > 0 AS DOUBLE)) / COUNT(1) AS days_consistent

        FROM
            {product_analytics}.agg_device_stats_primary

        WHERE
            date BETWEEN DATE_SUB("{date_start}", {window_days}) AND "{date_end}"
            AND (
                {matches}
            )

        GROUP BY
            WINDOW(date, "{window_days} DAYS", "1 DAY")
            , type
            , org_id
            , device_id
            , bus_id
            , request_id
            , response_id
            , obd_value
    )

SELECT
    CAST(date AS STRING) AS date
    , type
    , org_id
    , device_id
    , bus_id
    , request_id
    , response_id
    , obd_value
    , CAST(NULL AS DOUBLE) AS value
    , COALESCE(days_consistent >= {minimum_consistency_threshold}, FALSE) AS is_consistent
    , COALESCE(days_consistent, 0) AS days_consistent

FROM
    data

WHERE
    date BETWEEN "{date_start}" AND "{date_end}"

"""


@table(
    database=Database.PRODUCT_ANALYTICS,
    description=build_table_description(
        table_desc="Calculate the consistency of device data. Consistency is defined as the percentage of days in the past 3 days where the standard deviation of the data is 0 and the minimum value is greater than 0.",
        row_meaning="Consistency check on device data for a given metric. A value of 1 indicates that the data is consistent.",
        related_table_info={},
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
        asset_name=ProductAnalytics.AGG_DEVICE_STATS_SECONDARY_CONSISTENCY.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_KEYS,
        block_before_write=True,
    ),
    max_retries=5,
)
def agg_device_stats_secondary_consistency(context: AssetExecutionContext) -> str:
    partition_keys = partition_key_ranges_from_context(context)[0]

    return QUERY.format(
        date_start=partition_keys[0],
        date_end=partition_keys[-1],
        product_analytics=get_databases()[Database.PRODUCT_ANALYTICS],
        matches="\t\t\t\t\n OR ".join([f"(type = '{metric}')" for metric in METRICS]),
        window_days=3,
        minimum_consistency_threshold=0.95,
    )
