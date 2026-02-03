from dagster import AssetExecutionContext
from enum import Enum
from dataweb import build_general_dq_checks, get_databases, table
from dataweb.assets.datasets.product_analytics_staging import (
    fct_osdcommandschedulerstats,
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
from dataweb.userpkgs.firmware.metric import (
    Metric,
    MetricEnum,
    check_unique_metric_strings,
)
from dataweb.userpkgs.firmware.schema import (
    ColumnType,
    columns_to_names,
    columns_to_schema,
)
from dataweb.userpkgs.firmware.table import ProductAnalytics, ProductAnalyticsStaging
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.utils import (
    build_table_description,
    partition_key_ranges_from_context,
    schema_to_columns_with_property,
)

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

# Request/Response Ratio
# Note that we can uniquely identify a request by the tuple (bus_id, request_id, data_identifier),
# so we must group by those columns until we're ready to sum the final per-bus request and response
# counts.
QUERY = """

WITH
    data AS (
        SELECT
            date
            , type
            , org_id
            , device_id
            , bus_id
            , request_id
            , response_id
            , obd_value
            , COALESCE(sum, 0) AS value

        FROM {product_analytics}.agg_device_stats_primary

        WHERE
            date BETWEEN "{date_start}" AND "{date_end}"
            AND type in (
                {dependencies}
            )
    )

    , pivoted AS (
        SELECT *

        FROM data

        PIVOT (
            SUM(value) AS value
            FOR type IN (
                {pivoted}
            )
        )
    )

    , request_counts AS (
        SELECT
            date
            , org_id
            , device_id
            , bus_id
            , request_id
            , response_id
            , obd_value
            , data_identifier
            -- Clamp the number of responses to 100% of their respective requests
            -- to cover cases where responses are broadcast and therefore might be
            -- received at much greater rates than requested.
            , array_min(array(any_response_count, request_count)) AS response_count
            , request_count

        FROM pivoted
    )

    , request_rates AS (
        SELECT
            date
            , org_id
            , device_id
            , bus_id
            , SUM(response_count) / SUM(request_count) AS value

        FROM request_counts

        GROUP BY ALL
    )

SELECT
    date
    , "{output}" AS type
    , org_id
    , device_id
    , CAST(bus_id AS LONG) AS bus_id
    , CAST(NULL AS LONG) AS request_id
    , CAST(NULL AS LONG) AS response_id
    , CAST(NULL AS LONG) AS obd_value
    , value

FROM
    request_rates

"""


class TableMetric(MetricEnum):
    RATIO = Metric(
        type=ProductAnalyticsStaging.FCT_OSD_COMMAND_SCHEDULER_STATS,
        field=None,
        label="any_response_count-data_identifier-request_count",
    )


class TableDimension(str, Enum):
    VALUE = "value"


check_unique_metric_strings(TableMetric)

METRICS = [
    TableMetric.RATIO,
]


METRIC_DEPENDENCIES = [
    fct_osdcommandschedulerstats.TableMetric.REQUEST_COUNT,
    fct_osdcommandschedulerstats.TableMetric.ANY_RESPONSE_COUNT,
    fct_osdcommandschedulerstats.TableMetric.TOTAL_RESPONSE_COUNT,
    fct_osdcommandschedulerstats.TableMetric.DATA_IDENTIFIER,
]


@table(
    database=Database.PRODUCT_ANALYTICS,
    description=build_table_description(
        table_desc="Derive the request and response ratio for commands sent on vehicle networks. We expect this ratio to approach 1 over time.",
        row_meaning="Response ratio per J1939 device",
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
        asset_name=ProductAnalytics.AGG_DEVICE_STATS_SECONDARY_RESPONSE_RATIO.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_KEYS,
        block_before_write=True,
    ),
    max_retries=5,
)
def agg_device_stats_secondary_response_ratio(context: AssetExecutionContext) -> str:
    partition_keys = partition_key_ranges_from_context(context)[0]
    databases = get_databases()

    return QUERY.format(
        date_start=partition_keys[0],
        date_end=partition_keys[-1],
        product_analytics=databases[Database.PRODUCT_ANALYTICS],
        dependencies=", ".join(f"'{metric}'" for metric in METRIC_DEPENDENCIES),
        pivoted=", ".join(
            f"'{metric}' as {metric.field}" for metric in METRIC_DEPENDENCIES
        ),
        output=TableMetric.RATIO,
    )
