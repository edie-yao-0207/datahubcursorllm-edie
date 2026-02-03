from dataclasses import dataclass

from dagster import AssetExecutionContext
from dataweb import build_general_dq_checks, table
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
    columns_to_schema,
)
from dataweb.userpkgs.firmware.table import ProductAnalyticsStaging
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.utils import (
    build_table_description,
    merge_pyspark_dataframes,
    partition_key_ranges_from_context,
    schema_to_columns_with_property,
)
from pyspark.sql import DataFrame, SparkSession


class TableDimension(StrEnum):
    COUNT_TRANSITIONS = "count_transitions"


class TableMetric(MetricEnum):
    SEATBELT_TOGGLES = Metric(
        type=ProductAnalyticsStaging.FCT_STATE_CHANGE_EVENTS,
        field=TableDimension.COUNT_TRANSITIONS,
        label="seatbelt_toggles",
    )


check_unique_metric_strings(TableMetric)

SCHEMA = columns_to_schema(
    ColumnType.DATE,
    ColumnType.TYPE,
    ColumnType.TIME,
    ColumnType.ORG_ID,
    ColumnType.DEVICE_ID,
    ColumnType.BUS_ID,
    ColumnType.REQUEST_ID,
    ColumnType.RESPONSE_ID,
    ColumnType.OBD_VALUE,
    ColumnType.VALUE,
    Column(
        name=TableDimension.COUNT_TRANSITIONS,
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(
            comment="The number of transitions between the current and previous value."
        ),
    ),
)

PRIMARY_KEYS = schema_to_columns_with_property(SCHEMA)
NON_NULL_COLUMNS = schema_to_columns_with_property(SCHEMA, "nullable")


@dataclass
class StateChangeSettings:
    source: Metric
    """Metric containing state information to count transitions from."""

    dest: Metric
    """Destination metric containing the number of counted transitions."""


SETTINGS = [
    StateChangeSettings(
        source=KinesisStatsMetric.SEAT_BELT_DRIVER_INT_VALUE,
        dest=TableMetric.SEATBELT_TOGGLES,
    ),
]

QUERY = """

WITH
    data AS (
        SELECT
            date
            , time
            , org_id
            , object_id AS device_id
            , {source.field} as value
            , LAG({source.field}) OVER (partition by org_id, object_id order by time) AS prev_value

        FROM
            {source.type}

        WHERE
            date BETWEEN "{date_start}" AND "{date_end}"
            AND NOT value.is_databreak
            AND NOT value.is_end
    )

SELECT
    date
    , "{source.value}" AS type
    , MAX(time) AS time
    , org_id
    , device_id
    , CAST(NULL AS LONG) AS bus_id
    , CAST(NULL AS LONG) AS request_id
    , CAST(NULL AS LONG) AS response_id
    , CAST(NULL AS LONG) AS obd_value
    , CAST(NULL AS DOUBLE) AS value
    , COUNT_IF(value != prev_value) AS count_transitions

FROM
    data

GROUP BY ALL

"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc=(
            "Statistics on the number of times an enumerated value (e.g. seatbelt state or EV charging state) was changed in a given timeframe)."
            " This information can be useful to determine if a given signal is updating frequently when the signal is expected to change infrequently."
        ),
        row_meaning="Number of times a given type/field changed value on a given day",
        related_table_info={},
        table_type=TableType.STAGING,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DATAWEB_PARTITION_DATE,
    upstreams=[f"{AnyUpstream(setting.source.value.type)}" for setting in SETTINGS],
    write_mode=WarehouseWriteMode.OVERWRITE,
    backfill_batch_size=5,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.FCT_STATE_CHANGE_EVENTS.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
)
def fct_statechangeevents(context: AssetExecutionContext) -> DataFrame:
    partition_keys = partition_key_ranges_from_context(context)[0]

    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

    dfs = []
    for settings in SETTINGS:
        query = QUERY.format(
            date_start=partition_keys[0],
            date_end=partition_keys[-1],
            source=settings.source,
            dest=settings.dest,
        )

        context.log.info(query)
        df = spark.sql(query)

        dfs.append(df)

    return merge_pyspark_dataframes(dfs)
