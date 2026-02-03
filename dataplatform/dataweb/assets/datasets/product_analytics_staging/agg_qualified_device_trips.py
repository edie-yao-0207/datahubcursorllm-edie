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
from dataweb.userpkgs.firmware.schema import (
    Column,
    ColumnType,
    DataType,
    Metadata,
    columns_to_schema,
)
from dataweb.userpkgs.firmware.table import DataModelTelematics, ProductAnalyticsStaging
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.utils import (
    build_table_description,
    schema_to_columns_with_property,
)
from dataweb.userpkgs.query import (
    format_date_partition_query,
)

SCHEMA = columns_to_schema(
    ColumnType.DATE,
    ColumnType.ORG_ID,
    ColumnType.DEVICE_ID,
    Column(
        name="qualified",
        type=DataType.BOOLEAN,
        nullable=False,
        metadata=Metadata(
            comment="Indicates whether a device had a qualified trip on a given day."
        ),
    ),
    Column(
        name="total_distance_meters",
        type=DataType.DOUBLE,
        nullable=False,
        metadata=Metadata(
            comment="The total distance travelled by a device on a given day."
        ),
    ),
)

PRIMARY_KEYS = schema_to_columns_with_property(SCHEMA)
NON_NULL_COLUMNS = schema_to_columns_with_property(SCHEMA, "nullable")

QUERY = """
WITH

data AS (
    SELECT
    date
        , org_id
        , device_id
        , trip_type
        , distance_meters
        , duration_mins
        , engine_on_mins
    FROM datamodel_telematics.fct_trips
    WHERE date BETWEEN "{date_start}" AND "{date_end}"
    GROUP BY ALL
)

SELECT
    date
    , org_id
    , device_id
    , MAX(
        CASE
            -- Location based trips were initially designed for vehicle installations. If we have any trip over
            -- 1KM in length, we want to use this device in our metrics. If a device had no movement for a given
            -- day, then we don't know if diagnostics SHOULD be available. Diagnostics being available depends on
            -- a trip happening where we have enough time to start a session. Without this filtering we see noise
            -- where devices are included but haven't driven in months.
            WHEN trip_type = "location_based" THEN distance_meters > 1000

            -- Engine based trips only exist if they are enabled on an asset class. As of 2025-02-26 this is
            -- enabled to report for Operated Equipment. In the event of seeing engine_based trips, we should
            -- ignore distance travelled altogether. The trip is marked by the detection of engine ON / OFF
            -- events.
            -- https://app.launchdarkly.com/projects/samsara/flags/heavy-equipment-enabled/targeting?env=production&selected-env=production
            WHEN trip_type = "engine_based" THEN engine_on_mins > 2

            ELSE FALSE
        END
    ) AS qualified
    , SUM(distance_meters) AS total_distance_meters
FROM data
GROUP BY ALL
"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Track if a device has received a qualified trip in a given day for VDP metrics.",
        row_meaning="A given device has had a qualified trip on a given day.",
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
        AnyUpstream(DataModelTelematics.FCT_TRIPS),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    backfill_batch_size=1,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.AGG_QUALIFIED_DEVICE_TRIPS.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
)
def agg_qualified_device_trips(context: AssetExecutionContext) -> str:
    return format_date_partition_query(QUERY, context)
