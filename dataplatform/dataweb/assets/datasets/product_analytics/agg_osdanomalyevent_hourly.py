from dagster import AssetExecutionContext, DailyPartitionsDefinition
from dataweb import build_general_dq_checks, get_databases, table
from dataweb.userpkgs.constants import (
    ALL_COMPUTE_REGIONS,
    DATAENGINEERING,
    FRESHNESS_SLO_9AM_PST,
    Database,
    TableType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.utils import (
    build_table_description,
    partition_key_ranges_from_context,
    schema_to_columns_with_property,
)

SCHEMA = [
    {
        "name": "date",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "The date the event was logged (yyyy-mm-dd)."},
    },
    {
        "name": "hour_utc",
        "type": "integer",
        "nullable": True,
        "metadata": {"comment": "The hour of the anomaly."},
    },
    {
        "name": "org_id",
        "type": "long",
        "nullable": True,
        "metadata": {"comment": "The Internal ID for the customer`s Samsara org."},
    },
    {
        "name": "device_id",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "The ID of the customer device that the data belongs to"
        },
    },
    {
        "name": "build",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "The current build running when the anomaly was reported."
        },
    },
    {
        "name": "service_name_prefix",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "The prefix of the service name that appears in an anomaly; e.g., if in the kinesisstat serivce_name = obd:firmware.samsaradev.io/obd/vehicle/j1939/address_claim_bus.go:260, then service_name_prefix = obd."
        },
    },
    {
        "name": "manager_name",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Applications are sometimes bundled together in the same service (especially on VG) because it saves precious RAM. In those instances the unit of functionality is not the service, but rather something we call 'manager' (accel manager, log manager, etc.)."
        },
    },
    {
        "name": "short_or_truncated_description",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Either the shortened description from the definitions file or the first 100 characters of the full description."
        },
    },
    {
        "name": "anomaly_count",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "The total number of anomalies that occurred for the given dimensions."
        },
    },
]

PRIMARY_KEYS = [
    "date",
    "hour_utc",
    "org_id",
    "device_id",
    "build",
    "service_name_prefix",
    "manager_name",
    "short_or_truncated_description",
]

NON_NULL_KEYS = schema_to_columns_with_property(SCHEMA, "nullable")


@table(
    database=Database.PRODUCT_ANALYTICS,
    description=build_table_description(
        table_desc="""Counts anomaly events by hour per device, build, service, and shortened or truncated anomaly description.""",
        row_meaning="""Counts of anomaly events per dimensions""",
        related_table_info={},
        table_type=TableType.DAILY_DIMENSION,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[DATAENGINEERING],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DailyPartitionsDefinition(start_date="2024-01-01"),
    upstreams=["kinesisstats.fct_osdanomlyevent"],
    write_mode=WarehouseWriteMode.OVERWRITE,
    backfill_batch_size=1,
    dq_checks=build_general_dq_checks(
        asset_name="agg_osdanomalyevent_hourly",
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_KEYS,
    ),
)
def agg_osdanomalyevent_hourly(context: AssetExecutionContext) -> str:
    context.log.info("Updating agg_osdanomalyevent_hourly...")

    # A simple date range WHERE filter is sufficient for this query.
    partition_keys = partition_key_ranges_from_context(context)[0]
    FIRST_PARTITION_START = partition_keys[0]
    FIRST_PARTITION_END = partition_keys[-1]

    query = f"""--sql
    WITH anomalies_join_patterns AS (
        SELECT
            date
            , time
            , FROM_UNIXTIME(time/1000, 'yyyy-MM-dd HH:mm:ss') AS datetime_utc
            , TIMESTAMP_MILLIS(time) AS datetime_ms_utc
            , HOUR(TIMESTAMP_MILLIS(time)) AS hour_utc
            , org_id
            , object_id AS device_id
            , value.proto_value.anomaly_event.build
            , value.proto_value.anomaly_event.service_name
            , CASE
                WHEN CHARINDEX(':', value.proto_value.anomaly_event.service_name) = 0 THEN value.proto_value.anomaly_event.service_name
                ELSE LEFT(value.proto_value.anomaly_event.service_name, CHARINDEX(':', value.proto_value.anomaly_event.service_name) - 1)
            END AS service_name_prefix
            , value.proto_value.anomaly_event.manager_name
            , value.proto_value.anomaly_event.description
            , asd.short_description
        FROM kinesisstats.osdanomalyevent a
        LEFT OUTER JOIN definitions.anomaly_patterns asd
            ON a.value.proto_value.anomaly_event.description LIKE asd.pattern
        WHERE
            a.date BETWEEN "{FIRST_PARTITION_START}" AND "{FIRST_PARTITION_END}"
            AND NOT a.value.is_end
            AND NOT a.value.is_databreak
    )
    SELECT
        date
        , hour_utc
        , org_id
        , device_id
        , build
        , service_name_prefix
        , manager_name
        , LEFT(COALESCE(short_description, description), 100) AS short_or_truncated_description
        , COUNT(1) AS anomaly_count
    FROM anomalies_join_patterns
    GROUP BY 1,2,3,4,5,6,7,8
    --endsql"""
    query = query.format(
        FIRST_PARTITION_START=FIRST_PARTITION_START,
        FIRST_PARTITION_END=FIRST_PARTITION_END,
    )
    context.log.info(f"{query}")

    return query
