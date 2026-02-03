from dagster import AssetExecutionContext, DailyPartitionsDefinition
from dataweb import table
from dataweb.userpkgs.constants import (
    ALL_COMPUTE_REGIONS,
    DATAENGINEERING,
    Database,
    InstanceType,
    TableType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.query import create_run_config_overrides
from dataweb.userpkgs.utils import (
    build_table_description,
    partition_key_ranges_from_context,
)

SCHEMA = [
    {
        "name": "date",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "The date the event was logged (yyyy-mm-dd)."},
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
        "name": "time",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "The time the event was logged in milliseconds since the unix epoch (UTC)."
        },
    },
    {
        "name": "build",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "The current build running when the metrics were reported."
        },
    },
    {
        "name": "product_name",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "The product name reported by firmware, typically config.public_product_name."
        },
    },
    {
        "name": "hardware_version",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Identifies the version of hardware on which the firmware is running."
        },
    },
    {
        "name": "name",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "The name of the metric."},
    },
    {
        "name": "metric_type",
        "type": "integer",
        "nullable": True,
        "metadata": {"comment": "The metric type\n0: INVALID\n1: COUNT\n2: GAUGE."},
    },
    {
        "name": "value",
        "type": "float",
        "nullable": True,
        "metadata": {
            "comment": "The value of the metric, interpreted based on its type."
        },
    },
    {
        "name": "tags",
        "type": {"type": "array", "elementType": "string", "containsNull": True},
        "nullable": True,
        "metadata": {
            "comment": "List of tags associated with this metric when it was reported."
        },
    },
    {
        "name": "gauge_average_count",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "The total number of samples which used to calculate the gauge average metric, whose usage is all_tags_value = sum(metric.value * metric.gauge_average_count for each metric) / sum(metric.gauge_average_count for each metric)"
        },
    },
]


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="""Extract firmware metrics to individual rows per firmware metric.""",
        row_meaning="""A given firmware metric for a device""",
        related_table_info={},
        table_type=TableType.STAGING,
        freshness_slo_updated_by="9am PST",
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[DATAENGINEERING],
    schema=SCHEMA,
    primary_keys=["date", "org_id", "device_id", "time", "name"],
    partitioning=DailyPartitionsDefinition(start_date="2024-01-01"),
    upstreams=["kinesisstats.osdfirmwaremetrics"],
    write_mode=WarehouseWriteMode.OVERWRITE,
    # This appears to be superseded by the backfill_batch_size in the schedule definition.
    backfill_batch_size=4,
    run_config_overrides=create_run_config_overrides(
        driver_instance_type=InstanceType.RD_FLEET_2XLARGE,
        worker_instance_type=InstanceType.RD_FLEET_4XLARGE,
        max_workers=32,
    ),
)
def fct_osdfirmwaremetrics(context: AssetExecutionContext) -> str:
    context.log.info("Updating fct_osdfirmwaremetrics...")

    # A simple date range WHERE filter is sufficient for this query.
    partition_keys = partition_key_ranges_from_context(context)[0]
    FIRST_PARTITION_START = partition_keys[0]
    FIRST_PARTITION_END = partition_keys[-1]

    query = f"""--sql
    SELECT
        `date`,
        org_id,
        object_id AS device_id,
        `time`,
        value.proto_value.firmware_metrics.build,
        value.proto_value.firmware_metrics.product_name,
        value.proto_value.firmware_metrics.hardware_version,
        metric.name,
        metric.metric_type,
        metric.value,
        metric.tags,
        metric.gauge_average_count
    FROM
        kinesisstats_history.osdfirmwaremetrics
    LATERAL VIEW
        EXPLODE(value.proto_value.firmware_metrics.metrics) AS metric
    WHERE
        `date` BETWEEN "{FIRST_PARTITION_START}" AND "{FIRST_PARTITION_END}"
        AND NOT value.is_end
        AND NOT value.is_databreak
    --endsql"""
    query = query.format(
        FIRST_PARTITION_START=FIRST_PARTITION_START,
        FIRST_PARTITION_END=FIRST_PARTITION_END,
    )
    context.log.info(f"{query}")

    return query
