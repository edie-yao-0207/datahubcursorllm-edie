from dagster import AssetExecutionContext, DailyPartitionsDefinition
from dataweb import NonEmptyDQCheck, NonNullDQCheck, PrimaryKeyDQCheck, table
from dataweb.userpkgs.constants import (
    ALL_COMPUTE_REGIONS,
    DATAENGINEERING,
    FRESHNESS_SLO_9AM_PST,
    Database,
    TableType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.utils import build_table_description
from pyspark.sql import SparkSession

SCHEMA = [
    {
        "name": "date",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "The field which partitions the table, in 'YYYY-mm-dd' format."
        },
    },
    {
        "name": "uuid",
        "type": "string",
        "nullable": False,
        "metadata": {"comment": "Unique ID of the application"},
    },
    {
        "name": "name",
        "type": "string",
        "nullable": False,
        "metadata": {"comment": "Name of the application"},
    },
    {
        "name": "approval_state",
        "type": "string",
        "nullable": False,
        "metadata": {"comment": "State of the application's approval"},
    },
    {
        "name": "created_at",
        "type": "timestamp",
        "nullable": False,
        "metadata": {"comment": "Timestamp at which the application was created"},
    },
    {
        "name": "identifier",
        "type": "string",
        "nullable": False,
        "metadata": {"comment": "How the application is identified (ID or name)"},
    },
]

QUERY = """
    WITH
    developer_apps_name AS (
    SELECT name,
        MAX_BY(uuid,
            CASE WHEN approval_state = 'Unpublished' THEN 0
            WHEN approval_state = 'Past Version' THEN 1
            WHEN approval_state = 'Pending' THEN 2
            WHEN approval_state = 'Beta' THEN 3
            WHEN approval_state = 'Published' THEN 4 END) AS uuid,
        MIN(created_at) AS created_at,
        MAX_BY(approval_state,
            CASE WHEN approval_state = 'Unpublished' THEN 0
            WHEN approval_state = 'Past Version' THEN 1
            WHEN approval_state = 'Pending' THEN 2
            WHEN approval_state = 'Beta' THEN 3
            WHEN approval_state = 'Published' THEN 4 END) AS approval_state
    FROM clouddb.developer_apps TIMESTAMP AS OF '{partition_date} 23:59:59.999' apps
    GROUP BY 1),
    developer_apps_uuid AS (
    SELECT  uuid,
            MAX_BY(name, CASE WHEN approval_state = 'Unpublished' THEN 0
            WHEN approval_state = 'Past Version' THEN 1
            WHEN approval_state = 'Pending' THEN 2
            WHEN approval_state = 'Beta' THEN 3
            WHEN approval_state = 'Published' THEN 4 END) AS name,
            MIN(created_at) AS created_at,
            MAX_BY(approval_state,
            CASE WHEN approval_state = 'Unpublished' THEN 0
            WHEN approval_state = 'Past Version' THEN 1
            WHEN approval_state = 'Pending' THEN 2
            WHEN approval_state = 'Beta' THEN 3
            WHEN approval_state = 'Published' THEN 4 END) AS approval_state
    FROM clouddb.developer_apps TIMESTAMP AS OF '{partition_date} 23:59:59.999' apps
    GROUP BY 1),
    unified AS (
    (SELECT uuid,
        name,
        approval_state,
        created_at,
        'uuid' AS identifier
    FROM developer_apps_uuid)

    UNION ALL

    (SELECT uuid,
        name,
        approval_state,
        created_at,
        'name' AS identifier
    FROM developer_apps_name))

    SELECT '{partition_date}' AS date,
        *
    FROM unified
    """


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="""Deduplicate and stage developer app metadata""",
        row_meaning="""Developer app metadata""",
        related_table_info={},
        table_type=TableType.STAGING,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[DATAENGINEERING],
    schema=SCHEMA,
    primary_keys=["date", "uuid", "name", "approval_state", "identifier"],
    partitioning=DailyPartitionsDefinition(start_date="2024-09-12"),
    upstreams=["clouddb.developer_apps"],
    dq_checks=[
        NonEmptyDQCheck(name="dq_non_empty_stg_developer_apps"),
        NonNullDQCheck(name="dq_non_null_stg_developer_apps", non_null_columns=["date", "uuid", "name", "approval_state", "created_at", "identifier"], block_before_write=True),
        PrimaryKeyDQCheck(name="dq_pk_stg_developer_apps", primary_keys=["date", "uuid", "name", "approval_state", "identifier"], block_before_write=True)
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
)
def stg_developer_apps(context: AssetExecutionContext) -> str:
    context.log.info("Updating stg_developer_apps")
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    partition_date = context.partition_key
    query = QUERY.format(partition_date=partition_date)
    context.log.info(f"{query}")
    return query
