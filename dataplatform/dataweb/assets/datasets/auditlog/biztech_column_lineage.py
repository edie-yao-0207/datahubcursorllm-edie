from dagster import AssetExecutionContext, DailyPartitionsDefinition
from dataweb import NonEmptyDQCheck, table
from dataweb.userpkgs.constants import (
    DATAENGINEERING,
    FRESHNESS_SLO_9AM_PST,
    AWSRegion,
    Database,
    TableType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.utils import build_table_description, partition_key_ranges_from_context
from pyspark.sql import SparkSession

PRIMARY_KEYS = [
    "date",
    "event_time",
    "account_id",
    "metastore_id",
    "workspace_id",
    "source_table_full_name",
    "source_path",
    "source_type",
    "source_column_name",
    "target_table_full_name",
    "target_path",
    "target_type",
    "target_column_name"
]

SCHEMA = [
    {
        "name": "date",
        "type": "date",
        "nullable": False,
        "metadata": {
            "comment": "Date on which query was made"
        },
    },
    {
        "name": "event_time",
        "type": "timestamp",
        "nullable": False,
        "metadata": {
            "comment": "Time at which query was made"
        },
    },
    {
        "name": "account_id",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "AWS account where query was made"
        },
    },
    {
        "name": "metastore_id",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Metastore account where query was made"
        },
    },
    {
        "name": "workspace_id",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Workspace where query was made"
        },
    },
    {
        "name": "source_database_name",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Name of source database"
        },
    },
    {
        "name": "source_table_full_name",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Name of source table"
        },
    },
    {
        "name": "source_path",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "S3 path of source table"
        },
    },
    {
        "name": "source_type",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Source type for lineage"
        },
    },
    {
        "name": "source_column_name",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Name of source column"
        },
    },
    {
        "name": "target_database_name",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Name of target database"
        },
    },
    {
        "name": "target_table_full_name",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Name of target table"
        },
    },
    {
        "name": "target_path",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "S3 path of target table"
        },
    },
    {
        "name": "target_type",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Target type for lineage"
        },
    },
    {
        "name": "target_column_name",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Name of target column"
        },
    },
]

QUERY = """
    WITH column_lineage AS (
        SELECT
            event_date AS date,
            event_time,
            account_id,
            metastore_id,
            workspace_id,
            source_table_full_name,
            source_path,
            source_type,
            source_column_name,
            target_table_full_name,
            target_path,
            target_type,
            target_column_name
        FROM system.access.column_lineage
        WHERE event_date BETWEEN '{FIRST_PARTITION_START}' and '{FIRST_PARTITION_END}'
    )
    SELECT
        date,
        event_time,
        account_id,
        metastore_id,
        workspace_id,
        SPLIT(cl.source_table_full_name, '.')[1] AS source_database_name,
        source_table_full_name,
        source_path,
        source_type,
        source_column_name,
        SPLIT(cl.target_table_full_name, '.')[1] AS target_database_name,
        target_table_full_name,
        target_path,
        target_type,
        target_column_name
    FROM column_lineage cl
    JOIN dataplatform.database_owners do
        ON SPLIT(cl.source_table_full_name, '.')[1] = do.db
    WHERE REGEXP_LIKE(do.owner, '^(BizTech|BusinessSystems)')

    UNION ALL

    SELECT
        date,
        event_time,
        account_id,
        metastore_id,
        workspace_id,
        source_table_full_name,
        source_path,
        source_type,
        SPLIT(cl.source_table_full_name, '.')[1] AS source_database_name,
        source_column_name,
        SPLIT(cl.target_table_full_name, '.')[1] AS target_database_name,
        target_table_full_name,
        target_path,
        target_type,
        target_column_name
    FROM column_lineage cl
    WHERE (
        source_table_full_name LIKE 'edw%'
        OR source_table_full_name LIKE 'businessdbs%'
    )
"""

@table(
    database=Database.AUDITLOG,
    description=build_table_description(
        table_desc="""All column lineage events involving Biztech-owned schemas""",
        row_meaning="""Each row represents a column lineage event from a Biztech-owned schema""",
        related_table_info={},
        table_type=TableType.STAGING,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=[AWSRegion.US_WEST_2],
    owners=[DATAENGINEERING],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DailyPartitionsDefinition(start_date="2024-01-01"),
    write_mode=WarehouseWriteMode.OVERWRITE,
    backfill_batch_size=15,
    dq_checks=[
        NonEmptyDQCheck(name="dq_non_empty_biztech_column_lineage"),
    ],
)
def biztech_column_lineage(context: AssetExecutionContext) -> str:
    context.log.info("Updating biztech_column_lineage")
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    partition_keys = partition_key_ranges_from_context(context)[0]
    FIRST_PARTITION_START = partition_keys[0]
    FIRST_PARTITION_END = partition_keys[-1]
    query = QUERY.format(
        FIRST_PARTITION_START=FIRST_PARTITION_START,
        FIRST_PARTITION_END=FIRST_PARTITION_END,
    )
    context.log.info(f"{query}")
    return query
