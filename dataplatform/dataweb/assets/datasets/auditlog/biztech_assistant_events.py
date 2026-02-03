from dagster import AssetExecutionContext, DailyPartitionsDefinition, BackfillPolicy
from dataweb import NonNullDQCheck, PrimaryKeyDQCheck, table
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
        "name": "event_id",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Event ID of query"
        },
    },
    {
        "name": "workspace_id",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Databricks workspace where query was made"
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
        "name": "email",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Email of user making the query"
        },
    },
]

QUERY = """
    WITH workspace_owners AS (
        SELECT DISTINCT
            workspaceid,
            workspaceowner
        FROM billing.databricks_rollup
    )
    SELECT
        event_date AS date,
        event_time,
        event_id,
        workspace_id,
        account_id,
        initiated_by AS email
    FROM system.access.assistant_events assistant
    JOIN workspace_owners rollup
        ON assistant.workspace_id = rollup.workspaceid
    WHERE rollup.workspaceowner = 'biztech'
    AND event_date BETWEEN '{FIRST_PARTITION_START}' and '{FIRST_PARTITION_END}'
"""

@table(
    database=Database.AUDITLOG,
    description=build_table_description(
        table_desc="""Databricks Assistant events in the Biztech workspaces""",
        row_meaning="""Each row represents an Assistant event from the Biztech workspaces to see overall usage""",
        related_table_info={},
        table_type=TableType.STAGING,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=[AWSRegion.US_WEST_2],
    owners=[DATAENGINEERING],
    schema=SCHEMA,
    primary_keys=["event_id"],
    partitioning=DailyPartitionsDefinition(start_date="2024-01-01"),
    write_mode=WarehouseWriteMode.OVERWRITE,
    backfill_batch_size=15,
    dq_checks=[
        PrimaryKeyDQCheck(name="dq_pk_biztech_assistant_events", primary_keys=["event_id"]),
    ],
)
def biztech_assistant_events(context: AssetExecutionContext) -> str:
    context.log.info("Updating biztech_assistant_events")
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
