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
        "name": "email",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Email of user making the query"
        },
    },
    {
        "name": "action_name",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Action being performed by user"
        },
    },
    {
        "name": "service_name",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Service name corresponding to query"
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
        "name": "user_agent",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "User agent where query originated"
        },
    },
    {
        "name": "request_id",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Request ID of query"
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
        "name": "request_params",
        "type": {'type': 'map', 'keyType': 'string', 'valueType': 'string', 'valueContainsNull': True},
        "nullable": False,
        "metadata": {
            "comment": "Request params from event"
        },
    },
    {
        "name": "status_code",
        "type": "integer",
        "nullable": False,
        "metadata": {
            "comment": "Status code of the event"
        },
    },
    {
        "name": "error_message",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Error message of the event"
        },
    },
    {
        "name": "result",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Result of the event"
        },
    },
    {
        "name": "run_by",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "User running the event"
        },
    },
    {
        "name": "run_as",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Role being assumed to run the event"
        },
    },
    {
        "name": "session_id",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Session ID where the event was run"
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
    user_identity.email,
    action_name,
    service_name,
    COALESCE(NULLIF(audit.workspace_id, 0), audit.request_params['workspace_id']) AS workspace_id,
    account_id,
    user_agent,
    request_id,
    event_id,
    request_params,
    response.status_code,
    response.error_message,
    response.result,
    identity_metadata.run_by,
    identity_metadata.run_as,
    session_id
FROM system.access.audit audit
JOIN workspace_owners rollup
     ON COALESCE(NULLIF(audit.workspace_id, 0), audit.request_params['workspace_id']) = rollup.workspaceid
WHERE rollup.workspaceowner = 'biztech'
  AND event_date BETWEEN '{FIRST_PARTITION_START}' AND '{FIRST_PARTITION_END}'
"""

@table(
    database=Database.AUDITLOG,
    description=build_table_description(
        table_desc="""All audit events from the Biztech workspace""",
        row_meaning="""Each row represents an audit event from the Biztech workspace""",
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
        PrimaryKeyDQCheck(name="dq_pk_biztech_audit", primary_keys=["event_id"]),
    ],
)
def biztech_audit(context: AssetExecutionContext) -> str:
    context.log.info("Updating biztech_audit")
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
