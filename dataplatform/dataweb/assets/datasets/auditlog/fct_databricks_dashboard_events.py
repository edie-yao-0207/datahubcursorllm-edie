from dagster import AssetExecutionContext, DailyPartitionsDefinition
from dataweb import table
from dataweb.userpkgs.constants import (
    DATAENGINEERING,
    FRESHNESS_SLO_9AM_PST,
    AWSRegion,
    Database,
    TableType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.utils import build_table_description, partition_key_ranges_from_context, get_all_regions

SCHEMA = [
    {
        "name": "date",
        "type": "date",
        "nullable": False,
        "metadata": {
            "comment": "Date on which the dashboard event occurred"
        },
    },
    {
        "name": "time",
        "type": "timestamp",
        "nullable": False,
        "metadata": {
            "comment": "Time at which the dashboard event occurred"
        },
    },
    {
        "name": "event_id",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Unique identifier for the dashboard event"
        },
    },
    {
        "name": "workspace_id",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Databricks workspace ID where the dashboard event occurred"
        },
    },
    {
        "name": "user_email",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Email of user who performed the dashboard event. At times this is not an email, but a random ID. Check user_agent to determine where the request originated from."
        },
    },
    {
        "name": "user_agent",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Origination of request to perform the dashboard event. When user_email is not an email, this field helps you identify where the request originated from."
        },
    },
    {
        "name": "action_name",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Action performed on the dashboard. This is the action that the user performed on the dashboard. Possible values can be found here: https://docs.databricks.com/aws/en/admin/account-settings/audit-logs#aibi-dashboard-events."
        }
    },
    {
        "name": "dashboard_id",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "ID of the dashboard. To determine the ID of your dashboard, go to the dashboard and copy the ID from the URL. The ID is the part after the `/dashboardsv3/` but before `/published?o=...` in the URL."
        },
    },
]

QUERY = """
    SELECT
    event_date AS date,
    event_time AS time,
    event_id,
    workspace_id,
    user_identity.email AS user_email,
    user_agent,
    action_name,
    request_params.dashboard_id AS dashboard_id
FROM system.access.audit
WHERE event_date BETWEEN '{FIRST_PARTITION_START}' AND '{FIRST_PARTITION_END}'
    AND service_name = 'dashboards'
"""

@table(
    database=Database.AUDITLOG,
    description=build_table_description(
        table_desc="""All Databricks dashboard events across all workspaces. This includes all events that occur on a dashboard, including views, clones, and other actions. The full list of actions can be found here: https://docs.databricks.com/aws/en/admin/account-settings/audit-logs#aibi-dashboard-events.""",
        row_meaning="""Each row represents a dashboard event from any Databricks workspace""",
        related_table_info={},
        table_type=TableType.TRANSACTIONAL_FACT,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=get_all_regions(),
    owners=[DATAENGINEERING],
    schema=SCHEMA,
    primary_keys=["event_id"],
    partitioning=DailyPartitionsDefinition(start_date="2024-01-01"),
    write_mode=WarehouseWriteMode.OVERWRITE,
    backfill_batch_size=15,
    dq_checks=[],
)
def fct_databricks_dashboard_events(context: AssetExecutionContext) -> str:
    context.log.info("Updating fct_databricks_dashboard_events")
    partition_keys = partition_key_ranges_from_context(context)[0]
    FIRST_PARTITION_START = partition_keys[0]
    FIRST_PARTITION_END = partition_keys[-1]
    query = QUERY.format(
        FIRST_PARTITION_START=FIRST_PARTITION_START,
        FIRST_PARTITION_END=FIRST_PARTITION_END,
    )
    context.log.info(f"{query}")
    return query
