from dagster import AssetExecutionContext
from dataweb import (
    NonEmptyDQCheck,
    NonNullDQCheck,
    PrimaryKeyDQCheck,
    table
)
from dataweb.userpkgs.constants import (
    DATAENGINEERING,
    AWSRegion,
    Database,
    WarehouseWriteMode,
)
from dataweb.userpkgs.utils import (
    build_table_description,
    partition_key_ranges_from_context,
    TableType
)

fct_alert_details_global_schema = [
    {
        "name": "date",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "The UTC date corresponding to the timestamp when this alert incident was triggered"
        },
    },
    {
        "name": "actioned_date",
        "type": "date",
        "nullable": True,
        "metadata": {
            "comment": "The UTC date when the incident was opened in the Samsara dashboard"
        },
    },
    {
        "name": "org_id",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Unique identifier for organizations. An account has multiple organizations under it. Joins to datamodel_core.dim_organizations"
        },
    },
    {
        "name": "alert_config_id",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Hashed version of the alert_config_uuid to help with joining to the corresponding fact table"
        },
    },
    {
        "name": "alert_config_uuid",
        "type": "string",
        "nullable": False,
        "metadata": {"comment": "The unique identifer of a configured alert"},
    },
    {
        "name": "trigger_type_ids",
        "type": {"type": "array", "elementType": "long", "containsNull": True},
        "nullable": True,
        "metadata": {
            "comment": "List of trigger type IDs that were triggered by the alert incident. Maps to definitions.alert_trigger_types"
        },
    },
    {
        "name": "trigger_types",
        "type": {"type": "array", "elementType": "string", "containsNull": True},
        "nullable": True,
        "metadata": {
            "comment": "List of trigger types that were triggered by the alert incident. Maps to definitions.alert_trigger_types"
        },
    },
    {
        "name": "alert_action_types",
        "type": {"type": "array", "elementType": "string", "containsNull": True},
        "nullable": True,
        "metadata": {
            "comment": "List of action types that are configured to be taken when the alert is triggered. For e.g., DashboardNotification, Webhook"
        },
    },
    {
        "name": "alert_recipient_types",
        "type": {"type": "array", "elementType": "string", "containsNull": True},
        "nullable": True,
        "metadata": {
            "comment": "List of recipient types that are notified when the alerts are triggered. Possible values include 'contact' and 'user'"
        },
    },
    {
        "name": "alert_notification_types",
        "type": {"type": "array", "elementType": "string", "containsNull": True},
        "nullable": True,
        "metadata": {
            "comment": """When one of the alert actions is 'Notification', list of notification types that are sent out when the alert is triggered.
            Possible values include 'email', 'sms' and 'push'  """
        },
    },
    {
        "name": "alert_targets_object_type",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "The object type (for e.g., device, widget, driver etc.,) specified as a target for the alert"
        },
    },
    {
        "name": "alert_targets_object_ids",
        "type": {"type": "array", "elementType": "long", "containsNull": True},
        "nullable": True,
        "metadata": {"comment": "List of object IDs for the given alert type"},
    },
    {
        "name": "alert_status",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "The current status of the alert incident. One of - active/resolved"
        },
    },
    {
        "name": "notification_sent",
        "type": "boolean",
        "nullable": True,
        "metadata": {
            "comment": "True/False flag indicating whether an email, push or sms notification was sent when the incident was triggered"
        },
    },
    {
        "name": "object_type_id",
        "type": "integer",
        "nullable": False,
        "metadata": {
            "comment": "The object type ID (for e.g., device, widget, driver etc.,) that triggered this alert incident"
        },
    },
    {
        "name": "object_id",
        "type": "long",
        "nullable": False,
        "metadata": {"comment": "The object ID that triggered this alert incident"},
    },
    {
        "name": "alert_occurred_at_ms",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Time in epoch milliseconds for when the alert incident was triggered"
        },
    },
    {
        "name": "alert_resolved_at_ms",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "Time in epoch milliseconds for when the alert incident was resolved"
        },
    },
    {
        "name": "mp_processing_time_ms",
        "type": "double",
        "nullable": True,
        "metadata": {
            "comment": "Time that mixpanel processed the event in unix timestamp milliseconds. This will be -1 if the incident has not been opened."
        },
    },
    {
        "name": "time_to_open_ms",
        "type": "double",
        "nullable": True,
        "metadata": {
            "comment": "Time to open Mixpanel URL for a given incident. Will be -1 if not opened"
        },
    },
    {
        "name": "alert_config_created_ts_utc",
        "type": "timestamp",
        "nullable": False,
        "metadata": {"comment": "The creation timestamp of the alert config in UTC"},
    },
    {
        "name": "alert_config_deleted_ts_utc",
        "type": "timestamp",
        "nullable": True,
        "metadata": {
            "comment": "The timestamp in UTC when the alert config was deleted"
        },
    },
    {
        "name": "mp_current_url",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "The url of the page viewed by the user."},
    },
    {
        "name": "user_id",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "Samsara user id inferred from combination of useremail and organization. NULL if user was deleted from"
        },
    },
    {
        "name": "is_alert_viewed",
        "type": "boolean",
        "nullable": False,
        "metadata": {"comment": "Whether the alert was opened in Mixpanel or not"},
    },
    {
        "name": "region",
        "type": "string",
        "nullable": False,
        "metadata": {"comment": "Region where data comes from"},
    },
]


fct_alert_details_global_query = """
--sql
-- Grabbing the last 7 days of alerts as needed for our backfill
WITH all_alerts_us AS (
    SELECT
        date,
        org_id,
        alert_config_id,
        alert_config_uuid,
        trigger_type_ids,
        trigger_types,
        alert_status,
        notification_sent,
        alert_occurred_at_ms,
        alert_resolved_at_ms,
        alert_triggered_ts_utc,
        object_type_id,
        object_id
    FROM datamodel_platform.fct_alert_incidents
    WHERE date BETWEEN '{START_DATEID}' AND '{END_DATEID}'
),
all_alerts_eu AS (
    SELECT
        date,
        org_id,
        alert_config_id,
        alert_config_uuid,
        trigger_type_ids,
        trigger_types,
        alert_status,
        notification_sent,
        alert_occurred_at_ms,
        alert_resolved_at_ms,
        alert_triggered_ts_utc,
        object_type_id,
        object_id
    FROM delta.`s3://samsara-eu-datamodel-warehouse/datamodel_platform.db/fct_alert_incidents`
    WHERE date BETWEEN '{START_DATEID}' AND '{END_DATEID}'
),
all_alerts_ca AS (
    SELECT
        date,
        org_id,
        alert_config_id,
        alert_config_uuid,
        trigger_type_ids,
        trigger_types,
        alert_status,
        notification_sent,
        alert_occurred_at_ms,
        alert_resolved_at_ms,
        alert_triggered_ts_utc,
        object_type_id,
        object_id
    FROM data_tools_delta_share_ca.datamodel_platform.fct_alert_incidents
    WHERE date BETWEEN '{START_DATEID}' AND '{END_DATEID}'
),
-- Grabbing 14 days of Mixpanel data since we're assuming an incident should be ack'ed within a week of being triggered
-- Index 8 and 11 are based on the current URL pattern
url_components AS (
    SELECT
        DATE(`date`) AS date,
        mp_current_url,
        mp_processing_time_ms,
        user_id,
        org_id,
        SPLIT(mp_current_url, '/')[8] AS workflow_uuid,
        SPLIT(mp_current_url, '/')[11] AS alert_raised_ms
    FROM datamodel_platform_silver.stg_cloud_routes
    WHERE
        date BETWEEN DATE_SUB('{START_DATEID}', 7) AND '{END_DATEID}'
        AND mp_current_url LIKE '%fleet/workflows/incidents%'
),
-- Joins incidents with Mixpanel data based on URL pattern
alerts_metrics_us AS (
    SELECT
        /*+ RANGE_JOIN(url, 1) */
        aa.date,
        url.date AS actioned_date,
        aa.org_id,
        aa.alert_config_id,
        aa.alert_config_uuid,
        aa.trigger_type_ids,
        aa.trigger_types,
        ac.alert_action_types,
        ac.alert_recipient_types,
        ac.alert_notification_types,
        exploded_table.alert_targets_object_type,
        exploded_table.alert_targets_object_ids,
        aa.alert_status,
        aa.notification_sent,
        aa.object_type_id,
        aa.object_id,
        aa.alert_occurred_at_ms,
        aa.alert_resolved_at_ms,
        COALESCE(url.mp_processing_time_ms, -1) AS mp_processing_time_ms,
        COALESCE(url.mp_processing_time_ms - aa.alert_occurred_at_ms, -1) AS time_to_open_ms,
        ac.created_ts_utc AS alert_config_created_ts_utc,
        ac.deleted_ts_utc AS alert_config_deleted_ts_utc,
        url.mp_current_url,
        url.user_id,
        CASE WHEN url.mp_current_url IS NULL THEN false ELSE true END AS is_alert_viewed,
        'us-west-2' AS region
    FROM all_alerts_us aa
    JOIN datamodel_platform.dim_alert_configs ac
        ON ac.org_id = aa.org_id
        AND ac.alert_config_uuid = aa.alert_config_uuid
    LEFT JOIN url_components url
        ON aa.org_id = url.org_id
        AND url.date BETWEEN aa.date AND DATE_ADD(aa.date, 7)
        AND url.mp_current_url LIKE CONCAT('%/o/', aa.org_id, '/fleet/workflows/incidents/', url.workflow_uuid, '/', aa.object_type_id, '/', aa.object_id, '/', aa.alert_occurred_at_ms)
        AND aa.alert_occurred_at_ms = url.alert_raised_ms
        AND REPLACE(url.workflow_uuid, '-', '') = LOWER(aa.alert_config_uuid)
    LATERAL VIEW EXPLODE(ac.alert_targets_by_object_type) exploded_table AS alert_targets_object_type, alert_targets_object_ids
    WHERE
        ac.date = '{END_DATEID}'
        AND ac.alert_targets_by_object_type IS NOT NULL

    UNION ALL

    SELECT
        /*+ RANGE_JOIN(url, 1) */
        aa.date,
        url.date AS actioned_date,
        aa.org_id,
        aa.alert_config_id,
        aa.alert_config_uuid,
        aa.trigger_type_ids,
        aa.trigger_types,
        ac.alert_action_types,
        ac.alert_recipient_types,
        ac.alert_notification_types,
        NULL AS alert_targets_object_type,
        NULL AS alert_targets_object_ids,
        aa.alert_status,
        aa.notification_sent,
        aa.object_type_id,
        aa.object_id,
        aa.alert_occurred_at_ms,
        aa.alert_resolved_at_ms,
        COALESCE(url.mp_processing_time_ms, -1) AS mp_processing_time_ms,
        COALESCE(url.mp_processing_time_ms - aa.alert_occurred_at_ms, -1) AS time_to_open_ms,
        ac.created_ts_utc AS alert_config_created_ts_utc,
        ac.deleted_ts_utc AS alert_config_deleted_ts_utc,
        url.mp_current_url,
        url.user_id,
        CASE WHEN url.mp_current_url IS NULL THEN false ELSE true END AS is_alert_viewed,
        'us-west-2' AS region
    FROM all_alerts_us aa
    JOIN datamodel_platform.dim_alert_configs ac
        ON ac.org_id = aa.org_id
        AND ac.alert_config_uuid = aa.alert_config_uuid
    LEFT JOIN url_components url
        ON aa.org_id = url.org_id
        AND url.date BETWEEN aa.date AND DATE_ADD(aa.date, 7)
        AND url.mp_current_url LIKE CONCAT('%/o/', aa.org_id, '/fleet/workflows/incidents/', url.workflow_uuid, '/', aa.object_type_id, '/', aa.object_id, '/', aa.alert_occurred_at_ms)
        AND aa.alert_occurred_at_ms = url.alert_raised_ms
        AND REPLACE(url.workflow_uuid, '-', '') = LOWER(aa.alert_config_uuid)
    WHERE
        ac.date = '{END_DATEID}'
        AND ac.alert_targets_by_object_type IS NULL
),
alert_metrics_eu AS (
    SELECT
        /*+ RANGE_JOIN(url, 1) */
        aa.date,
        url.date AS actioned_date,
        aa.org_id,
        aa.alert_config_id,
        aa.alert_config_uuid,
        aa.trigger_type_ids,
        aa.trigger_types,
        ac.alert_action_types,
        ac.alert_recipient_types,
        ac.alert_notification_types,
        exploded_table.alert_targets_object_type,
        exploded_table.alert_targets_object_ids,
        aa.alert_status,
        aa.notification_sent,
        aa.object_type_id,
        aa.object_id,
        aa.alert_occurred_at_ms,
        aa.alert_resolved_at_ms,
        COALESCE(url.mp_processing_time_ms, -1) AS mp_processing_time_ms,
        COALESCE(url.mp_processing_time_ms - aa.alert_occurred_at_ms, -1) AS time_to_open_ms,
        ac.created_ts_utc AS alert_config_created_ts_utc,
        ac.deleted_ts_utc AS alert_config_deleted_ts_utc,
        url.mp_current_url,
        url.user_id,
        CASE WHEN url.mp_current_url IS NULL THEN false ELSE true END AS is_alert_viewed,
        'eu-west-1' AS region
    FROM all_alerts_eu aa
    JOIN delta.`s3://samsara-eu-datamodel-warehouse/datamodel_platform.db/dim_alert_configs` ac
        ON ac.org_id = aa.org_id
        AND ac.alert_config_uuid = aa.alert_config_uuid
    LEFT JOIN url_components url
        ON aa.org_id = url.org_id
        AND url.date BETWEEN aa.date AND DATE_ADD(aa.date, 7)
        AND url.mp_current_url LIKE CONCAT('%/o/', aa.org_id, '/fleet/workflows/incidents/', url.workflow_uuid, '/', aa.object_type_id, '/', aa.object_id, '/', aa.alert_occurred_at_ms)
        AND aa.alert_occurred_at_ms = url.alert_raised_ms
        AND REPLACE(url.workflow_uuid, '-', '') = LOWER(aa.alert_config_uuid)
    LATERAL VIEW EXPLODE(ac.alert_targets_by_object_type) exploded_table AS alert_targets_object_type, alert_targets_object_ids
    WHERE
        ac.date = '{END_DATEID}'
        AND ac.alert_targets_by_object_type IS NOT NULL

    UNION ALL

    SELECT
        /*+ RANGE_JOIN(url, 1) */
        aa.date,
        url.date AS actioned_date,
        aa.org_id,
        aa.alert_config_id,
        aa.alert_config_uuid,
        aa.trigger_type_ids,
        aa.trigger_types,
        ac.alert_action_types,
        ac.alert_recipient_types,
        ac.alert_notification_types,
        NULL AS alert_targets_object_type,
        NULL AS alert_targets_object_ids,
        aa.alert_status,
        aa.notification_sent,
        aa.object_type_id,
        aa.object_id,
        aa.alert_occurred_at_ms,
        aa.alert_resolved_at_ms,
        COALESCE(url.mp_processing_time_ms, -1) AS mp_processing_time_ms,
        COALESCE(url.mp_processing_time_ms - aa.alert_occurred_at_ms, -1) AS time_to_open_ms,
        ac.created_ts_utc AS alert_config_created_ts_utc,
        ac.deleted_ts_utc AS alert_config_deleted_ts_utc,
        url.mp_current_url,
        url.user_id,
        CASE WHEN url.mp_current_url IS NULL THEN false ELSE true END AS is_alert_viewed,
        'eu-west-1' AS region
    FROM all_alerts_eu aa
    JOIN delta.`s3://samsara-eu-datamodel-warehouse/datamodel_platform.db/dim_alert_configs` ac
        ON ac.org_id = aa.org_id
        AND ac.alert_config_uuid = aa.alert_config_uuid
    LEFT JOIN url_components url
        ON aa.org_id = url.org_id
        AND url.date BETWEEN aa.date AND DATE_ADD(aa.date, 7)
        AND url.mp_current_url LIKE CONCAT('%/o/', aa.org_id, '/fleet/workflows/incidents/', url.workflow_uuid, '/', aa.object_type_id, '/', aa.object_id, '/', aa.alert_occurred_at_ms)
        AND aa.alert_occurred_at_ms = url.alert_raised_ms
        AND REPLACE(url.workflow_uuid, '-', '') = LOWER(aa.alert_config_uuid)
    WHERE
        ac.date = '{END_DATEID}'
        AND ac.alert_targets_by_object_type IS NULL
),
alert_metrics_ca AS (
    SELECT
        /*+ RANGE_JOIN(url, 1) */
        aa.date,
        url.date AS actioned_date,
        aa.org_id,
        aa.alert_config_id,
        aa.alert_config_uuid,
        aa.trigger_type_ids,
        aa.trigger_types,
        ac.alert_action_types,
        ac.alert_recipient_types,
        ac.alert_notification_types,
        exploded_table.alert_targets_object_type,
        exploded_table.alert_targets_object_ids,
        aa.alert_status,
        aa.notification_sent,
        aa.object_type_id,
        aa.object_id,
        aa.alert_occurred_at_ms,
        aa.alert_resolved_at_ms,
        COALESCE(url.mp_processing_time_ms, -1) AS mp_processing_time_ms,
        COALESCE(url.mp_processing_time_ms - aa.alert_occurred_at_ms, -1) AS time_to_open_ms,
        ac.created_ts_utc AS alert_config_created_ts_utc,
        ac.deleted_ts_utc AS alert_config_deleted_ts_utc,
        url.mp_current_url,
        url.user_id,
        CASE WHEN url.mp_current_url IS NULL THEN false ELSE true END AS is_alert_viewed,
        'ca-central-1' AS region
    FROM all_alerts_ca aa
    JOIN data_tools_delta_share_ca.datamodel_platform.dim_alert_configs ac
        ON ac.org_id = aa.org_id
        AND ac.alert_config_uuid = aa.alert_config_uuid
    LEFT JOIN url_components url
        ON aa.org_id = url.org_id
        AND url.date BETWEEN aa.date AND DATE_ADD(aa.date, 7)
        AND url.mp_current_url LIKE CONCAT('%/o/', aa.org_id, '/fleet/workflows/incidents/', url.workflow_uuid, '/', aa.object_type_id, '/', aa.object_id, '/', aa.alert_occurred_at_ms)
        AND aa.alert_occurred_at_ms = url.alert_raised_ms
        AND REPLACE(url.workflow_uuid, '-', '') = LOWER(aa.alert_config_uuid)
    LATERAL VIEW EXPLODE(ac.alert_targets_by_object_type) exploded_table AS alert_targets_object_type, alert_targets_object_ids
    WHERE
        ac.date = '{END_DATEID}'
        AND ac.alert_targets_by_object_type IS NOT NULL

    UNION ALL

    SELECT
        /*+ RANGE_JOIN(url, 1) */
        aa.date,
        url.date AS actioned_date,
        aa.org_id,
        aa.alert_config_id,
        aa.alert_config_uuid,
        aa.trigger_type_ids,
        aa.trigger_types,
        ac.alert_action_types,
        ac.alert_recipient_types,
        ac.alert_notification_types,
        NULL AS alert_targets_object_type,
        NULL AS alert_targets_object_ids,
        aa.alert_status,
        aa.notification_sent,
        aa.object_type_id,
        aa.object_id,
        aa.alert_occurred_at_ms,
        aa.alert_resolved_at_ms,
        COALESCE(url.mp_processing_time_ms, -1) AS mp_processing_time_ms,
        COALESCE(url.mp_processing_time_ms - aa.alert_occurred_at_ms, -1) AS time_to_open_ms,
        ac.created_ts_utc AS alert_config_created_ts_utc,
        ac.deleted_ts_utc AS alert_config_deleted_ts_utc,
        url.mp_current_url,
        url.user_id,
        CASE WHEN url.mp_current_url IS NULL THEN false ELSE true END AS is_alert_viewed,
        'ca-central-1' AS region
    FROM all_alerts_ca aa
    JOIN data_tools_delta_share_ca.datamodel_platform.dim_alert_configs ac
        ON ac.org_id = aa.org_id
        AND ac.alert_config_uuid = aa.alert_config_uuid
    LEFT JOIN url_components url
        ON aa.org_id = url.org_id
        AND url.date BETWEEN aa.date AND DATE_ADD(aa.date, 7)
        AND url.mp_current_url LIKE CONCAT('%/o/', aa.org_id, '/fleet/workflows/incidents/', url.workflow_uuid, '/', aa.object_type_id, '/', aa.object_id, '/', aa.alert_occurred_at_ms)
        AND aa.alert_occurred_at_ms = url.alert_raised_ms
        AND REPLACE(url.workflow_uuid, '-', '') = LOWER(aa.alert_config_uuid)
    WHERE
        ac.date = '{END_DATEID}'
        AND ac.alert_targets_by_object_type IS NULL
)
SELECT *
FROM alerts_metrics_us

UNION

SELECT *
FROM alert_metrics_eu

UNION

SELECT *
FROM alert_metrics_ca;
"""


@table(
    database=Database.DATAMODEL_PLATFORM,
    description=build_table_description(
        table_desc="""This table provides a list of incidents triggered by alerts on a given day, along with visibility into when those incidents were seen in the Samsara dashboard.
            It includes details such as the times when the incidents occured, when they were resolved, and where those incidents are notifying end users.
    """,
        row_meaning="""Each row corresponds to a single instance of a user viewing an alert in the Samsara dashboard""",
        table_type=TableType.TRANSACTIONAL_FACT,
        freshness_slo_hours=28,
    ),
    regions=[AWSRegion.US_WEST_2],
    owners=[DATAENGINEERING],
    schema=fct_alert_details_global_schema,
    upstreams=[
        "us-west-2|eu-west-1|ca-central-1:datamodel_platform.dq_fct_alert_incidents",
        "us-west-2|eu-west-1|ca-central-1:datamodel_platform.dq_dim_alert_configs",
        "us-west-2:datamodel_platform_silver.dq_stg_cloud_routes"
    ],
    primary_keys=[
        "date",
        "org_id",
        "alert_config_id",
        "alert_occurred_at_ms",
        "alert_targets_object_type",
        "object_id",
        "mp_processing_time_ms",
        "region",
    ],
    partitioning=["date"],
    backfill_start_date="2023-01-01",
    backfill_batch_size=7,
    write_mode=WarehouseWriteMode.OVERWRITE,
    dq_checks=[
        NonEmptyDQCheck(name="dq_non_empty_fct_alert_details_global"),
        PrimaryKeyDQCheck(name="dq_pk_fct_alert_details_global", primary_keys=["date", "org_id", "alert_config_id", "alert_occurred_at_ms", "alert_targets_object_type", "object_id", "mp_processing_time_ms", "region"], block_before_write=True),
        NonNullDQCheck(name="dq_non_null_fct_alert_details_global", non_null_columns=["org_id", "alert_config_id", "alert_config_uuid", "alert_status", "object_type_id", "object_id", "alert_config_created_ts_utc", "region", "is_alert_viewed"], block_before_write=True)
    ],
)
def fct_alert_details_global(context: AssetExecutionContext) -> str:
    partition_keys = partition_key_ranges_from_context(context)[0]
    FIRST_PARTITION_START = partition_keys[0]
    FIRST_PARTITION_END = partition_keys[-1]
    query = fct_alert_details_global_query.format(
        START_DATEID=FIRST_PARTITION_START,
        END_DATEID=FIRST_PARTITION_END,
    )
    context.log.info(f"{query}")

    return query
