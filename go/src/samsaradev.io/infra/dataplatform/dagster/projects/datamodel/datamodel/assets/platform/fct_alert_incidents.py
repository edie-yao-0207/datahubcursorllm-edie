from dagster import AssetKey, BackfillPolicy, DailyPartitionsDefinition

from ...common.constants import (
    SLACK_ALERTS_CHANNEL_DATA_ENGINEERING,
    org_id_default_description,
)
from ...common.utils import (
    AWSRegions,
    Database,
    DQGroup,
    NonEmptyDQCheck,
    NonNullDQCheck,
    PrimaryKeyDQCheck,
    TableType,
    WarehouseWriteMode,
    apply_db_overrides,
    build_assets_from_sql,
    build_table_description,
    get_all_regions,
)

databases = {
    "database_bronze": Database.DATAMODEL_PLATFORM_BRONZE,
    "database_silver": Database.DATAMODEL_PLATFORM_SILVER,
    "database_gold": Database.DATAMODEL_PLATFORM,
}

database_dev_overrides = {
    "database_bronze_dev": Database.DATAMODEL_DEV,
    "database_silver_dev": Database.DATAMODEL_DEV,
    "database_gold_dev": Database.DATAMODEL_DEV,
}

databases = apply_db_overrides(databases, database_dev_overrides)

daily_partition_def = DailyPartitionsDefinition(start_date="2023-01-01")

pipeline_group_name = "fct_alert_incidents"

BACKFILL_POLICY = BackfillPolicy.multi_run(max_partitions_per_run=10)

dqs = DQGroup(
    group_name=pipeline_group_name,
    partition_def=daily_partition_def,
    slack_alerts_channel=SLACK_ALERTS_CHANNEL_DATA_ENGINEERING,
    regions=get_all_regions(),
)

stg_alert_incidents_schema = [
    {
        "name": "date",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "The UTC date corresponding to the timestamp when this alert incident was triggered"
        },
    },
    {
        "name": "org_id",
        "type": "long",
        "nullable": True,
        "metadata": {"comment": org_id_default_description},
    },
    {
        "name": "alert_config_uuid",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "The unique identifer of a configured alert"},
    },
    {
        "name": "alert_config_id",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "Hashed version of the alert_config_uuid to help with joining to the corresponding dim table"
        },
    },
    {
        "name": "alert_occurred_at_ms",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "Time in epoch milliseconds for when the alert incident was triggered"
        },
    },
    {
        "name": "alert_triggered_ts_utc",
        "type": "timestamp",
        "nullable": True,
        "metadata": {
            "comment": "Timestamp in UTC for when the alert incident was triggered"
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
        "name": "alert_resolved_ts_utc",
        "type": "timestamp",
        "nullable": True,
        "metadata": {
            "comment": "Timestamp in UTC for when the alert incident was resolved"
        },
    },
    {
        "name": "alert_updated_at_ms",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "Time in epoch milliseconds for when the alert incident was updated"
        },
    },
    {
        "name": "alert_updated_ts_utc",
        "type": "timestamp",
        "nullable": True,
        "metadata": {
            "comment": "Timestamp in UTC for when the alert incident was updated "
        },
    },
    {
        "name": "alert_status",
        "type": "string",
        "nullable": True,
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
        "name": "object_type_id",
        "type": "integer",
        "nullable": True,
        "metadata": {
            "comment": "The object type ID (for e.g., device, widget, driver etc.,) that triggered this alert incident"
        },
    },
    {
        "name": "object_type",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "List of trigger types that were triggered by the alert incident. Maps to definitions.alert_trigger_types"
        },
    },
    {
        "name": "object_id",
        "type": "long",
        "nullable": True,
        "metadata": {"comment": "The object ID that triggered this alert incident"},
    },
]


stg_alert_incidents_query = """
--sql

SELECT
  `date`,
  org_id,
  workflow_id AS alert_config_uuid,
  ABS(XXHASH64(workflow_id)) AS alert_config_id,
  occurred_at_ms AS alert_occurred_at_ms,
  TIMESTAMP(FROM_UNIXTIME(occurred_at_ms / 1000)) AS alert_triggered_ts_utc,
  resolved_at_ms AS alert_resolved_at_ms,
  TIMESTAMP(FROM_UNIXTIME(resolved_at_ms / 1000)) AS alert_resolved_ts_utc,
  updated_at_ms AS alert_updated_at_ms,
  TIMESTAMP(FROM_UNIXTIME(updated_at_ms / 1000)) AS alert_updated_ts_utc,
  CASE
    WHEN resolved_at_ms IS NULL THEN 'active'
    ELSE 'resolved'
  END AS alert_status,
  action_status IS NOT NULL AS notification_sent,
  ARRAY_SORT(
    ARRAY_DISTINCT(proto.trigger_matches.trigger_type)
  ) AS trigger_type_ids,
  ARRAY_SORT(ARRAY_DISTINCT(array_agg (tt.trigger_type))) AS trigger_types,

  -- Even if there are multiple trigger matches, all would contain the same device_id and object_type
  proto.trigger_matches[0].object_type AS object_type_id,
  SUBSTRING(object_ids, 0, INSTR(object_ids, '#') -1) AS object_type,
  proto.trigger_matches[0].object_id AS object_id
FROM
  workflowsdb_shards.workflow_incidents i
  LEFT JOIN definitions.alert_trigger_types tt ON ARRAY_CONTAINS(
    proto.trigger_matches.trigger_type,
    tt.trigger_type_id
  )
WHERE
  {PARTITION_FILTERS}
GROUP BY
  ALL
"""

assets = build_assets_from_sql(
    name="stg_alert_incidents",
    schema=stg_alert_incidents_schema,
    description="""A staging table that contains information on incidents triggered by alerts""",
    sql_query=stg_alert_incidents_query,
    primary_keys=["alert_config_id", "object_id", "alert_occurred_at_ms"],
    upstreams=[
        AssetKey(["workflowsdb_shards", "workflow_incidents"]),
    ],
    regions=get_all_regions(),
    group_name=pipeline_group_name,
    database=databases["database_silver_dev"],
    databases=databases,
    write_mode=WarehouseWriteMode.overwrite,
    partitions_def=daily_partition_def,
    backfill_policy=BACKFILL_POLICY,
)

stg_alert_incidents_1 = assets[AWSRegions.US_WEST_2.value]
stg_alert_incidents_2 = assets[AWSRegions.EU_WEST_1.value]
stg_alert_incidents_3 = assets[AWSRegions.CA_CENTRAL_1.value]

dqs["stg_alert_incidents"].append(
    PrimaryKeyDQCheck(
        name="dq_pk_stg_alert_incidents",
        table="stg_alert_incidents",
        primary_keys=["alert_config_id", "object_id", "alert_occurred_at_ms"],
        blocking=True,
        database=databases["database_silver_dev"],
    )
)

dqs["stg_alert_incidents"].append(
    NonEmptyDQCheck(
        name="dq_empty_stg_alert_incidents",
        table="stg_alert_incidents",
        blocking=True,
        database=databases["database_silver_dev"],
    )
)

fct_alert_incidents_schema = [
    {
        "name": "date",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "The UTC date corresponding to the timestamp when this alert incident was triggered"
        },
    },
    {
        "name": "org_id",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "Unique identifier for organizations. An account has multiple organizations under it. Joins to datamodel_core.dim_organizations"
        },
    },
    {
        "name": "alert_config_uuid",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "The unique identifer of a configured alert"},
    },
    {
        "name": "alert_config_id",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "Hashed version of the alert_config_uuid to help with joining to the corresponding dim table"
        },
    },
    {
        "name": "alert_occurred_at_ms",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "Time in epoch milliseconds for when the alert incident was triggered"
        },
    },
    {
        "name": "alert_triggered_ts_utc",
        "type": "timestamp",
        "nullable": True,
        "metadata": {
            "comment": "Timestamp in UTC for when the alert incident was triggered"
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
        "name": "alert_resolved_ts_utc",
        "type": "timestamp",
        "nullable": True,
        "metadata": {
            "comment": "Timestamp in UTC for when the alert incident was resolved"
        },
    },
    {
        "name": "alert_updated_at_ms",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "Time in epoch milliseconds for when the alert incident was updated"
        },
    },
    {
        "name": "alert_updated_ts_utc",
        "type": "timestamp",
        "nullable": True,
        "metadata": {
            "comment": "Timestamp in UTC for when the alert incident was updated "
        },
    },
    {
        "name": "alert_status",
        "type": "string",
        "nullable": True,
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
        "name": "object_type_id",
        "type": "integer",
        "nullable": True,
        "metadata": {
            "comment": "The object type ID (for e.g., device, widget, driver etc.,) that triggered this alert incident"
        },
    },
    {
        "name": "object_type",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "The object type (for e.g., device, widget, driver etc.,) that triggered this alert incident"
        },
    },
    {
        "name": "object_id",
        "type": "long",
        "nullable": True,
        "metadata": {"comment": "The object ID that triggered this alert incident"},
    },
]


fct_alert_incidents_query = """
--sql

SELECT
  `date`
  ,org_id
  ,alert_config_uuid
  ,alert_config_id
  ,alert_occurred_at_ms
  ,alert_triggered_ts_utc
  ,alert_resolved_at_ms
  ,alert_resolved_ts_utc
  ,alert_updated_at_ms
  ,alert_updated_ts_utc
  ,alert_status
  ,notification_sent
  ,trigger_type_ids
  ,trigger_types

  ,object_type_id
  ,object_type
  ,object_id
FROM
    `{database_silver_dev}`.stg_alert_incidents
WHERE
    {PARTITION_FILTERS}
"""

assets = build_assets_from_sql(
    name="fct_alert_incidents",
    schema=fct_alert_incidents_schema,
    description=build_table_description(
        table_desc="""This table provides a list of incidents triggered by alerts on a given day.
        It includes details such as the times when the incidents occured, resolved, as well as information on the triggers and objects that triggered the incident
""",
        row_meaning="""Each row corresponds to a single incident triggered by an alert""",
        related_table_info={
            "datamodel_platform.dim_alert_configs": """This table provides a daily snapshot of alert configurations. Can be joined using the alert_config_uuid/alert_config_id"""
        },
        table_type=TableType.TRANSACTIONAL_FACT,
        freshness_slo_updated_by="9am PST",
    ),
    sql_query=fct_alert_incidents_query,
    primary_keys=["alert_config_id", "object_id", "alert_occurred_at_ms"],
    upstreams=[AssetKey([databases["database_silver_dev"], "dq_stg_alert_incidents"])],
    regions=get_all_regions(),
    group_name=pipeline_group_name,
    database=databases["database_gold_dev"],
    databases=databases,
    write_mode=WarehouseWriteMode.overwrite,
    partitions_def=daily_partition_def,
    backfill_policy=BACKFILL_POLICY,
)

fct_alert_incidents_1 = assets[AWSRegions.US_WEST_2.value]
fct_alert_incidents_2 = assets[AWSRegions.EU_WEST_1.value]
fct_alert_incidents_3 = assets[AWSRegions.CA_CENTRAL_1.value]

dqs["fct_alert_incidents"].append(
    PrimaryKeyDQCheck(
        name="dq_pk_fct_alert_incidents",
        table="fct_alert_incidents",
        primary_keys=["alert_config_id", "object_id", "alert_occurred_at_ms"],
        blocking=True,
        database=databases["database_gold_dev"],
    )
)

dqs["fct_alert_incidents"].append(
    NonEmptyDQCheck(
        name="dq_empty_fct_alert_incidents",
        table="fct_alert_incidents",
        blocking=True,
        database=databases["database_gold_dev"],
    )
)

dqs["fct_alert_incidents"].append(
    NonNullDQCheck(
        name="dq_non_null_fct_alert_incidents",
        database=databases["database_gold_dev"],
        table="fct_alert_incidents",
        non_null_columns=[
            "org_id",
            "alert_config_uuid",
            "object_type",
            "object_id",
            "alert_status",
        ],
        blocking=True,
    )
)


dq_assets = dqs.generate()
