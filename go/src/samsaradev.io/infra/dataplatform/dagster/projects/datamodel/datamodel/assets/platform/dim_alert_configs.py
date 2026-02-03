from dagster import AssetKey, DailyPartitionsDefinition

from ...common.constants import (
    SLACK_ALERTS_CHANNEL_DATA_ENGINEERING,
    org_id_default_description,
)
from ...common.utils import (
    AWSRegions,
    Database,
    DQGroup,
    NonEmptyDQCheck,
    PrimaryKeyDQCheck,
    SQLDQCheck,
    TableType,
    TrendDQCheck,
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

daily_partition_def = DailyPartitionsDefinition(start_date="2024-06-09")
pipeline_group_name = "dim_alert_configs"

dqs = DQGroup(
    group_name=pipeline_group_name,
    partition_def=daily_partition_def,
    slack_alerts_channel=SLACK_ALERTS_CHANNEL_DATA_ENGINEERING,
    regions=get_all_regions(),
)


def get_sharded_timetravel_query(sharded_timetravel_query_template, region):
    # Shard configuration per region (based on workflowsdb.go):
    #   US: NumShards=6 -> shards 0-5
    #   EU: NumShards=2 -> shards 0-1
    #   CA: NumShards=1 -> shard 0 only
    if region == AWSRegions.US_WEST_2.value:
        shard_ids = list(range(6))  # [0, 1, 2, 3, 4, 5]
    elif region == AWSRegions.CA_CENTRAL_1.value:
        shard_ids = [0]  # Only shard 0 exists in CA
    else:
        shard_ids = list(range(2))  # [0, 1] for EU

    raw_workflowsdb_shards_query_template = """
    SELECT
        *,
        '{{DATEID}}' AS date
    FROM (
        SELECT /*+ BROADCAST(os) */ shards.*
            FROM (
                {sharded_timetravel_query}
            ) shards
                LEFT OUTER JOIN appconfigs.org_shards os
                    ON shards.org_id = os.org_id
                WHERE os.shard_type = 1 AND os.data_type = "workflows"
            )
    """

    sharded_timetravel_query = "            UNION ALL".join(
        [
            sharded_timetravel_query_template.format(shard_id=shard_id)
            for shard_id in shard_ids
        ]
    )

    return raw_workflowsdb_shards_query_template.format(
        sharded_timetravel_query=sharded_timetravel_query
    )


raw_workflowsdb_shards_workflow_configs_query_template = """
            SELECT
                "workflows_shard_{shard_id}" AS shard_name,
                _filename, _op, _raw_proto_data, _rowid, _timestamp, created_at, deleted_at, deleted_by_user_id,
                description, disabled, is_admin, name, org_id, partition, proto_data, severity, source_alert_id, updated_at, uuid
            FROM workflows_shard_{shard_id}db.workflow_configs{{TIMETRAVEL_DATE}}
"""

raw_workflowsdb_shards_actions_query_template = """
            SELECT
                "workflows_shard_{shard_id}" AS shard_name,
                 _filename, _op, _raw_proto_data, _rowid, _timestamp, created_at, deleted_at, deleted_by_user_id,
                org_id, partition, proto_data, repeat_ms, type, updated_at, uuid, workflow_uuid
            FROM workflows_shard_{shard_id}db.actions{{TIMETRAVEL_DATE}}
"""

raw_workflowsdb_shards_triggers_query_template = """
            SELECT
                "workflows_shard_{shard_id}" AS shard_name,
                 _filename, _op, _raw_proto_data, _rowid, _timestamp, created_at, deleted_at, deleted_by_user_id,
                 is_secondary, org_id, partition, proto_data, type, updated_at, uuid, workflow_uuid
            FROM workflows_shard_{shard_id}db.triggers{{TIMETRAVEL_DATE}}
"""

raw_workflowsdb_shards_trigger_targets_query_template = """
            SELECT
                "workflows_shard_{shard_id}" AS shard_name,
                 _filename, _op, _rowid, _timestamp, created_at, deleted_at, deleted_by_user_id, object_id, object_type, org_id, partition, trigger_uuid
            FROM workflows_shard_{shard_id}db.trigger_targets{{TIMETRAVEL_DATE}}
"""

stg_alert_configs_schema = [
    {
        "name": "date",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "The field which partitions the table, in 'YYYY-mm-dd' format."
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
            "comment": "Hashed version of the alert_config_uuid to help with joining to the corresponding fact table"
        },
    },
    {
        "name": "org_id",
        "type": "long",
        "nullable": True,
        "metadata": {"comment": org_id_default_description},
    },
    {
        "name": "alert_config_name",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "The name given to the configured alert"},
    },
    {
        "name": "alert_config_description",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "Description of the configured alert"},
    },
    {
        "name": "is_disabled",
        "type": "boolean",
        "nullable": True,
        "metadata": {
            "comment": "A True/False flag representing whether the alert has been disabled"
        },
    },
    {
        "name": "is_admin",
        "type": "boolean",
        "nullable": True,
        "metadata": {
            "comment": "A True/False indicating whether the config is a super user config. Alerts that are marked True are not visible to customers "
        },
    },
    {
        "name": "created_ts_utc",
        "type": "timestamp",
        "nullable": True,
        "metadata": {"comment": "Time at which the alert config was created"},
    },
    {
        "name": "updated_ts_utc",
        "type": "timestamp",
        "nullable": True,
        "metadata": {"comment": "Time at which the alert config was updated"},
    },
    {
        "name": "deleted_ts_utc",
        "type": "timestamp",
        "nullable": True,
        "metadata": {"comment": "Time at which the alert config was deleted"},
    },
    {
        "name": "has_time_range_settings",
        "type": "boolean",
        "nullable": True,
        "metadata": {"comment": "Whether the alert config has a time range or not"},
    },
    {
        "name": "primary_trigger_type_id",
        "type": "integer",
        "nullable": True,
        "metadata": {
            "comment": "The internal ID of the primary trigger type configured on the alert"
        },
    },
    {
        "name": "primary_trigger_type",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "The internal name of the primary trigger type configured on the alert"
        },
    },
    {
        "name": "secondary_trigger_type_id",
        "type": "integer",
        "nullable": True,
        "metadata": {
            "comment": "The internal ID of the secondary trigger type configured on the alert"
        },
    },
    {
        "name": "secondary_trigger_type",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "The internal name of the secondary trigger type configured on the alert"
        },
    },
    {
        "name": "alert_action_type_ids",
        "type": {"type": "array", "elementType": "integer", "containsNull": True},
        "nullable": True,
        "metadata": {
            "comment": "List of internal IDs of the action types that are configured to be taken when the alert is triggered"
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
        "name": "alert_recipient_user_ids",
        "type": {"type": "array", "elementType": "long", "containsNull": True},
        "nullable": True,
        "metadata": {
            "comment": "List of user IDs that will be notified when the alert is triggered"
        },
    },
    {
        "name": "alert_recipient_contact_ids",
        "type": {"type": "array", "elementType": "long", "containsNull": True},
        "nullable": True,
        "metadata": {
            "comment": "List of contact IDs that will be notified when the alert is triggered"
        },
    },
    {
        "name": "alert_targets_by_object_type_id",
        "type": {
            "type": "map",
            "keyType": "short",
            "valueType": {"type": "array", "elementType": "long", "containsNull": True},
            "valueContainsNull": True,
        },
        "nullable": True,
        "metadata": {
            "comment": "List of object IDs that the alert triggers are configured on, grouped by the object type ID (for e.g., device, widget, driver etc.,)"
        },
    },
    {
        "name": "alert_targets_by_object_type",
        "type": {
            "type": "map",
            "keyType": "string",
            "valueType": {"type": "array", "elementType": "long", "containsNull": True},
            "valueContainsNull": True,
        },
        "nullable": True,
        "metadata": {
            "comment": "List of object IDs that the alert triggers are configured on, grouped by the object type (for e.g., device, widget, driver etc.,)"
        },
    },
]


stg_alert_configs_query = """
--sql

WITH
  actions AS (
    SELECT
      workflow_uuid AS alert_config_uuid,
      TYPE AS alert_action_type_id,
      -- platform/workflows/actiontypes/action_types.go
      at.action_type AS alert_action_type,
      proto_data.notification.recipients.recipient_type AS alert_recipient_types,
      FLATTEN(
        proto_data.notification.recipients.enabled_notification_types
      ) AS alert_notification_types,
      -- RecipientType in platform/workflows/workflowsproto/action.proto
      FILTER (
        proto_data.notification.recipients,
        x -> x.recipient_type = 1
      ).recipient_id AS alert_recipient_user_ids,
      FILTER (
        proto_data.notification.recipients,
        x -> x.recipient_type = 0
      ).recipient_id AS alert_recipient_contact_ids
    FROM
      datamodel_platform_bronze.raw_workflowsdb_shards_actions sa
    JOIN definitions.alert_action_types at
        ON sa.type = at.action_type_id
    WHERE
      `date` = '{DATEID}'
  ),
  actions_agg AS (
    SELECT
      alert_config_uuid,
      ARRAY_DISTINCT(array_agg (alert_action_type_id)) AS alert_action_type_ids,
      ARRAY_DISTINCT(array_agg (alert_action_type)) AS alert_action_types,
      -- RecipientType, NotificationType in platform/workflows/workflowsproto/action.proto
      ARRAY_SORT(
        ARRAY_DISTINCT(
          FLATTEN(
            array_agg (
              TRANSFORM (
                alert_recipient_types,
                x -> CASE x
                  WHEN 0 THEN 'contact'
                  WHEN 1 THEN 'user'
                  ELSE NULL
                END
              )
            )
          )
        )
      ) AS alert_recipient_types,
      ARRAY_SORT(
        ARRAY_DISTINCT(
          FLATTEN(
            array_agg (
              TRANSFORM (
                alert_notification_types,
                x -> CASE x
                  WHEN 0 THEN 'sms'
                  WHEN 1 THEN 'email'
                  WHEN 2 THEN 'push'
                  ELSE NULL
                END
              )
            )
          )
        )
      ) AS alert_notification_types,
      ARRAY_SORT(
        ARRAY_DISTINCT(FLATTEN(array_agg (alert_recipient_user_ids)))
      ) AS alert_recipient_user_ids,
      ARRAY_SORT(
        ARRAY_DISTINCT(FLATTEN(array_agg (alert_recipient_contact_ids)))
      ) AS alert_recipient_contact_ids
    FROM
      actions
    GROUP BY
      alert_config_uuid
  ),
  triggers AS (
    SELECT
      t.workflow_uuid AS alert_config_uuid,
      t.uuid AS trigger_uuid,
      IF (COALESCE(t.is_secondary, 0) = 0, t.type, NULL) AS primary_trigger_type_id,
      IF (
        COALESCE(t.is_secondary, 0) = 0,
        tt.trigger_type,
        NULL
      ) AS primary_trigger_type,
      IF (COALESCE(t.is_secondary, 0) = 1, t.type, NULL) AS secondary_trigger_type_id,
      IF (
        COALESCE(t.is_secondary, 0) = 1,
        tt.trigger_type,
        NULL
      ) AS secondary_trigger_type
    FROM
      datamodel_platform_bronze.raw_workflowsdb_shards_triggers t
      LEFT JOIN definitions.alert_trigger_types tt ON t.type = tt.trigger_type_id
    WHERE
      t.`date` = '{DATEID}'
  ),
  triggers_agg AS (
    SELECT
      alert_config_uuid,
      MAX(primary_trigger_type_id) AS primary_trigger_type_id,
      MAX(primary_trigger_type) AS primary_trigger_type,
      MAX(secondary_trigger_type_id) AS secondary_trigger_type_id,
      MAX(secondary_trigger_type) AS secondary_trigger_type
    FROM
      triggers
    GROUP BY
      1
  ),
  trigger_targets AS (
    SELECT
      trigger_uuid,
      object_type,
      object_id
    FROM
      datamodel_platform_bronze.raw_workflowsdb_shards_trigger_targets
    WHERE
      `date` = '{DATEID}'
  ),
  targets AS (
    SELECT
      t.alert_config_uuid,
      tt.object_type AS object_type_id,
      -- alerts only support these object types as triggers
      -- hubproto/objectstatproto/object_stat_id.proto
      CASE tt.object_type
        WHEN 1 THEN 'device'
        WHEN 2 THEN 'widget'
        WHEN 5 THEN 'driver'
        WHEN 10 THEN 'machineinput'
        WHEN 23 THEN 'onvifcamerastream'
        WHEN 30 THEN 'tag'
        WHEN 35 THEN 'workforcecameradevice'
        WHEN 40 THEN 'organization'
        ELSE NULL
      END AS object_type,
      ARRAY_SORT(ARRAY_DISTINCT(array_agg (tt.object_id))) AS object_ids
    FROM
      triggers t
      LEFT JOIN datamodel_platform_bronze.raw_workflowsdb_shards_trigger_targets tt ON t.trigger_uuid = tt.trigger_uuid
    WHERE
      tt.object_id > 0
    GROUP BY
      1,
      2,
      3
  ),
  targets_agg AS (
    SELECT
      alert_config_uuid,
      MAP_FROM_ENTRIES(COLLECT_LIST(STRUCT (object_type_id, object_ids))) AS alert_targets_by_object_type_id,
      MAP_FROM_ENTRIES(COLLECT_LIST(STRUCT (object_type, object_ids))) AS alert_targets_by_object_type
    FROM
      targets
    GROUP BY
      alert_config_uuid
  )
SELECT
  '{DATEID}' AS `date`,
  UUID AS alert_config_uuid,
  ABS(XXHASH64(UUID)) AS alert_config_id,
  org_id,
  name AS alert_config_name,
  description AS alert_config_description,
  CAST(disabled AS BOOLEAN) AS is_disabled,
  CAST(is_admin AS BOOLEAN) AS is_admin,
  TIMESTAMP(created_at) AS created_ts_utc,
  TIMESTAMP(updated_at) AS updated_ts_utc,
  TIMESTAMP(deleted_at) AS deleted_ts_utc,
  proto_data.time_range_settings.time_ranges IS NOT NULL AS has_time_range_settings,
  -- trigger details
  primary_trigger_type_id,
  primary_trigger_type,
  secondary_trigger_type_id,
  secondary_trigger_type,
  -- actions
  alert_action_type_ids,
  alert_action_types,
  alert_recipient_types,
  alert_notification_types,
  alert_recipient_user_ids,
  alert_recipient_contact_ids,
  -- trigger targets
  alert_targets_by_object_type_id,
  alert_targets_by_object_type
FROM
  datamodel_platform_bronze.raw_workflowsdb_shards_workflow_configs cfg
  LEFT JOIN triggers_agg t ON t.alert_config_uuid = cfg.uuid
  LEFT JOIN actions_agg a ON a.alert_config_uuid = cfg.uuid
  LEFT JOIN targets_agg ta ON ta.alert_config_uuid = cfg.uuid
WHERE
  cfg.`date` = '{DATEID}'

--endsql
"""

dim_alert_configs_schema = [
    {
        "name": "date",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "The field which partitions the table, in 'YYYY-mm-dd' format."
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
            "comment": "Hashed version of the alert_config_uuid to help with joining to the corresponding fact table"
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
        "name": "alert_config_name",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "The name given to the configured alert"},
    },
    {
        "name": "alert_config_description",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "Description of the configured alert"},
    },
    {
        "name": "is_disabled",
        "type": "boolean",
        "nullable": True,
        "metadata": {
            "comment": "A True/False flag representing whether the alert has been disabled"
        },
    },
    {
        "name": "is_admin",
        "type": "boolean",
        "nullable": True,
        "metadata": {
            "comment": "A True/False flag indicating whether the config is a super user config. Alerts that are marked True are not visible to customers"
        },
    },
    {
        "name": "created_ts_utc",
        "type": "timestamp",
        "nullable": True,
        "metadata": {"comment": "The creation timestamp of the alert config in UTC"},
    },
    {
        "name": "updated_ts_utc",
        "type": "timestamp",
        "nullable": True,
        "metadata": {
            "comment": "The timestamp in UTC when the alert config was updated"
        },
    },
    {
        "name": "deleted_ts_utc",
        "type": "timestamp",
        "nullable": True,
        "metadata": {
            "comment": "The timestamp in UTC when the alert config was deleted"
        },
    },
    {
        "name": "has_time_range_settings",
        "type": "boolean",
        "nullable": True,
        "metadata": {
            "comment": "Indicates whether incidents are logged only during the time range specified on the alert config"
        },
    },
    {
        "name": "primary_trigger_type_id",
        "type": "integer",
        "nullable": True,
        "metadata": {
            "comment": "The internal ID of the primary trigger type configured on the alert"
        },
    },
    {
        "name": "primary_trigger_type",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "The internal name of the primary trigger type configured on the alert"
        },
    },
    {
        "name": "secondary_trigger_type_id",
        "type": "integer",
        "nullable": True,
        "metadata": {
            "comment": "The internal ID of the secondary trigger type configured on the alert"
        },
    },
    {
        "name": "secondary_trigger_type",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "The internal name of the secondary trigger type configured on the alert"
        },
    },
    {
        "name": "alert_action_type_ids",
        "type": {"type": "array", "elementType": "integer", "containsNull": True},
        "nullable": True,
        "metadata": {
            "comment": "List of internal IDs of the action types that are configured to be taken when the alert is triggered"
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
        "name": "alert_recipient_user_ids",
        "type": {"type": "array", "elementType": "long", "containsNull": True},
        "nullable": True,
        "metadata": {
            "comment": "List of user IDs that will be notified when the alert is triggered"
        },
    },
    {
        "name": "alert_recipient_contact_ids",
        "type": {"type": "array", "elementType": "long", "containsNull": True},
        "nullable": True,
        "metadata": {
            "comment": "List of contact IDs that will be notified when the alert is triggered"
        },
    },
    {
        "name": "alert_targets_by_object_type_id",
        "type": {
            "type": "map",
            "keyType": "short",
            "valueType": {"type": "array", "elementType": "long", "containsNull": True},
            "valueContainsNull": True,
        },
        "nullable": True,
        "metadata": {
            "comment": "List of object IDs that the alert triggers are configured on, grouped by the object type ID (for e.g., device, widget, driver etc.,)"
        },
    },
    {
        "name": "alert_targets_by_object_type",
        "type": {
            "type": "map",
            "keyType": "string",
            "valueType": {"type": "array", "elementType": "long", "containsNull": True},
            "valueContainsNull": True,
        },
        "nullable": True,
        "metadata": {
            "comment": "List of object IDs that the alert triggers are configured on, grouped by the object type (for e.g., device, widget, driver etc.,)"
        },
    },
]

dim_alert_configs_query = """
--sql

SELECT
  `date`,
  alert_config_uuid,
  alert_config_id,
  org_id,
  alert_config_name,
  alert_config_description,
  is_disabled,
  is_admin,
  created_ts_utc,
  updated_ts_utc,
  deleted_ts_utc,
  has_time_range_settings,
  -- trigger details
  primary_trigger_type_id,
  primary_trigger_type,
  secondary_trigger_type_id,
  secondary_trigger_type,
  -- actions
  alert_action_type_ids,
  alert_action_types,
  alert_recipient_types,
  alert_notification_types,
  alert_recipient_user_ids,
  alert_recipient_contact_ids,
  -- trigger targets
  alert_targets_by_object_type_id,
  alert_targets_by_object_type
FROM
  {database_silver_dev}.stg_alert_configs
WHERE
  `date` = '{DATEID}'

--endsql
"""


assets = build_assets_from_sql(
    name="raw_workflowsdb_shards_workflow_configs",
    schema=None,
    sql_query=get_sharded_timetravel_query(
        raw_workflowsdb_shards_workflow_configs_query_template,
        AWSRegions.US_WEST_2.value,
    ),
    description="""Grabs the latest workflow configs data""",
    primary_keys=[],
    upstreams=[],
    regions=[AWSRegions.US_WEST_2.value],
    group_name=pipeline_group_name,
    database=databases["database_bronze_dev"],
    databases=databases,
    write_mode=WarehouseWriteMode.overwrite,
    partitions_def=daily_partition_def,
)
raw_workflowsdb_shards_workflow_configs_1 = assets[AWSRegions.US_WEST_2.value]

assets = build_assets_from_sql(
    name="raw_workflowsdb_shards_workflow_configs",
    schema=None,
    sql_query=get_sharded_timetravel_query(
        raw_workflowsdb_shards_workflow_configs_query_template,
        AWSRegions.EU_WEST_1.value,
    ),
    description="""Grabs the latest workflow configs data""",
    primary_keys=[],
    upstreams=[],
    regions=[AWSRegions.EU_WEST_1.value],
    group_name=pipeline_group_name,
    database=databases["database_bronze_dev"],
    databases=databases,
    write_mode=WarehouseWriteMode.overwrite,
    partitions_def=daily_partition_def,
)
raw_workflowsdb_shards_workflow_configs_2 = assets[AWSRegions.EU_WEST_1.value]

assets = build_assets_from_sql(
    name="raw_workflowsdb_shards_workflow_configs",
    schema=None,
    sql_query=get_sharded_timetravel_query(
        raw_workflowsdb_shards_workflow_configs_query_template,
        AWSRegions.CA_CENTRAL_1.value,
    ),
    description="""Grabs the latest workflow configs data""",
    primary_keys=[],
    upstreams=[],
    regions=[AWSRegions.CA_CENTRAL_1.value],
    group_name=pipeline_group_name,
    database=databases["database_bronze_dev"],
    databases=databases,
    write_mode=WarehouseWriteMode.overwrite,
    partitions_def=daily_partition_def,
)
raw_workflowsdb_shards_workflow_configs_3 = assets[AWSRegions.CA_CENTRAL_1.value]


assets = build_assets_from_sql(
    name="raw_workflowsdb_shards_actions",
    schema=None,
    sql_query=get_sharded_timetravel_query(
        raw_workflowsdb_shards_actions_query_template, AWSRegions.US_WEST_2.value
    ),
    description="""Grabs the latest workflow actions data""",
    primary_keys=[],
    upstreams=[],
    regions=[AWSRegions.US_WEST_2.value],
    group_name=pipeline_group_name,
    database=databases["database_bronze_dev"],
    databases=databases,
    write_mode=WarehouseWriteMode.overwrite,
    partitions_def=daily_partition_def,
)
raw_workflowsdb_shards_actions_1 = assets[AWSRegions.US_WEST_2.value]

assets = build_assets_from_sql(
    name="raw_workflowsdb_shards_actions",
    schema=None,
    sql_query=get_sharded_timetravel_query(
        raw_workflowsdb_shards_actions_query_template, AWSRegions.EU_WEST_1.value
    ),
    description="""Grabs the latest workflow actions data""",
    primary_keys=[],
    upstreams=[],
    regions=[AWSRegions.EU_WEST_1.value],
    group_name=pipeline_group_name,
    database=databases["database_bronze_dev"],
    databases=databases,
    write_mode=WarehouseWriteMode.overwrite,
    partitions_def=daily_partition_def,
)
raw_workflowsdb_shards_actions_2 = assets[AWSRegions.EU_WEST_1.value]

assets = build_assets_from_sql(
    name="raw_workflowsdb_shards_actions",
    schema=None,
    sql_query=get_sharded_timetravel_query(
        raw_workflowsdb_shards_actions_query_template, AWSRegions.CA_CENTRAL_1.value
    ),
    description="""Grabs the latest workflow actions data""",
    primary_keys=[],
    upstreams=[],
    regions=[AWSRegions.CA_CENTRAL_1.value],
    group_name=pipeline_group_name,
    database=databases["database_bronze_dev"],
    databases=databases,
    write_mode=WarehouseWriteMode.overwrite,
    partitions_def=daily_partition_def,
)
raw_workflowsdb_shards_actions_3 = assets[AWSRegions.CA_CENTRAL_1.value]


assets = build_assets_from_sql(
    name="raw_workflowsdb_shards_triggers",
    schema=None,
    sql_query=get_sharded_timetravel_query(
        raw_workflowsdb_shards_triggers_query_template, AWSRegions.US_WEST_2.value
    ),
    description="""Grabs the latest workflow triggers data""",
    primary_keys=[],
    upstreams=[],
    regions=[AWSRegions.US_WEST_2.value],
    group_name=pipeline_group_name,
    database=databases["database_bronze_dev"],
    databases=databases,
    write_mode=WarehouseWriteMode.overwrite,
    partitions_def=daily_partition_def,
)
raw_workflowsdb_shards_triggers_1 = assets[AWSRegions.US_WEST_2.value]

assets = build_assets_from_sql(
    name="raw_workflowsdb_shards_triggers",
    schema=None,
    sql_query=get_sharded_timetravel_query(
        raw_workflowsdb_shards_triggers_query_template, AWSRegions.EU_WEST_1.value
    ),
    description="""Grabs the latest workflow triggers data""",
    primary_keys=[],
    upstreams=[],
    regions=[AWSRegions.EU_WEST_1.value],
    group_name=pipeline_group_name,
    database=databases["database_bronze_dev"],
    databases=databases,
    write_mode=WarehouseWriteMode.overwrite,
    partitions_def=daily_partition_def,
)
raw_workflowsdb_shards_triggers_2 = assets[AWSRegions.EU_WEST_1.value]

assets = build_assets_from_sql(
    name="raw_workflowsdb_shards_triggers",
    schema=None,
    sql_query=get_sharded_timetravel_query(
        raw_workflowsdb_shards_triggers_query_template, AWSRegions.CA_CENTRAL_1.value
    ),
    description="""Grabs the latest workflow triggers data""",
    primary_keys=[],
    upstreams=[],
    regions=[AWSRegions.CA_CENTRAL_1.value],
    group_name=pipeline_group_name,
    database=databases["database_bronze_dev"],
    databases=databases,
    write_mode=WarehouseWriteMode.overwrite,
    partitions_def=daily_partition_def,
)
raw_workflowsdb_shards_triggers_3 = assets[AWSRegions.CA_CENTRAL_1.value]

assets = build_assets_from_sql(
    name="raw_workflowsdb_shards_trigger_targets",
    schema=None,
    sql_query=get_sharded_timetravel_query(
        raw_workflowsdb_shards_trigger_targets_query_template,
        AWSRegions.US_WEST_2.value,
    ),
    description="""Grabs the latest workflow trigger targets data""",
    primary_keys=[],
    upstreams=[],
    regions=[AWSRegions.US_WEST_2.value],
    group_name=pipeline_group_name,
    database=databases["database_bronze_dev"],
    databases=databases,
    write_mode=WarehouseWriteMode.overwrite,
    partitions_def=daily_partition_def,
)
raw_workflowsdb_shards_trigger_targets_1 = assets[AWSRegions.US_WEST_2.value]

assets = build_assets_from_sql(
    name="raw_workflowsdb_shards_trigger_targets",
    schema=None,
    sql_query=get_sharded_timetravel_query(
        raw_workflowsdb_shards_trigger_targets_query_template,
        AWSRegions.EU_WEST_1.value,
    ),
    description="""Grabs the latest workflow trigger targets data""",
    primary_keys=[],
    upstreams=[],
    regions=[AWSRegions.EU_WEST_1.value],
    group_name=pipeline_group_name,
    database=databases["database_bronze_dev"],
    databases=databases,
    write_mode=WarehouseWriteMode.overwrite,
    partitions_def=daily_partition_def,
)
raw_workflowsdb_shards_trigger_targets_2 = assets[AWSRegions.EU_WEST_1.value]

assets = build_assets_from_sql(
    name="raw_workflowsdb_shards_trigger_targets",
    schema=None,
    sql_query=get_sharded_timetravel_query(
        raw_workflowsdb_shards_trigger_targets_query_template,
        AWSRegions.CA_CENTRAL_1.value,
    ),
    description="""Grabs the latest workflow trigger targets data""",
    primary_keys=[],
    upstreams=[],
    regions=[AWSRegions.CA_CENTRAL_1.value],
    group_name=pipeline_group_name,
    database=databases["database_bronze_dev"],
    databases=databases,
    write_mode=WarehouseWriteMode.overwrite,
    partitions_def=daily_partition_def,
)
raw_workflowsdb_shards_trigger_targets_3 = assets[AWSRegions.CA_CENTRAL_1.value]

assets = build_assets_from_sql(
    name="stg_alert_configs",
    schema=stg_alert_configs_schema,
    sql_query=stg_alert_configs_query,
    description=build_table_description(
        table_desc="""This table provides a daily snapshot of alert configurations.
        It includes details such as the name and description of each alert, as well as information on the triggers that activate these alerts
        and the actions that are taken once an alert is triggered.
        """,
        row_meaning="""Configuration details of an alert as recorded on a specific date.
This allows you to track and analyze organizations over time.
If you only care about a single date,
filter for the alert uuid on that date (or the latest date for the most up to date information)""",
        table_type=TableType.DAILY_DIMENSION,
        freshness_slo_updated_by="9am PST",
    ),
    primary_keys=["date", "alert_config_id"],
    upstreams=[
        AssetKey(
            [
                databases["database_bronze_dev"],
                "raw_workflowsdb_shards_workflow_configs",
            ]
        ),
        AssetKey([databases["database_bronze_dev"], "raw_workflowsdb_shards_actions"]),
        AssetKey([databases["database_bronze_dev"], "raw_workflowsdb_shards_triggers"]),
        AssetKey(
            [databases["database_bronze_dev"], "raw_workflowsdb_shards_trigger_targets"]
        ),
    ],
    regions=get_all_regions(),
    group_name=pipeline_group_name,
    database=databases["database_silver_dev"],
    databases=databases,
    write_mode=WarehouseWriteMode.overwrite,
    partitions_def=daily_partition_def,
)
stg_alert_configs_1 = assets[AWSRegions.US_WEST_2.value]
stg_alert_configs_2 = assets[AWSRegions.EU_WEST_1.value]
stg_alert_configs_3 = assets[AWSRegions.CA_CENTRAL_1.value]

dqs["stg_alert_configs"].append(
    PrimaryKeyDQCheck(
        name="dq_pk_stg_alert_configs",
        table="stg_alert_configs",
        primary_keys=["date", "alert_config_id"],
        blocking=True,
        database=databases["database_silver_dev"],
    )
)

dqs["stg_alert_configs"].append(
    NonEmptyDQCheck(
        name="dq_empty_stg_alert_configs",
        table="stg_alert_configs",
        blocking=True,
        database=databases["database_silver_dev"],
    )
)

assets = build_assets_from_sql(
    name="dim_alert_configs",
    description=build_table_description(
        table_desc="""This table provides a daily snapshot of alert configurations.
        It includes details such as the name and description of each alert, as well as information on the triggers that activate these alerts
        and the actions that are taken once an alert is triggered.
        """,
        row_meaning="""Configuration details of an alert as recorded on a specific date.
This allows you to track and analyze organizations over time.
If you only care about a single date,
filter for the alert uuid on that date (or the latest date for the most up to date information)""",
        table_type=TableType.DAILY_DIMENSION,
        freshness_slo_updated_by="9am PST",
    ),
    schema=dim_alert_configs_schema,
    sql_query=dim_alert_configs_query,
    primary_keys=["date", "alert_config_id"],
    upstreams=[
        AssetKey([databases["database_silver_dev"], "dq_stg_alert_configs"]),
    ],
    regions=get_all_regions(),
    group_name=pipeline_group_name,
    database=databases["database_gold_dev"],
    databases=databases,
    write_mode=WarehouseWriteMode.overwrite,
    partitions_def=daily_partition_def,
)
dim_alert_configs_1 = assets[AWSRegions.US_WEST_2.value]
dim_alert_configs_2 = assets[AWSRegions.EU_WEST_1.value]
dim_alert_configs_3 = assets[AWSRegions.CA_CENTRAL_1.value]

dqs["dim_alert_configs"].append(
    PrimaryKeyDQCheck(
        name="dq_pk_dim_alert_configs",
        table="dim_alert_configs",
        primary_keys=["date", "alert_config_id"],
        blocking=True,
        database=databases["database_gold_dev"],
    )
)

dqs["dim_alert_configs"].append(
    NonEmptyDQCheck(
        name="dq_empty_dim_alert_configs",
        table="dim_alert_configs",
        blocking=True,
        database=databases["database_gold_dev"],
    )
)

dqs["dim_alert_configs"].append(
    TrendDQCheck(
        name="dq_trend_dim_alert_configs",
        database=databases["database_gold_dev"],
        table="dim_alert_configs",
        blocking=False,
        tolerance=0.1,
    )
)

dqs["dim_alert_configs"].append(
    SQLDQCheck(
        name="dq_sql_dim_alert_configs_primary_trigger_type_null",
        database=databases["database_gold_dev"],
        sql_query="""SELECT COUNT(alert_config_uuid) AS observed_value
                     FROM {database_gold_dev}.dim_alert_configs
                     WHERE primary_trigger_type IS NULL
                     AND primary_trigger_type_id IS NOT NULL
                     AND primary_trigger_type_id NOT IN (5006, 5007) --trigger types that no longer exist
                     """.format(
            DATEID="{DATEID}", database_gold_dev=databases["database_gold_dev"]
        ),
        expected_value=0,
        blocking=False,
    )
)

dq_assets = dqs.generate()
