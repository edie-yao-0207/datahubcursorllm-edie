from dagster import AssetKey, DailyPartitionsDefinition

from ...common.constants import SLACK_ALERTS_CHANNEL_DATA_ENGINEERING
from ...common.utils import (
    AWSRegions,
    Database,
    DQGroup,
    NonEmptyDQCheck,
    NonNullDQCheck,
    PrimaryKeyDQCheck,
    TableType,
    TrendDQCheck,
    WarehouseWriteMode,
    apply_db_overrides,
    build_assets_from_sql,
    build_table_description,
    get_all_regions,
)

databases = {
    "database_bronze": Database.DATAMODEL_TELEMATICS_BRONZE,
    "database_silver": Database.DATAMODEL_TELEMATICS_SILVER,
    "database_gold": Database.DATAMODEL_TELEMATICS,
}

database_dev_overrides = {
    "database_bronze_dev": Database.DATAMODEL_DEV,
    "database_silver_dev": Database.DATAMODEL_DEV,
    "database_gold_dev": Database.DATAMODEL_DEV,
}

databases = apply_db_overrides(databases, database_dev_overrides)

daily_partition_def = DailyPartitionsDefinition(start_date="2023-09-01")

pipeline_group_name = "dim_eld_relevant_devices"

dqs = DQGroup(
    group_name=pipeline_group_name,
    partition_def=daily_partition_def,
    slack_alerts_channel=SLACK_ALERTS_CHANNEL_DATA_ENGINEERING,
    regions=get_all_regions(),
)

stg_eld_relevant_devices_schema = [
    {
        "name": "date",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "The calendar date in 'YYYY-mm-dd' format",
        },
    },
    {
        "name": "org_id",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The Internal ID for the customer's Samsara org",
        },
    },
    {
        "name": "device_id",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The ID of the customer device (vehicle gateway)",
        },
    },
    {
        "name": "make",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "The make of the vehicle"},
    },
    {
        "name": "model",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "The model of the vehicle"},
    },
    {
        "name": "year",
        "type": "long",
        "nullable": True,
        "metadata": {"comment": "The year of the vehicle"},
    },
    {
        "name": "is_unregulated_vehicle",
        "type": "boolean",
        "nullable": True,
        "metadata": {"comment": "Whether the vehicle is unregulated"},
    },
    {
        "name": "has_device_override",
        "type": "boolean",
        "nullable": True,
        "metadata": {
            "comment": "Whether the vehicle is explicitly configured to not send us data"
        },
    },
    {
        "name": "has_org_override",
        "type": "boolean",
        "nullable": True,
        "metadata": {
            "comment": "Whether the org is explicitly configured to not send us data"
        },
    },
    {
        "name": "is_eld_relevant_make_model",
        "type": "boolean",
        "nullable": True,
        "metadata": {
            "comment": "Whether the vehicle make-model is officially listed as falling under the scope of ELD regulations"
        },
    },
    {
        "name": "has_heartbeat_l365d",
        "type": "boolean",
        "nullable": True,
        "metadata": {
            "comment": "Whether the vehicle gateway has recorded a heartbeat in the last 365 days"
        },
    },
    {
        "name": "has_hos_log_l183d",
        "type": "boolean",
        "nullable": True,
        "metadata": {
            "comment": "Whether the vehicle has a recorded HOS log in the last 183 days"
        },
    },
    {
        "name": "has_assigned_or_annotated_hos_log",
        "type": "boolean",
        "nullable": True,
        "metadata": {
            "comment": "Whether the vehicle has ever recorded an assigned or annotated HOS log"
        },
    },
]

stg_eld_relevant_devices_query = """
--sql

WITH
  devices AS (
    SELECT
      device_id,
      org_id,
      product_id,
      associated_vehicle_make,
      associated_vehicle_model,
      associated_vehicle_year,
      is_unregulated_vehicle,
      vehicle_obd_type,
      config_override_json
    FROM
      datamodel_core_silver.stg_vg_devices
      -- For backfills involving older partitions, pick the latest partition available from stg_vg_devices
    WHERE
      `date` = (
        SELECT
          MIN(`date`)
        FROM
          datamodel_core_silver.stg_vg_devices
        WHERE
          `date` >= '{DATEID}'
      )
  ),
  organizations AS (
    SELECT
      org_id,
      internal_type,
      group_config_override
    FROM
      datamodel_core_silver.stg_organizations
      -- For backfills involving older partitions, pick the latest partition available from stg_organizations
    WHERE
      `date` = (
        SELECT
          MIN(`date`)
        FROM
          datamodel_core_silver.stg_organizations
        WHERE
          `date` >= '{DATEID}'
      )
  ),
  eld_relevant_mm AS (
    SELECT
      LOWER(TRIM(make)) AS make,
      LOWER(TRIM(model)) AS model
    FROM
      datamodel_core_silver.stg_eld_relevant_make_models
    GROUP BY
      1,
      2
  ),
  hos_logs AS (
    SELECT
      `date`,
      device_id,
      org_id,
      last_hos_log_date,
      last_assigned_hos_log_date,
      last_annotated_hos_log_date
    FROM
      datamodel_telematics.fct_hos_logs
    WHERE
      `date` = '{DATEID}'
  ),
  device_activity AS (
    SELECT
        org_id,
        object_id AS device_id
    FROM
        kinesisstats_history.osdhubserverdeviceheartbeat
    WHERE
        VALUE.is_databreak = 'false'
        AND VALUE.is_end = 'false'
        AND `date` >= date_sub ('{DATEID}', 365)
    GROUP BY
    1,
    2
  )
SELECT
  '{DATEID}' AS `date`,
  d.org_id,
  d.device_id,
  LOWER(TRIM(d.associated_vehicle_make)) AS make,
  LOWER(TRIM(d.associated_vehicle_model)) AS model,
  d.associated_vehicle_year AS `year`,
  d.is_unregulated_vehicle,
  CASE
    WHEN
    --Widget dropdown (OBD set to OFF) vehicle list
    d.vehicle_obd_type = 3
    OR
    --Probe type = 3 via device level
    get_json_object(d.config_override_json, '$.obd_config.probe_type') = '3'
    OR
    --Cable ID = 0 (VG34 power only cable override) via device level
    --Cable ID = 8 (VG54 power only cable override) via device level
    get_json_object(d.config_override_json, '$.cable_id_override.cable_id') IN ('0', '8') THEN TRUE
    ELSE FALSE
  END AS has_device_override,
  CASE
    WHEN
    --Probe type = 3 via device level
    get_json_object(o.group_config_override, '$.obd_config.probe_type') = '3'
    OR
    --Cable ID = 0 (VG34 power only cable override) via device level
    --Cable ID = 8 (VG54 power only cable override) via device level
    get_json_object(o.group_config_override, '$.cable_id_override.cable_id') IN ('0', '8') THEN TRUE
    ELSE FALSE
  END AS has_org_override,
  COALESCE((mm.make IS NOT NULL), FALSE) AS is_eld_relevant_make_model,
  CASE
    WHEN act.device_id IS NOT NULL THEN TRUE
    ELSE FALSE
  END AS has_heartbeat_l365d,
  CASE
    WHEN fhl.last_hos_log_date >= date_sub (fhl.`date`, 183) THEN TRUE
    ELSE FALSE
  END AS has_hos_log_l183d,
  CASE
    WHEN COALESCE(
      fhl.last_assigned_hos_log_date,
      fhl.last_annotated_hos_log_date
    ) IS NOT NULL THEN TRUE
    ELSE FALSE
  END AS has_assigned_or_annotated_hos_log
FROM
  devices d
  JOIN organizations o ON o.org_id = d.org_id
  LEFT JOIN eld_relevant_mm mm ON LOWER(TRIM(d.associated_vehicle_make)) = mm.make
  AND LOWER(TRIM(d.associated_vehicle_model)) = mm.model
  LEFT JOIN device_activity act ON d.device_id = act.device_id
  AND d.org_id = act.org_id
  LEFT JOIN hos_logs fhl ON d.device_id = fhl.device_id
  AND d.org_id = fhl.org_id
WHERE
  1 = 1
  AND d.product_id IN (24, 53, 35, 89, 178)
  AND o.internal_type = 0
"""

stg_eld_relevant_devices_assets = build_assets_from_sql(
    name="stg_eld_relevant_devices",
    schema=stg_eld_relevant_devices_schema,
    description="""A staging table that contains daily device-specific attributes used in the calculation
                of the device's Electronic Logging Device (ELD) relevance""",
    sql_query=stg_eld_relevant_devices_query,
    primary_keys=["date", "device_id", "org_id"],
    upstreams=[
        AssetKey([Database.DATAMODEL_TELEMATICS, "fct_hos_logs"]),
        AssetKey([Database.DATAMODEL_CORE_SILVER, "stg_eld_relevant_make_models"]),
        AssetKey([Database.DATAMODEL_CORE_SILVER, "stg_vg_devices"]),
        AssetKey([Database.DATAMODEL_CORE_SILVER, "stg_organizations"]),
    ],
    regions=get_all_regions(),
    group_name=pipeline_group_name,
    database=databases["database_silver_dev"],
    databases=databases,
    write_mode=WarehouseWriteMode.overwrite,
    partitions_def=daily_partition_def,
)

stg_eld_relevant_devices_us = stg_eld_relevant_devices_assets[
    AWSRegions.US_WEST_2.value
]
stg_eld_relevant_devices_eu = stg_eld_relevant_devices_assets[
    AWSRegions.EU_WEST_1.value
]
stg_eld_relevant_devices_ca = stg_eld_relevant_devices_assets[
    AWSRegions.CA_CENTRAL_1.value
]

dqs["stg_eld_relevant_devices"].append(
    PrimaryKeyDQCheck(
        name="dq_pk_stg_eld_relevant_devices",
        table="stg_eld_relevant_devices",
        primary_keys=["date", "device_id", "org_id"],
        blocking=True,
        database=databases["database_silver_dev"],
    )
)

dim_eld_relevant_devices_schema = [
    {
        "name": "date",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "The calendar date in 'YYYY-mm-dd' format",
        },
    },
    {
        "name": "org_id",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The Internal ID for the customer's Samsara org",
        },
    },
    {
        "name": "device_id",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The ID of the customer device",
        },
    },
    {
        "name": "is_eld_relevant",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": """Whether the device is considered ELD relvant based on the criteria defined in
            https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/4648066/HoS+ELD+Vehicle+Definitions+Working+Doc"""
        },
    },
]

dim_eld_relevant_devices_query = """
--sql

SELECT
    `date`,
    org_id,
    device_id,
    case
    when
      has_heartbeat_l365d
      and year >= 2000
      and is_eld_relevant_make_model --- filter out vehicles not explicitly marked as ELD relevant in the MM csv from legal/product
      and has_hos_log_l183d --- filter out devices without logs in half year
      and has_assigned_or_annotated_hos_log --- filtering out vehicles that have never had an assigned HoS log
      and not(is_unregulated_vehicle) --- filtering out explicitly unregulated vehicles
      and not(has_device_override)
      and not(has_org_override) --- filtering out devices and orgs with explicit data overrides
    then true else false
  end AS is_eld_relevant
FROM
    `{database_silver_dev}`.stg_eld_relevant_devices
WHERE
    `date` = '{DATEID}'
"""

dim_eld_relevant_devices_assets = build_assets_from_sql(
    name="dim_eld_relevant_devices",
    schema=dim_eld_relevant_devices_schema,
    description=build_table_description(
        table_desc="""A dimension table that offers daily insights into the Electronic Logging Device (ELD) relevance of a device.
                The related staging table (stg_eld_relevant_devices) has details on factors affecting this data point""",
        row_meaning=""" Each corresponds to a unique device (VG) as recorded on a specific date,
        and a flag (is_eld_relevant) stating whether the device is considered to be ELD relevant as of that date.
        Filter for a specific date to determine the list of ELD relevant devices on that date or on the latest date for the most up to date list.""",
        related_table_info={},
        table_type=TableType.DAILY_DIMENSION,
        freshness_slo_updated_by="9am PST",
    ),
    sql_query=dim_eld_relevant_devices_query,
    primary_keys=["date", "device_id", "org_id"],
    upstreams=[
        AssetKey([databases["database_silver_dev"], "dq_stg_eld_relevant_devices"])
    ],
    regions=get_all_regions(),
    group_name=pipeline_group_name,
    database=databases["database_gold_dev"],
    databases=databases,
    write_mode=WarehouseWriteMode.overwrite,
    partitions_def=daily_partition_def,
)

dim_eld_relevant_devices_us = dim_eld_relevant_devices_assets[
    AWSRegions.US_WEST_2.value
]
dim_eld_relevant_devices_eu = dim_eld_relevant_devices_assets[
    AWSRegions.EU_WEST_1.value
]
dim_eld_relevant_devices_ca = dim_eld_relevant_devices_assets[
    AWSRegions.CA_CENTRAL_1.value
]

dqs["dim_eld_relevant_devices"].append(
    PrimaryKeyDQCheck(
        name="dq_pk_dim_eld_relevant_devices",
        table="dim_eld_relevant_devices",
        primary_keys=["date", "device_id", "org_id"],
        blocking=True,
        database=databases["database_gold_dev"],
    )
)

dqs["dim_eld_relevant_devices"].append(
    NonEmptyDQCheck(
        name="dq_non_empty_dim_eld_relevant_devices",
        database=databases["database_gold_dev"],
        table="dim_eld_relevant_devices",
        blocking=True,
    )
)

dqs["dim_eld_relevant_devices"].append(
    NonNullDQCheck(
        name="dq_non_null_dim_eld_relevant_devices",
        database=databases["database_gold_dev"],
        table="dim_eld_relevant_devices",
        non_null_columns=["org_id", "device_id", "is_eld_relevant"],
        blocking=True,
    )
)

dqs["dim_eld_relevant_devices"].append(
    TrendDQCheck(
        name="dim_eld_relevant_devices_check_day_over_day_row_count",
        database=databases["database_gold_dev"],
        table="dim_eld_relevant_devices",
        tolerance=0.02,
        blocking=False,
    )
)


dq_assets = dqs.generate()
