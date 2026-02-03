import json
import re
from collections import defaultdict
from datetime import datetime, timedelta

from dagster import (
    AssetDep,
    AssetKey,
    BackfillPolicy,
    DailyPartitionsDefinition,
    IdentityPartitionMapping,
    TimeWindowPartitionMapping,
    asset,
)
from pyspark.sql import DataFrame, SparkSession

from ...common.constants import (
    SLACK_ALERTS_CHANNEL_DATA_ENGINEERING,
    driver_id_default_description,
    org_id_default_description,
    sam_number_default_description,
)
from ...common.utils import (
    AWSRegions,
    Database,
    DQGroup,
    JoinableDQCheck,
    NonEmptyDQCheck,
    NonNegativeDQCheck,
    NonNullDQCheck,
    PrimaryKeyDQCheck,
    TableType,
    TrendDQCheck,
    WarehouseWriteMode,
    adjust_partition_def_for_canada,
    apply_db_overrides,
    build_table_description,
    get_all_regions,
    get_code_location,
)

GROUP_NAME = "driver_activity"
REQUIRED_RESOURCE_KEYS = {"databricks_pyspark_step_launcher"}
REQUIRED_RESOURCE_KEYS_EU = {"databricks_pyspark_step_launcher_eu"}
REQUIRED_RESOURCE_KEYS_CA = {"databricks_pyspark_step_launcher_ca"}
SLACK_ALERTS_CHANNEL = SLACK_ALERTS_CHANNEL_DATA_ENGINEERING

RAW_TABLES_PARTITIONS_DEF = DailyPartitionsDefinition(start_date="2024-05-01")
FCT_PARTITIONS_DEF = DailyPartitionsDefinition(start_date="2020-11-01")
FCT_PARTITIONS_DEF_CA = adjust_partition_def_for_canada(FCT_PARTITIONS_DEF)
DIM_PARTITIONS_DEF = DailyPartitionsDefinition(start_date="2024-05-01")
DIM_PARTITIONS_DEF_CA = adjust_partition_def_for_canada(DIM_PARTITIONS_DEF)

dqs = DQGroup(
    group_name=GROUP_NAME,
    partition_def=FCT_PARTITIONS_DEF,
    slack_alerts_channel=SLACK_ALERTS_CHANNEL,
    regions=get_all_regions(),
)

dim_dqs = DQGroup(
    group_name=GROUP_NAME,
    partition_def=DIM_PARTITIONS_DEF,
    slack_alerts_channel=SLACK_ALERTS_CHANNEL,
    regions=get_all_regions(),
)

databases = {
    "database_bronze": Database.DATAMODEL_PLATFORM_BRONZE,
    "database_silver": Database.DATAMODEL_PLATFORM_SILVER,
    "database_gold": Database.DATAMODEL_PLATFORM,
    "database_core_gold": Database.DATAMODEL_CORE,
    "database_core_bronze": Database.DATAMODEL_CORE_BRONZE,
}

database_dev_overrides = {
    "database_bronze_dev": Database.DATAMODEL_DEV,
    "database_silver_dev": Database.DATAMODEL_DEV,
    "database_gold_dev": Database.DATAMODEL_DEV,
    "database_core_gold_dev": Database.DATAMODEL_DEV,
    "database_core_bronze_dev": Database.DATAMODEL_DEV,
}

databases = apply_db_overrides(databases, database_dev_overrides)

RAW_CLOUDDB_DRIVERS_QUERY = """
SELECT
   '{DATEID}' AS date,
    *
FROM clouddb.drivers
TIMESTAMP AS OF '{DATEID} 23:59:59.999'
"""

STG_DRIVER_APP_EVENTS_QUERY = """
    SELECT date,
        driver_id,
        org_id,
        event_type,
        COUNT(*) AS event_count
    FROM datastreams_history.mobile_logs
    WHERE driver_id IS NOT NULL
    AND driver_id != -1
    AND date BETWEEN '{START_DATEID}' AND '{END_DATEID}'
    GROUP BY 1,2,3,4
"""

STG_DRIVER_APP_EVENTS_SCHEMA = [
    {
        "name": "date",
        "type": "date",
        "nullable": False,
        "metadata": {
            "comment": "Date of partition sourced from datastreams.mobile_logs"
        },
    },
    {
        "name": "driver_id",
        "type": "long",
        "nullable": False,
        "metadata": {"comment": driver_id_default_description},
    },
    {
        "name": "org_id",
        "type": "long",
        "nullable": False,
        "metadata": {"comment": org_id_default_description},
    },
    {
        "name": "event_type",
        "type": "string",
        "nullable": False,
        "metadata": {"comment": "Type of event sourced from datastreams.mobile_logs."},
    },
    {
        "name": "event_count",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Count of events per driver_id, org_id, event_type, and date partition"
        },
    },
]

FCT_DRIVER_ACTIVITY_QUERY = """
    SELECT date,
        driver_id,
        org_id
    FROM datamodel_platform_silver.stg_driver_app_events
    WHERE date BETWEEN '{START_DATEID}' AND '{END_DATEID}'
    GROUP BY 1,2,3
"""

FCT_DRIVER_ACTIVITY_SCHEMA = [
    {
        "name": "date",
        "type": "date",
        "nullable": False,
        "metadata": {
            "comment": "Date of partition sourced from datamodel_platform_silver.stg_driver_app_events."
        },
    },
    {
        "name": "driver_id",
        "type": "long",
        "nullable": False,
        "metadata": {"comment": driver_id_default_description},
    },
    {
        "name": "org_id",
        "type": "long",
        "nullable": False,
        "metadata": {"comment": org_id_default_description},
    },
]

BUILD_DIM_DRIVERS_QUERY = """
WITH organizations AS (
  SELECT *
  FROM datamodel_core.dim_organizations
  WHERE date = (SELECT MAX(date) FROM datamodel_core.dim_organizations)),
  activity AS (
  SELECT driver_id,
        MAX(date) AS last_mobile_login_date,
        MIN(date) AS first_mobile_login_date
  FROM {database_gold}.fct_driver_activity
  WHERE date <= '{DATEID}'
  GROUP BY 1)

SELECT '{DATEID}' AS date,
       drivers.id AS driver_id,
       drivers.created_at,
       drivers.updated_at,
       drivers.org_id,
       organizations.internal_type,
       organizations.sam_number,
       organizations.org_name,
       organizations.account_name,
       drivers.phone,
       drivers.created_user_id AS created_by,
       drivers.group_id,
       drivers.username,
       drivers.license_number,
       drivers.license_state,
       drivers.eld_exempt,
       drivers.eld_exempt_reason,
       drivers.eld_big_day_exemption_enabled,
       drivers.eld_adverse_weather_exemption_enabled,
       drivers.eld_pc_enabled,
       drivers.eld_ym_enabled,
       drivers.timezone,
       drivers.eld_day_start_hour,
       drivers.deleted_at,
       CASE WHEN drivers.deleted_at > '2017-01-01' THEN TRUE ELSE FALSE END AS is_deleted,
       drivers.carrier_name,
       drivers.carrier_address,
       drivers.carrier_us_dot_number,
       drivers.carrier_name_override,
       drivers.carrier_address_override,
       drivers.carrier_us_dot_number_override,
       drivers.peer_group_tag_id,
       drivers.pinned_vehicle_id,
       drivers.locale_override,
       drivers.unit_system_override,
       drivers.language_override,
       drivers.currency_override,
       drivers.tachograph_card_number,
       drivers.eld_defer_off_duty_exemption_enabled,
       drivers.eld_canada_adverse_driving_exemption_enabled,
       drivers.in_motion_screen_auto_popup_enabled,
       drivers.vehicle_group_tag_id,
       drivers.waiting_time_duty_status_enabled,
       drivers.eld_utility_exemption_enabled,
       drivers.home_terminal_name,
       drivers.home_terminal_address,
       drivers.trailer_group_tag_id,
       drivers.eld_agriculture_exemption_enabled,
       activity.first_mobile_login_date,
       activity.last_mobile_login_date
    FROM datamodel_platform_bronze.raw_clouddb_drivers drivers
    LEFT OUTER JOIN organizations
        ON organizations.org_id = drivers.org_id
    LEFT OUTER JOIN activity
        ON activity.driver_id = drivers.id
    WHERE drivers.date = '{DATEID}'"""

INSERT_DIM_DRIVERS_QUERY = """
WITH organizations AS (
  SELECT *
  FROM datamodel_core.dim_organizations
  WHERE date = (SELECT MAX(date) FROM datamodel_core.dim_organizations)),
  activity AS (
  SELECT driver_id,
        MAX(date) AS last_mobile_login_date,
        MIN(date) AS first_mobile_login_date
  FROM datamodel_platform.fct_driver_activity
  WHERE date = '{DATEID}'
  GROUP BY 1),
  prev AS (
  SELECT *
  FROM datamodel_platform.dim_drivers
  WHERE date = DATE_ADD('{DATEID}',-1)),
  curr AS (
  SELECT *
  FROM datamodel_platform_bronze.raw_clouddb_drivers
  WHERE date = '{DATEID}')

SELECT '{DATEID}' AS date,
       COALESCE(prev.driver_id, curr.id) AS driver_id,
       COALESCE(curr.created_at, prev.created_at) AS created_at,
       COALESCE(curr.updated_at, prev.updated_at) AS updated_at,
       COALESCE(curr.org_id, prev.org_id) AS org_id,
       COALESCE(organizations.internal_type, prev.internal_type) AS internal_type,
       COALESCE(organizations.sam_number, prev.sam_number) AS sam_number,
       COALESCE(organizations.org_name, prev.org_name) AS org_name,
       COALESCE(organizations.account_name, prev.account_name) AS account_name,
       COALESCE(curr.phone, prev.phone) AS phone,
       COALESCE(curr.created_user_id, prev.created_by) AS created_by,
       COALESCE(curr.group_id, prev.group_id) AS group_id,
       COALESCE(curr.username, prev.username) AS username,
       COALESCE(curr.license_number, prev.license_number) AS license_number,
       COALESCE(curr.license_state, prev.license_state) AS license_state,
       COALESCE(curr.eld_exempt, prev.eld_exempt) AS eld_exempt,
       COALESCE(curr.eld_exempt_reason, prev.eld_exempt_reason) AS eld_exempt_reason,
       COALESCE(curr.eld_big_day_exemption_enabled, prev.eld_big_day_exemption_enabled) AS eld_big_day_exemption_enabled,
       COALESCE(curr.eld_adverse_weather_exemption_enabled, prev.eld_adverse_weather_exemption_enabled) AS eld_adverse_weather_exemption_enabled,
       COALESCE(curr.eld_pc_enabled, prev.eld_pc_enabled) AS eld_pc_enabled,
       COALESCE(curr.eld_ym_enabled, prev.eld_ym_enabled) AS eld_ym_enabled,
       COALESCE(curr.timezone, prev.timezone) AS timezone,
       COALESCE(curr.eld_day_start_hour, prev.eld_day_start_hour) AS eld_day_start_hour,
       COALESCE(curr.deleted_at, prev.deleted_at) AS deleted_at,
       CASE WHEN COALESCE(curr.deleted_at, prev.deleted_at) > '2017-01-01' THEN TRUE ELSE FALSE END AS is_deleted,
       COALESCE(curr.carrier_name, prev.carrier_name) AS carrier_name,
       COALESCE(curr.carrier_address, prev.carrier_address) AS carrier_address,
       COALESCE(curr.carrier_us_dot_number, prev.carrier_us_dot_number) AS carrier_us_dot_number,
       COALESCE(curr.carrier_name_override, prev.carrier_name_override) AS carrier_name_override,
       COALESCE(curr.carrier_address_override, prev.carrier_address_override) AS carrier_address_override,
       COALESCE(curr.carrier_us_dot_number_override, prev.carrier_us_dot_number_override) AS carrier_us_dot_number_override,
       COALESCE(curr.peer_group_tag_id, prev.peer_group_tag_id) AS peer_group_tag_id,
       COALESCE(curr.pinned_vehicle_id, prev.pinned_vehicle_id) AS pinned_vehicle_id,
       COALESCE(curr.locale_override, prev.locale_override) AS locale_override,
       COALESCE(curr.unit_system_override, prev.unit_system_override) AS unit_system_override,
       COALESCE(curr.language_override, prev.language_override) AS language_override,
       COALESCE(curr.currency_override, prev.currency_override) AS currency_override,
       COALESCE(curr.tachograph_card_number, prev.tachograph_card_number) AS tachograph_card_number,
       COALESCE(curr.eld_defer_off_duty_exemption_enabled, prev.eld_defer_off_duty_exemption_enabled) AS eld_defer_off_duty_exemption_enabled,
       COALESCE(curr.eld_canada_adverse_driving_exemption_enabled, prev.eld_canada_adverse_driving_exemption_enabled) AS eld_canada_adverse_driving_exemption_enabled,
       COALESCE(curr.in_motion_screen_auto_popup_enabled, prev.in_motion_screen_auto_popup_enabled) AS in_motion_screen_auto_popup_enabled,
       COALESCE(curr.vehicle_group_tag_id, prev.vehicle_group_tag_id) AS vehicle_group_tag_id,
       COALESCE(curr.waiting_time_duty_status_enabled, prev.waiting_time_duty_status_enabled) AS waiting_time_duty_status_enabled,
       COALESCE(curr.eld_utility_exemption_enabled, prev.eld_utility_exemption_enabled) AS eld_utility_exemption_enabled,
       COALESCE(curr.home_terminal_name, prev.home_terminal_name) AS home_terminal_name,
       COALESCE(curr.home_terminal_address, prev.home_terminal_address) AS home_terminal_address,
       COALESCE(curr.trailer_group_tag_id, prev.trailer_group_tag_id) AS trailer_group_tag_id,
       COALESCE(curr.eld_agriculture_exemption_enabled, prev.eld_agriculture_exemption_enabled) AS eld_agriculture_exemption_enabled,
       COALESCE(activity.first_mobile_login_date, prev.first_mobile_login_date) AS first_mobile_login_date,
       COALESCE(activity.last_mobile_login_date, prev.last_mobile_login_date) AS last_mobile_login_date
    FROM curr
    FULL OUTER JOIN prev
        ON prev.driver_id = curr.id
    LEFT OUTER JOIN organizations
        ON organizations.org_id = COALESCE(prev.driver_id, curr.id)
    LEFT OUTER JOIN activity
        ON activity.driver_id = COALESCE(prev.driver_id, curr.id)
"""

DIM_DRIVERS_SCHEMA = [
    {
        "name": "date",
        "type": "string",
        "nullable": False,
        "metadata": {"comment": "Date of partition of snapshot."},
    },
    {
        "name": "driver_id",
        "type": "long",
        "nullable": False,
        "metadata": {"comment": driver_id_default_description},
    },
    {
        "name": "created_at",
        "type": "timestamp",
        "nullable": False,
        "metadata": {
            "comment": "Timestamp of when the driver was created (sourced from clouddb.drivers)"
        },
    },
    {
        "name": "updated_at",
        "type": "timestamp",
        "nullable": False,
        "metadata": {
            "comment": "Timestamp of when the driver was updated (sourced from clouddb.drivers)"
        },
    },
    {
        "name": "org_id",
        "type": "long",
        "nullable": False,
        "metadata": {"comment": org_id_default_description},
    },
    {
        "name": "internal_type",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Internal type of driver organization sourced from datamodel_core.dim_organizations."
        },
    },
    {
        "name": "sam_number",
        "type": "string",
        "nullable": False,
        "metadata": {"comment": sam_number_default_description},
    },
    {
        "name": "org_name",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Name of driver organization sourced from datamodel_core.dim_organizations."
        },
    },
    {
        "name": "account_name",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Salesforce account name of driver organization sourced from datamodel_core.dim_organizations."
        },
    },
    {
        "name": "phone",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Phone number of driver (sourced from clouddb.drivers)"
        },
    },
    {
        "name": "created_by",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "id of the user who created the driver (sourced from clouddb.drivers)"
        },
    },
    {
        "name": "group_id",
        "type": "long",
        "nullable": False,
        "metadata": {"comment": "Group id of driver (sourced from clouddb.drivers)"},
    },
    {
        "name": "username",
        "type": "string",
        "nullable": False,
        "metadata": {"comment": "Username of driver (sourced from clouddb.drivers)"},
    },
    {
        "name": "license_number",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "License number of driver (sourced from clouddb.drivers)"
        },
    },
    {
        "name": "license_state",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "License state of driver (sourced from clouddb.drivers)"
        },
    },
    {
        "name": "eld_exempt",
        "type": "byte",
        "nullable": False,
        "metadata": {
            "comment": "ELD exemption of driver (sourced from clouddb.drivers)"
        },
    },
    {
        "name": "eld_exempt_reason",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "ELD exemption reason of driver (sourced from clouddb.drivers)"
        },
    },
    {
        "name": "eld_big_day_exemption_enabled",
        "type": "byte",
        "nullable": False,
        "metadata": {
            "comment": "ELD big day exemption enabled of driver (sourced from clouddb.drivers)"
        },
    },
    {
        "name": "eld_adverse_weather_exemption_enabled",
        "type": "byte",
        "nullable": False,
        "metadata": {
            "comment": "ELD adverse weather exemption enabled of driver (sourced from clouddb.drivers)"
        },
    },
    {
        "name": "eld_pc_enabled",
        "type": "byte",
        "nullable": False,
        "metadata": {
            "comment": "ELD pc enabled for driver (sourced from clouddb.drivers)"
        },
    },
    {
        "name": "eld_ym_enabled",
        "type": "byte",
        "nullable": False,
        "metadata": {
            "comment": "ELD ym enabled for driver (sourced from clouddb.drivers)"
        },
    },
    {
        "name": "timezone",
        "type": "string",
        "nullable": False,
        "metadata": {"comment": "Timezone of driver (sourced from clouddb.drivers)"},
    },
    {
        "name": "eld_day_start_hour",
        "type": "integer",
        "nullable": False,
        "metadata": {
            "comment": "ELD day start hour of driver (sourced from clouddb.drivers)"
        },
    },
    {
        "name": "deleted_at",
        "type": "timestamp",
        "nullable": False,
        "metadata": {
            "comment": "When (if) driver was deleted (sourced from clouddb.drivers)"
        },
    },
    {
        "name": "is_deleted",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "TRUE if deleted_at is one of the zero dates (0101-01-01, 0101-01-02), otherwise FALSE"
        },
    },
    {
        "name": "carrier_name",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Carrier name of driver (sourced from clouddb.drivers)"
        },
    },
    {
        "name": "carrier_address",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Carrier address of driver (sourced from clouddb.drivers)"
        },
    },
    {
        "name": "carrier_us_dot_number",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Carrier us DOT number of driver (sourced from clouddb.drivers)"
        },
    },
    {
        "name": "carrier_name_override",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Carrier name override of driver (sourced from clouddb.drivers)"
        },
    },
    {
        "name": "carrier_address_override",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Carrier address override of driver (sourced from clouddb.drivers)"
        },
    },
    {
        "name": "carrier_us_dot_number_override",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Carrier US DOT number override of driver (sourced from clouddb.drivers)"
        },
    },
    {
        "name": "peer_group_tag_id",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Peer group tag ID of driver (sourced from clouddb.drivers)"
        },
    },
    {
        "name": "pinned_vehicle_id",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Pinned vehicle ID of driver (sourced from clouddb.drivers)"
        },
    },
    {
        "name": "locale_override",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Locale override of driver (sourced from clouddb.drivers)"
        },
    },
    {
        "name": "unit_system_override",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Unit system override of driver (sourced from clouddb.drivers)"
        },
    },
    {
        "name": "language_override",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Language override of driver (sourced from clouddb.drivers)"
        },
    },
    {
        "name": "currency_override",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Currency override of driver (sourced from clouddb.drivers)"
        },
    },
    {
        "name": "tachograph_card_number",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Tachograph card number of driver (sourced from clouddb.drivers)"
        },
    },
    {
        "name": "eld_defer_off_duty_exemption_enabled",
        "type": "byte",
        "nullable": False,
        "metadata": {
            "comment": "ELD defer off duty exemption enabled for driver (sourced from clouddb.drivers)"
        },
    },
    {
        "name": "eld_canada_adverse_driving_exemption_enabled",
        "type": "byte",
        "nullable": False,
        "metadata": {
            "comment": "ELD Canada adverse driving exemption enabled for driver (sourced from clouddb.drivers)"
        },
    },
    {
        "name": "in_motion_screen_auto_popup_enabled",
        "type": "byte",
        "nullable": False,
        "metadata": {
            "comment": "In motion screen auto popup enabled for driver (sourced from clouddb.drivers)"
        },
    },
    {
        "name": "vehicle_group_tag_id",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Vehicle group tag ID of driver (sourced from clouddb.drivers)"
        },
    },
    {
        "name": "waiting_time_duty_status_enabled",
        "type": "byte",
        "nullable": False,
        "metadata": {
            "comment": "Waiting time duty status enabled for driver (sourced from clouddb.drivers)"
        },
    },
    {
        "name": "eld_utility_exemption_enabled",
        "type": "byte",
        "nullable": False,
        "metadata": {
            "comment": "ELD utility exemption enabled for driver (sourced from clouddb.drivers)"
        },
    },
    {
        "name": "home_terminal_name",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Home terminal name of driver (sourced from clouddb.drivers)"
        },
    },
    {
        "name": "home_terminal_address",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Home terminal address of driver (sourced from clouddb.drivers)"
        },
    },
    {
        "name": "trailer_group_tag_id",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Trailer group tag ID of driver (sourced from clouddb.drivers)"
        },
    },
    {
        "name": "eld_agriculture_exemption_enabled",
        "type": "byte",
        "nullable": False,
        "metadata": {
            "comment": "ELD agriculture exemption enabled for driver (sourced from clouddb.drivers)"
        },
    },
    {
        "name": "first_mobile_login_date",
        "type": "date",
        "nullable": False,
        "metadata": {
            "comment": "First date of usage from datastreams_history.mobile_logs, showing first mobile app usage date"
        },
    },
    {
        "name": "last_mobile_login_date",
        "type": "date",
        "nullable": False,
        "metadata": {
            "comment": "Last date of usage from datastreams_history.mobile_logs, showing last mobile app usage date"
        },
    },
]


@asset(
    name="raw_clouddb_drivers",
    owners=["team:DataEngineering"],
    description="Daily snapshot of clouddb.drivers table",
    compute_kind="sql",
    metadata={
        "database": databases["database_bronze"],
        "owners": ["team:DataEngineering"],
        "write_mode": WarehouseWriteMode.overwrite,
        "schema": [],
        "code_location": get_code_location(),
        "description": "Daily snapshot of clouddb.drivers table",
    },
    required_resource_keys=REQUIRED_RESOURCE_KEYS,
    group_name=GROUP_NAME,
    partitions_def=RAW_TABLES_PARTITIONS_DEF,
    key_prefix=[AWSRegions.US_WEST_2.value, databases["database_bronze"]],
)
def raw_clouddb_drivers(context) -> DataFrame:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    query = RAW_CLOUDDB_DRIVERS_QUERY.format(DATEID=context.partition_key, **databases)
    context.log.info(query)
    df = spark.sql(query)
    context.log.info(df)
    return df


@asset(
    name="raw_clouddb_drivers",
    owners=["team:DataEngineering"],
    description="Daily snapshot of clouddb.drivers table",
    compute_kind="sql",
    metadata={
        "database": databases["database_bronze"],
        "owners": ["team:DataEngineering"],
        "write_mode": WarehouseWriteMode.overwrite,
        "schema": [],
        "code_location": get_code_location(),
        "description": "Daily snapshot of clouddb.drivers table",
    },
    required_resource_keys=REQUIRED_RESOURCE_KEYS_EU,
    group_name=GROUP_NAME,
    partitions_def=RAW_TABLES_PARTITIONS_DEF,
    key_prefix=[AWSRegions.EU_WEST_1.value, databases["database_bronze"]],
)
def raw_clouddb_drivers_eu(context) -> DataFrame:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    query = RAW_CLOUDDB_DRIVERS_QUERY.format(DATEID=context.partition_key, **databases)
    context.log.info(query)
    df = spark.sql(query)
    context.log.info(df)
    return df


@asset(
    name="raw_clouddb_drivers",
    owners=["team:DataEngineering"],
    description="Daily snapshot of clouddb.drivers table",
    compute_kind="sql",
    metadata={
        "database": databases["database_bronze"],
        "owners": ["team:DataEngineering"],
        "write_mode": WarehouseWriteMode.overwrite,
        "schema": [],
        "code_location": get_code_location(),
        "description": "Daily snapshot of clouddb.drivers table",
    },
    required_resource_keys=REQUIRED_RESOURCE_KEYS_CA,
    group_name=GROUP_NAME + "_ca",
    partitions_def=RAW_TABLES_PARTITIONS_DEF,
    key_prefix=[AWSRegions.CA_CENTRAL_1.value, databases["database_bronze"]],
)
def raw_clouddb_drivers_ca(context) -> DataFrame:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    query = RAW_CLOUDDB_DRIVERS_QUERY.format(DATEID=context.partition_key, **databases)
    context.log.info(query)
    df = spark.sql(query)
    context.log.info(df)
    return df


@asset(
    name="stg_driver_app_events",
    owners=["team:DataEngineering"],
    description="Daily aggregate of driver mobile app events from datastreams_history.mobile_logs",
    compute_kind="sql",
    metadata={
        "database": databases["database_silver"],
        "owners": ["team:DataEngineering"],
        "write_mode": WarehouseWriteMode.overwrite,
        "schema": STG_DRIVER_APP_EVENTS_SCHEMA,
        "code_location": get_code_location(),
        "primary_keys": ["date", "driver_id", "org_id", "event_type"],
        "description": "Daily aggregate of driver mobile app events from datastreams_history.mobile_logs",
    },
    required_resource_keys=REQUIRED_RESOURCE_KEYS_EU,
    group_name=GROUP_NAME + "_eu",
    partitions_def=FCT_PARTITIONS_DEF,
    backfill_policy=BackfillPolicy.multi_run(max_partitions_per_run=30),
    key_prefix=[AWSRegions.EU_WEST_1.value, databases["database_silver"]],
)
def stg_driver_app_events_eu(context) -> DataFrame:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    start_date = context.asset_partition_key_range.start
    end_date = context.asset_partition_key_range.end
    query = STG_DRIVER_APP_EVENTS_QUERY.format(
        START_DATEID=start_date, END_DATEID=end_date, **databases
    )
    context.log.info(query)
    df = spark.sql(query)
    context.log.info(df)
    return df


@asset(
    name="stg_driver_app_events",
    owners=["team:DataEngineering"],
    description="Daily aggregate of driver mobile app events from datastreams_history.mobile_logs",
    compute_kind="sql",
    metadata={
        "database": databases["database_silver"],
        "owners": ["team:DataEngineering"],
        "write_mode": WarehouseWriteMode.overwrite,
        "schema": STG_DRIVER_APP_EVENTS_SCHEMA,
        "code_location": get_code_location(),
        "primary_keys": ["date", "driver_id", "org_id", "event_type"],
        "description": "Daily aggregate of driver mobile app events from datastreams_history.mobile_logs",
    },
    required_resource_keys=REQUIRED_RESOURCE_KEYS,
    group_name=GROUP_NAME,
    partitions_def=FCT_PARTITIONS_DEF,
    backfill_policy=BackfillPolicy.multi_run(max_partitions_per_run=30),
    key_prefix=[AWSRegions.US_WEST_2.value, databases["database_silver"]],
)
def stg_driver_app_events(context) -> DataFrame:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    start_date = context.asset_partition_key_range.start
    end_date = context.asset_partition_key_range.end
    query = STG_DRIVER_APP_EVENTS_QUERY.format(
        START_DATEID=start_date, END_DATEID=end_date, **databases
    )
    context.log.info(query)
    df = spark.sql(query)
    context.log.info(df)
    return df


@asset(
    name="stg_driver_app_events",
    owners=["team:DataEngineering"],
    description="Daily aggregate of driver mobile app events from datastreams_history.mobile_logs",
    compute_kind="sql",
    metadata={
        "database": databases["database_silver"],
        "owners": ["team:DataEngineering"],
        "write_mode": WarehouseWriteMode.overwrite,
        "schema": STG_DRIVER_APP_EVENTS_SCHEMA,
        "code_location": get_code_location(),
        "primary_keys": ["date", "driver_id", "org_id", "event_type"],
        "description": "Daily aggregate of driver mobile app events from datastreams_history.mobile_logs",
    },
    required_resource_keys=REQUIRED_RESOURCE_KEYS_CA,
    group_name=GROUP_NAME + "_ca",
    partitions_def=FCT_PARTITIONS_DEF_CA,
    backfill_policy=BackfillPolicy.multi_run(max_partitions_per_run=30),
    key_prefix=[AWSRegions.CA_CENTRAL_1.value, databases["database_silver"]],
)
def stg_driver_app_events_ca(context) -> DataFrame:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    start_date = context.asset_partition_key_range.start
    end_date = context.asset_partition_key_range.end
    query = STG_DRIVER_APP_EVENTS_QUERY.format(
        START_DATEID=start_date, END_DATEID=end_date, **databases
    )
    context.log.info(query)
    df = spark.sql(query)
    context.log.info(df)
    return df


dqs["stg_driver_app_events"].append(
    PrimaryKeyDQCheck(
        name="dq_pk_stg_driver_app_events",
        table="stg_driver_app_events",
        primary_keys=["date", "driver_id", "org_id", "event_type"],
        blocking=True,
        database=databases["database_silver"],
    )
)


fct_driver_activity_description = build_table_description(
    table_desc="""This table provides a condensed representation of drivers active on a particular date.""",
    row_meaning="""One row in this table represents one day of driver app usage sourced from datamodel_platform_silver.stg_driver_app_events.""",
    table_type=TableType.TRANSACTIONAL_FACT,
    freshness_slo_updated_by="9am PST",
)


@asset(
    name="fct_driver_activity",
    owners=["team:DataEngineering"],
    description=fct_driver_activity_description,
    compute_kind="sql",
    metadata={
        "database": databases["database_gold"],
        "owners": ["team:DataEngineering"],
        "write_mode": WarehouseWriteMode.overwrite,
        "schema": FCT_DRIVER_ACTIVITY_SCHEMA,
        "code_location": get_code_location(),
        "primary_keys": ["date", "driver_id", "org_id"],
        "description": fct_driver_activity_description,
    },
    required_resource_keys=REQUIRED_RESOURCE_KEYS,
    group_name=GROUP_NAME,
    partitions_def=FCT_PARTITIONS_DEF,
    backfill_policy=BackfillPolicy.multi_run(max_partitions_per_run=30),
    key_prefix=[AWSRegions.US_WEST_2.value, databases["database_gold"]],
    deps=[
        AssetDep(
            AssetKey(
                [
                    AWSRegions.US_WEST_2.value,
                    databases["database_silver"],
                    "dq_stg_driver_app_events",
                ]
            ),
            partition_mapping=IdentityPartitionMapping(),
        ),
    ],
)
def fct_driver_activity(context) -> DataFrame:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    start_date = context.asset_partition_key_range.start
    end_date = context.asset_partition_key_range.end
    query = FCT_DRIVER_ACTIVITY_QUERY.format(
        START_DATEID=start_date, END_DATEID=end_date, **databases
    )
    context.log.info(query)
    df = spark.sql(query)
    context.log.info(df)

    return df


@asset(
    name="fct_driver_activity",
    owners=["team:DataEngineering"],
    description=fct_driver_activity_description,
    compute_kind="sql",
    metadata={
        "database": databases["database_gold"],
        "owners": ["team:DataEngineering"],
        "write_mode": WarehouseWriteMode.overwrite,
        "schema": FCT_DRIVER_ACTIVITY_SCHEMA,
        "code_location": get_code_location(),
        "primary_keys": ["date", "driver_id", "org_id"],
        "description": fct_driver_activity_description,
    },
    required_resource_keys=REQUIRED_RESOURCE_KEYS_EU,
    group_name=GROUP_NAME + "_eu",
    partitions_def=FCT_PARTITIONS_DEF,
    backfill_policy=BackfillPolicy.multi_run(max_partitions_per_run=30),
    key_prefix=[AWSRegions.EU_WEST_1.value, databases["database_gold"]],
    deps=[
        AssetDep(
            AssetKey(
                [
                    AWSRegions.EU_WEST_1.value,
                    databases["database_silver"],
                    "dq_stg_driver_app_events",
                ]
            ),
            partition_mapping=IdentityPartitionMapping(),
        ),
    ],
)
def fct_driver_activity_eu(context) -> DataFrame:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    start_date = context.asset_partition_key_range.start
    end_date = context.asset_partition_key_range.end
    query = FCT_DRIVER_ACTIVITY_QUERY.format(
        START_DATEID=start_date, END_DATEID=end_date, **databases
    )
    context.log.info(query)
    df = spark.sql(query)
    context.log.info(df)

    return df


@asset(
    name="fct_driver_activity",
    owners=["team:DataEngineering"],
    description=fct_driver_activity_description,
    compute_kind="sql",
    metadata={
        "database": databases["database_gold"],
        "owners": ["team:DataEngineering"],
        "write_mode": WarehouseWriteMode.overwrite,
        "schema": FCT_DRIVER_ACTIVITY_SCHEMA,
        "code_location": get_code_location(),
        "primary_keys": ["date", "driver_id", "org_id"],
        "description": fct_driver_activity_description,
    },
    required_resource_keys=REQUIRED_RESOURCE_KEYS_CA,
    group_name=GROUP_NAME + "_ca",
    partitions_def=FCT_PARTITIONS_DEF_CA,
    backfill_policy=BackfillPolicy.multi_run(max_partitions_per_run=30),
    key_prefix=[AWSRegions.CA_CENTRAL_1.value, databases["database_gold"]],
    deps=[
        AssetDep(
            AssetKey(
                [
                    AWSRegions.CA_CENTRAL_1.value,
                    databases["database_silver"],
                    "dq_stg_driver_app_events",
                ]
            ),
            partition_mapping=IdentityPartitionMapping(),
        ),
    ],
)
def fct_driver_activity_ca(context) -> DataFrame:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    start_date = context.asset_partition_key_range.start
    end_date = context.asset_partition_key_range.end
    query = FCT_DRIVER_ACTIVITY_QUERY.format(
        START_DATEID=start_date, END_DATEID=end_date, **databases
    )
    context.log.info(query)
    df = spark.sql(query)
    context.log.info(df)

    return df


# Primary Key DQ Check
dqs["fct_driver_activity"].append(
    PrimaryKeyDQCheck(
        name="dq_pk_fct_driver_activity",
        table="fct_driver_activity",
        primary_keys=["date", "driver_id", "org_id"],
        blocking=True,
        database=databases["database_gold_dev"],
    )
)

# Empty DQ Check
dqs["fct_driver_activity"].append(
    NonEmptyDQCheck(
        name="dq_empty_fct_driver_activity",
        table="fct_driver_activity",
        blocking=True,
        database=databases["database_gold_dev"],
    )
)

# Non-null DQ Check
dqs["fct_driver_activity"].append(
    NonNullDQCheck(
        name="dq_null_fct_driver_activity",
        database=databases["database_gold_dev"],
        table="fct_driver_activity",
        non_null_columns=["date", "driver_id", "org_id"],
        blocking=True,
    )
)

# Joinability check to dim_organizations
dqs["fct_driver_activity"].append(
    JoinableDQCheck(
        name="dq_joinable_fct_driver_activity_to_dim_organizations",
        database=databases["database_gold_dev"],
        database_2=databases["database_core_gold_dev"],
        input_asset_1="fct_driver_activity",
        input_asset_2="dim_organizations",
        join_keys=[("org_id", "org_id")],
        blocking=True,
        null_right_table_rows_ratio=0.01,
    )
)


dim_drivers_description = build_table_description(
    table_desc="""This table represents up to date metadata for each driver id in the clouddb.drivers table enhanced with organizational metadata and last_mobile_login_date sourced from datastreams.mobile_logs.
    """,
    row_meaning="""One row in this table represents up to date metadata for a driver (sourced from clouddb.drivers), organizational metadata, and driver app activity.""",
    table_type=TableType.DAILY_DIMENSION,
    freshness_slo_updated_by="9am PST",
)


@asset(
    name="dim_drivers",
    owners=["team:DataEngineering"],
    description=dim_drivers_description,
    compute_kind="sql",
    metadata={
        "database": databases["database_gold"],
        "owners": ["team:DataEngineering"],
        "write_mode": WarehouseWriteMode.overwrite,
        "schema": DIM_DRIVERS_SCHEMA,
        "code_location": get_code_location(),
        "primary_keys": ["date", "driver_id"],
        "description": dim_drivers_description,
    },
    required_resource_keys=REQUIRED_RESOURCE_KEYS,
    group_name=GROUP_NAME,
    partitions_def=DIM_PARTITIONS_DEF,
    key_prefix=[AWSRegions.US_WEST_2.value, databases["database_gold"]],
    deps=[
        AssetDep(
            AssetKey(
                [
                    AWSRegions.US_WEST_2.value,
                    databases["database_bronze"],
                    "raw_clouddb_drivers",
                ]
            ),
            partition_mapping=IdentityPartitionMapping(),
        ),
        AssetDep(
            AssetKey(
                [
                    AWSRegions.US_WEST_2.value,
                    databases["database_gold"],
                    "dq_fct_driver_activity",
                ]
            ),
            partition_mapping=IdentityPartitionMapping(),
        ),
        AssetDep(
            AssetKey(
                [
                    AWSRegions.US_WEST_2.value,
                    databases["database_gold"],
                    "dim_drivers",
                ]
            ),
            partition_mapping=TimeWindowPartitionMapping(
                start_offset=-1, end_offset=-1
            ),
        ),
    ],
)
def dim_drivers(context) -> DataFrame:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    partition_date_str = context.partition_key
    if partition_date_str == DIM_PARTITIONS_DEF.start.strftime("%Y-%m-%d"):
        query = BUILD_DIM_DRIVERS_QUERY.format(DATEID=partition_date_str, **databases)
    else:
        query = INSERT_DIM_DRIVERS_QUERY.format(DATEID=partition_date_str, **databases)
    context.log.info(query)
    df = spark.sql(query)
    context.log.info(df)
    return df


@asset(
    name="dim_drivers",
    owners=["team:DataEngineering"],
    description=dim_drivers_description,
    compute_kind="sql",
    metadata={
        "database": databases["database_gold"],
        "owners": ["team:DataEngineering"],
        "write_mode": WarehouseWriteMode.overwrite,
        "schema": DIM_DRIVERS_SCHEMA,
        "code_location": get_code_location(),
        "primary_keys": ["date", "driver_id"],
        "description": dim_drivers_description,
    },
    required_resource_keys=REQUIRED_RESOURCE_KEYS_EU,
    group_name=GROUP_NAME + "_eu",
    partitions_def=DIM_PARTITIONS_DEF,
    key_prefix=[AWSRegions.EU_WEST_1.value, databases["database_gold"]],
    deps=[
        AssetDep(
            AssetKey(
                [
                    AWSRegions.EU_WEST_1.value,
                    databases["database_bronze"],
                    "raw_clouddb_drivers",
                ]
            ),
            partition_mapping=IdentityPartitionMapping(),
        ),
        AssetDep(
            AssetKey(
                [
                    AWSRegions.EU_WEST_1.value,
                    databases["database_gold"],
                    "dq_fct_driver_activity",
                ]
            ),
            partition_mapping=IdentityPartitionMapping(),
        ),
        AssetDep(
            AssetKey(
                [
                    AWSRegions.EU_WEST_1.value,
                    databases["database_gold"],
                    "dim_drivers",
                ]
            ),
            partition_mapping=TimeWindowPartitionMapping(
                start_offset=-1, end_offset=-1
            ),
        ),
    ],
)
def dim_drivers_eu(context) -> DataFrame:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    partition_date_str = context.partition_key
    if partition_date_str == DIM_PARTITIONS_DEF.start.strftime("%Y-%m-%d"):
        query = BUILD_DIM_DRIVERS_QUERY.format(DATEID=partition_date_str, **databases)
    else:
        query = INSERT_DIM_DRIVERS_QUERY.format(DATEID=partition_date_str, **databases)
    context.log.info(query)
    df = spark.sql(query)
    context.log.info(df)
    return df


@asset(
    name="dim_drivers",
    owners=["team:DataEngineering"],
    description=dim_drivers_description,
    compute_kind="sql",
    metadata={
        "database": databases["database_gold"],
        "owners": ["team:DataEngineering"],
        "write_mode": WarehouseWriteMode.overwrite,
        "schema": DIM_DRIVERS_SCHEMA,
        "code_location": get_code_location(),
        "primary_keys": ["date", "driver_id"],
        "description": dim_drivers_description,
    },
    required_resource_keys=REQUIRED_RESOURCE_KEYS_CA,
    group_name=GROUP_NAME + "_ca",
    partitions_def=DIM_PARTITIONS_DEF_CA,
    key_prefix=[AWSRegions.CA_CENTRAL_1.value, databases["database_gold"]],
    deps=[
        AssetDep(
            AssetKey(
                [
                    AWSRegions.CA_CENTRAL_1.value,
                    databases["database_bronze"],
                    "raw_clouddb_drivers",
                ]
            ),
            partition_mapping=IdentityPartitionMapping(),
        ),
        AssetDep(
            AssetKey(
                [
                    AWSRegions.CA_CENTRAL_1.value,
                    databases["database_gold"],
                    "dq_fct_driver_activity",
                ]
            ),
            partition_mapping=IdentityPartitionMapping(),
        ),
        AssetDep(
            AssetKey(
                [
                    AWSRegions.CA_CENTRAL_1.value,
                    databases["database_gold"],
                    "dim_drivers",
                ]
            ),
            partition_mapping=TimeWindowPartitionMapping(
                start_offset=-1, end_offset=-1
            ),
        ),
    ],
)
def dim_drivers_ca(context) -> DataFrame:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    partition_date_str = context.partition_key
    if partition_date_str == DIM_PARTITIONS_DEF_CA.start.strftime("%Y-%m-%d"):
        query = BUILD_DIM_DRIVERS_QUERY.format(DATEID=partition_date_str, **databases)
    else:
        query = INSERT_DIM_DRIVERS_QUERY.format(DATEID=partition_date_str, **databases)
    context.log.info(query)
    df = spark.sql(query)
    context.log.info(df)
    return df


# Primary Key DQ Check
dim_dqs["dim_drivers"].append(
    PrimaryKeyDQCheck(
        name="dq_pk_dim_drivers",
        table="dim_drivers",
        primary_keys=["date", "driver_id"],
        blocking=True,
        database=databases["database_gold_dev"],
    )
)

# Empty DQ Check
dim_dqs["dim_drivers"].append(
    NonEmptyDQCheck(
        name="dq_empty_dim_drivers",
        table="dim_drivers",
        blocking=True,
        database=databases["database_gold_dev"],
    )
)

# Trend based DQ check
dim_dqs["dim_drivers"].append(
    TrendDQCheck(
        name="dq_trend_dim_drivers",
        database=databases["database_gold_dev"],
        table="dim_drivers",
        blocking=False,
        tolerance=0.1,
    )
)

# Non-null DQ Check
dim_dqs["dim_drivers"].append(
    NonNullDQCheck(
        name="dq_null_dim_drivers",
        database=databases["database_gold_dev"],
        table="dim_drivers",
        non_null_columns=["org_id", "driver_id", "group_id"],
        blocking=True,
    )
)

# Joinability check to dim_organizations
dim_dqs["dim_drivers"].append(
    JoinableDQCheck(
        name="dq_joinable_dim_drivers_to_dim_organizations",
        database=databases["database_gold_dev"],
        database_2=databases["database_core_gold_dev"],
        input_asset_1="dim_drivers",
        input_asset_2="dim_organizations",
        join_keys=[("org_id", "org_id")],
        blocking=True,
        null_right_table_rows_ratio=0.01,
    )
)

dq_assets = dqs.generate()
dim_dq_assets = dim_dqs.generate()
