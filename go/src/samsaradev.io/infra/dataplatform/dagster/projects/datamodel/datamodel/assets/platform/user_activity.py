import json
import re
from collections import defaultdict
from datetime import datetime, timedelta

import numpy
import pandas as pd
from dagster import (
    AssetDep,
    AssetKey,
    BackfillPolicy,
    DailyPartitionsDefinition,
    IdentityPartitionMapping,
    LastPartitionMapping,
    Nothing,
    StaticPartitionsDefinition,
    TimeWindowPartitionMapping,
    asset,
)
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import DateType

from ...common.constants import (
    SLACK_ALERTS_CHANNEL_DATA_ENGINEERING,
    org_id_default_description,
)
from ...common.utils import (
    AWSRegions,
    Database,
    DQGroup,
    JoinableDQCheck,
    NonEmptyDQCheck,
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

GROUP_NAME = "user_activity"
REQUIRED_RESOURCE_KEYS = {"databricks_pyspark_step_launcher"}
REQUIRED_RESOURCE_KEYS_EU = {"databricks_pyspark_step_launcher_eu"}
REQUIRED_RESOURCE_KEYS_CA = {"databricks_pyspark_step_launcher_ca"}
SLACK_ALERTS_CHANNEL = SLACK_ALERTS_CHANNEL_DATA_ENGINEERING

RAW_TABLES_PARTITIONS_DEF = DailyPartitionsDefinition(start_date="2024-05-01")
FCT_PARTITIONS_DEF = DailyPartitionsDefinition(start_date="2020-11-01")
FCT_PARTITIONS_DEF_EU = DailyPartitionsDefinition(start_date="2024-05-01")
FCT_PARTITIONS_DEF_CA_BASE = DailyPartitionsDefinition(start_date="2024-05-01")
FCT_PARTITIONS_DEF_CA = adjust_partition_def_for_canada(FCT_PARTITIONS_DEF_CA_BASE)
DIM_PARTITIONS_DEF = DailyPartitionsDefinition(start_date="2024-05-01")
DIM_PARTITIONS_DEF_EU = DailyPartitionsDefinition(start_date="2024-05-01")
DIM_PARTITIONS_DEF_CA_BASE = DailyPartitionsDefinition(start_date="2024-05-01")
DIM_PARTITIONS_DEF_CA = adjust_partition_def_for_canada(DIM_PARTITIONS_DEF_CA_BASE)

dqs = DQGroup(
    group_name=GROUP_NAME,
    partition_def=FCT_PARTITIONS_DEF,
    slack_alerts_channel=SLACK_ALERTS_CHANNEL,
    regions=[AWSRegions.US_WEST_2.value],
)

dqs_eu = DQGroup(
    group_name=GROUP_NAME,
    partition_def=FCT_PARTITIONS_DEF_EU,
    slack_alerts_channel=SLACK_ALERTS_CHANNEL,
    regions=[AWSRegions.EU_WEST_1.value],
)

dqs_ca = DQGroup(
    group_name=GROUP_NAME,
    partition_def=FCT_PARTITIONS_DEF_CA_BASE,
    slack_alerts_channel=SLACK_ALERTS_CHANNEL,
    regions=[AWSRegions.CA_CENTRAL_1.value],
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

RAW_CLOUDDB_USERS_QUERY = """
SELECT
   '{DATEID}' AS date,
    *
FROM clouddb.users
TIMESTAMP AS OF '{DATEID} 23:59:59.999'
"""

RAW_CLOUDDB_USERS_ORGANIZATIONS_QUERY = """
SELECT
   '{DATEID}' AS date,
    *
FROM clouddb.users_organizations
TIMESTAMP AS OF '{DATEID} 23:59:59.999'
"""
RAW_CLOUDDB_CUSTOM_ROLES_QUERY = """
SELECT
   '{DATEID}' AS date,
    *
FROM clouddb.custom_roles
TIMESTAMP AS OF '{DATEID} 23:59:59.999'
"""
STG_CLOUD_ROUTES_QUERY = """
WITH
 user_organizations_pre AS (
  SELECT DISTINCT uo.user_id,
         uo.organization_id AS org_id
  FROM datamodel_platform_bronze.raw_clouddb_users_organizations uo
  WHERE uo.date = (SELECT MAX(date) FROM datamodel_platform_bronze.raw_clouddb_users_organizations)

  UNION ALL

  SELECT DISTINCT uo.user_id,
         uo.organization_id AS org_id
  FROM delta.`s3://samsara-eu-datamodel-warehouse/datamodel_platform_bronze.db/raw_clouddb_users_organizations` uo
  WHERE uo.date = (SELECT MAX(date) FROM delta.`s3://samsara-eu-datamodel-warehouse/datamodel_platform_bronze.db/raw_clouddb_users_organizations`)

  UNION ALL

  SELECT DISTINCT uo.user_id,
         uo.organization_id AS org_id
  FROM data_tools_delta_share_ca.datamodel_platform_bronze.raw_clouddb_users_organizations uo
  WHERE uo.date = (SELECT MAX(date) FROM data_tools_delta_share_ca.datamodel_platform_bronze.raw_clouddb_users_organizations)),
  users_organizations AS (
    SELECT u.email_lower AS email,
           uop.user_id,
           uop.org_id
    FROM datamodel_platform_bronze.raw_clouddb_users u
    INNER JOIN user_organizations_pre uop
      ON uop.user_id = u.id
    WHERE u.date = (SELECT MAX(date) FROM datamodel_platform_bronze.raw_clouddb_users)

    UNION ALL

    SELECT u.email_lower AS email,
           uop.user_id,
           uop.org_id
    FROM delta.`s3://samsara-eu-datamodel-warehouse/datamodel_platform_bronze.db/raw_clouddb_users` u
    INNER JOIN user_organizations_pre uop
      ON uop.user_id = u.id
    WHERE u.date = (SELECT MAX(date) FROM delta.`s3://samsara-eu-datamodel-warehouse/datamodel_platform_bronze.db/raw_clouddb_users`)

    UNION ALL

    SELECT u.email_lower AS email,
           uop.user_id,
           uop.org_id
    FROM data_tools_delta_share_ca.datamodel_platform_bronze.raw_clouddb_users u
    INNER JOIN user_organizations_pre uop
      ON uop.user_id = u.id
    WHERE u.date = (SELECT MAX(date) FROM data_tools_delta_share_ca.datamodel_platform_bronze.raw_clouddb_users)),
 organizations AS (
  SELECT id, internal_type
  FROM datamodel_core_bronze.raw_clouddb_organizations o
  WHERE date = (SELECT MAX(date) FROM datamodel_core_bronze.raw_clouddb_organizations)

  UNION ALL

  SELECT id, internal_type
  FROM delta.`s3://samsara-eu-datamodel-warehouse/datamodel_core_bronze.db/raw_clouddb_organizations` o
  WHERE date = (SELECT MAX(date) FROM delta.`s3://samsara-eu-datamodel-warehouse/datamodel_core_bronze.db/raw_clouddb_organizations`)

  UNION ALL

  SELECT org_id AS id, internal_type
  FROM data_tools_delta_share_ca.datamodel_core.dim_organizations o
  WHERE date = (SELECT MAX(date) FROM data_tools_delta_share_ca.datamodel_core.dim_organizations)),
 base AS (
    SELECT
    DATE(FROM_UNIXTIME(routes.time)) AS date,
    CASE WHEN routes.mp_user_id LIKE '%@%'
         THEN LOWER(routes.mp_user_id)
         WHEN routes.mp_device_id LIKE '%@%'
         THEN LOWER(routes.mp_device_id)
         END AS useremail,
    TRY_CAST(routes.orgid AS BIGINT) AS org_id,
    routes.ip_address,
    routes.mp_lib,
    routes.mp_browser_version,
    routes.utm_medium,
    routes.mp_keyword,
    routes.mp_os,
    routes.mp_initial_referrer,
    routes.mp_current_url,
    CAST(from_unixtime(routes.time,'yyyy-MM-dd HH:mm:ss') AS TIMESTAMP) AS time,
    routes.utm_term,
    routes.distinct_id,
    routes.routename,
    routes.mp_processing_time_ms,
    routes.mp_region,
    'cloud.route' AS mp_event_name,
    routes.mp_lib_version,
    routes.mp_device_id,
    routes.mp_screen_width,
    routes.mp_browser,
    routes.mp_initial_referring_domain,
    routes.mp_country_code,
    routes.mp_screen_height,
    routes.mp_city,
    routes.mp_insert_id,
    routes.mp_search_engine,
    routes.utm_content,
    routes.utm_campaign,
    routes.utm_source,
    CASE WHEN routes.mp_user_id IS NULL AND routes.mp_device_id IS NULL
            THEN FALSE
         WHEN routes.mp_user_id ilike '%samsara.com' OR routes.mp_device_id ilike '%samsara.com'
            THEN FALSE
         ELSE TRUE END is_customer_email,
    CASE WHEN o.internal_type = 0
         THEN TRUE ELSE FALSE END AS is_customer_org,
    CASE WHEN routes.mp_user_id IS NULL AND routes.mp_device_id IS NULL
            THEN FALSE
         WHEN (COALESCE(routes.mp_user_id, '') NOT ilike '%samsara.com'
                AND COALESCE(routes.mp_device_id, '') NOT ilike '%samsara.com')
             AND o.internal_type = 0
        THEN TRUE ELSE FALSE END AS is_customer_event,
    CASE WHEN TRY_CAST(routes.distinct_id AS BIGINT) IS NOT NULL
         THEN CAST(routes.distinct_id AS BIGINT)
         WHEN uo1.user_id IS NOT NULL THEN uo1.user_id
         WHEN uo2.user_id IS NOT NULL THEN uo2.user_id
         ELSE NULL END AS user_id,
    RANK() OVER (PARTITION BY useremail, mp_insert_id ORDER BY rand(10)) AS rank_dedup
    FROM mixpanel_samsara.cloud_route routes
    LEFT OUTER JOIN organizations o
        ON o.id = TRY_CAST(routes.orgid AS BIGINT)
    LEFT OUTER JOIN users_organizations uo1
        ON uo1.email = lower(routes.mp_user_id)
        AND uo1.org_id = TRY_CAST(routes.orgid AS BIGINT)
    LEFT OUTER JOIN users_organizations uo2
        ON uo2.email = lower(routes.mp_device_id)
        AND uo2.org_id = TRY_CAST(routes.orgid AS BIGINT)
    WHERE time BETWEEN TO_UNIX_TIMESTAMP(CONCAT('{START_DATEID}',  ' 00:00:00'))
        AND TO_UNIX_TIMESTAMP(CONCAT('{END_DATEID}',  ' 23:59:59')))

    SELECT * EXCEPT(rank_dedup)
    FROM base
    WHERE rank_dedup = 1
"""

STG_CLOUD_ROUTES_SCHEMA = [
    {
        "name": "date",
        "type": "date",
        "nullable": False,
        "metadata": {
            "comment": "Date partition of table. Sourced from time of data arrival into mixpanel -> cloud_routes.time."
        },
    },
    {
        "name": "useremail",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "The mp_user_id of the user corresponding to the event in mixpanel."
        },
    },
    {
        "name": "org_id",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "The organization id that the user was logged into for the mixpanel event."
        },
    },
    {
        "name": "ip_address",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "The IP address recorded for the mixpanel event. Can be null."
        },
    },
    {
        "name": "mp_lib",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "The Mixpanel library name associated with the event. Always equal to web for cloud dashboard events."
        },
    },
    {
        "name": "mp_browser_version",
        "type": "double",
        "nullable": True,
        "metadata": {
            "comment": "The Mixpanel browser version associated with the event."
        },
    },
    {
        "name": "utm_medium",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "The Mixpanel utm_medium associated with the event."},
    },
    {
        "name": "mp_keyword",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "The Mixpanel search keyword associated with the event"
        },
    },
    {
        "name": "mp_os",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "The user's operating system of the event as determined by mixpanel"
        },
    },
    {
        "name": "mp_initial_referrer",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "The user's referrer to the cloud dashboard as determined by mixpanel."
        },
    },
    {
        "name": "mp_current_url",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "The URL of the page viewed by the user."},
    },
    {
        "name": "time",
        "type": "timestamp",
        "nullable": False,
        "metadata": {"comment": "Timestamp of the mixpanel event creation."},
    },
    {
        "name": "utm_term",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "The utm_term recorded by mixpanel."},
    },
    {
        "name": "distinct_id",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Unique identifier, user_id or email, recorded by mixpanel."
        },
    },
    {
        "name": "routename",
        "type": "string",
        "nullable": False,
        "metadata": {"comment": "Name of samsara route of page view."},
    },
    {
        "name": "mp_processing_time_ms",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment": "Time that Mixpanel processed the event in unix timestamp milliseconds."
        },
    },
    {
        "name": "mp_region",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "Region of user's location recorded by Mixpanel."},
    },
    {
        "name": "mp_event_name",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Name of Mixpanel analytic event. Always cloud.route for the stg_cloud_routes table."
        },
    },
    {
        "name": "mp_lib_version",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Version of Mixpanel library in use for logging event."
        },
    },
    {
        "name": "mp_device_id",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Device ID for the user's web browser assigned by Mixpanel."
        },
    },
    {
        "name": "mp_screen_width",
        "type": "double",
        "nullable": True,
        "metadata": {
            "comment": "Screen width of the user's web browser determined by Mixpanel."
        },
    },
    {
        "name": "mp_browser",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "Name of user's web browser determined by Mixpanel."},
    },
    {
        "name": "mp_initial_referring_domain",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Name of website initially referring user to cloud dashboard."
        },
    },
    {
        "name": "mp_country_code",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Country code of user inferred by Mixpanel, ex: US, GB."
        },
    },
    {
        "name": "mp_screen_height",
        "type": "double",
        "nullable": True,
        "metadata": {"comment": "Height of user's web browser determined by Mixpanel."},
    },
    {
        "name": "mp_city",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "City of user determined by Mixpanel."},
    },
    {
        "name": "mp_insert_id",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Unique ID of analytic event logged by Mixpanel. Primary key for table."
        },
    },
    {
        "name": "mp_search_engine",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Search engine used by user preceding page view determined by Mixpanel."
        },
    },
    {
        "name": "utm_content",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "The utm_content preceding the page view determined by Mixpanel."
        },
    },
    {
        "name": "utm_campaign",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "The utm_campaign preceding the page view determined by Mixpanel."
        },
    },
    {
        "name": "utm_source",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "The utm_source preceding the page view determined by Mixpanel."
        },
    },
    {
        "name": "is_customer_email",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "TRUE if at least one of mp_user_id or mp_device_id is non-null and neither contains samsara.com; FALSE when both are null or when either contains samsara.com."
        },
    },
    {
        "name": "is_customer_org",
        "type": "boolean",
        "nullable": False,
        "metadata": {"comment": "TRUE if organization has internal_type = 0."},
    },
    {
        "name": "is_customer_event",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "TRUE if both is_customer_org and is_customer_email are TRUE (so FALSE when both mp_user_id and mp_device_id are null)."
        },
    },
    {
        "name": "user_id",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "Samsara user ID inferred from combination of useremail and organization. NULL if user was deleted from"
        },
    },
]

STG_USER_LOGIN_EVENTS_QUERY = """
    SELECT  DATE(date) AS date,
            user_id,
            org_id,
            LOWER(REGEXP_EXTRACT(summary, r"(\\b[^@\\s]+\\@[^@\\s]+\\b)",1)) AS email,
            count(*) AS login_count
    FROM auditsdb_shards.audits a
    WHERE a.audit_type_id IN (1, 3, 72, 132)
    --audit type 1 is login through email and password
    --audit type 3 is google sign in
    --audit type 72 is single sign on
    --audit type 132 is MFA

    AND date BETWEEN '{START_DATEID}' AND '{END_DATEID}'
    GROUP BY 1,2,3,4
"""

STG_USER_LOGIN_EVENTS_SCHEMA = [
    {
        "name": "date",
        "type": "date",
        "nullable": False,
        "metadata": {
            "comment": "Date of partition sourced from auditdb_shards.audits."
        },
    },
    {
        "name": "user_id",
        "type": "long",
        "nullable": False,
        "metadata": {"comment": "User_id of login event."},
    },
    {
        "name": "email",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "Email of login event."},
    },
    {
        "name": "org_id",
        "type": "long",
        "nullable": False,
        "metadata": {"comment": org_id_default_description},
    },
    {
        "name": "login_count",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Count of login events per date partition, sourced from auditsdb_shards.audits filtered to audit_type_id=1"
        },
    },
]

STG_FLEET_APP_EVENTS_QUERY = """
    SELECT date,

        user_id,
        org_id,
        event_type,
        COUNT(*) AS event_count
    FROM datastreams_history.mobile_logs
    WHERE user_id IS NOT NULL
    AND user_id != -1
    AND date BETWEEN '{START_DATEID}' AND '{END_DATEID}'
    GROUP BY 1,2,3,4
"""

STG_FLEET_APP_EVENTS_SCHEMA = [
    {
        "name": "date",
        "type": "date",
        "nullable": False,
        "metadata": {
            "comment": "Date of partition sourced from datastreams.mobile_logs"
        },
    },
    {
        "name": "user_id",
        "type": "long",
        "nullable": False,
        "metadata": {"comment": "User_id of event. Driver_id's are in separate table."},
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
            "comment": "Count of events per user_id, org_id, event_type, and date partition"
        },
    },
]

FCT_USER_ACTIVITY_QUERY = """
(SELECT date,
       user_id,
       org_id,
       event_type,
       event_count,
       'fleet_app' AS source
FROM datamodel_platform_silver.stg_fleet_app_events
WHERE date BETWEEN '{START_DATE}' AND '{END_DATE}'
AND user_id IS NOT NULL)
UNION ALL
(SELECT date,
       user_id,
       org_id,
       email AS event_type,
       login_count AS event_count,
       'login' AS source
FROM datamodel_platform_silver.stg_user_login_events
WHERE date BETWEEN '{START_DATE}' AND '{END_DATE}'
AND user_id IS NOT NULL)
UNION ALL
(SELECT scr.date,
       scr.user_id,
       scr.org_id,
       scr.routename AS event_type,
       COUNT(*) AS event_count,
       'web' AS source
FROM datamodel_platform_silver.stg_cloud_routes scr
JOIN datamodel_core.dim_organizations o
ON scr.date = o.date
AND scr.org_id = o.org_id
WHERE scr.date BETWEEN '{START_DATE}' AND '{END_DATE}'
AND scr.user_id IS NOT NULL
GROUP BY 1,2,3,4)"""

FCT_USER_ACTIVITY_EU_QUERY = """
(SELECT date,
       user_id,
       org_id,
       event_type,
       event_count,
       'fleet_app' AS source
FROM datamodel_platform_silver.stg_fleet_app_events
WHERE date BETWEEN '{START_DATE}' AND '{END_DATE}'
AND user_id IS NOT NULL)
UNION ALL
(SELECT date,
       user_id,
       org_id,
       email AS event_type,
       login_count AS event_count,
       'login' AS source
FROM datamodel_platform_silver.stg_user_login_events
WHERE date BETWEEN '{START_DATE}' AND '{END_DATE}'
AND user_id IS NOT NULL)
UNION ALL
(SELECT scr.date,
       scr.user_id,
       scr.org_id,
       scr.routename AS event_type,
       COUNT(*) AS event_count,
       'web' AS source
FROM mixpanel_delta_share.datamodel_platform_silver.stg_cloud_routes scr
JOIN datamodel_core.dim_organizations o
ON scr.date = o.date
AND scr.org_id = o.org_id
WHERE scr.date BETWEEN '{START_DATE}' AND '{END_DATE}'
AND scr.user_id IS NOT NULL
GROUP BY 1,2,3,4)"""

FCT_USER_ACTIVITY_SCHEMA = [
    {
        "name": "date",
        "type": "date",
        "nullable": False,
        "metadata": {"comment": "Date of partition."},
    },
    {
        "name": "user_id",
        "type": "long",
        "nullable": False,
        "metadata": {"comment": "User_id of event"},
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
        "metadata": {
            "comment": "Type of event sourced from mobile/web. In the case of a login event, we use email here to deduplicate."
        },
    },
    {
        "name": "event_count",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Count of events per user_id, org_id, event_type, and date partition."
        },
    },
    {
        "name": "source",
        "type": "string",
        "nullable": False,
        "metadata": {"comment": "Source of event, one of 'web', 'fleet_app', 'login'"},
    },
]

BUILD_DIM_USERS_ORGANIZATIONS_QUERY = """
    WITH
    raw_users_organizations AS (
    SELECT user_id,
            organization_id AS org_id,
            ARRAY_AGG(DISTINCT role_id) AS role_ids,
            ARRAY_AGG(DISTINCT custom_role_uuid) AS custom_role_uuids
    FROM datamodel_platform_bronze.raw_clouddb_users_organizations
    WHERE date = '{DATEID}'
    GROUP BY 1,2),
    users_organizations AS (
    SELECT raw_users_organizations.user_id,
            raw_users_organizations.org_id,
            organizations.internal_type,
            organizations.sam_number,
            organizations.org_name,
            organizations.account_name,
            raw_users_organizations.role_ids,
            raw_users_organizations.custom_role_uuids
    FROM raw_users_organizations
    LEFT OUTER JOIN datamodel_core.dim_organizations organizations
        ON organizations.org_id = raw_users_organizations.org_id
        AND organizations.date = (SELECT MAX(DATE) FROM datamodel_core.dim_organizations)),
    activity AS (
    SELECT user_id,
            org_id,
            MIN(CASE WHEN source = 'fleet_app' THEN date ELSE NULL END) AS first_fleet_app_usage_date,
            MIN(CASE WHEN source = 'web' THEN date ELSE NULL END) AS first_web_usage_date,
            MIN(CASE WHEN source = 'login' THEN date ELSE NULL END) AS first_login_date,
            MAX(CASE WHEN source = 'fleet_app' THEN date ELSE NULL END) AS last_fleet_app_usage_date,
            MAX(CASE WHEN source = 'web' THEN date ELSE NULL END) AS last_web_usage_date,
            MAX(CASE WHEN source = 'login' THEN date ELSE NULL END) AS last_login_date
    FROM datamodel_platform.fct_user_activity
    WHERE date <= '{DATEID}'
    GROUP BY 1,2),
    activity_30d AS (
    SELECT user_id,
            org_id,
            SUM(event_count) AS login_count_30d
    FROM datamodel_platform.fct_user_activity
    WHERE date BETWEEN DATE_ADD('{DATEID}', -29) AND DATE('{DATEID}')
    and source = 'login'
    GROUP BY 1,2)

    SELECT
    DATE('{DATEID}') AS date,
    users.id AS user_id,
    users_organizations.org_id,
    users.email_lower AS email,
    CASE WHEN users.email ilike '%@samsara.com' THEN TRUE ELSE FALSE END AS is_samsara_email,
    users.name,
    users.created_at,
    users.updated_at,
    users.picture_url,
    users.google_oauth2,
    users.salesforce_contact_id,
    users.is_auth0,
    users.language_override,
    users.service_account_id,
    users.unit_system_override,
    users.timezone_override,
    users.phone,
    users.use_whatsapp,
    users.whatsapp_confirmation_sent,
    users.can_receive_sms,
    users_organizations.* EXCEPT (user_id, org_id),
    activity.first_fleet_app_usage_date AS first_fleet_app_usage_date,
    activity.first_fleet_app_usage_date AS last_fleet_app_usage_date,
    activity.first_web_usage_date AS first_web_usage_date,
    activity.last_web_usage_date AS last_web_usage_date,
    activity.last_login_date AS first_web_login_date,
    activity.last_login_date AS last_web_login_date,
    activity_30d.login_count_30d
    FROM datamodel_platform_bronze.raw_clouddb_users users
    LEFT OUTER JOIN users_organizations
    ON users_organizations.user_id = users.id
    LEFT OUTER JOIN activity
    ON activity.user_id = users.id
    AND activity.org_id = users_organizations.org_id
    LEFT OUTER JOIN activity_30d
    ON activity_30d.user_id = users.id
    AND activity_30d.org_id = users_organizations.org_id
    WHERE users.date = '{DATEID}'
    """

INSERT_DIM_USERS_ORGANIZATIONS_QUERY = """
     WITH
 raw_users_organizations AS (
   SELECT user_id,
          organization_id AS org_id,
          ARRAY_AGG(DISTINCT role_id) AS role_ids,
          ARRAY_AGG(DISTINCT custom_role_uuid) AS custom_role_uuids
   FROM datamodel_platform_bronze.raw_clouddb_users_organizations
   WHERE date = '{DATEID}'
   GROUP BY 1,2),
 users_organizations AS (
  SELECT raw_users_organizations.user_id,
         raw_users_organizations.org_id,
         organizations.internal_type,
         organizations.sam_number,
         organizations.org_name,
         organizations.account_name,
         raw_users_organizations.role_ids,
         raw_users_organizations.custom_role_uuids
  FROM raw_users_organizations
  LEFT OUTER JOIN datamodel_core.dim_organizations organizations
    ON organizations.org_id = raw_users_organizations.org_id
    AND organizations.date = (SELECT MAX(DATE) FROM datamodel_core.dim_organizations)),
  activity AS (
  SELECT user_id,
        org_id,
        MIN(CASE WHEN source = 'fleet_app' THEN date ELSE NULL END) AS first_fleet_app_usage_date,
        MAX(CASE WHEN source = 'fleet_app' THEN date ELSE NULL END) AS last_fleet_app_usage_date,
        MIN(CASE WHEN source = 'web' THEN date ELSE NULL END) AS first_web_usage_date,
        MAX(CASE WHEN source = 'web' THEN date ELSE NULL END) AS last_web_usage_date,
        MIN(CASE WHEN source = 'login' THEN date ELSE NULL END) AS first_login_date,
        MAX(CASE WHEN source = 'login' THEN date ELSE NULL END) AS last_login_date
  FROM datamodel_platform.fct_user_activity
  WHERE date = '{DATEID}'
  GROUP BY 1,2),
  activity_30d AS (
  SELECT user_id,
        org_id,
        SUM(event_count) AS login_count_30d
  FROM datamodel_platform.fct_user_activity
  WHERE date BETWEEN DATE_ADD('{DATEID}', -29) AND DATE('{DATEID}')
  and source = 'login'
  GROUP BY 1,2),
  prev AS (
  SELECT *
  FROM {database_gold}.dim_users_organizations
  WHERE date = DATE_ADD('{DATEID}', -1)),
  curr AS (
  SELECT users.* ,
         uo.* EXCEPT (user_id)
  FROM {database_bronze}.raw_clouddb_users users
  LEFT OUTER JOIN users_organizations uo
    ON uo.user_id = users.id
  WHERE users.date = '{DATEID}')

SELECT
  DATE('{DATEID}') AS date,
  COALESCE(curr.id, prev.user_id) AS user_id,
  COALESCE(curr.org_id, prev.org_id) AS org_id,
  COALESCE(curr.email_lower, prev.email) AS email,
  COALESCE(CASE WHEN curr.email ilike '%@samsara.com' THEN TRUE ELSE FALSE END, prev.is_samsara_email) AS is_samsara_email,
  COALESCE(curr.name, prev.name) AS name,
  COALESCE(curr.created_at, prev.created_at) AS created_at,
  COALESCE(curr.updated_at, prev.updated_at) AS updated_at,
  COALESCE(curr.picture_url, prev.picture_url) AS picture_url,
  COALESCE(curr.google_oauth2, prev.google_oauth2) AS google_oauth2,
  COALESCE(curr.salesforce_contact_id, prev.salesforce_contact_id) AS salesforce_contact_id,
  COALESCE(curr.is_auth0, prev.is_auth0) AS is_auth0,
  COALESCE(curr.language_override, prev.language_override) AS language_override,
  COALESCE(curr.service_account_id, prev.service_account_id) AS service_account_id,
  COALESCE(curr.unit_system_override, prev.unit_system_override) AS unit_system_override,
  COALESCE(curr.timezone_override, prev.timezone_override) AS timezone_override,
  COALESCE(curr.phone, prev.phone) AS phone,
  COALESCE(curr.use_whatsapp, prev.use_whatsapp) AS use_whatsapp,
  COALESCE(curr.whatsapp_confirmation_sent, prev.whatsapp_confirmation_sent) AS whatsapp_confirmation_sent,
  COALESCE(curr.can_receive_sms, prev.can_receive_sms) AS can_receive_sms,
  COALESCE(users_organizations.internal_type, prev.internal_type) AS internal_type,
  COALESCE(users_organizations.sam_number, prev.sam_number) AS sam_number,
  COALESCE(users_organizations.org_name, prev.org_name) AS org_name,
  COALESCE(users_organizations.account_name, prev.account_name) AS account_name,
  COALESCE(users_organizations.role_ids, prev.role_ids) AS role_ids,
  COALESCE(users_organizations.custom_role_uuids, prev.custom_role_uuids) AS custom_role_uuids,
  COALESCE(prev.first_fleet_app_usage_date, activity.first_fleet_app_usage_date) AS first_fleet_app_usage_date,
  COALESCE(activity.last_fleet_app_usage_date, prev.last_fleet_app_usage_date) AS last_fleet_app_usage_date,
  COALESCE(prev.first_web_usage_date, activity.first_web_usage_date) AS first_web_usage_date,
  COALESCE(activity.last_web_usage_date, prev.last_web_usage_date) AS last_web_usage_date,
  COALESCE(prev.first_web_login_date, activity.first_login_date) AS first_web_login_date,
  COALESCE(activity.last_login_date, prev.last_web_login_date) AS last_web_login_date,
  COALESCE(activity_30d.login_count_30d, prev.login_count_30d) AS login_count_30d
FROM prev
FULL OUTER JOIN curr
 ON curr.id = prev.user_id
 AND COALESCE(curr.org_id, -1) = COALESCE(prev.org_id, -1)
LEFT OUTER JOIN users_organizations
  ON users_organizations.user_id = prev.user_id
  AND users_organizations.org_id = prev.org_id
LEFT OUTER JOIN activity
  ON activity.user_id = COALESCE(curr.id, prev.user_id)
  AND activity.org_id = COALESCE(users_organizations.org_id, prev.org_id)
LEFT OUTER JOIN activity_30d
  ON activity_30d.user_id = COALESCE(curr.id, prev.user_id)
  AND activity_30d.org_id = COALESCE(users_organizations.org_id, prev.org_id)
"""

DIM_USERS_ORGANIZATIONS_SCHEMA = [
    {
        "name": "date",
        "type": "date",
        "nullable": False,
        "metadata": {"comment": "Date of partition."},
    },
    {
        "name": "user_id",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Unique id of user. Becomes unique row of table per date with org_id."
        },
    },
    {
        "name": "org_id",
        "type": "long",
        "nullable": False,
        "metadata": {"comment": org_id_default_description},
    },
    {
        "name": "email",
        "type": "string",
        "nullable": False,
        "metadata": {"comment": "Lowered email address of user from clouddb.users."},
    },
    {
        "name": "is_samsara_email",
        "type": "boolean",
        "nullable": False,
        "metadata": {"comment": "True if email ends with samsara.com, else false."},
    },
    {
        "name": "name",
        "type": "string",
        "nullable": False,
        "metadata": {"comment": "Name of user (imported from clouddb.users)"},
    },
    {
        "name": "created_at",
        "type": "timestamp",
        "nullable": False,
        "metadata": {
            "comment": "Timestamp of when user was created (imported from clouddb.users)"
        },
    },
    {
        "name": "updated_at",
        "type": "timestamp",
        "nullable": False,
        "metadata": {
            "comment": "Timestamp of when user was last updated (imported from clouddb.users)"
        },
    },
    {
        "name": "picture_url",
        "type": "string",
        "nullable": False,
        "metadata": {"comment": "URL of user's picture (imported from clouddb.users)"},
    },
    {
        "name": "google_oauth2",
        "type": "byte",
        "nullable": False,
        "metadata": {
            "comment": "Flag (1=True or 0=False) of whether user is a google oauth2 user (imported from clouddb.users)"
        },
    },
    {
        "name": "salesforce_contact_id",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Salesforce contact ID of user (imported from clouddb.users)"
        },
    },
    {
        "name": "is_auth0",
        "type": "byte",
        "nullable": False,
        "metadata": {
            "comment": "Flag (1=True or 0=False) of whether user is an auth0 user (imported from clouddb.users)"
        },
    },
    {
        "name": "language_override",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Language override of user (imported from clouddb.users)"
        },
    },
    {
        "name": "service_account_id",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Service account ID of user (imported from clouddb.users)"
        },
    },
    {
        "name": "unit_system_override",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Unit system override of user (imported from clouddb.users)"
        },
    },
    {
        "name": "timezone_override",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Timezone override of user (imported from clouddb.users)"
        },
    },
    {
        "name": "phone",
        "type": "string",
        "nullable": False,
        "metadata": {"comment": "Phone number of user (imported from clouddb.users)"},
    },
    {
        "name": "use_whatsapp",
        "type": "byte",
        "nullable": False,
        "metadata": {
            "comment": "Flag (1=True or 0=False) of whether user uses whatsapp (imported from clouddb.users)"
        },
    },
    {
        "name": "whatsapp_confirmation_sent",
        "type": "byte",
        "nullable": False,
        "metadata": {
            "comment": "Flag (1=True or 0=False) of whether whatsapp confirmation was sent to user (imported from clouddb.users)"
        },
    },
    {
        "name": "can_receive_sms",
        "type": "byte",
        "nullable": False,
        "metadata": {
            "comment": "Flag (1=True or 0=False) of whether user can receive sms (imported from clouddb.users)"
        },
    },
    {
        "name": "internal_type",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Internal type of org_id from datamodel_core.dim_organizations."
        },
    },
    {
        "name": "sam_number",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Samsara account of org_id from datamodel_core.dim_organizations."
        },
    },
    {
        "name": "org_name",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Name of organization from datamodel_core.dim_organizations."
        },
    },
    {
        "name": "account_name",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Name of organization's account from datamodel_core.dim_organizations."
        },
    },
    {
        "name": "role_ids",
        "type": {"type": "array", "elementType": "long", "containsNull": True},
        "nullable": False,
        "metadata": {
            "comment": "IDs of samsara created roles for user and organization from clouddb.users_organizations.role_id."
        },
    },
    {
        "name": "custom_role_uuids",
        "type": {"type": "array", "elementType": "string", "containsNull": True},
        "nullable": False,
        "metadata": {
            "comment": "UUIDs of custom defined roles for user and organization from clouddb.users_organizations.custom_role_uuid."
        },
    },
    {
        "name": "first_fleet_app_usage_date",
        "type": "date",
        "nullable": False,
        "metadata": {
            "comment": "First date of fleet app usage as of date since 2018-01-01 from datastreams_history.mobile_logs for this user_id and org_id."
        },
    },
    {
        "name": "last_fleet_app_usage_date",
        "type": "date",
        "nullable": False,
        "metadata": {
            "comment": "Last date of fleet_app_usage as of date since 2018-01-01 from datastreams_history.mobile_logs for this user_id and org_id."
        },
    },
    {
        "name": "first_web_usage_date",
        "type": "date",
        "nullable": False,
        "metadata": {
            "comment": "First date of web app usage as of date since 2018-01-01 from mixpanel.cloud_route for this user_id and org_id."
        },
    },
    {
        "name": "last_web_usage_date",
        "type": "date",
        "nullable": False,
        "metadata": {
            "comment": "Last date of web app usage as of date since 2018-01-01 from mixpanel.cloud_route for this user_id and org_id."
        },
    },
    {
        "name": "first_web_login_date",
        "type": "date",
        "nullable": False,
        "metadata": {
            "comment": "First date of web login since 2018-01-01 as of date from auditsdb_shards.audits filtered to audit_type_id = 1 for this user_id and org_id."
        },
    },
    {
        "name": "last_web_login_date",
        "type": "date",
        "nullable": False,
        "metadata": {
            "comment": "Most recent date of web login since 2018-01-01 as of date from auditsdb_shards.audits filtered audit_type_id = 1 for this user_id and org_id."
        },
    },
    {
        "name": "login_count_30d",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Count of logins in auditsdb_shards.audits.audit_type_id = 1 in past 30 days for this user_id and org_id."
        },
    },
]

DIM_USERS_QUERY = """
        WITH agg AS (
            SELECT
                uo.user_id,
                MIN(uo.first_fleet_app_usage_date) AS first_fleet_app_usage_date,
                MAX(uo.last_fleet_app_usage_date) AS last_fleet_app_usage_date,
                MIN(uo.first_web_usage_date) AS first_web_usage_date,
                MAX(uo.last_web_usage_date) AS last_web_usage_date,
                MIN(uo.first_web_login_date) AS first_web_login_date,
                MAX(uo.last_web_login_date) AS last_web_login_date,
                COUNT(DISTINCT CASE WHEN uo.login_count_30d > 0 THEN uo.org_id ELSE NULL END) AS count_orgs_30d_logins,
                ARRAY_AGG(DISTINCT CASE WHEN uo.login_count_30d > 0 THEN uo.org_id ELSE NULL END) AS org_ids_30d_logins,
                ARRAY_AGG(DISTINCT uo.org_id) AS org_ids,
                ARRAY_AGG(DISTINCT uo.sam_number) AS sam_numbers
            FROM datamodel_platform.dim_users_organizations uo
            WHERE date = '{DATEID}'
            GROUP BY uo.user_id
        ),
        orgs_ranked AS (
            SELECT
                uo.date,
                uo.user_id,
                uo.org_id,
                ROW_NUMBER() OVER (PARTITION BY uo.user_id ORDER BY uo.org_id) AS org_rank,
                uo.email,
                uo.is_samsara_email,
                uo.name,
                uo.created_at,
                uo.updated_at,
                uo.picture_url,
                uo.google_oauth2,
                uo.salesforce_contact_id,
                uo.is_auth0,
                uo.language_override,
                uo.service_account_id,
                uo.unit_system_override,
                uo.timezone_override,
                uo.phone,
                uo.use_whatsapp,
                uo.whatsapp_confirmation_sent,
                uo.can_receive_sms
            FROM datamodel_platform.dim_users_organizations uo
            WHERE date = '{DATEID}'
        )
        SELECT
            orgs_ranked.date,
            orgs_ranked.user_id,
            orgs_ranked.email,
            orgs_ranked.is_samsara_email,
            orgs_ranked.name,
            orgs_ranked.created_at,
            orgs_ranked.updated_at,
            orgs_ranked.picture_url,
            orgs_ranked.google_oauth2,
            orgs_ranked.salesforce_contact_id,
            orgs_ranked.is_auth0,
            orgs_ranked.language_override,
            orgs_ranked.service_account_id,
            orgs_ranked.unit_system_override,
            orgs_ranked.timezone_override,
            orgs_ranked.phone,
            orgs_ranked.use_whatsapp,
            orgs_ranked.whatsapp_confirmation_sent,
            orgs_ranked.can_receive_sms,
            agg.first_fleet_app_usage_date,
            agg.last_fleet_app_usage_date,
            agg.first_web_usage_date,
            agg.last_web_usage_date,
            agg.first_web_login_date,
            agg.last_web_login_date,
            agg.count_orgs_30d_logins,
            agg.org_ids_30d_logins,
            agg.org_ids,
            agg.sam_numbers
        FROM orgs_ranked
        INNER JOIN agg ON orgs_ranked.user_id = agg.user_id
        WHERE orgs_ranked.org_rank = 1
"""

DIM_USERS_SCHEMA = [
    {
        "name": "date",
        "type": "date",
        "nullable": False,
        "metadata": {"comment": "Date of partition."},
    },
    {
        "name": "user_id",
        "type": "long",
        "nullable": False,
        "metadata": {"comment": "Unique ID of user and row of table per date."},
    },
    {
        "name": "email",
        "type": "string",
        "nullable": False,
        "metadata": {"comment": "Lowered email address of user."},
    },
    {
        "name": "is_samsara_email",
        "type": "boolean",
        "nullable": False,
        "metadata": {"comment": "True if email ends with samsara.com, else false."},
    },
    {
        "name": "name",
        "type": "string",
        "nullable": False,
        "metadata": {"comment": "Name of user (imported from clouddb.users)"},
    },
    {
        "name": "created_at",
        "type": "timestamp",
        "nullable": False,
        "metadata": {
            "comment": "Timestamp of when user was created (imported from clouddb.users)"
        },
    },
    {
        "name": "updated_at",
        "type": "timestamp",
        "nullable": False,
        "metadata": {
            "comment": "Timestamp of when user was last updated (imported from clouddb.users)"
        },
    },
    {
        "name": "picture_url",
        "type": "string",
        "nullable": False,
        "metadata": {"comment": "URL of user's picture (imported from clouddb.users)"},
    },
    {
        "name": "google_oauth2",
        "type": "byte",
        "nullable": False,
        "metadata": {
            "comment": "Flag (1=True or 0=False) of whether user is a google oauth2 user (imported from clouddb.users)"
        },
    },
    {
        "name": "salesforce_contact_id",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Salesforce contact ID of user (imported from clouddb.users)"
        },
    },
    {
        "name": "is_auth0",
        "type": "byte",
        "nullable": False,
        "metadata": {
            "comment": "Flag (1=True or 0=False) of whether user is an auth0 user (imported from clouddb.users)"
        },
    },
    {
        "name": "language_override",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Language override of user (imported from clouddb.users)"
        },
    },
    {
        "name": "service_account_id",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Service account ID of user (imported from clouddb.users)"
        },
    },
    {
        "name": "unit_system_override",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Unit system override of user (imported from clouddb.users)"
        },
    },
    {
        "name": "timezone_override",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Timezone override of user (imported from clouddb.users)"
        },
    },
    {
        "name": "phone",
        "type": "string",
        "nullable": False,
        "metadata": {"comment": "Phone number of user (imported from clouddb.users)"},
    },
    {
        "name": "use_whatsapp",
        "type": "byte",
        "nullable": False,
        "metadata": {
            "comment": "Flag (1=True or 0=False) of whether user uses whatsapp (imported from clouddb.users)"
        },
    },
    {
        "name": "whatsapp_confirmation_sent",
        "type": "byte",
        "nullable": False,
        "metadata": {
            "comment": "Flag (1=True or 0=False) of whether whatsapp confirmation was sent to user (imported from clouddb.users)"
        },
    },
    {
        "name": "can_receive_sms",
        "type": "byte",
        "nullable": False,
        "metadata": {
            "comment": "Flag (1=True or 0=False) of whether user can receive sms (imported from clouddb.users)"
        },
    },
    {
        "name": "first_fleet_app_usage_date",
        "type": "date",
        "nullable": False,
        "metadata": {
            "comment": "First date of fleet app usage as of date since 2018-01-01 from datastreams_history.mobile_logs for this user_id across all related org_ids."
        },
    },
    {
        "name": "last_fleet_app_usage_date",
        "type": "date",
        "nullable": False,
        "metadata": {
            "comment": "Most recent date of fleet app usage as of date since 2018-01-01 from datastreams_history.mobile_logs for this user_id across all related org_ids."
        },
    },
    {
        "name": "first_web_usage_date",
        "type": "date",
        "nullable": False,
        "metadata": {
            "comment": "First date of web usage as of date since 2018-01-01 from mixpanel.cloud_route for this user_id across all org_ids."
        },
    },
    {
        "name": "last_web_usage_date",
        "type": "date",
        "nullable": False,
        "metadata": {
            "comment": "Most recent date of web usage as of date since 2018-01-01 from mixpanel.cloud_route for this user_id across all org_ids."
        },
    },
    {
        "name": "first_web_login_date",
        "type": "date",
        "nullable": False,
        "metadata": {
            "comment": "First date of web login since 2018-01-01 from auditsdb_shards.auditss filtered to audit_type_id = 1 for this user_id across all org_ids."
        },
    },
    {
        "name": "last_web_login_date",
        "type": "date",
        "nullable": False,
        "metadata": {
            "comment": "Most recent date of web login as of date since 2018-01-01 from auditsdb_shards.auditss filtered to audit_type_id = 1 for this user_id across all org_ids."
        },
    },
    {
        "name": "count_orgs_30d_logins",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Count of distinct organizations where the user_id had a login event (auditsdb_shards.audits.audit_type_id = 1) in the past 30 days."
        },
    },
    {
        "name": "org_ids_30d_logins",
        "type": {"type": "array", "elementType": "long", "containsNull": True},
        "nullable": False,
        "metadata": {
            "comment": "Array of organizations where the user_id had a login event (auditsdb_shards.audits.audit_type_id = 1) in the past 30 days."
        },
    },
    {
        "name": "org_ids",
        "type": {"type": "array", "elementType": "long", "containsNull": True},
        "nullable": False,
        "metadata": {
            "comment": "Array of organizations for user from clouddb.users_organizations."
        },
    },
    {
        "name": "sam_numbers",
        "type": {"type": "array", "elementType": "string", "containsNull": True},
        "nullable": False,
        "metadata": {
            "comment": "Array of samsara account numbers for user_id's organizations from datamodel_core.dim_organizations"
        },
    },
]


@asset(
    name="raw_clouddb_users",
    owners=["team:DataEngineering"],
    compute_kind="sql",
    metadata={
        "database": databases["database_bronze"],
        "owners": ["team:DataEngineering"],
        "write_mode": WarehouseWriteMode.overwrite,
        "schema": [],
        "code_location": get_code_location(),
    },
    required_resource_keys=REQUIRED_RESOURCE_KEYS,
    group_name=GROUP_NAME,
    partitions_def=RAW_TABLES_PARTITIONS_DEF,
    key_prefix=[AWSRegions.US_WEST_2.value, databases["database_bronze"]],
)
def raw_clouddb_users(context) -> DataFrame:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    query = RAW_CLOUDDB_USERS_QUERY.format(DATEID=context.partition_key, **databases)
    context.log.info(query)
    df = spark.sql(query)
    context.log.info(df)
    return df


@asset(
    name="raw_clouddb_users",
    owners=["team:DataEngineering"],
    compute_kind="sql",
    metadata={
        "database": databases["database_bronze"],
        "owners": ["team:DataEngineering"],
        "write_mode": WarehouseWriteMode.overwrite,
        "schema": [],
        "code_location": get_code_location(),
    },
    required_resource_keys=REQUIRED_RESOURCE_KEYS_EU,
    group_name=GROUP_NAME,
    partitions_def=RAW_TABLES_PARTITIONS_DEF,
    key_prefix=[AWSRegions.EU_WEST_1.value, databases["database_bronze"]],
)
def raw_clouddb_users_eu(context) -> DataFrame:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    query = RAW_CLOUDDB_USERS_QUERY.format(DATEID=context.partition_key, **databases)
    context.log.info(query)
    df = spark.sql(query)
    context.log.info(df)
    return df


@asset(
    name="raw_clouddb_users_organizations",
    owners=["team:DataEngineering"],
    description="Daily snapshot of clouddb.users_organizations",
    compute_kind="sql",
    metadata={
        "database": databases["database_bronze"],
        "owners": ["team:DataEngineering"],
        "write_mode": WarehouseWriteMode.overwrite,
        "schema": [],
        "code_location": get_code_location(),
        "description": "Daily snapshot of clouddb.users_organizations",
    },
    required_resource_keys=REQUIRED_RESOURCE_KEYS,
    group_name=GROUP_NAME,
    partitions_def=RAW_TABLES_PARTITIONS_DEF,
    key_prefix=[AWSRegions.US_WEST_2.value, databases["database_bronze"]],
)
def raw_clouddb_users_organizations(context) -> DataFrame:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    query = RAW_CLOUDDB_USERS_ORGANIZATIONS_QUERY.format(
        DATEID=context.partition_key, **databases
    )
    context.log.info(query)
    df = spark.sql(query)
    context.log.info(df)
    return df


@asset(
    name="raw_clouddb_users_organizations",
    owners=["team:DataEngineering"],
    description="Daily snapshot of clouddb.users_organizations",
    compute_kind="sql",
    metadata={
        "database": databases["database_bronze"],
        "owners": ["team:DataEngineering"],
        "write_mode": WarehouseWriteMode.overwrite,
        "schema": [],
        "code_location": get_code_location(),
        "description": "Daily snapshot of clouddb.users_organizations",
    },
    required_resource_keys=REQUIRED_RESOURCE_KEYS_EU,
    group_name=GROUP_NAME,
    partitions_def=RAW_TABLES_PARTITIONS_DEF,
    key_prefix=[AWSRegions.EU_WEST_1.value, databases["database_bronze"]],
)
def raw_clouddb_users_organizations_eu(context) -> DataFrame:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    query = RAW_CLOUDDB_USERS_ORGANIZATIONS_QUERY.format(
        DATEID=context.partition_key, **databases
    )
    context.log.info(query)
    df = spark.sql(query)
    context.log.info(df)
    return df


@asset(
    name="raw_clouddb_custom_roles",
    owners=["team:DataEngineering"],
    description="Daily snapshot of clouddb.custom_roles",
    compute_kind="sql",
    metadata={
        "database": databases["database_bronze"],
        "owners": ["team:DataEngineering"],
        "write_mode": WarehouseWriteMode.overwrite,
        "schema": [],
        "code_location": get_code_location(),
        "description": "Daily snapshot of clouddb.custom_roles",
    },
    required_resource_keys=REQUIRED_RESOURCE_KEYS,
    group_name=GROUP_NAME,
    partitions_def=RAW_TABLES_PARTITIONS_DEF,
    key_prefix=[AWSRegions.US_WEST_2.value, databases["database_bronze"]],
)
def raw_clouddb_custom_roles(context) -> DataFrame:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    query = RAW_CLOUDDB_CUSTOM_ROLES_QUERY.format(
        DATEID=context.partition_key, **databases
    )
    context.log.info(query)
    df = spark.sql(query)
    context.log.info(df)
    return df


@asset(
    name="raw_clouddb_custom_roles",
    owners=["team:DataEngineering"],
    description="Daily snapshot of clouddb.custom_roles",
    compute_kind="sql",
    metadata={
        "database": databases["database_bronze"],
        "owners": ["team:DataEngineering"],
        "write_mode": WarehouseWriteMode.overwrite,
        "schema": [],
        "code_location": get_code_location(),
        "description": "Daily snapshot of clouddb.custom_roles",
    },
    required_resource_keys=REQUIRED_RESOURCE_KEYS_EU,
    group_name=GROUP_NAME,
    partitions_def=RAW_TABLES_PARTITIONS_DEF,
    key_prefix=[AWSRegions.EU_WEST_1.value, databases["database_bronze"]],
)
def raw_clouddb_custom_roles_eu(context) -> DataFrame:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    query = RAW_CLOUDDB_CUSTOM_ROLES_QUERY.format(
        DATEID=context.partition_key, **databases
    )
    context.log.info(query)
    df = spark.sql(query)
    context.log.info(df)
    return df


@asset(
    name="raw_clouddb_users",
    owners=["team:DataEngineering"],
    compute_kind="sql",
    metadata={
        "database": databases["database_bronze"],
        "owners": ["team:DataEngineering"],
        "write_mode": WarehouseWriteMode.overwrite,
        "schema": [],
        "code_location": get_code_location(),
    },
    required_resource_keys=REQUIRED_RESOURCE_KEYS_CA,
    group_name=GROUP_NAME,
    partitions_def=RAW_TABLES_PARTITIONS_DEF,
    key_prefix=[AWSRegions.CA_CENTRAL_1.value, databases["database_bronze"]],
)
def raw_clouddb_users_ca(context) -> DataFrame:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    query = RAW_CLOUDDB_USERS_QUERY.format(DATEID=context.partition_key, **databases)
    context.log.info(query)
    df = spark.sql(query)
    context.log.info(df)
    return df


@asset(
    name="raw_clouddb_users_organizations",
    owners=["team:DataEngineering"],
    description="Daily snapshot of clouddb.users_organizations",
    compute_kind="sql",
    metadata={
        "database": databases["database_bronze"],
        "owners": ["team:DataEngineering"],
        "write_mode": WarehouseWriteMode.overwrite,
        "schema": [],
        "code_location": get_code_location(),
        "description": "Daily snapshot of clouddb.users_organizations",
    },
    required_resource_keys=REQUIRED_RESOURCE_KEYS_CA,
    group_name=GROUP_NAME,
    partitions_def=RAW_TABLES_PARTITIONS_DEF,
    key_prefix=[AWSRegions.CA_CENTRAL_1.value, databases["database_bronze"]],
)
def raw_clouddb_users_organizations_ca(context) -> DataFrame:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    query = RAW_CLOUDDB_USERS_ORGANIZATIONS_QUERY.format(
        DATEID=context.partition_key, **databases
    )
    context.log.info(query)
    df = spark.sql(query)
    context.log.info(df)
    return df


@asset(
    name="raw_clouddb_custom_roles",
    owners=["team:DataEngineering"],
    description="Daily snapshot of clouddb.custom_roles",
    compute_kind="sql",
    metadata={
        "database": databases["database_bronze"],
        "owners": ["team:DataEngineering"],
        "write_mode": WarehouseWriteMode.overwrite,
        "schema": [],
        "code_location": get_code_location(),
        "description": "Daily snapshot of clouddb.custom_roles",
    },
    required_resource_keys=REQUIRED_RESOURCE_KEYS_CA,
    group_name=GROUP_NAME,
    partitions_def=RAW_TABLES_PARTITIONS_DEF,
    key_prefix=[AWSRegions.CA_CENTRAL_1.value, databases["database_bronze"]],
)
def raw_clouddb_custom_roles_ca(context) -> DataFrame:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    query = RAW_CLOUDDB_CUSTOM_ROLES_QUERY.format(
        DATEID=context.partition_key, **databases
    )
    context.log.info(query)
    df = spark.sql(query)
    context.log.info(df)
    return df


@asset(
    name="stg_cloud_routes",
    owners=["team:DataEngineering"],
    description=build_table_description(
        table_desc="""This table provides all cloud route events from mixpanel deduplicated by mp_insert_id. This table runs daily and is partitioned by date with the mp_insert_id as the primary key. To filter to customer events, use is_customer_event=TRUE.""",
        row_meaning="""One row in this table represents one mixpanel page view event.""",
        table_type=TableType.TRANSACTIONAL_FACT,
        freshness_slo_updated_by="9am PST",
    ),
    compute_kind="sql",
    metadata={
        "database": databases["database_silver"],
        "owners": ["team:DataEngineering"],
        "write_mode": WarehouseWriteMode.overwrite,
        "schema": STG_CLOUD_ROUTES_SCHEMA,
        "code_location": get_code_location(),
        "primary_keys": ["date", "mp_insert_id"],
    },
    required_resource_keys=REQUIRED_RESOURCE_KEYS,
    group_name=GROUP_NAME,
    partitions_def=FCT_PARTITIONS_DEF,
    key_prefix=[AWSRegions.US_WEST_2.value, databases["database_silver"]],
    backfill_policy=BackfillPolicy.multi_run(max_partitions_per_run=30),
    deps=[
        AssetDep(
            AssetKey(
                [
                    AWSRegions.US_WEST_2.value,
                    databases["database_core_bronze"],
                    "raw_clouddb_organizations",
                ]
            ),
            partition_mapping=LastPartitionMapping(),
        ),
        AssetDep(
            AssetKey(
                [
                    AWSRegions.EU_WEST_1.value,
                    databases["database_core_bronze"],
                    "raw_clouddb_organizations",
                ]
            ),
            partition_mapping=LastPartitionMapping(),
        ),
        AssetDep(
            AssetKey(
                [
                    AWSRegions.CA_CENTRAL_1.value,
                    databases["database_core_gold"],
                    "dim_organizations",
                ]
            ),
            partition_mapping=LastPartitionMapping(),
        ),
        AssetDep(
            AssetKey(
                [
                    AWSRegions.US_WEST_2.value,
                    databases["database_bronze"],
                    "raw_clouddb_users",
                ]
            ),
            partition_mapping=LastPartitionMapping(),
        ),
        AssetDep(
            AssetKey(
                [
                    AWSRegions.EU_WEST_1.value,
                    databases["database_bronze"],
                    "raw_clouddb_users",
                ]
            ),
            partition_mapping=LastPartitionMapping(),
        ),
        AssetDep(
            AssetKey(
                [
                    AWSRegions.CA_CENTRAL_1.value,
                    databases["database_bronze"],
                    "raw_clouddb_users",
                ]
            ),
            partition_mapping=LastPartitionMapping(),
        ),
        AssetDep(
            AssetKey(
                [
                    AWSRegions.US_WEST_2.value,
                    databases["database_bronze"],
                    "raw_clouddb_users_organizations",
                ]
            ),
            partition_mapping=LastPartitionMapping(),
        ),
        AssetDep(
            AssetKey(
                [
                    AWSRegions.EU_WEST_1.value,
                    databases["database_bronze"],
                    "raw_clouddb_users_organizations",
                ]
            ),
            partition_mapping=LastPartitionMapping(),
        ),
        AssetDep(
            AssetKey(
                [
                    AWSRegions.CA_CENTRAL_1.value,
                    databases["database_bronze"],
                    "raw_clouddb_users_organizations",
                ]
            ),
            partition_mapping=LastPartitionMapping(),
        ),
    ],
)
def stg_cloud_routes(context) -> DataFrame:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    start_date = context.asset_partition_key_range.start
    end_date = context.asset_partition_key_range.end
    query = STG_CLOUD_ROUTES_QUERY.format(
        START_DATEID=start_date, END_DATEID=end_date, **databases
    )
    context.log.info(query)
    df = spark.sql(query)
    context.log.info(df)
    return df


dqs["stg_cloud_routes"].append(
    PrimaryKeyDQCheck(
        name="dq_pk_stg_cloud_routes",
        table="stg_cloud_routes",
        primary_keys=["date", "mp_insert_id"],
        blocking=True,
        database=databases["database_silver"],
    )
)

dqs["stg_cloud_routes"].append(
    NonEmptyDQCheck(
        name="dq_empty_stg_cloud_routes",
        table="stg_cloud_routes",
        blocking=True,
        database=databases["database_silver"],
    )
)


stg_user_login_events_description = build_table_description(
    table_desc="""This table provides daily records partitioned by date for all user cloud dashboard logins as recorded in the auditsdb_shards.audits table.""",
    row_meaning="""One row in this table represents one user_id's daily login count on the cloud dashboard.""",
    table_type=TableType.DAILY_DIMENSION,
    freshness_slo_updated_by="9am PST",
)


@asset(
    name="stg_user_login_events",
    owners=["team:DataEngineering"],
    description=stg_user_login_events_description,
    compute_kind="sql",
    metadata={
        "database": databases["database_silver"],
        "owners": ["team:DataEngineering"],
        "write_mode": WarehouseWriteMode.overwrite,
        "schema": STG_USER_LOGIN_EVENTS_SCHEMA,
        "code_location": get_code_location(),
        "primary_keys": ["date", "user_id", "org_id", "email"],
        "description": stg_user_login_events_description,
    },
    required_resource_keys=REQUIRED_RESOURCE_KEYS,
    group_name=GROUP_NAME,
    partitions_def=FCT_PARTITIONS_DEF,
    backfill_policy=BackfillPolicy.multi_run(max_partitions_per_run=30),
    key_prefix=[AWSRegions.US_WEST_2.value, databases["database_silver"]],
)
def stg_user_login_events(context) -> DataFrame:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    start_date = context.asset_partition_key_range.start
    end_date = context.asset_partition_key_range.end
    query = STG_USER_LOGIN_EVENTS_QUERY.format(
        START_DATEID=start_date, END_DATEID=end_date, **databases
    )
    context.log.info(query)
    df = spark.sql(query)
    context.log.info(df)
    return df


@asset(
    name="stg_user_login_events",
    owners=["team:DataEngineering"],
    description=stg_user_login_events_description,
    compute_kind="sql",
    metadata={
        "database": databases["database_silver"],
        "owners": ["team:DataEngineering"],
        "write_mode": WarehouseWriteMode.overwrite,
        "schema": STG_USER_LOGIN_EVENTS_SCHEMA,
        "code_location": get_code_location(),
        "primary_keys": ["date", "user_id", "org_id", "email"],
        "description": stg_user_login_events_description,
    },
    required_resource_keys=REQUIRED_RESOURCE_KEYS_EU,
    group_name=GROUP_NAME,
    partitions_def=FCT_PARTITIONS_DEF_EU,
    backfill_policy=BackfillPolicy.multi_run(max_partitions_per_run=30),
    key_prefix=[AWSRegions.EU_WEST_1.value, databases["database_silver"]],
)
def stg_user_login_events_eu(context) -> DataFrame:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    start_date = context.asset_partition_key_range.start
    end_date = context.asset_partition_key_range.end
    query = STG_USER_LOGIN_EVENTS_QUERY.format(
        START_DATEID=start_date, END_DATEID=end_date, **databases
    )
    context.log.info(query)
    df = spark.sql(query)
    context.log.info(df)
    return df


@asset(
    name="stg_user_login_events",
    owners=["team:DataEngineering"],
    description=stg_user_login_events_description,
    compute_kind="sql",
    metadata={
        "database": databases["database_silver"],
        "owners": ["team:DataEngineering"],
        "write_mode": WarehouseWriteMode.overwrite,
        "schema": STG_USER_LOGIN_EVENTS_SCHEMA,
        "code_location": get_code_location(),
        "primary_keys": ["date", "user_id", "org_id", "email"],
        "description": stg_user_login_events_description,
    },
    required_resource_keys=REQUIRED_RESOURCE_KEYS_CA,
    group_name=GROUP_NAME,
    partitions_def=FCT_PARTITIONS_DEF_CA,
    backfill_policy=BackfillPolicy.multi_run(max_partitions_per_run=30),
    key_prefix=[AWSRegions.CA_CENTRAL_1.value, databases["database_silver"]],
)
def stg_user_login_events_ca(context) -> DataFrame:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    start_date = context.asset_partition_key_range.start
    end_date = context.asset_partition_key_range.end
    query = STG_USER_LOGIN_EVENTS_QUERY.format(
        START_DATEID=start_date, END_DATEID=end_date, **databases
    )
    context.log.info(query)
    df = spark.sql(query)
    context.log.info(df)
    return df


@asset(
    name="stg_fleet_app_events",
    owners=["team:DataEngineering"],
    compute_kind="sql",
    metadata={
        "database": databases["database_silver"],
        "owners": ["team:DataEngineering"],
        "write_mode": WarehouseWriteMode.overwrite,
        "schema": STG_FLEET_APP_EVENTS_SCHEMA,
        "code_location": get_code_location(),
        "primary_keys": ["date", "user_id", "org_id", "event_type"],
    },
    required_resource_keys=REQUIRED_RESOURCE_KEYS_CA,
    group_name=GROUP_NAME,
    partitions_def=FCT_PARTITIONS_DEF_CA,
    backfill_policy=BackfillPolicy.multi_run(max_partitions_per_run=30),
    key_prefix=[AWSRegions.CA_CENTRAL_1.value, databases["database_silver"]],
)
def stg_fleet_app_events_ca(context) -> DataFrame:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    start_date = context.asset_partition_key_range.start
    end_date = context.asset_partition_key_range.end
    query = STG_FLEET_APP_EVENTS_QUERY.format(
        START_DATEID=start_date, END_DATEID=end_date, **databases
    )
    context.log.info(query)
    df = spark.sql(query)
    context.log.info(df)
    return df


dqs["stg_user_login_events"].append(
    PrimaryKeyDQCheck(
        name="dq_pk_stg_user_login_events",
        table="stg_user_login_events",
        primary_keys=["date", "user_id", "org_id", "email"],
        blocking=True,
        database=databases["database_silver"],
    )
)

dqs["stg_user_login_events"].append(
    NonEmptyDQCheck(
        name="dq_empty_stg_user_login_events",
        table="stg_user_login_events",
        blocking=True,
        database=databases["database_silver"],
    )
)


@asset(
    name="stg_fleet_app_events",
    owners=["team:DataEngineering"],
    compute_kind="sql",
    metadata={
        "database": databases["database_silver"],
        "owners": ["team:DataEngineering"],
        "write_mode": WarehouseWriteMode.overwrite,
        "schema": STG_FLEET_APP_EVENTS_SCHEMA,
        "code_location": get_code_location(),
        "primary_keys": ["date", "user_id", "org_id", "event_type"],
    },
    required_resource_keys=REQUIRED_RESOURCE_KEYS_EU,
    group_name=GROUP_NAME,
    partitions_def=FCT_PARTITIONS_DEF_EU,
    backfill_policy=BackfillPolicy.multi_run(max_partitions_per_run=30),
    key_prefix=[AWSRegions.EU_WEST_1.value, databases["database_silver"]],
)
def stg_fleet_app_events_eu(context) -> DataFrame:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    start_date = context.asset_partition_key_range.start
    end_date = context.asset_partition_key_range.end
    query = STG_FLEET_APP_EVENTS_QUERY.format(
        START_DATEID=start_date, END_DATEID=end_date, **databases
    )
    context.log.info(query)
    df = spark.sql(query)
    context.log.info(df)
    return df


@asset(
    name="stg_fleet_app_events",
    owners=["team:DataEngineering"],
    compute_kind="sql",
    metadata={
        "database": databases["database_silver"],
        "owners": ["team:DataEngineering"],
        "write_mode": WarehouseWriteMode.overwrite,
        "schema": STG_FLEET_APP_EVENTS_SCHEMA,
        "code_location": get_code_location(),
        "primary_keys": ["date", "user_id", "org_id", "event_type"],
    },
    required_resource_keys=REQUIRED_RESOURCE_KEYS,
    group_name=GROUP_NAME,
    partitions_def=FCT_PARTITIONS_DEF,
    backfill_policy=BackfillPolicy.multi_run(max_partitions_per_run=30),
    key_prefix=[AWSRegions.US_WEST_2.value, databases["database_silver"]],
)
def stg_fleet_app_events(context) -> DataFrame:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    start_date = context.asset_partition_key_range.start
    end_date = context.asset_partition_key_range.end
    query = STG_FLEET_APP_EVENTS_QUERY.format(
        START_DATEID=start_date, END_DATEID=end_date, **databases
    )
    context.log.info(query)
    df = spark.sql(query)
    context.log.info(df)
    return df


dqs["stg_fleet_app_events"].append(
    PrimaryKeyDQCheck(
        name="dq_pk_stg_fleet_app_events",
        table="stg_fleet_app_events",
        primary_keys=["date", "user_id", "org_id", "event_type"],
        blocking=True,
        database=databases["database_silver"],
    )
)


@asset(
    name="fct_user_activity",
    owners=["team:DataEngineering"],
    description=build_table_description(
        table_desc="""This table presents daily event counts for each event type for each surface that a user might interact with the Samsara platform. This fact table incorporates events from the web platform sourced from mixpanel, fleet app sourced from datastreams.mobile_logs, and user login activity sourced from the auditsdb_shards.audits table.
        """,
        row_meaning="""One row in this table represents the the event count for an event type for a user for a source (web, mobile, login).""",
        table_type=TableType.TRANSACTIONAL_FACT,
        freshness_slo_updated_by="9am PST",
    ),
    compute_kind="sql",
    metadata={
        "database": databases["database_gold"],
        "owners": ["team:DataEngineering"],
        "write_mode": WarehouseWriteMode.overwrite,
        "schema": FCT_USER_ACTIVITY_SCHEMA,
        "code_location": get_code_location(),
        "primary_keys": ["date", "user_id", "org_id", "event_type", "source"],
        "description": build_table_description(
            table_desc="""This table presents daily event counts for each event type for each surface that a user might interact with the Samsara platform. This fact table incorporates events from the web platform sourced from mixpanel, fleet app sourced from datastreams.mobile_logs, and user login activity sourced from the auditsdb_shards.audits table.
        """,
            row_meaning="""One row in this table represents the the event count for an event type for a user for a source (web, mobile, login).""",
            table_type=TableType.TRANSACTIONAL_FACT,
            freshness_slo_updated_by="9am PST",
        ),
    },
    required_resource_keys=REQUIRED_RESOURCE_KEYS,
    group_name=GROUP_NAME,
    partitions_def=FCT_PARTITIONS_DEF,
    key_prefix=[AWSRegions.US_WEST_2.value, databases["database_gold"]],
    backfill_policy=BackfillPolicy.multi_run(max_partitions_per_run=30),
    deps=[
        AssetDep(
            AssetKey(
                [
                    AWSRegions.US_WEST_2.value,
                    databases["database_silver"],
                    "dq_stg_user_login_events",
                ]
            ),
            partition_mapping=IdentityPartitionMapping(),
        ),
        AssetDep(
            AssetKey(
                [
                    AWSRegions.US_WEST_2.value,
                    databases["database_silver"],
                    "stg_fleet_app_events",
                ]
            ),
            partition_mapping=IdentityPartitionMapping(),
        ),
        AssetDep(
            AssetKey(
                [
                    AWSRegions.US_WEST_2.value,
                    databases["database_silver"],
                    "stg_cloud_routes",
                ]
            ),
            partition_mapping=IdentityPartitionMapping(),
        ),
    ],
)
def fct_user_activity(context) -> DataFrame:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    start_date = context.asset_partition_key_range.start
    end_date = context.asset_partition_key_range.end
    query = FCT_USER_ACTIVITY_QUERY.format(
        START_DATE=start_date, END_DATE=end_date, **databases
    )
    context.log.info(query)
    df = spark.sql(query)
    context.log.info(df)
    return df


@asset(
    name="fct_user_activity",
    owners=["team:DataEngineering"],
    description=build_table_description(
        table_desc="""This table presents daily event counts for each event type for each surface that a user might interact with the Samsara platform. This fact table incorporates events from the web platform sourced from mixpanel, fleet app sourced from datastreams.mobile_logs, and user login activity sourced from the auditsdb_shards.audits table.
        """,
        row_meaning="""One row in this table represents the the event count for an event type for a user for a source (web, mobile, login).""",
        table_type=TableType.TRANSACTIONAL_FACT,
        freshness_slo_updated_by="9am PST",
    ),
    compute_kind="sql",
    metadata={
        "database": databases["database_gold"],
        "owners": ["team:DataEngineering"],
        "write_mode": WarehouseWriteMode.overwrite,
        "schema": FCT_USER_ACTIVITY_SCHEMA,
        "code_location": get_code_location(),
        "primary_keys": ["date", "user_id", "org_id", "event_type", "source"],
        "description": build_table_description(
            table_desc="""This table presents daily event counts for each event type for each surface that a user might interact with the Samsara platform. This fact table incorporates events from the web platform sourced from mixpanel, fleet app sourced from datastreams.mobile_logs, and user login activity sourced from the auditsdb_shards.audits table.
        """,
            row_meaning="""One row in this table represents the the event count for an event type for a user for a source (web, mobile, login).""",
            table_type=TableType.TRANSACTIONAL_FACT,
            freshness_slo_updated_by="9am PST",
        ),
    },
    required_resource_keys=REQUIRED_RESOURCE_KEYS_EU,
    group_name=GROUP_NAME,
    partitions_def=FCT_PARTITIONS_DEF_EU,
    key_prefix=[AWSRegions.EU_WEST_1.value, databases["database_gold"]],
    backfill_policy=BackfillPolicy.multi_run(max_partitions_per_run=30),
    deps=[
        AssetDep(
            AssetKey(
                [
                    AWSRegions.EU_WEST_1.value,
                    databases["database_silver"],
                    "dq_stg_user_login_events",
                ]
            ),
            partition_mapping=IdentityPartitionMapping(),
        ),
        AssetDep(
            AssetKey(
                [
                    AWSRegions.EU_WEST_1.value,
                    databases["database_silver"],
                    "stg_fleet_app_events",
                ]
            ),
            partition_mapping=IdentityPartitionMapping(),
        ),
        AssetDep(
            AssetKey(
                [
                    AWSRegions.US_WEST_2.value,
                    databases["database_silver"],
                    "stg_cloud_routes",
                ]
            ),
            partition_mapping=IdentityPartitionMapping(),
        ),
    ],
)
def fct_user_activity_eu(context) -> DataFrame:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    start_date = context.asset_partition_key_range.start
    end_date = context.asset_partition_key_range.end
    query = FCT_USER_ACTIVITY_EU_QUERY.format(
        START_DATE=start_date, END_DATE=end_date, **databases
    )
    context.log.info(query)
    df = spark.sql(query)
    context.log.info(df)
    return df


@asset(
    name="fct_user_activity",
    owners=["team:DataEngineering"],
    description=build_table_description(
        table_desc="""This table provides daily user activity metrics aggregated by user and organization. This table runs daily and is partitioned by date with the user_id and org_id as the primary keys. This table is used to power user activity dashboards and analytics.""",
        row_meaning="""One row in this table represents one user's activity metrics for one organization on one day.""",
        table_type=TableType.TRANSACTIONAL_FACT,
        freshness_slo_updated_by="9am PST",
    ),
    compute_kind="sql",
    metadata={
        "database": databases["database_gold"],
        "owners": ["team:DataEngineering"],
        "primary_keys": ["date", "user_id", "org_id"],
        "write_mode": WarehouseWriteMode.overwrite,
        "schema": FCT_USER_ACTIVITY_SCHEMA,
        "code_location": get_code_location(),
        "description": build_table_description(
            table_desc="""This table provides daily user activity metrics aggregated by user and organization. This table runs daily and is partitioned by date with the user_id and org_id as the primary keys. This table is used to power user activity dashboards and analytics.""",
            row_meaning="""One row in this table represents one user's activity metrics for one organization on one day.""",
            table_type=TableType.TRANSACTIONAL_FACT,
            freshness_slo_updated_by="9am PST",
        ),
    },
    required_resource_keys=REQUIRED_RESOURCE_KEYS_CA,
    group_name=GROUP_NAME,
    partitions_def=FCT_PARTITIONS_DEF_CA,
    key_prefix=[AWSRegions.CA_CENTRAL_1.value, databases["database_gold"]],
    deps=[
        AssetDep(
            AssetKey(
                [
                    AWSRegions.CA_CENTRAL_1.value,
                    databases["database_silver"],
                    "dq_stg_user_login_events",
                ]
            ),
            partition_mapping=IdentityPartitionMapping(),
        ),
        AssetDep(
            AssetKey(
                [
                    AWSRegions.CA_CENTRAL_1.value,
                    databases["database_silver"],
                    "stg_fleet_app_events",
                ]
            ),
            partition_mapping=IdentityPartitionMapping(),
        ),
        AssetDep(
            AssetKey(
                [
                    AWSRegions.US_WEST_2.value,
                    databases["database_silver"],
                    "stg_cloud_routes",
                ]
            ),
            partition_mapping=IdentityPartitionMapping(),
        ),
    ],
)
def fct_user_activity_ca(context) -> DataFrame:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    start_date = context.asset_partition_key_range.start
    end_date = context.asset_partition_key_range.end
    query = FCT_USER_ACTIVITY_EU_QUERY.format(
        START_DATE=start_date, END_DATE=end_date, **databases
    )
    context.log.info(query)
    df = spark.sql(query)
    context.log.info(df)
    return df


dqs["fct_user_activity"].append(
    PrimaryKeyDQCheck(
        name="dq_pk_fct_user_activity",
        table="fct_user_activity",
        primary_keys=["date", "user_id", "org_id", "event_type", "source"],
        blocking=True,
        database=databases["database_gold"],
    )
)

dqs["fct_user_activity"].append(
    NonEmptyDQCheck(
        name="dq_empty_fct_user_activity",
        table="fct_user_activity",
        blocking=True,
        database=databases["database_gold"],
    )
)

dqs["fct_user_activity"].append(
    JoinableDQCheck(
        name="dq_joinable_fct_user_activity_to_dim_users_organizations",
        database=databases["database_gold"],
        database_2=databases["database_core_gold"],
        input_asset_1="fct_user_activity",
        input_asset_2="dim_organizations",
        join_keys=[("org_id", "org_id")],
        blocking=True,
        null_right_table_rows_ratio=0.01,
    )
)

dqs["fct_user_activity"].append(
    NonNullDQCheck(
        name="dq_non_null_fct_user_activity",
        table="fct_user_activity",
        non_null_columns=["user_id", "source", "event_count", "event_type"],
        blocking=True,
        database=databases["database_gold"],
    )
)


@asset(
    name="dim_users_organizations",
    owners=["team:DataEngineering"],
    description=build_table_description(
        table_desc="""This table shows daily updates to users and is at the granularity of user and organization. As users can be members of multiple organizations, this table shows information by organization and user.
        """,
        row_meaning="""One row in this table the most recent metadata for the user_id and org_id.""",
        table_type=TableType.DAILY_DIMENSION,
        freshness_slo_updated_by="9am PST",
    ),
    compute_kind="sql",
    metadata={
        "database": databases["database_gold"],
        "owners": ["team:DataEngineering"],
        "write_mode": WarehouseWriteMode.overwrite,
        "schema": DIM_USERS_ORGANIZATIONS_SCHEMA,
        "code_location": get_code_location(),
        "primary_keys": ["date", "user_id", "org_id"],
        "description": build_table_description(
            table_desc="""This table shows daily updates to users and is at the granularity of user and organization. As users can be members of multiple organizations, this table shows information by organization and user.
        """,
            row_meaning="""One row in this table the most recent metadata for the user_id and org_id.""",
            table_type=TableType.DAILY_DIMENSION,
            freshness_slo_updated_by="9am PST",
        ),
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
                    databases["database_gold"],
                    "fct_user_activity",
                ]
            ),
            partition_mapping=IdentityPartitionMapping(),
        ),
        AssetDep(
            AssetKey(
                [
                    AWSRegions.US_WEST_2.value,
                    databases["database_bronze"],
                    "raw_clouddb_users",
                ]
            ),
            partition_mapping=IdentityPartitionMapping(),
        ),
        AssetDep(
            AssetKey(
                [
                    AWSRegions.US_WEST_2.value,
                    databases["database_bronze"],
                    "raw_clouddb_users_organizations",
                ]
            ),
            partition_mapping=IdentityPartitionMapping(),
        ),
        AssetDep(
            AssetKey(
                [
                    AWSRegions.US_WEST_2.value,
                    databases["database_gold"],
                    "dim_users_organizations",
                ]
            ),
            partition_mapping=TimeWindowPartitionMapping(
                start_offset=-1, end_offset=-1
            ),
        ),
    ],
)
def dim_users_organizations(context) -> DataFrame:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    partition_date_str = context.partition_key
    if partition_date_str == DIM_PARTITIONS_DEF.start.strftime("%Y-%m-%d"):
        query = BUILD_DIM_USERS_ORGANIZATIONS_QUERY.format(
            DATEID=partition_date_str, **databases
        )
    else:
        query = INSERT_DIM_USERS_ORGANIZATIONS_QUERY.format(
            DATEID=partition_date_str, **databases
        )
    context.log.info(query)
    df = spark.sql(query)
    context.log.info(df)
    return df


@asset(
    name="dim_users_organizations",
    owners=["team:DataEngineering"],
    description=build_table_description(
        table_desc="""This table shows daily updates to users and is at the granularity of user and organization. As users can be members of multiple organizations, this table shows information by organization and user.
        """,
        row_meaning="""One row in this table the most recent metadata for the user_id and org_id.""",
        table_type=TableType.DAILY_DIMENSION,
        freshness_slo_updated_by="9am PST",
    ),
    compute_kind="sql",
    metadata={
        "database": databases["database_gold"],
        "owners": ["team:DataEngineering"],
        "write_mode": WarehouseWriteMode.overwrite,
        "schema": DIM_USERS_ORGANIZATIONS_SCHEMA,
        "code_location": get_code_location(),
        "primary_keys": ["date", "user_id", "org_id"],
        "description": build_table_description(
            table_desc="""This table shows daily updates to users and is at the granularity of user and organization. As users can be members of multiple organizations, this table shows information by organization and user.
        """,
            row_meaning="""One row in this table the most recent metadata for the user_id and org_id.""",
            table_type=TableType.DAILY_DIMENSION,
            freshness_slo_updated_by="9am PST",
        ),
    },
    required_resource_keys=REQUIRED_RESOURCE_KEYS_EU,
    group_name=GROUP_NAME,
    partitions_def=DIM_PARTITIONS_DEF,
    key_prefix=[AWSRegions.EU_WEST_1.value, databases["database_gold"]],
    deps=[
        AssetDep(
            AssetKey(
                [
                    AWSRegions.EU_WEST_1.value,
                    databases["database_gold"],
                    "fct_user_activity",
                ]
            ),
            partition_mapping=IdentityPartitionMapping(),
        ),
        AssetDep(
            AssetKey(
                [
                    AWSRegions.EU_WEST_1.value,
                    databases["database_bronze"],
                    "raw_clouddb_users",
                ]
            ),
            partition_mapping=IdentityPartitionMapping(),
        ),
        AssetDep(
            AssetKey(
                [
                    AWSRegions.EU_WEST_1.value,
                    databases["database_bronze"],
                    "raw_clouddb_users_organizations",
                ]
            ),
            partition_mapping=IdentityPartitionMapping(),
        ),
        AssetDep(
            AssetKey(
                [
                    AWSRegions.EU_WEST_1.value,
                    databases["database_gold"],
                    "dim_users_organizations",
                ]
            ),
            partition_mapping=TimeWindowPartitionMapping(
                start_offset=-1, end_offset=-1
            ),
        ),
    ],
)
def dim_users_organizations_eu(context) -> DataFrame:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    partition_date_str = context.partition_key
    if partition_date_str == DIM_PARTITIONS_DEF_EU.start.strftime("%Y-%m-%d"):
        query = BUILD_DIM_USERS_ORGANIZATIONS_QUERY.format(
            DATEID=partition_date_str, **databases
        )
    else:
        query = INSERT_DIM_USERS_ORGANIZATIONS_QUERY.format(
            DATEID=partition_date_str, **databases
        )
    context.log.info(query)
    df = spark.sql(query)
    context.log.info(df)
    return df


@asset(
    name="dim_users_organizations",
    owners=["team:DataEngineering"],
    description=build_table_description(
        table_desc="""This table shows daily updates to users and is at the granularity of user and organization. As users can be members of multiple organizations, this table shows information by organization and user.
        """,
        row_meaning="""One row in this table the most recent metadata for the user_id and org_id.""",
        table_type=TableType.DAILY_DIMENSION,
        freshness_slo_updated_by="9am PST",
    ),
    compute_kind="sql",
    metadata={
        "database": databases["database_gold"],
        "owners": ["team:DataEngineering"],
        "primary_keys": ["date", "user_id", "org_id"],
        "write_mode": WarehouseWriteMode.overwrite,
        "schema": DIM_USERS_ORGANIZATIONS_SCHEMA,
        "code_location": get_code_location(),
        "description": build_table_description(
            table_desc="""This table shows daily updates to users and is at the granularity of user and organization. As users can be members of multiple organizations, this table shows information by organization and user.
        """,
            row_meaning="""One row in this table the most recent metadata for the user_id and org_id.""",
            table_type=TableType.DAILY_DIMENSION,
            freshness_slo_updated_by="9am PST",
        ),
    },
    required_resource_keys=REQUIRED_RESOURCE_KEYS_CA,
    group_name=GROUP_NAME,
    partitions_def=DIM_PARTITIONS_DEF_CA,
    key_prefix=[AWSRegions.CA_CENTRAL_1.value, databases["database_gold"]],
    deps=[
        AssetDep(
            AssetKey(
                [
                    AWSRegions.CA_CENTRAL_1.value,
                    databases["database_gold"],
                    "fct_user_activity",
                ]
            ),
            partition_mapping=IdentityPartitionMapping(),
        ),
        AssetDep(
            AssetKey(
                [
                    AWSRegions.CA_CENTRAL_1.value,
                    databases["database_bronze"],
                    "raw_clouddb_users",
                ]
            ),
            partition_mapping=IdentityPartitionMapping(),
        ),
        AssetDep(
            AssetKey(
                [
                    AWSRegions.CA_CENTRAL_1.value,
                    databases["database_bronze"],
                    "raw_clouddb_users_organizations",
                ]
            ),
            partition_mapping=IdentityPartitionMapping(),
        ),
        AssetDep(
            AssetKey(
                [
                    AWSRegions.CA_CENTRAL_1.value,
                    databases["database_gold"],
                    "dim_users_organizations",
                ]
            ),
            partition_mapping=TimeWindowPartitionMapping(
                start_offset=-1, end_offset=-1
            ),
        ),
    ],
)
def dim_users_organizations_ca(context) -> DataFrame:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    partition_date_str = context.partition_key
    if partition_date_str == DIM_PARTITIONS_DEF_CA.start.strftime("%Y-%m-%d"):
        query = BUILD_DIM_USERS_ORGANIZATIONS_QUERY.format(
            DATEID=partition_date_str, **databases
        )
    else:
        query = INSERT_DIM_USERS_ORGANIZATIONS_QUERY.format(
            DATEID=partition_date_str, **databases
        )
    context.log.info(query)
    df = spark.sql(query)
    context.log.info(df)
    return df


dim_dqs["dim_users_organizations"].append(
    PrimaryKeyDQCheck(
        name="dq_pk_dim_users_organizations",
        table="dim_users_organizations",
        primary_keys=["date", "user_id", "org_id"],
        blocking=True,
        database=databases["database_gold_dev"],
    )
)

dim_dqs["dim_users_organizations"].append(
    NonEmptyDQCheck(
        name="dq_empty_dim_users_organizations",
        table="dim_users_organizations",
        blocking=True,
        database=databases["database_gold_dev"],
    )
)

dim_dqs["dim_users_organizations"].append(
    TrendDQCheck(
        name="dq_trend_dim_users_organizations",
        database=databases["database_gold_dev"],
        table="dim_users_organizations",
        blocking=False,
        tolerance=0.02,
    )
)


@asset(
    name="dim_users",
    owners=["team:DataEngineering"],
    description=build_table_description(
        table_desc="""This table shows daily updates to users and is at the granularity of user_id, aggregated across user organizations. As users can be members of multiple organizations, this table shows information by user.
        """,
        row_meaning="""One row in this table the most recent metadata for the user_id.""",
        table_type=TableType.DAILY_DIMENSION,
        freshness_slo_updated_by="9am PST",
    ),
    compute_kind="sql",
    metadata={
        "database": databases["database_gold"],
        "owners": ["team:DataEngineering"],
        "write_mode": WarehouseWriteMode.overwrite,
        "schema": DIM_USERS_SCHEMA,
        "code_location": get_code_location(),
        "primary_keys": ["date", "user_id"],
        "description": build_table_description(
            table_desc="""This table shows daily updates to users and is at the granularity of user_id, aggregated across user organizations. As users can be members of multiple organizations, this table shows information by user.
        """,
            row_meaning="""One row in this table the most recent metadata for the user_id.""",
            table_type=TableType.DAILY_DIMENSION,
            freshness_slo_updated_by="9am PST",
        ),
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
                    databases["database_gold"],
                    "dim_users_organizations",
                ]
            ),
            partition_mapping=IdentityPartitionMapping(),
        )
    ],
)
def dim_users(context) -> DataFrame:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    partition_date_str = context.partition_key
    query = DIM_USERS_QUERY.format(DATEID=partition_date_str, **databases)
    context.log.info(query)
    df = spark.sql(query)
    context.log.info(df)
    return df


@asset(
    name="dim_users",
    owners=["team:DataEngineering"],
    description=build_table_description(
        table_desc="""This table shows daily updates to users and is at the granularity of user_id, aggregated across user organizations. As users can be members of multiple organizations, this table shows information by user.
        """,
        row_meaning="""One row in this table the most recent metadata for the user_id.""",
        table_type=TableType.DAILY_DIMENSION,
        freshness_slo_updated_by="9am PST",
    ),
    compute_kind="sql",
    metadata={
        "database": databases["database_gold"],
        "owners": ["team:DataEngineering"],
        "write_mode": WarehouseWriteMode.overwrite,
        "schema": DIM_USERS_SCHEMA,
        "code_location": get_code_location(),
        "primary_keys": ["date", "user_id"],
        "description": build_table_description(
            table_desc="""This table shows daily updates to users and is at the granularity of user_id, aggregated across user organizations. As users can be members of multiple organizations, this table shows information by user.
        """,
            row_meaning="""One row in this table the most recent metadata for the user_id.""",
            table_type=TableType.DAILY_DIMENSION,
            freshness_slo_updated_by="9am PST",
        ),
    },
    required_resource_keys=REQUIRED_RESOURCE_KEYS_EU,
    group_name=GROUP_NAME,
    partitions_def=DIM_PARTITIONS_DEF,
    key_prefix=[AWSRegions.EU_WEST_1.value, databases["database_gold"]],
    deps=[
        AssetDep(
            AssetKey(
                [
                    AWSRegions.EU_WEST_1.value,
                    databases["database_gold"],
                    "dim_users_organizations",
                ]
            ),
            partition_mapping=IdentityPartitionMapping(),
        )
    ],
)
def dim_users_eu(context) -> DataFrame:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    partition_date_str = context.partition_key
    query = DIM_USERS_QUERY.format(DATEID=partition_date_str, **databases)
    context.log.info(query)
    df = spark.sql(query)
    context.log.info(df)
    return df


@asset(
    name="dim_users",
    owners=["team:DataEngineering"],
    description=build_table_description(
        table_desc="""This table shows daily updates to users and is at the granularity of user_id, aggregated across user organizations. As users can be members of multiple organizations, this table shows information by user.
        """,
        row_meaning="""One row in this table the most recent metadata for the user_id.""",
        table_type=TableType.DAILY_DIMENSION,
        freshness_slo_updated_by="9am PST",
    ),
    compute_kind="sql",
    metadata={
        "database": databases["database_gold"],
        "owners": ["team:DataEngineering"],
        "write_mode": WarehouseWriteMode.overwrite,
        "schema": DIM_USERS_SCHEMA,
        "code_location": get_code_location(),
        "primary_keys": ["date", "user_id"],
        "description": build_table_description(
            table_desc="""This table shows daily updates to users and is at the granularity of user_id, aggregated across user organizations. As users can be members of multiple organizations, this table shows information by user.
        """,
            row_meaning="""One row in this table the most recent metadata for the user_id.""",
            table_type=TableType.DAILY_DIMENSION,
            freshness_slo_updated_by="9am PST",
        ),
    },
    required_resource_keys=REQUIRED_RESOURCE_KEYS_CA,
    group_name=GROUP_NAME,
    partitions_def=DIM_PARTITIONS_DEF_CA,
    key_prefix=[AWSRegions.CA_CENTRAL_1.value, databases["database_gold"]],
    deps=[
        AssetDep(
            AssetKey(
                [
                    AWSRegions.CA_CENTRAL_1.value,
                    databases["database_gold"],
                    "dim_users_organizations",
                ]
            ),
            partition_mapping=IdentityPartitionMapping(),
        )
    ],
)
def dim_users_ca(context) -> DataFrame:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    partition_date_str = context.partition_key
    query = DIM_USERS_QUERY.format(DATEID=partition_date_str, **databases)
    context.log.info(query)
    df = spark.sql(query)
    context.log.info(df)
    return df


# Primary Key DQ Check
dim_dqs["dim_users"].append(
    PrimaryKeyDQCheck(
        name="dq_pk_dim_users",
        table="dim_users",
        primary_keys=["date", "user_id"],
        blocking=True,
        database=databases["database_gold_dev"],
    )
)

# Empty DQ Check
dim_dqs["dim_users"].append(
    NonEmptyDQCheck(
        name="dq_empty_dim_users",
        table="dim_users",
        blocking=True,
        database=databases["database_gold_dev"],
    )
)

# Trend based DQ check
dim_dqs["dim_users"].append(
    TrendDQCheck(
        name="dq_trend_dim_users",
        database=databases["database_gold_dev"],
        table="dim_users",
        blocking=False,
        tolerance=0.1,
    )
)

# Add EU DQ checks
dqs_eu["stg_user_login_events"].append(
    PrimaryKeyDQCheck(
        name="dq_pk_stg_user_login_events_eu",
        table="stg_user_login_events",
        primary_keys=["date", "user_id", "org_id", "email"],
        blocking=True,
        database=databases["database_silver"],
    )
)

dqs_eu["stg_user_login_events"].append(
    NonEmptyDQCheck(
        name="dq_empty_stg_user_login_events_eu",
        table="stg_user_login_events",
        blocking=True,
        database=databases["database_silver"],
    )
)

dqs_eu["stg_fleet_app_events"].append(
    PrimaryKeyDQCheck(
        name="dq_pk_stg_fleet_app_events_eu",
        table="stg_fleet_app_events",
        primary_keys=["date", "user_id", "org_id", "event_type"],
        blocking=True,
        database=databases["database_silver"],
    )
)

dqs_eu["fct_user_activity"].append(
    PrimaryKeyDQCheck(
        name="dq_pk_fct_user_activity_eu",
        table="fct_user_activity",
        primary_keys=["date", "user_id", "org_id", "event_type", "source"],
        blocking=True,
        database=databases["database_gold"],
    )
)

dqs_eu["fct_user_activity"].append(
    NonEmptyDQCheck(
        name="dq_empty_fct_user_activity_eu",
        table="fct_user_activity",
        blocking=True,
        database=databases["database_gold"],
    )
)

dqs_eu["fct_user_activity"].append(
    JoinableDQCheck(
        name="dq_joinable_fct_user_activity_to_dim_users_organizations_eu",
        database=databases["database_gold"],
        database_2=databases["database_core_gold"],
        input_asset_1="fct_user_activity",
        input_asset_2="dim_organizations",
        join_keys=[("org_id", "org_id")],
        blocking=True,
        null_right_table_rows_ratio=0.01,
    )
)

dqs_eu["fct_user_activity"].append(
    NonNullDQCheck(
        name="dq_non_null_fct_user_activity_eu",
        table="fct_user_activity",
        non_null_columns=["user_id", "source", "event_count", "event_type"],
        blocking=True,
        database=databases["database_gold"],
    )
)

# Add CA DQ checks
dqs_ca["stg_user_login_events"].append(
    PrimaryKeyDQCheck(
        name="dq_pk_stg_user_login_events_ca",
        table="stg_user_login_events",
        primary_keys=["date", "user_id", "org_id", "email"],
        blocking=True,
        database=databases["database_silver"],
    )
)

dqs_ca["stg_user_login_events"].append(
    NonEmptyDQCheck(
        name="dq_empty_stg_user_login_events_ca",
        table="stg_user_login_events",
        blocking=True,
        database=databases["database_silver"],
    )
)

dqs_ca["stg_fleet_app_events"].append(
    PrimaryKeyDQCheck(
        name="dq_pk_stg_fleet_app_events_ca",
        table="stg_fleet_app_events",
        primary_keys=["date", "user_id", "org_id", "event_type"],
        blocking=True,
        database=databases["database_silver"],
    )
)

dqs_ca["fct_user_activity"].append(
    PrimaryKeyDQCheck(
        name="dq_pk_fct_user_activity_ca",
        table="fct_user_activity",
        primary_keys=["date", "user_id", "org_id", "event_type", "source"],
        blocking=True,
        database=databases["database_gold"],
    )
)

dqs_ca["fct_user_activity"].append(
    NonEmptyDQCheck(
        name="dq_empty_fct_user_activity_ca",
        table="fct_user_activity",
        blocking=True,
        database=databases["database_gold"],
    )
)

dqs_ca["fct_user_activity"].append(
    JoinableDQCheck(
        name="dq_joinable_fct_user_activity_to_dim_users_organizations_ca",
        database=databases["database_gold"],
        database_2=databases["database_core_gold"],
        input_asset_1="fct_user_activity",
        input_asset_2="dim_organizations",
        join_keys=[("org_id", "org_id")],
        blocking=True,
        null_right_table_rows_ratio=0.02,
    )
)

dqs_ca["fct_user_activity"].append(
    NonNullDQCheck(
        name="dq_non_null_fct_user_activity_ca",
        table="fct_user_activity",
        non_null_columns=["user_id", "source", "event_count", "event_type"],
        blocking=True,
        database=databases["database_gold"],
    )
)

dq_assets = dqs.generate()
dq_assets_eu = dqs_eu.generate()
dq_assets_ca = dqs_ca.generate()
fct_dqs_assets = dim_dqs.generate()
