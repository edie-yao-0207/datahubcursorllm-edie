from collections import defaultdict
from datetime import datetime

import pandas as pd
from dagster import (
    AssetDep,
    AssetKey,
    BackfillPolicy,
    DailyPartitionsDefinition,
    IdentityPartitionMapping,
    MultiPartitionsDefinition,
    Nothing,
    StaticPartitionsDefinition,
    TimeWindowPartitionMapping,
    asset,
)
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from ...common.constants import SLACK_ALERTS_CHANNEL_DATA_ENGINEERING
from ...common.stats import similarity_score
from ...common.utils import (
    AWSRegions,
    Database,
    DQGroup,
    JoinableDQCheck,
    NonEmptyDQCheck,
    PrimaryKeyDQCheck,
    TableType,
    TrendDQCheck,
    WarehouseWriteMode,
    adjust_partition_def_for_canada,
    apply_db_overrides,
    build_assets_from_sql,
    build_table_description,
    get_all_regions,
    get_code_location,
)

SLACK_ALERTS_CHANNEL = SLACK_ALERTS_CHANNEL_DATA_ENGINEERING
GROUP_NAME = "dim_device_activation"
REQUIRED_RESOURCE_KEYS = {"databricks_pyspark_step_launcher"}
REQUIRED_RESOURCE_KEYS_EU = {"databricks_pyspark_step_launcher_eu"}
REQUIRED_RESOURCE_KEYS_CA = {"databricks_pyspark_step_launcher_ca"}
ACTIVATIONS_PARTITION_DEFS = DailyPartitionsDefinition(start_date="2016-05-18")
RAW_TABLES_PARTITION_DEFS = DailyPartitionsDefinition(start_date="2024-03-24")
DIM_DEVICE_OWNER_ACTIVATOR_PARTITIONS_DEF = DailyPartitionsDefinition(
    start_date="2024-03-24"
)
DIM_DEVICE_OWNER_ACTIVATOR_PARTITIONS_DEF_CA = adjust_partition_def_for_canada(
    DIM_DEVICE_OWNER_ACTIVATOR_PARTITIONS_DEF
)

dqs = DQGroup(
    group_name=GROUP_NAME,
    partition_def=DIM_DEVICE_OWNER_ACTIVATOR_PARTITIONS_DEF,
    slack_alerts_channel=SLACK_ALERTS_CHANNEL_DATA_ENGINEERING,
    regions=get_all_regions(),
)

databases = {
    "database_bronze": Database.DATAMODEL_CORE_BRONZE,
    "database_silver": Database.DATAMODEL_CORE_SILVER,
    "database_gold": Database.DATAMODEL_CORE,
}

database_dev_overrides = {
    "database_bronze_dev": Database.DATAMODEL_DEV,
    "database_silver_dev": Database.DATAMODEL_DEV,
    "database_gold_dev": Database.DATAMODEL_DEV,
}

databases = apply_db_overrides(databases, database_dev_overrides)

STG_DEVICE_ACTIVATION_SCHEMA = [
    {
        "name": "date",
        "type": "date",
        "nullable": False,
        "metadata": {
            "comment": "Date of partition. Sourced from date field in auditsdb_shards.audits."
        },
    },
    {
        "name": "activated_at",
        "type": "timestamp",
        "nullable": False,
        "metadata": {
            "comment": "Timestamp of gateway activation, deduplicated by serial, user_id, date, device_id."
        },
    },
    {
        "name": "device_id",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Device_id at time of activation. As this changes post activation this would not allow for current lookups."
        },
    },
    {
        "name": "user_id",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "User_id of device activator sourced from auditsdb_shards.audits."
        },
    },
    {
        "name": "org_id",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Org_id of device_activator sourced from auditsdb_shards.audits."
        },
    },
    {
        "name": "summary",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Summary of device activation, used to source email, group_name, and serial."
        },
    },
    {
        "name": "serial",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Serial number of device being activated. Sourced from regex on summary field."
        },
    },
    {
        "name": "email",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Email address of user activating device. Sourced from regex on summary field."
        },
    },
    {
        "name": "group_name",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Group name of device post activation. Sourced from regex on summary field."
        },
    },
]

STG_DEVICE_ACTIVATION_QUERY = """
    SELECT
        MAX(audits.happened_at) AS activated_at,
        DATE(audits.date) AS date,
        audits.device_id,
        audits.user_id,
        audits.org_id,
        audits.summary,
        REGEXP_REPLACE(
                REGEXP_EXTRACT(audits.summary, "\'([^\']+)\'", 1),
                '-',
                ''
            ) AS serial,
        LOWER(REGEXP_EXTRACT(audits.summary, r"(\\b[^@\\s]+\\@[^@\\s]+\\b)",1)) AS email,
        REGEXP_EXTRACT(audits.summary, "group '(.*)'", 1) AS group_name
    FROM auditsdb_shards.audits audits
    WHERE
        audits.audit_type_id = 13
        AND audits.date between '{START_DATEID}' AND '{END_DATEID}'
    GROUP BY 2,3,4,5,6 """

DIM_DEVICE_OWNER_ACTIVATOR_SCHEMA = [
    {
        "name": "date",
        "type": "string",
        "nullable": False,
        "metadata": {"comment": "Date of partition."},
    },
    {
        "name": "serial",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Serial number of gateway device, a unique identifier for a gateway device. Sourced from productsdb.gateways"
        },
    },
    {
        "name": "device_id",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Most recent device id associated with the gateway id and serial. This may change over time if the vehicle changes. Sourced from productsdb.gateways"
        },
    },
    {
        "name": "gateway_id",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Gateway ID of a gateway device, a unique identifier for a gateway device. Sourced from auditsdb_shards.audits."
        },
    },
    {
        "name": "device_type",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Type of device. This dataset is limited to VG devices. Sourced from definitions.products"
        },
    },
    {
        "name": "product_id",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Product ID of device type. Sourced from productsdb.gateways."
        },
    },
    {
        "name": "device_name",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Name of vehicle associated with device, custom import from customer. Sourced from productsdb.devices."
        },
    },
    {
        "name": "first_heartbeat_date",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "First date of device heartbeat. Sourced from datamodel_core.lifetime_device_online "
        },
    },
    {
        "name": "last_heartbeat_date",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Last date of device heartbeat. Sourced from datamodel_core.lifetime_device_online "
        },
    },
    {
        "name": "last_activation_date",
        "type": "date",
        "nullable": False,
        "metadata": {
            "comment": "Most recent activation date of device. Sourced from auditsdb_shards.audits. "
        },
    },
    {
        "name": "last_activation_org_id",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Most recent org id to activate device. Sourced from auditsdb_shards.audits. "
        },
    },
    {
        "name": "last_activation_email",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Most recent user email used to activate device. Sourced from auditsdb_shards.audits. "
        },
    },
    {
        "name": "last_activation_partner",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Name of partner installer sourced from last_activation_email domain and definitions.installer_domain_name."
        },
    },
    {
        "name": "last_activation_group_name",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Most recent group name used to activate device. Sourced from auditsdb_shards.audits."
        },
    },
    {
        "name": "last_activation_sam_number",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Most recent SAM number of org id used to activate device. Sourced from datamodel_core.dim_organizations."
        },
    },
    {
        "name": "last_activation_sfdc_account_name",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Most recent SFDC account name of org id used to activate device. Sourced from datamodel_core.dim_organizations."
        },
    },
    {
        "name": "last_activation_sdfc_account_id",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Most recent SFDC account ID used to activate device. Sourced from datamodel_core.dim_organizations."
        },
    },
    {
        "name": "order_date",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Most recent order date of device. Sourced from edw.silver.dim_device_transaction."
        },
    },
    {
        "name": "order_transaction_id",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment": "Most recent order transaction ID of device. Sourced from edw.silver.dim_device_transaction."
        },
    },
    {
        "name": "order_sam_number",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Most recent order SAM number of device. Sourced from edw.silver.dim_device_transaction."
        },
    },
    {
        "name": "order_number",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Most recent order number of device. Sourced from edw.silver.dim_device_transaction."
        },
    },
    {
        "name": "order_sfdc_account_id",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Most recent order SFDC account ID of device. Sourced from edw.silver.dim_device_transaction."
        },
    },
    {
        "name": "order_sfdc_account_name",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Most recent order SFDC account name of device. Sourced from edw.silver.dim_device_transaction."
        },
    },
    {
        "name": "last_activation_sfdc_account_url",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Salesforce account URL constructed from last_activation_sfdc_account_id. "
        },
    },
    {
        "name": "order_sfdc_account_url",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Salesforce account URL constructed from order_sfdc_account_id. "
        },
    },
    {
        "name": "gateway_cloud_dashboard_url",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Salesforce account URL constructed from order_sfdc_account_id. "
        },
    },
    {
        "name": "is_owner_activator_mismatch",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "TRUE if SAM number is present for both activator and owner/orderer and SAM number does not match. Otherwise false."
        },
    },
    {
        "name": "account_match_status",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": """Field describing correspondence of SAM number between activator and owner/orderer of device.
                        (1) match: if activator SAM and owner/orderer SAM are present and equal
                        (2) mismatch: if activator SAM and owner/orderer SAM are present and not equal
                        (3) missing_activator_sam_number: if only the activator SAM number is missing from datamodel_core.dim_organizations
                        (4) missing_shipment_sam_number: if only the shipment SAM number is missing from edw.silver.dim_device_transaction
                        (5) missing_both_sam_numbers: if both activator SAM and owner/orderer SAM are missing
                        """
        },
    },
    {
        "name": "owner_activator_similarity_score",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment": "Similarity score of SFDC account name text for activator and owner/orderer."
        },
    },
]

dim_transaction_hierarchy_path = {
    "EU": """ edw_delta_share.silver.dim_device_transaction""",
    "US": """ edw.silver.dim_device_transaction""",
    "CA": """ edw_delta_share.silver.dim_device_transaction""",
}

BUILD_DIM_DEVICE_OWNER_ACTIVATOR_QUERY = """
WITH
accounts AS (
  SELECT sam_number,
         MAX_BY(account_id, account_last_modified_date) AS account_id,
         MAX_BY(account_name, account_last_modified_date) AS account_name
  FROM datamodel_core.dim_organizations
  WHERE date = '{DATEID}'
  AND sam_number IS NOT NULL
  AND account_id IS NOT NULL
  GROUP BY 1),
gateway_history AS (
select device_id,
       gateway_id,
       timestamp as start_interval,
       lead(timestamp) over (partition by device_id order by timestamp) as end_interval
from clouddb.gateway_device_history),
online_devices as (
SELECT device_id,
       device_type,
       first_date AS first_heartbeat_date,
       latest_date AS last_heartbeat_date
FROM datamodel_core.lifetime_device_online
WHERE date = '{DATEID}'),
gateways_online AS (
SELECT
       gateway_history.gateway_id,
       online_devices.device_id,
       MIN(online_devices.first_heartbeat_date) AS first_heartbeat_date,
       MAX(online_devices.last_heartbeat_date) AS last_heartbeat_date
FROM online_devices
INNER JOIN gateway_history
  ON gateway_history.device_id = online_devices.device_id
  AND ((online_devices.first_heartbeat_date BETWEEN gateway_history.start_interval AND gateway_history.end_interval)
       OR (online_devices.first_heartbeat_date > gateway_history.start_interval AND gateway_history.end_interval IS NULL))
GROUP BY 1,2),
activations AS (
SELECT activations.date,
       activations.activated_at,
       activations.group_name,
       activations.email,
       activations.org_id,
       COALESCE(CASE WHEN len(activations.serial) = 10 and activations.serial REGEXP '[a-zA-Z]'
                     THEN activations.serial ELSE NULL END,
                gateways.serial, device_gateways.serial) AS serial,
       COALESCE(gateways.id, gateway_history.gateway_id, serial_gateways.id) AS gateway_id,
       COALESCE(gateways.product_id, device_gateways.product_id, serial_gateways.product_id) AS product_id,
       products.name AS device_type,
       COALESCE(CASE WHEN gateways.id IS NULL THEN activations.device_id ELSE NULL END,
                device_history.device_id,
                gateways.device_id) AS device_id
FROM datamodel_core_silver.stg_device_activation activations
/* source the gateway from the device_id if the activations.device_id is a gateway id*/
LEFT OUTER JOIN datamodel_core_bronze.raw_productsdb_gateways gateways
  ON gateways.date = '{DATEID}'
  AND gateways.id = activations.device_id
/* if activations.device_id is a gateway_id, source the device_id at time of activation */
LEFT OUTER JOIN gateway_history device_history
  ON device_history.gateway_id = activations.device_id
   AND ((activations.activated_at BETWEEN device_history.start_interval AND device_history.end_interval)
      OR (activations.activated_at >= device_history.start_interval AND device_history.end_interval IS NULL))
/* source the gateway id from the device_id if the device_id is a device_id */
LEFT OUTER JOIN gateway_history
  ON gateway_history.device_id = activations.device_id
  AND ((activations.activated_at BETWEEN gateway_history.start_interval AND gateway_history.end_interval)
      OR (activations.activated_at >= gateway_history.start_interval AND gateway_history.end_interval IS NULL))
/*if the gateway id is sourced from the device id find the serial number */
LEFT OUTER JOIN datamodel_core_bronze.raw_productsdb_gateways device_gateways
  ON device_gateways.date = '{DATEID}'
  AND device_gateways.id = gateway_history.gateway_id
/*if the serial is natively populated source the gateway id via the serial */
LEFT OUTER JOIN datamodel_core_bronze.raw_productsdb_gateways serial_gateways
  ON serial_gateways.date = '{DATEID}'
  AND serial_gateways.serial = activations.serial
LEFT OUTER JOIN definitions.products
  ON products.product_id = COALESCE(gateways.product_id, device_gateways.product_id, serial_gateways.product_id)
WHERE activations.date <= '{DATEID}'),
gateways AS (
SELECT
       activations.serial,
       activations.gateway_id,
       activations.device_id,
       activations.product_id,
       activations.device_type,
       gateways_online.first_heartbeat_date,
       gateways_online.last_heartbeat_date,
       activations.date AS last_activation_date,
       activations.org_id AS last_activation_org_id,
       activations.email AS last_activation_email,
       activations.group_name AS last_activation_group_name,
       activations.activated_at,
       ROW_NUMBER() OVER (PARTITION BY activations.serial ORDER BY activations.activated_at, gateways_online.last_heartbeat_date DESC) AS row_num
FROM activations
LEFT OUTER JOIN gateways_online
  ON gateways_online.gateway_id = activations.gateway_id
  AND gateways_online.device_id = activations.device_id
WHERE (activations.device_type like '%VG%'
       OR activations.device_type like '%CM%'
       OR activations.device_type like '%AG%')),
shipments AS (
SELECT * EXCEPT(device_serial, sam_number),
       CASE WHEN len(sam_number) < 2 THEN NULL ELSE sam_number END AS sam_number,
       REPLACE(device_serial, '-', '') AS serial,
       RANK() OVER (PARTITION BY REPLACE(device_serial, '-', '') ORDER BY shipped_date_ts DESC, transaction_line_id DESC) AS rank
FROM {DIM_TRANSACTION_HIERARCHY_PATH}
WHERE order_number NOT LIKE 'R%'
 AND LEN(device_serial) = 12
 AND order_date <= '{DATEID}')


SELECT
  '{DATEID}' AS date,
  gateways.serial,
  gateways.gateway_id,
  gateways.device_id,
  gateways.device_type,
  gateways.product_id,
  raw_devices.name AS device_name,
  gateways.first_heartbeat_date,
  gateways.last_heartbeat_date,
  gateways.last_activation_date,
  gateways.last_activation_org_id,
  gateways.last_activation_email,
  installers.partner_name AS last_activation_partner,
  gateways.last_activation_group_name,
  activator_organizations.sam_number AS last_activation_sam_number,
  activator_organizations.account_name AS last_activation_sfdc_account_name,
  activator_organizations.account_id AS last_activation_sdfc_account_id,
  shipments.order_date,
  shipments.transaction_id AS order_transaction_id,
  shipments.sam_number AS order_sam_number,
  shipments.order_number,
  order_accounts.account_id AS order_sfdc_account_id,
  order_accounts.account_name AS order_sfdc_account_name,
  CONCAT('https://samsara.lightning.force.com/lightning/r/Account/',activator_organizations.account_id,'/view') AS last_activation_sfdc_account_url,
  CONCAT('https://samsara.lightning.force.com/lightning/r/Account/',order_accounts.account_id,'/view') AS order_sfdc_account_url,
  CONCAT('https://cloud.samsara.com/o/', CAST(activator_organizations.org_id AS STRING), '/devices/', CAST(gateways.device_id AS STRING), '/vehicle') AS gateway_cloud_dashboard_url,
   CASE WHEN activator_organizations.sam_number != order_accounts.sam_number
            AND activator_organizations.sam_number IS NOT NULL
            AND order_accounts.sam_number IS NOT NULL THEN TRUE ELSE FALSE END AS is_owner_activator_mismatch,
  CASE WHEN activator_organizations.sam_number = order_accounts.sam_number
            AND activator_organizations.sam_number IS NOT NULL
            AND order_accounts.sam_number IS NOT NULL THEN 'match'
       WHEN activator_organizations.sam_number != order_accounts.sam_number
            AND activator_organizations.sam_number IS NOT NULL
            AND order_accounts.sam_number IS NOT NULL THEN 'mismatch'
       WHEN activator_organizations.sam_number IS NULL
            AND order_accounts.sam_number IS NOT NULL THEN 'missing_activator_sam_number'
       WHEN activator_organizations.sam_number IS NOT NULL
            AND order_accounts.sam_number IS NULL THEN 'missing_shipment_sam_number'
       WHEN activator_organizations.sam_number IS NULL
            AND order_accounts.sam_number IS NULL THEN 'missing_both_sam_numbers'
       END AS account_match_status
FROM gateways
LEFT OUTER JOIN datamodel_core_bronze.raw_productdb_devices raw_devices
  ON raw_devices.serial = gateways.serial
  AND raw_devices.date = '{DATEID}'
LEFT OUTER JOIN datamodel_core.dim_organizations activator_organizations
  ON activator_organizations.org_id = gateways.last_activation_org_id
  AND activator_organizations.date = '{DATEID}'
LEFT OUTER JOIN shipments
  ON shipments.serial = gateways.serial
  AND shipments.order_date < gateways.last_activation_date
  AND shipments.rank = 1
LEFT OUTER JOIN accounts order_accounts
  ON order_accounts.sam_number = shipments.sam_number
LEFT OUTER JOIN definitions.installer_domain_name installers
  ON gateways.last_activation_email ILIKE CONCAT('%',installers.domain_name, '%')
WHERE activator_organizations.internal_type = 0
      AND gateways.row_num = 1
"""

INSERT_DIM_DEVICE_OWNER_ACTIVATOR_QUERY = """
WITH
accounts AS (
  SELECT sam_number,
         MAX_BY(account_id, account_last_modified_date) AS account_id,
         MAX_BY(account_name, account_last_modified_date) AS account_name
  FROM datamodel_core.dim_organizations
  WHERE date = '{DATEID}'
  AND sam_number IS NOT NULL
  AND account_id IS NOT NULL
  GROUP BY 1),
gateway_history AS (
select device_id,
       gateway_id,
       timestamp as start_interval,
       lead(timestamp) over (partition by device_id order by timestamp) as end_interval
from clouddb.gateway_device_history),
online_devices as (
SELECT device_id,
       device_type,
       first_date AS first_heartbeat_date,
       latest_date AS last_heartbeat_date
FROM datamodel_core.lifetime_device_online
WHERE date = '{DATEID}'),
gateways_online AS (
SELECT
       gateway_history.gateway_id,
       online_devices.device_id,
       MIN(online_devices.first_heartbeat_date) AS first_heartbeat_date,
       MAX(online_devices.last_heartbeat_date) AS last_heartbeat_date
FROM online_devices
INNER JOIN gateway_history
  ON gateway_history.device_id = online_devices.device_id
  AND ((online_devices.first_heartbeat_date BETWEEN gateway_history.start_interval AND gateway_history.end_interval)
       OR (online_devices.first_heartbeat_date > gateway_history.start_interval AND gateway_history.end_interval IS NULL))
GROUP BY 1,2),
activations AS (
SELECT activations.date,
       activations.activated_at,
       activations.group_name,
       activations.email,
       activations.org_id,
       COALESCE(CASE WHEN len(activations.serial) = 10 and activations.serial REGEXP '[a-zA-Z]'
                     THEN activations.serial ELSE NULL END,
                gateways.serial, device_gateways.serial) AS serial,
       COALESCE(gateways.id, gateway_history.gateway_id, serial_gateways.id) AS gateway_id,
       COALESCE(gateways.product_id, device_gateways.product_id, serial_gateways.product_id) AS product_id,
       products.name AS device_type,
       COALESCE(CASE WHEN gateways.id IS NULL THEN activations.device_id ELSE NULL END,
                device_history.device_id,
                gateways.device_id) AS device_id
FROM datamodel_core_silver.stg_device_activation activations
/* source the gateway from the device_id if the activations.device_id is a gateway id*/
LEFT OUTER JOIN datamodel_core_bronze.raw_productsdb_gateways gateways
  ON gateways.date = '{DATEID}'
  AND gateways.id = activations.device_id
/* if activations.device_id is a gateway_id, source the device_id at time of activation */
LEFT OUTER JOIN gateway_history device_history
  ON device_history.gateway_id = activations.device_id
   AND ((activations.activated_at BETWEEN device_history.start_interval AND device_history.end_interval)
      OR (activations.activated_at >= device_history.start_interval AND device_history.end_interval IS NULL))
/* source the gateway id from the device_id if the device_id is a device_id */
LEFT OUTER JOIN gateway_history
  ON gateway_history.device_id = activations.device_id
  AND ((activations.activated_at BETWEEN gateway_history.start_interval AND gateway_history.end_interval)
      OR (activations.activated_at >= gateway_history.start_interval AND gateway_history.end_interval IS NULL))
/*if the gateway id is sourced from the device id find the serial number */
LEFT OUTER JOIN datamodel_core_bronze.raw_productsdb_gateways device_gateways
  ON device_gateways.date = '{DATEID}'
  AND device_gateways.id = gateway_history.gateway_id
/*if the serial is natively populated source the gateway id via the serial */
LEFT OUTER JOIN datamodel_core_bronze.raw_productsdb_gateways serial_gateways
  ON serial_gateways.date = '{DATEID}'
  AND serial_gateways.serial = activations.serial
LEFT OUTER JOIN definitions.products
  ON products.product_id = COALESCE(gateways.product_id, device_gateways.product_id, serial_gateways.product_id)
WHERE activations.date = '{DATEID}'),
gateways_ranked AS (
SELECT
       activations.serial,
       activations.gateway_id,
       activations.device_id,
       activations.product_id,
       activations.device_type,
       gateways_online.first_heartbeat_date,
       gateways_online.last_heartbeat_date,
       activations.date AS last_activation_date,
       activations.org_id AS last_activation_org_id,
       activations.email AS last_activation_email,
       activations.group_name AS last_activation_group_name,
       activations.activated_at,
       ROW_NUMBER() OVER (PARTITION BY activations.serial ORDER BY activations.activated_at, gateways_online.last_heartbeat_date DESC) AS rnk
FROM activations
LEFT OUTER JOIN gateways_online
  ON gateways_online.gateway_id = activations.gateway_id
  AND gateways_online.device_id = activations.device_id
WHERE (activations.device_type like '%VG%'
       OR activations.device_type like '%CM%'
       OR activations.device_type like '%AG%')),
gateways AS (
SELECT *
FROM gateways_ranked
WHERE rnk = 1),
shipments AS (
SELECT * EXCEPT(device_serial),
       REPLACE(device_serial, '-', '') AS serial,
       ROW_NUMBER() OVER (PARTITION BY REPLACE(device_serial, '-', '') ORDER BY shipped_date_ts DESC, transaction_line_id DESC) AS rnk
FROM {DIM_TRANSACTION_HIERARCHY_PATH}
WHERE order_number NOT LIKE 'R%'),
/* updates last heartbeat date in this CTE */
previous_dim_partition AS (
  SELECT prev.* EXCEPT (
        prev.last_heartbeat_date),
         COALESCE(gateways_online.last_heartbeat_date, prev.last_heartbeat_date) AS last_heartbeat_date,
         ROW_NUMBER() OVER (PARTITION BY prev.device_id, prev.serial, prev.last_activation_org_id, prev.last_activation_email ORDER BY (SELECT NULL)) AS rnk
  FROM datamodel_core.dim_device_owner_activator prev
  LEFT OUTER JOIN gateways_online
    ON gateways_online.gateway_id = prev.gateway_id
    AND gateways_online.device_id = prev.device_id
  WHERE prev.date = DATEADD(DAY, -1, DATE('{DATEID}'))),
prev AS (
SELECT *
FROM previous_dim_partition
WHERE rnk = 1)

SELECT
  '{DATEID}' AS date,
  COALESCE(gateways.serial, prev.serial) AS serial,
  COALESCE(gateways.gateway_id, prev.gateway_id) AS gateway_id,
  COALESCE(gateways.device_id, prev.device_id) AS device_id,
  COALESCE(gateways.device_type, prev.device_type) AS device_type,
  COALESCE(gateways.product_id, prev.product_id) AS product_id,
  COALESCE(raw_devices.name, prev.device_name) AS device_name,
  COALESCE(gateways.first_heartbeat_date, prev.first_heartbeat_date) AS first_heartbeat_date,
  COALESCE(gateways.last_heartbeat_date, prev.last_heartbeat_date) AS last_heartbeat_date,
  COALESCE(gateways.last_activation_date, prev.last_activation_date) AS last_activation_date,
  COALESCE(gateways.last_activation_org_id, prev.last_activation_org_id) AS last_activation_org_id,
  COALESCE(gateways.last_activation_email, prev.last_activation_email) AS last_activation_email,
  COALESCE(installers.partner_name, prev.last_activation_partner) AS last_activation_partner,
  COALESCE(gateways.last_activation_group_name, prev.last_activation_group_name) AS last_activation_group_name,
  COALESCE(activator_organizations.sam_number, prev.last_activation_sam_number) AS last_activation_sam_number,
  COALESCE(activator_organizations.account_name, prev.last_activation_sfdc_account_name) AS last_activation_sfdc_account_name,
  COALESCE(activator_organizations.account_id, prev.last_activation_sdfc_account_id) AS last_activation_sdfc_account_id,
  COALESCE(shipments.order_date, prev.order_date) AS order_date,
  COALESCE(shipments.transaction_id, prev.order_transaction_id) AS order_transaction_id,
  COALESCE(shipments.sam_number, prev.order_sam_number) AS order_sam_number,
  COALESCE(shipments.order_number, prev.order_number) AS order_number,
  COALESCE(order_accounts.account_id, prev.order_sfdc_account_id) AS order_sfdc_account_id,
  COALESCE(order_accounts.account_name, prev.order_sfdc_account_name) AS order_sfdc_account_name,
  COALESCE(CONCAT('https://samsara.lightning.force.com/lightning/r/Account/',activator_organizations.account_id,'/view'),
  prev.last_activation_sfdc_account_url) AS last_activation_sfdc_account_url,
  COALESCE(
  CONCAT('https://samsara.lightning.force.com/lightning/r/Account/',order_accounts.account_id,'/view'),
  prev.order_sfdc_account_url) AS order_sfdc_account_url,
  COALESCE(CONCAT('https://cloud.samsara.com/o/', CAST(activator_organizations.org_id AS STRING), '/devices/', CAST(gateways.device_id AS STRING), '/vehicle'), prev.gateway_cloud_dashboard_url) AS gateway_cloud_dashboard_url,
  CASE WHEN prev.is_owner_activator_mismatch IS NOT NULL
            THEN prev.is_owner_activator_mismatch
       WHEN activator_organizations.sam_number != order_accounts.sam_number
            AND activator_organizations.sam_number IS NOT NULL
            AND order_accounts.sam_number IS NOT NULL THEN TRUE ELSE FALSE END AS is_owner_activator_mismatch,
 CASE WHEN prev.account_match_status IS NOT NULL THEN prev.account_match_status
      WHEN activator_organizations.sam_number = order_accounts.sam_number
            AND activator_organizations.sam_number IS NOT NULL
            AND order_accounts.sam_number IS NOT NULL THEN 'match'
       WHEN activator_organizations.sam_number != order_accounts.sam_number
            AND activator_organizations.sam_number IS NOT NULL
            AND order_accounts.sam_number IS NOT NULL THEN 'mismatch'
       WHEN activator_organizations.sam_number IS NULL
            AND order_accounts.sam_number IS NOT NULL THEN 'missing_activator_sam_number'
       WHEN activator_organizations.sam_number IS NOT NULL
            AND order_accounts.sam_number IS NULL THEN 'missing_shipment_sam_number'
       WHEN activator_organizations.sam_number IS NULL
            AND order_accounts.sam_number IS NULL THEN 'missing_both_sam_numbers'
       END AS account_match_status
FROM prev
FULL OUTER JOIN gateways
  ON gateways.serial = prev.serial
  AND gateways.device_id = prev.device_id
  AND gateways.last_activation_email = prev.last_activation_email
  AND gateways.last_activation_org_id = prev.last_activation_org_id
LEFT OUTER JOIN datamodel_core_bronze.raw_productdb_devices raw_devices
  ON raw_devices.serial = COALESCE(gateways.serial, prev.serial)
  AND raw_devices.date = '{DATEID}'
LEFT OUTER JOIN datamodel_core.dim_organizations activator_organizations
  ON activator_organizations.org_id = COALESCE(gateways.last_activation_org_id, prev.last_activation_org_id)
  AND activator_organizations.date = '{DATEID}'
LEFT OUTER JOIN shipments
  ON shipments.serial = gateways.serial
  AND shipments.order_date < gateways.last_activation_date
  AND shipments.rnk = 1
LEFT OUTER JOIN accounts order_accounts
  ON order_accounts.sam_number = shipments.sam_number
LEFT OUTER JOIN definitions.installer_domain_name installers
  ON COALESCE(gateways.last_activation_email, prev.last_activation_email) ILIKE CONCAT('%',installers.domain_name, '%')
WHERE activator_organizations.internal_type = 0
"""


stg_device_activation_description = build_table_description(
    table_desc="""This is a staging table for device activation events.""",
    row_meaning="""One row in this table represents an activation event for a device from the audits table""",
    table_type=TableType.TRANSACTIONAL_FACT,
    freshness_slo_updated_by="9am PST",
)


@asset(
    name="stg_device_activation",
    owners=["team:DataEngineering"],
    description=stg_device_activation_description,
    compute_kind="sql",
    metadata={
        "database": databases["database_silver"],
        "owners": ["team:DataEngineering"],
        "write_mode": WarehouseWriteMode.overwrite,
        "schema": STG_DEVICE_ACTIVATION_SCHEMA,
        "code_location": get_code_location(),
        "description": stg_device_activation_description,
    },
    required_resource_keys=REQUIRED_RESOURCE_KEYS,
    group_name=GROUP_NAME,
    partitions_def=ACTIVATIONS_PARTITION_DEFS,
    key_prefix=[AWSRegions.US_WEST_2.value, databases["database_silver"]],
    backfill_policy=BackfillPolicy.multi_run(max_partitions_per_run=30),
)
def stg_device_activation(context) -> DataFrame:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    start_date = context.asset_partition_key_range.start
    end_date = context.asset_partition_key_range.end
    query = STG_DEVICE_ACTIVATION_QUERY.format(
        START_DATEID=start_date, END_DATEID=end_date, **databases
    )
    context.log.info(query)
    df = spark.sql(query)
    context.log.info(query)
    context.log.info(df)
    return df


@asset(
    name="stg_device_activation",
    owners=["team:DataEngineering"],
    description=stg_device_activation_description,
    compute_kind="sql",
    metadata={
        "database": databases["database_silver"],
        "owners": ["team:DataEngineering"],
        "write_mode": WarehouseWriteMode.overwrite,
        "schema": STG_DEVICE_ACTIVATION_SCHEMA,
        "code_location": get_code_location(),
        "description": stg_device_activation_description,
    },
    required_resource_keys=REQUIRED_RESOURCE_KEYS_EU,
    group_name=GROUP_NAME + "_eu",
    partitions_def=ACTIVATIONS_PARTITION_DEFS,
    key_prefix=[AWSRegions.EU_WEST_1.value, databases["database_silver"]],
    backfill_policy=BackfillPolicy.multi_run(max_partitions_per_run=30),
)
def stg_device_activation_eu(context) -> DataFrame:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    start_date = context.asset_partition_key_range.start
    end_date = context.asset_partition_key_range.end
    query = STG_DEVICE_ACTIVATION_QUERY.format(
        START_DATEID=start_date, END_DATEID=end_date, **databases
    )
    context.log.info(query)
    df = spark.sql(query)
    context.log.info(df)
    return df


@asset(
    name="stg_device_activation",
    owners=["team:DataEngineering"],
    description=stg_device_activation_description,
    compute_kind="sql",
    metadata={
        "database": databases["database_silver"],
        "owners": ["team:DataEngineering"],
        "write_mode": WarehouseWriteMode.overwrite,
        "schema": STG_DEVICE_ACTIVATION_SCHEMA,
        "code_location": get_code_location(),
        "description": stg_device_activation_description,
    },
    required_resource_keys=REQUIRED_RESOURCE_KEYS_CA,
    group_name=GROUP_NAME + "_ca",
    partitions_def=ACTIVATIONS_PARTITION_DEFS,
    key_prefix=[AWSRegions.CA_CENTRAL_1.value, databases["database_silver"]],
    backfill_policy=BackfillPolicy.multi_run(max_partitions_per_run=30),
)
def stg_device_activation_ca(context) -> DataFrame:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    start_date = context.asset_partition_key_range.start
    end_date = context.asset_partition_key_range.end
    query = STG_DEVICE_ACTIVATION_QUERY.format(
        START_DATEID=start_date, END_DATEID=end_date, **databases
    )
    context.log.info(query)
    df = spark.sql(query)
    context.log.info(df)
    return df


dim_device_owner_activator_description = build_table_description(
    table_desc="""This table matches most recent shipment and activation information for gateway devices to determine the correspondence between owner and activator.""",
    row_meaning="""One row in this table represents the relevant owner and activator metadata for the coresponding date partition.""",
    table_type=TableType.DAILY_DIMENSION,
    freshness_slo_updated_by="9am PST",
)


@asset(
    name="dim_device_owner_activator",
    owners=["team:DataEngineering"],
    description=dim_device_owner_activator_description,
    compute_kind="sql",
    metadata={
        "database": databases["database_gold"],
        "owners": ["team:DataEngineering"],
        "write_mode": WarehouseWriteMode.overwrite,
        "schema": DIM_DEVICE_OWNER_ACTIVATOR_SCHEMA,
        "code_location": get_code_location(),
        "primary_keys": [
            "date",
            "serial",
            "device_id",
            "last_activation_org_id",
            "last_activation_email",
        ],
        "description": dim_device_owner_activator_description,
    },
    required_resource_keys=REQUIRED_RESOURCE_KEYS,
    group_name=GROUP_NAME,
    partitions_def=DIM_DEVICE_OWNER_ACTIVATOR_PARTITIONS_DEF,
    key_prefix=[AWSRegions.US_WEST_2.value, databases["database_gold"]],
    deps=[
        AssetDep(
            AssetKey(
                [
                    AWSRegions.US_WEST_2.value,
                    databases["database_bronze"],
                    "raw_productdb_devices",
                ]
            ),
            partition_mapping=IdentityPartitionMapping(),
        ),
        AssetDep(
            AssetKey(
                [
                    AWSRegions.US_WEST_2.value,
                    databases["database_gold"],
                    "dq_dim_organizations",
                ]
            ),
            partition_mapping=IdentityPartitionMapping(),
        ),
        AssetDep(
            AssetKey(
                [
                    AWSRegions.US_WEST_2.value,
                    databases["database_gold"],
                    "lifetime_device_online",
                ]
            ),
            partition_mapping=IdentityPartitionMapping(),
        ),
        AssetDep(
            AssetKey(
                [
                    AWSRegions.US_WEST_2.value,
                    databases["database_bronze"],
                    "raw_productsdb_gateways",
                ]
            ),
            partition_mapping=IdentityPartitionMapping(),
        ),
        AssetDep(
            AssetKey(
                [
                    AWSRegions.US_WEST_2.value,
                    databases["database_gold"],
                    "dim_device_owner_activator",
                ]
            ),
            partition_mapping=TimeWindowPartitionMapping(
                start_offset=-1, end_offset=-1
            ),
        ),
    ],
)
def dim_device_owner_activator(context) -> DataFrame:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    partition_date_str = context.partition_key
    if partition_date_str == DIM_DEVICE_OWNER_ACTIVATOR_PARTITIONS_DEF.start.strftime(
        "%Y-%m-%d"
    ):
        query = BUILD_DIM_DEVICE_OWNER_ACTIVATOR_QUERY.format(
            DATEID=partition_date_str,
            DIM_TRANSACTION_HIERARCHY_PATH=dim_transaction_hierarchy_path["US"],
            **databases
        )
    else:
        query = INSERT_DIM_DEVICE_OWNER_ACTIVATOR_QUERY.format(
            DATEID=partition_date_str,
            DIM_TRANSACTION_HIERARCHY_PATH=dim_transaction_hierarchy_path["US"],
            **databases
        )
    context.log.info(query)
    df = spark.sql(query).toPandas()
    df["owner_activator_similarity_score"] = 0.0
    for index, row in df.iterrows():
        df.at[index, "owner_activator_similarity_score"] = similarity_score(
            row["order_sfdc_account_name"], row["last_activation_sfdc_account_name"]
        )
    df = spark.createDataFrame(df)
    context.log.info(df)
    return df


@asset(
    name="dim_device_owner_activator",
    owners=["team:DataEngineering"],
    description=dim_device_owner_activator_description,
    compute_kind="sql",
    metadata={
        "database": databases["database_gold"],
        "owners": ["team:DataEngineering"],
        "write_mode": WarehouseWriteMode.overwrite,
        "schema": DIM_DEVICE_OWNER_ACTIVATOR_SCHEMA,
        "code_location": get_code_location(),
        "primary_keys": ["date", "serial", "device_id"],
        "description": dim_device_owner_activator_description,
    },
    required_resource_keys=REQUIRED_RESOURCE_KEYS_EU,
    group_name=GROUP_NAME + "_eu",
    partitions_def=DIM_DEVICE_OWNER_ACTIVATOR_PARTITIONS_DEF,
    key_prefix=[AWSRegions.EU_WEST_1.value, databases["database_gold"]],
    deps=[
        AssetDep(
            AssetKey(
                [
                    AWSRegions.EU_WEST_1.value,
                    databases["database_bronze"],
                    "raw_productdb_devices",
                ]
            ),
            partition_mapping=IdentityPartitionMapping(),
        ),
        AssetDep(
            AssetKey(
                [
                    AWSRegions.EU_WEST_1.value,
                    databases["database_gold"],
                    "dq_dim_organizations",
                ]
            ),
            partition_mapping=IdentityPartitionMapping(),
        ),
        AssetDep(
            AssetKey(
                [
                    AWSRegions.EU_WEST_1.value,
                    databases["database_gold"],
                    "lifetime_device_online",
                ]
            ),
            partition_mapping=IdentityPartitionMapping(),
        ),
        AssetDep(
            AssetKey(
                [
                    AWSRegions.EU_WEST_1.value,
                    databases["database_bronze"],
                    "raw_productsdb_gateways",
                ]
            ),
            partition_mapping=IdentityPartitionMapping(),
        ),
        AssetDep(
            AssetKey(
                [
                    AWSRegions.EU_WEST_1.value,
                    databases["database_gold"],
                    "dim_device_owner_activator",
                ]
            ),
            partition_mapping=TimeWindowPartitionMapping(
                start_offset=-1, end_offset=-1
            ),
        ),
    ],
)
def dim_device_owner_activator_eu(context) -> DataFrame:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    partition_date_str = context.partition_key
    if partition_date_str == DIM_DEVICE_OWNER_ACTIVATOR_PARTITIONS_DEF.start.strftime(
        "%Y-%m-%d"
    ):
        query = BUILD_DIM_DEVICE_OWNER_ACTIVATOR_QUERY.format(
            DATEID=partition_date_str,
            DIM_TRANSACTION_HIERARCHY_PATH=dim_transaction_hierarchy_path["EU"],
            **databases
        )
    else:
        query = INSERT_DIM_DEVICE_OWNER_ACTIVATOR_QUERY.format(
            DATEID=partition_date_str,
            DIM_TRANSACTION_HIERARCHY_PATH=dim_transaction_hierarchy_path["EU"],
            **databases
        )
    context.log.info(query)
    df = spark.sql(query).toPandas()
    df["owner_activator_similarity_score"] = 0.0
    for index, row in df.iterrows():
        df.at[index, "owner_activator_similarity_score"] = similarity_score(
            row["order_sfdc_account_name"], row["last_activation_sfdc_account_name"]
        )
    df = spark.createDataFrame(df)
    context.log.info(df)
    return df


@asset(
    name="dim_device_owner_activator",
    owners=["team:DataEngineering"],
    description=dim_device_owner_activator_description,
    compute_kind="sql",
    metadata={
        "database": databases["database_gold"],
        "owners": ["team:DataEngineering"],
        "write_mode": WarehouseWriteMode.overwrite,
        "schema": DIM_DEVICE_OWNER_ACTIVATOR_SCHEMA,
        "code_location": get_code_location(),
        "primary_keys": ["date", "serial", "device_id"],
        "description": dim_device_owner_activator_description,
    },
    required_resource_keys=REQUIRED_RESOURCE_KEYS_CA,
    group_name=GROUP_NAME + "_ca",
    partitions_def=DIM_DEVICE_OWNER_ACTIVATOR_PARTITIONS_DEF_CA,
    key_prefix=[AWSRegions.CA_CENTRAL_1.value, databases["database_gold"]],
    deps=[
        AssetDep(
            AssetKey(
                [
                    AWSRegions.CA_CENTRAL_1.value,
                    databases["database_bronze"],
                    "raw_productdb_devices",
                ]
            ),
            partition_mapping=IdentityPartitionMapping(),
        ),
        AssetDep(
            AssetKey(
                [
                    AWSRegions.CA_CENTRAL_1.value,
                    databases["database_gold"],
                    "dq_dim_organizations",
                ]
            ),
            partition_mapping=IdentityPartitionMapping(),
        ),
        AssetDep(
            AssetKey(
                [
                    AWSRegions.CA_CENTRAL_1.value,
                    databases["database_gold"],
                    "lifetime_device_online",
                ]
            ),
            partition_mapping=IdentityPartitionMapping(),
        ),
        AssetDep(
            AssetKey(
                [
                    AWSRegions.CA_CENTRAL_1.value,
                    databases["database_bronze"],
                    "raw_productsdb_gateways",
                ]
            ),
            partition_mapping=IdentityPartitionMapping(),
        ),
        AssetDep(
            AssetKey(
                [
                    AWSRegions.CA_CENTRAL_1.value,
                    databases["database_gold"],
                    "dim_device_owner_activator",
                ]
            ),
            partition_mapping=TimeWindowPartitionMapping(
                start_offset=-1, end_offset=-1
            ),
        ),
    ],
)
def dim_device_owner_activator_ca(context) -> DataFrame:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    partition_date_str = context.partition_key
    if partition_date_str == (
        DIM_DEVICE_OWNER_ACTIVATOR_PARTITIONS_DEF_CA.start.strftime("%Y-%m-%d")
    ):
        query = BUILD_DIM_DEVICE_OWNER_ACTIVATOR_QUERY.format(
            DATEID=partition_date_str,
            DIM_TRANSACTION_HIERARCHY_PATH=dim_transaction_hierarchy_path["CA"],
            **databases
        )
    else:
        query = INSERT_DIM_DEVICE_OWNER_ACTIVATOR_QUERY.format(
            DATEID=partition_date_str,
            DIM_TRANSACTION_HIERARCHY_PATH=dim_transaction_hierarchy_path["CA"],
            **databases
        )
    context.log.info(query)
    df = spark.sql(query).toPandas()
    df["owner_activator_similarity_score"] = 0.0
    for index, row in df.iterrows():
        df.at[index, "owner_activator_similarity_score"] = similarity_score(
            row["order_sfdc_account_name"], row["last_activation_sfdc_account_name"]
        )
    df = spark.createDataFrame(df)
    df = (
        df.withColumn("device_name", F.lit(None).cast(StringType()))
        .withColumn("first_heartbeat_date", F.lit(None).cast(StringType()))
        .withColumn("last_activation_partner", F.lit(None).cast(StringType()))
        .withColumn("last_activation_sam_number", F.lit(None).cast(StringType()))
        .withColumn("last_activation_sdfc_account_id", F.lit(None).cast(StringType()))
        .withColumn("last_activation_sfdc_account_name", F.lit(None).cast(StringType()))
        .withColumn("last_activation_sfdc_account_url", F.lit(None).cast(StringType()))
        .withColumn("last_heartbeat_date", F.lit(None).cast(StringType()))
        .withColumn("order_date", F.lit(None).cast(StringType()))
        .withColumn("order_number", F.lit(None).cast(StringType()))
        .withColumn("order_sam_number", F.lit(None).cast(StringType()))
        .withColumn("order_sfdc_account_id", F.lit(None).cast(StringType()))
        .withColumn("order_sfdc_account_name", F.lit(None).cast(StringType()))
        .withColumn("order_sfdc_account_url", F.lit(None).cast(StringType()))
    )
    context.log.info(df)
    return df


dqs["dim_device_owner_activator"].append(
    PrimaryKeyDQCheck(
        name="dq_pk_dim_device_owner_activator",
        table="dim_device_owner_activator",
        primary_keys=[
            "date",
            "device_id",
            "serial",
            "last_activation_org_id",
            "last_activation_email",
        ],
        blocking=True,
        database=databases["database_gold_dev"],
    )
)

dqs["dim_device_owner_activator"].append(
    NonEmptyDQCheck(
        name="dq_empty_dim_device_owner_activator",
        table="dim_device_owner_activator",
        blocking=True,
        database=databases["database_gold_dev"],
    )
)

dqs["dim_device_owner_activator"].append(
    TrendDQCheck(
        name="dq_trend_dim_device_owner_activator",
        database=databases["database_gold_dev"],
        table="dim_device_owner_activator",
        blocking=False,
        tolerance=0.02,
    )
)

dqs["dim_device_owner_activator"].append(
    JoinableDQCheck(
        name="dq_joinable_dim_device_owner_activator_to_dim_devices",
        database=databases["database_gold_dev"],
        database_2=databases["database_gold_dev"],
        input_asset_1="dim_device_owner_activator",
        input_asset_2="dim_devices",
        join_keys=[("device_id", "device_id")],
        blocking=True,
        null_right_table_rows_ratio=0.01,
    )
)

dq_assets = dqs.generate()
