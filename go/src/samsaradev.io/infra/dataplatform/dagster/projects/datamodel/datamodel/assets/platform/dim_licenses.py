import json
import re
from collections import defaultdict
from datetime import datetime, timedelta

import numpy
import pandas as pd
from dagster import (
    AssetDep,
    AssetKey,
    DailyPartitionsDefinition,
    IdentityPartitionMapping,
    LastPartitionMapping,
    Nothing,
    TimeWindowPartitionMapping,
    asset,
)
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import DateType

from ...common.constants import (
    SLACK_ALERTS_CHANNEL_DATA_ENGINEERING,
    sam_number_default_description,
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
    adjust_partition_def_for_canada,
    apply_db_overrides,
    build_table_description,
    get_all_regions,
    get_code_location,
)

GROUP_NAME = "dim_licenses"
REQUIRED_RESOURCE_KEYS = {"databricks_pyspark_step_launcher"}
REQUIRED_RESOURCE_KEYS_EU = {"databricks_pyspark_step_launcher_eu"}
REQUIRED_RESOURCE_KEYS_CA = {"databricks_pyspark_step_launcher_ca"}
DAILY_PARTITIONS_DEF = DailyPartitionsDefinition(start_date="2023-12-09", end_offset=0)
DAILY_PARTITIONS_DEF_CA = adjust_partition_def_for_canada(DAILY_PARTITIONS_DEF)

dqs = DQGroup(
    group_name=GROUP_NAME,
    partition_def=DAILY_PARTITIONS_DEF,
    slack_alerts_channel=SLACK_ALERTS_CHANNEL_DATA_ENGINEERING,
    regions=get_all_regions(),
)

databases = {
    "database_bronze": Database.DATAMODEL_PLATFORM_BRONZE,
    "database_silver": Database.DATAMODEL_PLATFORM_SILVER,
    "database_gold": Database.DATAMODEL_PLATFORM,
    "database_core_bronze": Database.DATAMODEL_CORE_BRONZE,
}

database_dev_overrides = {
    "database_bronze_dev": Database.DATAMODEL_DEV,
    "database_silver_dev": Database.DATAMODEL_DEV,
    "database_gold_dev": Database.DATAMODEL_DEV,
}

databases = apply_db_overrides(databases, database_dev_overrides)

tier_map = {
    "addon": "Add-On",
    "asat": "Add-On",
    "compliance": "Add-On",
    "cm-review-ent": "Add-On",
    "cm-plus": "Add-On",
    "basic": "Essential",
    "ent": "Enterprise",
    "enterprise": "Enterprise",
    "ess": "Essential",
    "ps": "Public sector",
    "prem": "Premier",
    "pubsec": "Public sector",
    "premier": "Premier",
    "premier-ps": "Premier Public Sector",
    "plus": "Premier",
    "express": "Enterprise",
}

device_map = {
    "ag-pwr": "Powered Asset Gateway",
    "ag-pwr-plus": "Powered Asset Gateway Plus",
    "ag-unpwr": "Unpowered Asset Gateway",
    "ag-trlr": "Smart Trailer",
    "ag4": "Unpowered Asset Gateway",
    "ag4p": "Powered Asset Gateway",
    "ag2": "Powered Asset Gateway Plus",
    "ag2t": "Powered Asset Gateway Plus",
    "at-tag": "Asset Tag",
    "cm1": "Outward Camera",
    "cm2": "Dual Camera",
    "cm-d": "Dual Camera",
    "cm-s": "Outward Camera",
    "sc1": "Site Third Party Camera Stream",
    "sc11": "Samsara Dome Camera",
    "sc21": "Samsara Bullet Camera",
    "sg1": "Site Gateway",
    "sg1-g": "Site Gateway SG1-G",
    "sg1-g32": "Site Gateway SG1-G32",
    "sg1x": "Site Gateway Lite",
    "vg": "Vehicle Gateway",
    "trlr": "Smart Trailer",
}

# Copy all the data to preserve the integrity of the raw data.
stg_active_licenses_query = """
    SELECT DATE('{DATEID}') AS date,
        CAST(orgid AS long) AS org_id,
        sku,
        CAST(islegacylicense AS BOOLEAN) AS is_legacy_license,
        COALESCE(CAST(istrial AS BOOLEAN), FALSE) AS is_trial,
	    CAST(quantity AS int) AS quantity,
        CAST(refreshedat AS TIMESTAMP) AS refreshed_at,
        CAST(dynamoexpireat AS TIMESTAMP) AS dynamo_expire_at,
        TIMESTAMP(FROM_UNIXTIME(_approximate_creation_date_time/1000.0)) AS approximate_creation_date_time,
        _event_id AS event_id,
        samnumber AS sam_number
    FROM dynamodb.cached_active_skus TIMESTAMP AS OF '{DATEID} 23:59:59.999'
"""

stg_active_licenses_backfill_query = """
    SELECT DATE('{DATEID}') AS date,
        org_id,
        sku,
        is_legacy_license,
        is_trial,
	    quantity,
        refreshed_at,
        dynamo_expire_at,
        approximate_creation_date_time,
        event_id,
        sam_number
    FROM {database_bronze}.stg_active_licenses
    WHERE date = (SELECT MAX(date) AS date
                    FROM {database_bronze}.stg_active_licenses licenses
                    WHERE date <= '{DATEID}')
"""

STG_ACTIVE_LICENSES_SCHEMA = [
    {
        "name": "date",
        "type": "date",
        "nullable": False,
        "metadata": {
            "comment": "Date of snapshot of copy of dynamodb.cached_active_skus table."
        },
    },
    {
        "name": "org_id",
        "type": "long",
        "nullable": False,
        "metadata": {"comment": "ID of organization of license sku holder."},
    },
    {
        "name": "sku",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "License sku, representing organization`s subscription."
        },
    },
    {
        "name": "is_legacy_license",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Boolean value equal to TRUE when license is part of pricing and packaging program, otherwise FALSE."
        },
    },
    {
        "name": "is_trial",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Boolean value equal to TRUE when license is under free trial, otherwise FALSE or NULL."
        },
    },
    {
        "name": "quantity",
        "type": "integer",
        "nullable": True,
        "metadata": {
            "comment": "Quantity of licenses held by organization as of date."
        },
    },
    {
        "name": "refreshed_at",
        "type": "timestamp",
        "nullable": False,
        "metadata": {
            "comment": "Timestamp for when polling of license sku to org_id occurred and was written to dynamodb table."
        },
    },
    {
        "name": "dynamo_expire_at",
        "type": "timestamp",
        "nullable": False,
        "metadata": {
            "comment": "Timestamp for field entry into dynamodb to expire at."
        },
    },
    {
        "name": "approximate_creation_date_time",
        "type": "timestamp",
        "nullable": False,
        "metadata": {
            "comment": "Date of snapshot of copy of dynamodb.cached_active_skus table."
        },
    },
    {
        "name": "event_id",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "UUID of event to copy row out of dynamodb and into Databricks."
        },
    },
    {
        "name": "sam_number",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": sam_number_default_description},
    },
]

product_hierarchy_path = {
    "EU": """ edw_delta_share.salesforce_sterling.plm_product_hierarchy """,
    "US": """ edw.salesforce_sterling.plm_product_hierarchy """,
    "CA": """ edw_delta_share.salesforce_sterling.plm_product_hierarchy """,
}

build_dim_licenses_query = """
WITH dim_org AS (
SELECT do.id AS org_id,
       do.internal_type
FROM datamodel_core_bronze.raw_clouddb_organizations do
WHERE do.date = '{DATEID}'),
sku_metadata AS (
SELECT product_code,
     MAX_BY(description, last_modified_date) AS description,
     MAX_BY(family, last_modified_date) AS product_family,
     MAX_BY(sub_product_line_c, last_modified_date) AS sub_product_line,
     MAX_BY(name, last_modified_date) AS name,
     MAX(last_modified_date) AS last_modified_date
FROM {PRODUCT_HIERARCHY_PATH}
WHERE product_code LIKE 'LIC%'
GROUP BY 1)

SELECT  DATE('{DATEID}') AS date,
        licenses.org_id,
        licenses.sku,
        licenses.quantity AS quantity,
        licenses.is_legacy_license,
        licenses.is_trial,
        licenses.refreshed_at,
        licenses.date AS date_first_active,
        /*if the date is the first date that the sku appears and the quantity is 0
        the license cannot be expired.
        if the quantity is 0, the dynamodb record was updated with the license's true expiration
        and the license should be marked as expired */
        CASE WHEN COALESCE(licenses.quantity, 0) > 0
             THEN CAST(NULL AS DATE)
             WHEN DATE(dynamo_expire_at) < '{DATEID}'
             THEN DATE(dynamo_expire_at)
             ELSE DATE('{DATEID}') END AS date_expired,
        licenses.sam_number,
        dim_org.internal_type AS internal_type,
         --for metadata fields, take the most recently updated field by sku,
        sku.name,
        sku.description,
        sku.product_family,
        sku.sub_product_line
 FROM {database_bronze}.stg_active_licenses licenses
 LEFT OUTER JOIN dim_org
  ON dim_org.org_id = licenses.org_id
 LEFT OUTER JOIN sku_metadata sku
        ON sku.product_code = licenses.sku
 WHERE licenses.date = '{DATEID}'
"""

insert_dim_licenses_query = """
WITH
dim_org AS (
SELECT do.id AS org_id,
       do.internal_type
FROM datamodel_core_bronze.raw_clouddb_organizations do
WHERE do.date = '{DATEID}'),
stg_licenses AS (
SELECT  DATE('{DATEID}') AS date,
        licenses.org_id,
        licenses.sku,
        licenses.quantity,
        licenses.is_legacy_license,
        licenses.is_trial,
        licenses.refreshed_at,
        licenses.date AS date_first_active,
        --if the date is the first date and the sku appears, it cannot be expired
        CAST(NULL AS DATE) AS date_expired,
        licenses.dynamo_expire_at,
        licenses.sam_number,
        dim_org.internal_type
 FROM datamodel_platform_bronze.stg_active_licenses licenses
 LEFT OUTER JOIN dim_org
  ON dim_org.org_id = licenses.org_id
 WHERE licenses.date = '{DATEID}'),
 dim_licenses AS (
  SELECT *
  FROM datamodel_platform.dim_licenses
  WHERE date = DATEADD(DAY, -1, DATE('{DATEID}'))),
 sku_metadata AS (
    SELECT product_code,
        MAX_BY(description, last_modified_date) AS description,
        MAX_BY(family, last_modified_date) AS product_family,
        MAX_BY(sub_product_line_c, last_modified_date) AS sub_product_line,
        MAX_BY(name, last_modified_date) AS name,
        MAX(last_modified_date) AS last_modified_date
    FROM {PRODUCT_HIERARCHY_PATH}
    WHERE product_code LIKE 'LIC%'
    GROUP BY 1),
 license_updates AS (
 SELECT DATE('{DATEID}') AS date,
        COALESCE(stg.org_id, licenses.org_id) AS org_id,
        COALESCE(stg.sku, licenses.sku) AS sku,
        COALESCE(stg.quantity, licenses.quantity) AS quantity,
        COALESCE(stg.is_legacy_license, licenses.is_legacy_license) AS is_legacy_license,
        COALESCE(stg.is_trial, licenses.is_trial) AS is_trial,
        COALESCE(stg.refreshed_at, licenses.refreshed_at) AS refreshed_at,
        COALESCE(licenses.date_first_active, stg.date_first_active) AS date_first_active,
        --if the license is already expired, stay expired
        --if the license is present in the staging table, it is not expired
        CASE WHEN (licenses.date_expired IS NOT NULL
                   AND stg.org_id IS NULL
                   AND stg.sku IS NULL)
             THEN licenses.date_expired
        --else if the license is expired in dynamodb, expire it on the dynamo expiry date
             WHEN DATE(stg.dynamo_expire_at) < '{DATEID}'
             THEN DATE(stg.dynamo_expire_at)
        --if the quantity is 0 then expire the license
             WHEN COALESCE(stg.quantity, licenses.quantity, 0) = 0
             THEN '{DATEID}'
        --else if the license is not expired and is not in today's staging data then expire it
             WHEN licenses.org_id IS NOT NULL AND licenses.sku IS NOT NULL
                  AND stg.org_id IS NULL AND stg.sku IS NULL
             THEN licenses.date
             ELSE NULL END AS date_expired,
        COALESCE(stg.sam_number, licenses.sam_number) AS sam_number,
        COALESCE(stg.internal_type, licenses.internal_type) AS internal_type,
        licenses.description,
        licenses.product_family,
        licenses.sub_product_line,
        licenses.name
 FROM dim_licenses licenses
 FULL OUTER JOIN stg_licenses stg
  ON stg.org_id = licenses.org_id
  AND stg.sku = licenses.sku)

     SELECT licenses.date,
           licenses.org_id,
           licenses.sku,
           licenses.quantity,
           licenses.is_legacy_license,
           licenses.is_trial,
           licenses.refreshed_at,
           licenses.date_first_active,
           licenses.date_expired,
           licenses.sam_number,
           licenses.internal_type AS internal_type,
          --for metadata fields, take the most recently updated field by sku
          COALESCE(sku.name, licenses.name) AS name,
          COALESCE(sku.description, licenses.description) AS description,
          COALESCE(sku.product_family, licenses.product_family) AS product_family,
          COALESCE(sku.sub_product_line, licenses.sub_product_line) AS sub_product_line
    FROM license_updates licenses
    LEFT OUTER JOIN sku_metadata sku
        ON sku.product_code = licenses.sku
"""

DIM_LICENSES_SCHEMA = [
    {
        "name": "date",
        "type": "date",
        "nullable": False,
        "metadata": {
            "comment": "Date of snapshot of copy of dynamodb.cached_active_skus table."
        },
    },
    {
        "name": "org_id",
        "type": "long",
        "nullable": False,
        "metadata": {"comment": "ID of organization of license sku."},
    },
    {
        "name": "sku",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "License sku, does not represent quantity of licenses only that at least one license is held. "
        },
    },
    {
        "name": "quantity",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "Quantity of licenses held, can be NULL if no value is provided."
        },
    },
    {
        "name": "is_legacy_license",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Boolean value equal to TRUE when license is part of pricing and packaging program, otherwise FALSE."
        },
    },
    {
        "name": "is_trial",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Boolean value equal to TRUE when license is under free trial, otherwise FALSE or NULL."
        },
    },
    {
        "name": "refreshed_at",
        "type": "timestamp",
        "nullable": False,
        "metadata": {
            "comment": "Timestamp for when polling of license sku to org_id occurred and was written to dynamodb table."
        },
    },
    {
        "name": "date_first_active",
        "type": "date",
        "nullable": False,
        "metadata": {
            "comment": "Date where sku was first polled as being active for org_id. Earliest date is 2023-10-30."
        },
    },
    {
        "name": "date_expired",
        "type": "date",
        "nullable": False,
        "metadata": {"comment": "Date that sku was last seen being active for org_id"},
    },
    {
        "name": "sam_number",
        "type": "string",
        "nullable": False,
        "metadata": {"comment": sam_number_default_description},
    },
    {
        "name": "internal_type",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Field representing internal status of organizations. 0 represents customer organizations."
        },
    },
    {
        "name": "name",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Most recent name of license sku as of query date from edw.salesforce_sterling.plm_product_hierarchy."
        },
    },
    {
        "name": "description",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Most recent description of license sku as of query date from edw.salesforce_sterling.plm_product_hierarchy."
        },
    },
    {
        "name": "product_family",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Most recent family of license sku as of query date from edw.salesforce_sterling.plm_product_hierarchy."
        },
    },
    {
        "name": "sub_product_line",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Most recent sub_product_line of license sku as of query date from edw.salesforce_sterling.plm_product_hierarchy."
        },
    },
    {
        "name": "tier",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Pricing and packaging tier of license, sourced from the license SKU - premier, essential, public sector, enterprise"
        },
    },
    {
        "name": "platform_tier",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "If available, pricing and packaging tier of platform license component. Sourced from license SKU."
        },
    },
    {
        "name": "device",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Device type associated with license, sourced from license SKU."
        },
    },
    {
        "name": "is_platform",
        "type": "boolean",
        "nullable": True,
        "metadata": {
            "comment": "TRUE if the license has a platform tier component, otherwise FALSE. FALSE for all legacy licenses."
        },
    },
]

STG_LICENSE_ASSIGNMENT_QUERY = """
    SELECT
    '{DATEID}' AS date,
    assignments.created_at,
    assignments.updated_at,
    assignments.end_time,
    assignments.uuid,
    assignments.org_id,
    assignments.sam_number,
    assignments.sku,
    assignments.entity_id_int AS entity_id,
    assignments.entity_type,
    CASE WHEN assignments.entity_type = 1 THEN 'user'
        WHEN assignments.entity_type = 2 THEN 'driver'
        WHEN assignments.entity_type = 3 THEN 'device'
        END AS entity_type_name,
    assignments.order_id,
    assignments.peripheral_id,
    assignments.peripheral_type,
    gateways.serial AS serial
    FROM licenseentitydb.license_assignments
        TIMESTAMP AS OF CONCAT(GREATEST(DATE('{DATEID}'), DATE_ADD(DATE(current_timestamp()),-6)), " 22:59:59.999") assignments
    LEFT OUTER JOIN
        (SELECT *
         FROM datamodel_core_bronze.raw_productsdb_gateways
         WHERE date = (CASE WHEN '{DATEID}' < (SELECT MIN(date) FROM datamodel_core_bronze.raw_productsdb_gateways)
                           THEN (SELECT MIN(date) FROM datamodel_core_bronze.raw_productsdb_gateways)
                           ELSE '{DATEID}' END)) gateways
        ON gateways.device_id = assignments.entity_id_int
    WHERE assignments.uuid IS NOT NULL
"""

STG_LICENSE_ASSIGNMENT_SCHEMA = [
    {
        "name": "date",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Date of snapshot of licenseentitydb.license_assignments table."
        },
    },
    {
        "name": "created_at",
        "type": "timestamp",
        "nullable": False,
        "metadata": {
            "comment": "Timestamp of license assignment creation event from licenseentitydb.license_assignments table."
        },
    },
    {
        "name": "updated_at",
        "type": "timestamp",
        "nullable": False,
        "metadata": {
            "comment": "Timestamp of update to license assignment event from licenseentitydb.license_assignments table."
        },
    },
    {
        "name": "end_time",
        "type": "timestamp",
        "nullable": True,
        "metadata": {
            "comment": "Timestamp of expiration of license assignment, if populated from licenseentitydb.license_assignments table."
        },
    },
    {
        "name": "uuid",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Unique ID of assignment event from licenseentitydb.license_assignments table."
        },
    },
    {
        "name": "org_id",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Organization ID of license assignment event from licenseentitydb.license_assignments table."
        },
    },
    {
        "name": "sam_number",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "SAM number of license assignment event from licenseentitydb.license_assignments table."
        },
    },
    {
        "name": "sku",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "SKU of license from licenseentitydb.license_assignments table."
        },
    },
    {
        "name": "entity_id",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "ID of entity that license is assigned to from licenseentitydb.license_assignments table, ex: device_id."
        },
    },
    {
        "name": "entity_type",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Type of entity that license is assigned to in licenseentitydb.license_assignments table."
        },
    },
    {
        "name": "entity_type_name",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Name of entity type that license is assigned to in licenseentitydb.license_assignments table, one of user,driver,device."
        },
    },
    {
        "name": "order_id",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "ID of order of license from the from the licenseentitydb.license_assignments table."
        },
    },
    {
        "name": "peripheral_id",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "Device ID of peripheral from the licenseentitydb.license_assignments table, if populated."
        },
    },
    {
        "name": "peripheral_type",
        "type": "short",
        "nullable": True,
        "metadata": {
            "comment": "Type of device peripheral from the licenseentitydb.license_assignments table."
        },
    },
    {
        "name": "serial",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Serial of device ID for gateway devices from productsdb.gateways table."
        },
    },
]

BUILD_DIM_LICENSE_ASSIGNMENT_QUERY = """
WITH sku_metadata AS (
    SELECT UPPER(product_code) AS sku,
        MAX_BY(family, last_modified_date) AS product_family,
        MAX_BY(sub_product_line_c, last_modified_date) AS sub_product_line
    FROM {PRODUCT_HIERARCHY_PATH}
    WHERE product_code LIKE 'LIC%'
    GROUP BY 1)

SELECT
      '{DATEID}' AS date,
      stg.uuid,
      stg.created_at,
      stg.updated_at,
      stg.end_time,
      stg.org_id,
      stg.sam_number,
      stg.sku,
      stg.entity_id,
      stg.entity_type,
      stg.entity_type_name,
      stg.order_id,
      stg.peripheral_id,
      stg.peripheral_type,
      stg.serial,
      CASE WHEN date(stg.end_time) <= '{DATEID}' THEN TRUE ELSE FALSE END AS is_expired,
      sku_metadata.product_family,
      sku_metadata.sub_product_line
FROM {database_bronze}.stg_license_assignment stg
LEFT OUTER JOIN sku_metadata
  ON sku_metadata.sku = stg.sku
WHERE stg.date = '{DATEID}'
"""

INSERT_DIM_LICENSE_ASSIGNMENT_QUERY = """
WITH sku_metadata AS (
    SELECT UPPER(product_code) AS sku,
        MAX_BY(family, last_modified_date) AS product_family,
        MAX_BY(sub_product_line_c, last_modified_date) AS sub_product_line
    FROM {PRODUCT_HIERARCHY_PATH}
    WHERE product_code LIKE 'LIC%'
    GROUP BY 1),
prev AS (
SELECT *
FROM {database_gold}.dim_license_assignment
WHERE date = DATE_ADD(DATE('{DATEID}'), -1))

SELECT
      '{DATEID}' AS date,
      COALESCE(curr.uuid, prev.uuid) AS uuid,
      COALESCE(curr.created_at, prev.created_at) AS created_at,
      COALESCE(curr.updated_at, prev.updated_at) AS updated_at,
      COALESCE(curr.end_time, prev.end_time) AS end_time,
      COALESCE(curr.org_id, prev.org_id) AS org_id,
      COALESCE(curr.sam_number, prev.sam_number) AS sam_number,
      COALESCE(curr.sku, prev.sku) AS sku,
      COALESCE(curr.entity_id, prev.entity_id) AS entity_id,
      COALESCE(curr.entity_type, prev.entity_type) AS entity_type,
      COALESCE(curr.entity_type_name, prev.entity_type_name) AS entity_type_name,
      COALESCE(curr.order_id, prev.order_id) AS order_id,
      COALESCE(curr.peripheral_id, prev.peripheral_id) AS peripheral_id,
      COALESCE(curr.peripheral_type, prev.peripheral_type) AS peripheral_type,
      COALESCE(curr.serial, prev.serial) AS serial,
      COALESCE(CASE WHEN date(curr.end_time) <= '{DATEID}' THEN TRUE ELSE FALSE END, prev.is_expired) AS is_expired,
      COALESCE(sku_metadata.product_family, prev.product_family) AS product_family,
      COALESCE(sku_metadata.sub_product_line, prev.sub_product_line) AS sub_product_line
FROM {database_bronze}.stg_license_assignment curr
LEFT OUTER JOIN sku_metadata
  ON sku_metadata.sku = curr.sku
FULL OUTER JOIN prev
    ON prev.uuid = curr.uuid
WHERE curr.date = '{DATEID}'
"""

DIM_LICENSE_ASSIGNMENT_SCHEMA = [
    {
        "name": "date",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Date of snapshot of licenseentitydb.license_assignments table."
        },
    },
    {
        "name": "created_at",
        "type": "timestamp",
        "nullable": False,
        "metadata": {
            "comment": "Timestamp of license assignment creation event from licenseentitydb.license_assignments table."
        },
    },
    {
        "name": "updated_at",
        "type": "timestamp",
        "nullable": False,
        "metadata": {
            "comment": "Timestamp of update to license assignment event from licenseentitydb.license_assignments table."
        },
    },
    {
        "name": "end_time",
        "type": "timestamp",
        "nullable": True,
        "metadata": {
            "comment": "Timestamp of expiration of license assignment, if populated from licenseentitydb.license_assignments table."
        },
    },
    {
        "name": "uuid",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Unique ID of assignment event from licenseentitydb.license_assignments table."
        },
    },
    {
        "name": "org_id",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Organization ID of license assignment event from licenseentitydb.license_assignments table."
        },
    },
    {
        "name": "sam_number",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "SAM number of license assignment event from licenseentitydb.license_assignments table."
        },
    },
    {
        "name": "sku",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "SKU of license from licenseentitydb.license_assignments table."
        },
    },
    {
        "name": "entity_id",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "ID of entity that license is assigned to from licenseentitydb.license_assignments table, ex: device_id."
        },
    },
    {
        "name": "entity_type",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Type of entity that license is assigned to in licenseentitydb.license_assignments table."
        },
    },
    {
        "name": "entity_type_name",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Name of entity type that license is assigned to in licenseentitydb.license_assignments table, one of user,driver,device."
        },
    },
    {
        "name": "order_id",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "ID of order of license from the from the licenseentitydb.license_assignments table."
        },
    },
    {
        "name": "peripheral_id",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "Device ID of peripheral from the licenseentitydb.license_assignments table, if populated."
        },
    },
    {
        "name": "peripheral_type",
        "type": "short",
        "nullable": True,
        "metadata": {
            "comment": "Type of device peripheral from the licenseentitydb.license_assignments table."
        },
    },
    {
        "name": "serial",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Serial of device ID for gateway devices from productsdb.gateways table."
        },
    },
    {
        "name": "is_expired",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "TRUE if license assignment UUID event is expired as of date partition."
        },
    },
    {
        "name": "product_family",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Product family of license (telematics, safety, etc) from edw.salesforce_sterling.plm_product_hierarchy."
        },
    },
    {
        "name": "sub_product_line",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Sub product line of license (cm2, vg) from edw.salesforce_sterling.plm_product_hierarchy."
        },
    },
]


stg_license_assignment_description = build_table_description(
    table_desc="""This table presents a daily snapshot of all license assignment events, including ones that are currently expired. To calculate assigned licenses on a particular day that are unexpired, query the partition for that day with the filter is_expired = FALSE.
    """,
    row_meaning="""One row in this table represents one license assignment event by UUID""",
    table_type=TableType.STAGING,
    freshness_slo_updated_by="9am PST",
)


@asset(
    name="stg_license_assignment",
    owners=["team:DataEngineering"],
    description=stg_license_assignment_description,
    compute_kind="sql",
    metadata={
        "database": databases["database_bronze"],
        "owners": ["team:DataEngineering"],
        "write_mode": WarehouseWriteMode.overwrite,
        "schema": STG_LICENSE_ASSIGNMENT_SCHEMA,
        "code_location": get_code_location(),
        "description": stg_license_assignment_description,
    },
    required_resource_keys=REQUIRED_RESOURCE_KEYS,
    group_name=GROUP_NAME,
    partitions_def=DAILY_PARTITIONS_DEF,
    key_prefix=[AWSRegions.US_WEST_2.value, databases["database_bronze"]],
    deps=[
        AssetDep(
            AssetKey(
                [
                    AWSRegions.US_WEST_2.value,
                    databases["database_core_bronze"],
                    "raw_productsdb_gateways",
                ]
            ),
            partition_mapping=LastPartitionMapping(),
        ),
    ],
)
def stg_license_assignment(context) -> DataFrame:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    partition_date_str = context.partition_key
    query = STG_LICENSE_ASSIGNMENT_QUERY.format(DATEID=partition_date_str, **databases)
    context.log.info(query)
    df = spark.sql(query)
    context.log.info(df)
    return df


@asset(
    name="stg_license_assignment",
    owners=["team:DataEngineering"],
    description=stg_license_assignment_description,
    compute_kind="sql",
    metadata={
        "database": databases["database_bronze"],
        "owners": ["team:DataEngineering"],
        "write_mode": WarehouseWriteMode.overwrite,
        "schema": STG_LICENSE_ASSIGNMENT_SCHEMA,
        "code_location": get_code_location(),
        "description": stg_license_assignment_description,
    },
    required_resource_keys=REQUIRED_RESOURCE_KEYS_EU,
    group_name=GROUP_NAME + "_eu",
    partitions_def=DAILY_PARTITIONS_DEF,
    key_prefix=[AWSRegions.EU_WEST_1.value, databases["database_bronze"]],
    deps=[
        AssetDep(
            AssetKey(
                [
                    AWSRegions.EU_WEST_1.value,
                    databases["database_core_bronze"],
                    "raw_productsdb_gateways",
                ]
            ),
            partition_mapping=LastPartitionMapping(),
        ),
    ],
)
def stg_license_assignment_eu(context) -> DataFrame:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    partition_date_str = context.partition_key
    query = STG_LICENSE_ASSIGNMENT_QUERY.format(DATEID=partition_date_str, **databases)
    context.log.info(query)
    df = spark.sql(query)
    context.log.info(df)
    return df


@asset(
    name="dim_license_assignment",
    owners=["team:DataEngineering"],
    description=build_table_description(
        table_desc="""This table presents a daily snapshot of all license assignment events, including ones that are currently expired. To calculate assigned licenses on a particular day that are unexpired, query the partition for that day with the filter is_expired = FALSE.
        """,
        row_meaning="""One row in this table represents one license assignment event by UUID""",
        table_type=TableType.DAILY_DIMENSION,
        freshness_slo_updated_by="9am PST",
    ),
    compute_kind="sql",
    metadata={
        "database": databases["database_gold"],
        "owners": ["team:DataEngineering"],
        "primary_keys": [
            "date",
            "uuid",
        ],
        "write_mode": WarehouseWriteMode.overwrite,
        "schema": DIM_LICENSE_ASSIGNMENT_SCHEMA,
        "code_location": get_code_location(),
        "description": build_table_description(
            table_desc="""This table presents a daily snapshot of all license assignment events, including ones that are currently expired. To calculate assigned licenses on a particular day that are unexpired, query the partition for that day with the filter is_expired = FALSE.
        """,
            row_meaning="""One row in this table represents one license assignment event by UUID""",
            table_type=TableType.DAILY_DIMENSION,
            freshness_slo_updated_by="9am PST",
        ),
    },
    required_resource_keys=REQUIRED_RESOURCE_KEYS,
    group_name=GROUP_NAME,
    partitions_def=DAILY_PARTITIONS_DEF,
    key_prefix=[AWSRegions.US_WEST_2.value, databases["database_gold"]],
    deps=[
        AssetDep(
            AssetKey(
                [
                    AWSRegions.US_WEST_2.value,
                    databases["database_bronze"],
                    "stg_license_assignment",
                ]
            ),
            partition_mapping=IdentityPartitionMapping(),
        ),
        AssetDep(
            AssetKey(
                [
                    AWSRegions.US_WEST_2.value,
                    databases["database_gold"],
                    "dim_license_assignment",
                ]
            ),
            partition_mapping=TimeWindowPartitionMapping(
                start_offset=-1, end_offset=-1
            ),
        ),
    ],
)
def dim_license_assignment(context) -> DataFrame:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    partition_date_str = context.partition_key
    if partition_date_str == DAILY_PARTITIONS_DEF.start.strftime("%Y-%m-%d"):
        query = BUILD_DIM_LICENSE_ASSIGNMENT_QUERY.format(
            DATEID=partition_date_str,
            PRODUCT_HIERARCHY_PATH=product_hierarchy_path["US"],
            **databases
        )
    else:
        query = INSERT_DIM_LICENSE_ASSIGNMENT_QUERY.format(
            DATEID=partition_date_str,
            PRODUCT_HIERARCHY_PATH=product_hierarchy_path["US"],
            **databases
        )
    context.log.info(query)
    df = spark.sql(query)
    context.log.info(df)
    return df


@asset(
    name="dim_license_assignment",
    owners=["team:DataEngineering"],
    description=build_table_description(
        table_desc="""This table presents a daily snapshot of all license assignment events, including ones that are currently expired. To calculate assigned licenses on a particular day that are unexpired, query the partition for that day with the filter is_expired = FALSE.
        """,
        row_meaning="""One row in this table represents one license assignment event by UUID""",
        table_type=TableType.DAILY_DIMENSION,
        freshness_slo_updated_by="9am PST",
    ),
    compute_kind="sql",
    metadata={
        "database": databases["database_gold"],
        "owners": ["team:DataEngineering"],
        "write_mode": WarehouseWriteMode.overwrite,
        "schema": DIM_LICENSE_ASSIGNMENT_SCHEMA,
        "code_location": get_code_location(),
        "description": build_table_description(
            table_desc="""This table presents a daily snapshot of all license assignment events, including ones that are currently expired. To calculate assigned licenses on a particular day that are unexpired, query the partition for that day with the filter is_expired = FALSE.
        """,
            row_meaning="""One row in this table represents one license assignment event by UUID""",
            table_type=TableType.DAILY_DIMENSION,
            freshness_slo_updated_by="9am PST",
        ),
    },
    required_resource_keys=REQUIRED_RESOURCE_KEYS_EU,
    group_name=GROUP_NAME + "_eu",
    partitions_def=DAILY_PARTITIONS_DEF,
    key_prefix=[AWSRegions.EU_WEST_1.value, databases["database_gold"]],
    deps=[
        AssetDep(
            AssetKey(
                [
                    AWSRegions.EU_WEST_1.value,
                    databases["database_bronze"],
                    "stg_license_assignment",
                ]
            ),
            partition_mapping=IdentityPartitionMapping(),
        ),
        AssetDep(
            AssetKey(
                [
                    AWSRegions.EU_WEST_1.value,
                    databases["database_gold"],
                    "dim_license_assignment",
                ]
            ),
            partition_mapping=TimeWindowPartitionMapping(
                start_offset=-1, end_offset=-1
            ),
        ),
    ],
)
def dim_license_assignment_eu(context) -> DataFrame:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    partition_date_str = context.partition_key
    if partition_date_str == DAILY_PARTITIONS_DEF.start.strftime("%Y-%m-%d"):
        query = BUILD_DIM_LICENSE_ASSIGNMENT_QUERY.format(
            DATEID=partition_date_str,
            PRODUCT_HIERARCHY_PATH=product_hierarchy_path["EU"],
            **databases
        )
    else:
        query = INSERT_DIM_LICENSE_ASSIGNMENT_QUERY.format(
            DATEID=partition_date_str,
            PRODUCT_HIERARCHY_PATH=product_hierarchy_path["EU"],
            **databases
        )
    context.log.info(query)
    df = spark.sql(query)
    context.log.info(df)
    return df


dqs["dim_license_assignment"].append(
    PrimaryKeyDQCheck(
        name="dq_pk_dim_license_assignment",
        table="dim_license_assignment",
        primary_keys=[
            "date",
            "uuid",
        ],
        blocking=True,
        database=databases["database_gold_dev"],
    )
)

dqs["dim_license_assignment"].append(
    NonEmptyDQCheck(
        name="dq_empty_dim_license_assignment",
        table="dim_license_assignment",
        blocking=True,
        database=databases["database_gold_dev"],
    )
)

dqs["dim_license_assignment"].append(
    NonNullDQCheck(
        name="dq_null_dim_license_assignment",
        database=databases["database_gold_dev"],
        table="dim_license_assignment",
        non_null_columns=[
            "date",
            "created_at",
            "updated_at",
            "uuid",
            "org_id",
            "sam_number",
            "sku",
            "entity_id",
            "entity_type",
            "entity_type_name",
            "order_id",
            "is_expired",
        ],
        blocking=True,
    )
)


stg_active_licenses_description = build_table_description(
    table_desc="""Daily snapshot of dynamodb.cached_active_skus""",
    row_meaning="""Each row represents license information for an active SKU.""",
    table_type=TableType.STAGING,
    freshness_slo_updated_by="9am PST",
)


@asset(
    name="stg_active_licenses",
    owners=["team:DataEngineering"],
    description=stg_active_licenses_description,
    compute_kind="sql",
    metadata={
        "database": databases["database_bronze"],
        "owners": ["team:DataEngineering"],
        "primary_keys": ["date", "org_id", "sku"],
        "write_mode": WarehouseWriteMode.overwrite,
        "schema": STG_ACTIVE_LICENSES_SCHEMA,
        "code_location": get_code_location(),
        "description": stg_active_licenses_description,
    },
    required_resource_keys=REQUIRED_RESOURCE_KEYS,
    group_name=GROUP_NAME,
    partitions_def=DAILY_PARTITIONS_DEF,
    key_prefix=[AWSRegions.US_WEST_2.value, databases["database_bronze"]],
)
def stg_active_licenses(context) -> DataFrame:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    partition_date_str = context.partition_key
    query = stg_active_licenses_query.format(DATEID=partition_date_str, **databases)
    context.log.info(query)
    df = spark.sql(query)
    context.log.info(df)
    return df


@asset(
    name="stg_active_licenses",
    owners=["team:DataEngineering"],
    description=stg_active_licenses_description,
    compute_kind="sql",
    metadata={
        "database": databases["database_bronze"],
        "owners": ["team:DataEngineering"],
        "primary_keys": ["date", "org_id", "sku"],
        "write_mode": WarehouseWriteMode.overwrite,
        "schema": STG_ACTIVE_LICENSES_SCHEMA,
        "code_location": get_code_location(),
        "description": stg_active_licenses_description,
    },
    required_resource_keys=REQUIRED_RESOURCE_KEYS_EU,
    group_name=GROUP_NAME + "_eu",
    partitions_def=DAILY_PARTITIONS_DEF,
    key_prefix=[AWSRegions.EU_WEST_1.value, databases["database_bronze"]],
)
def stg_active_licenses_eu(context) -> DataFrame:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    partition_date_str = context.partition_key
    partition_datetime = datetime.strptime(partition_date_str, "%Y-%m-%d")
    # if the snapshot is not available, use the last available date
    if partition_datetime < (datetime.now() - timedelta(days=7)):
        query = stg_active_licenses_backfill_query.format(
            DATEID=partition_date_str, **databases
        )
    else:
        query = stg_active_licenses_query.format(DATEID=partition_date_str, **databases)
    context.log.info(query)
    df = spark.sql(query)
    context.log.info(df)
    return df


dim_licenses_description = build_table_description(
    table_desc="""This table presents a daily view of the total quantity of active skus by org_id.
    The source of this data is a daily polling of all organizations to find associated license sku counts.
    A sku quantity of 0 or a non null date_expired would indicate an expired license.
    The is_legacy field is used to denote whether the license is part of the pricing and packaging program.
    The business unit is available in the product_family field.

    NOTE: This table is now deprecated. Please use edw.silver.fct_license_orders going forward.
    """,
    row_meaning="""One row in this table represents the quantity of a license sku per org_id per date.""",
    table_type=TableType.DAILY_DIMENSION,
    freshness_slo_updated_by="9am PST",
)


@asset(
    name="dim_licenses",
    owners=["team:DataEngineering"],
    description=dim_licenses_description,
    compute_kind="sql",
    metadata={
        "database": databases["database_gold"],
        "owners": ["team:DataEngineering"],
        "primary_keys": ["date", "org_id", "sku"],
        "write_mode": WarehouseWriteMode.overwrite,
        "schema": DIM_LICENSES_SCHEMA,
        "code_location": get_code_location(),
        "description": dim_licenses_description,
    },
    required_resource_keys=REQUIRED_RESOURCE_KEYS,
    group_name=GROUP_NAME,
    partitions_def=DAILY_PARTITIONS_DEF,
    key_prefix=[AWSRegions.US_WEST_2.value, databases["database_gold"]],
    deps=[
        AssetDep(
            AssetKey(
                [
                    AWSRegions.US_WEST_2.value,
                    databases["database_gold"],
                    "dim_licenses",
                ]
            ),
            partition_mapping=TimeWindowPartitionMapping(
                start_offset=-1, end_offset=-1
            ),
        ),
        AssetDep(
            AssetKey(
                [
                    AWSRegions.US_WEST_2.value,
                    databases["database_bronze"],
                    "stg_active_licenses",
                ]
            ),
            partition_mapping=IdentityPartitionMapping(),
        ),
        AssetDep(
            AssetKey(
                [
                    AWSRegions.US_WEST_2.value,
                    databases["database_core_bronze"],
                    "raw_clouddb_organizations",
                ]
            ),
            partition_mapping=IdentityPartitionMapping(),
        ),
    ],
)
def dim_licenses(context) -> DataFrame:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    partition_date_str = context.partition_key
    if partition_date_str == DAILY_PARTITIONS_DEF.start.strftime("%Y-%m-%d"):
        query = build_dim_licenses_query.format(
            DATEID=partition_date_str,
            PRODUCT_HIERARCHY_PATH=product_hierarchy_path["US"],
            **databases
        )
    else:
        query = insert_dim_licenses_query.format(
            DATEID=partition_date_str,
            PRODUCT_HIERARCHY_PATH=product_hierarchy_path["US"],
            **databases
        )
    context.log.info(query)
    df = spark.sql(query).toPandas()
    df["tier"] = ""
    df["platform_tier"] = ""
    df["device"] = ""
    df["is_platform"] = False
    for index, row in df.iterrows():
        tier = ""
        platform_tier = ""
        device = ""
        is_platform = False
        sku = row["sku"].lower()
        product_family = ""
        if row["product_family"] is not None:
            product_family = row["product_family"].lower()

        # sort keys so longest key is matched first, as premier-ps contains prem, ps tiers
        tier_keys = [
            re.escape(key) for key in sorted(tier_map.keys(), key=len, reverse=True)
        ]
        tier_regex_pattern = r"(?:^|-)" + r"(" + "|".join(tier_keys) + r")" + r"(?:$|-)"
        tier_regex = re.compile(tier_regex_pattern)
        tier_matches = tier_regex.findall(sku)

        device_keys = [
            re.escape(key) for key in sorted(device_map.keys(), key=len, reverse=True)
        ]
        device_regex_pattern = (
            r"(?:^|-)" + r"(" + "|".join(device_keys) + r")" + r"(?:$|-)"
        )
        device_regex = re.compile(device_regex_pattern)
        device_matches = device_regex.findall(sku)

        if len(device_matches) == 1:
            device = device_matches[0]

        if "pltfm" in sku:
            is_platform = True
        else:
            is_platform = False
        # if there is at least one tier match, it can be platform or not platform or both
        if len(tier_matches) == 1:
            # if is_platform is True, then only contains a platform tier
            if is_platform == True:
                platform_tier = tier_map[tier_matches[0]]
            # if is_platform is False, then only contains a non platform tier
            else:
                tier = tier_map[tier_matches[0]]
        # if there are two tier matches, platform must be the second one
        if len(tier_matches) == 2 and is_platform == True:
            tier = tier_map[tier_matches[0]]
            platform_tier = tier_map[tier_matches[1]]

        # these businesses have non tiered SKUs as Add-On tier SKUs
        if product_family in [
            "safety",
            "telematics",
            "other",
            "smart trailers & connected equipment",
        ]:
            if tier == "" and device == "":
                tier = "Add-On"
            if "express" in sku and device == "":
                tier = "Add-On"

        df.at[index, "tier"] = tier
        df.at[index, "platform_tier"] = platform_tier
        df.at[index, "device"] = device
        df.at[index, "is_platform"] = is_platform

    df["date_expired"] = pd.to_datetime(df["date_expired"])
    df["quantity"] = df["quantity"].astype("Int64")
    df["internal_type"] = df["internal_type"].astype("Int64")
    df = spark.createDataFrame(df)
    df = df.withColumn("date_expired", df.date_expired.cast(DateType()))
    context.log.info(df)
    return df


@asset(
    name="dim_licenses",
    owners=["team:DataEngineering"],
    description=dim_licenses_description,
    compute_kind="sql",
    metadata={
        "database": databases["database_gold"],
        "owners": ["team:DataEngineering"],
        "primary_keys": ["date", "org_id", "sku"],
        "write_mode": WarehouseWriteMode.overwrite,
        "schema": DIM_LICENSES_SCHEMA,
        "code_location": get_code_location(),
        "description": dim_licenses_description,
    },
    required_resource_keys=REQUIRED_RESOURCE_KEYS_EU,
    group_name=GROUP_NAME + "_eu",
    partitions_def=DAILY_PARTITIONS_DEF,
    key_prefix=[AWSRegions.EU_WEST_1.value, databases["database_gold"]],
    deps=[
        AssetDep(
            AssetKey(
                [
                    AWSRegions.EU_WEST_1.value,
                    databases["database_gold"],
                    "dim_licenses",
                ]
            ),
            partition_mapping=TimeWindowPartitionMapping(
                start_offset=-1, end_offset=-1
            ),
        ),
        AssetDep(
            AssetKey(
                [
                    AWSRegions.EU_WEST_1.value,
                    databases["database_bronze"],
                    "stg_active_licenses",
                ]
            ),
            partition_mapping=IdentityPartitionMapping(),
        ),
        AssetDep(
            AssetKey(
                [
                    AWSRegions.EU_WEST_1.value,
                    databases["database_core_bronze"],
                    "raw_clouddb_organizations",
                ]
            ),
            partition_mapping=IdentityPartitionMapping(),
        ),
    ],
)
def dim_licenses_eu(context) -> DataFrame:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    partition_date_str = context.partition_key
    if partition_date_str == DAILY_PARTITIONS_DEF.start.strftime("%Y-%m-%d"):
        query = build_dim_licenses_query.format(
            DATEID=partition_date_str,
            PRODUCT_HIERARCHY_PATH=product_hierarchy_path["EU"],
            **databases
        )
    else:
        query = insert_dim_licenses_query.format(
            DATEID=partition_date_str,
            PRODUCT_HIERARCHY_PATH=product_hierarchy_path["EU"],
            **databases
        )
    context.log.info(query)
    df = spark.sql(query).toPandas()
    df["tier"] = ""
    df["platform_tier"] = ""
    df["device"] = ""
    df["is_platform"] = False
    for index, row in df.iterrows():
        tier = ""
        platform_tier = ""
        device = ""
        is_platform = False
        sku = row["sku"].lower()
        product_family = ""
        if row["product_family"] is not None:
            product_family = row["product_family"].lower()

        # sort keys so longest key is matched first, as premier-ps contains prem, ps tiers
        tier_keys = [
            re.escape(key) for key in sorted(tier_map.keys(), key=len, reverse=True)
        ]
        tier_regex_pattern = r"(?:^|-)" + r"(" + "|".join(tier_keys) + r")" + r"(?:$|-)"
        tier_regex = re.compile(tier_regex_pattern)
        tier_matches = tier_regex.findall(sku)

        device_keys = [
            re.escape(key) for key in sorted(device_map.keys(), key=len, reverse=True)
        ]
        device_regex_pattern = (
            r"(?:^|-)" + r"(" + "|".join(device_keys) + r")" + r"(?:$|-)"
        )
        device_regex = re.compile(device_regex_pattern)
        device_matches = device_regex.findall(sku)

        if len(device_matches) == 1:
            device = device_matches[0]

        if "pltfm" in sku:
            is_platform = True
        else:
            is_platform = False
        # if there is at least one tier match, it can be platform or not platform or both
        if len(tier_matches) == 1:
            # if is_platform is True, then only contains a platform tier
            if is_platform == True:
                platform_tier = tier_map[tier_matches[0]]
            # if is_platform is False, then only contains a non platform tier
            else:
                tier = tier_map[tier_matches[0]]
        # if there are two tier matches, platform must be the second one
        if len(tier_matches) == 2 and is_platform == True:
            tier = tier_map[tier_matches[0]]
            platform_tier = tier_map[tier_matches[1]]

        # these businesses have non tiered SKUs as Add-On tier SKUs
        if product_family in [
            "safety",
            "telematics",
            "other",
            "smart trailers & connected equipment",
        ]:
            if tier == "" and device == "":
                tier = "Add-On"
            if "express" in sku and device == "":
                tier = "Add-On"

        df.at[index, "tier"] = tier
        df.at[index, "platform_tier"] = platform_tier
        df.at[index, "device"] = device
        df.at[index, "is_platform"] = is_platform

    df["date_expired"] = pd.to_datetime(df["date_expired"])
    df["quantity"] = df["quantity"].astype("Int64")
    df["internal_type"] = df["internal_type"].astype("Int64")
    df = spark.createDataFrame(df)
    df = df.withColumn("date_expired", df.date_expired.cast(DateType()))
    context.log.info(df)
    return df


@asset(
    name="stg_license_assignment",
    owners=["team:DataEngineering"],
    description=stg_license_assignment_description,
    compute_kind="sql",
    metadata={
        "database": databases["database_bronze"],
        "owners": ["team:DataEngineering"],
        "write_mode": WarehouseWriteMode.overwrite,
        "schema": STG_LICENSE_ASSIGNMENT_SCHEMA,
        "code_location": get_code_location(),
        "description": stg_license_assignment_description,
    },
    required_resource_keys=REQUIRED_RESOURCE_KEYS_CA,
    group_name=GROUP_NAME + "_ca",
    partitions_def=DAILY_PARTITIONS_DEF_CA,
    key_prefix=[AWSRegions.CA_CENTRAL_1.value, databases["database_bronze"]],
    deps=[
        AssetDep(
            AssetKey(
                [
                    AWSRegions.CA_CENTRAL_1.value,
                    databases["database_core_bronze"],
                    "raw_productsdb_gateways",
                ]
            ),
            partition_mapping=LastPartitionMapping(),
        ),
    ],
)
def stg_license_assignment_ca(context) -> DataFrame:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    partition_date_str = context.partition_key
    query = STG_LICENSE_ASSIGNMENT_QUERY.format(DATEID=partition_date_str, **databases)
    context.log.info(query)
    df = spark.sql(query)
    context.log.info(df)
    return df


@asset(
    name="dim_license_assignment",
    owners=["team:DataEngineering"],
    description=build_table_description(
        table_desc="""This table presents a daily snapshot of all license assignment events, including ones that are currently expired. To calculate assigned licenses on a particular day that are unexpired, query the partition for that day with the filter is_expired = FALSE.
        """,
        row_meaning="""One row in this table represents one license assignment event by UUID""",
        table_type=TableType.DAILY_DIMENSION,
        freshness_slo_updated_by="9am PST",
    ),
    compute_kind="sql",
    metadata={
        "database": databases["database_gold"],
        "owners": ["team:DataEngineering"],
        "write_mode": WarehouseWriteMode.overwrite,
        "schema": DIM_LICENSE_ASSIGNMENT_SCHEMA,
        "code_location": get_code_location(),
        "description": build_table_description(
            table_desc="""This table presents a daily snapshot of all license assignment events, including ones that are currently expired. To calculate assigned licenses on a particular day that are unexpired, query the partition for that day with the filter is_expired = FALSE.
        """,
            row_meaning="""One row in this table represents one license assignment event by UUID""",
            table_type=TableType.DAILY_DIMENSION,
            freshness_slo_updated_by="9am PST",
        ),
    },
    required_resource_keys=REQUIRED_RESOURCE_KEYS_CA,
    group_name=GROUP_NAME + "_ca",
    partitions_def=DAILY_PARTITIONS_DEF_CA,
    key_prefix=[AWSRegions.CA_CENTRAL_1.value, databases["database_gold"]],
    deps=[
        AssetDep(
            AssetKey(
                [
                    AWSRegions.CA_CENTRAL_1.value,
                    databases["database_bronze"],
                    "stg_license_assignment",
                ]
            )
        )
    ],
)
def dim_license_assignment_ca(context) -> DataFrame:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    partition_date_str = context.partition_key
    if partition_date_str == DAILY_PARTITIONS_DEF_CA.start.strftime("%Y-%m-%d"):
        query = BUILD_DIM_LICENSE_ASSIGNMENT_QUERY.format(
            DATEID=partition_date_str,
            PRODUCT_HIERARCHY_PATH=product_hierarchy_path["CA"],
            **databases
        )
    else:
        query = INSERT_DIM_LICENSE_ASSIGNMENT_QUERY.format(
            DATEID=partition_date_str,
            PRODUCT_HIERARCHY_PATH=product_hierarchy_path["CA"],
            **databases
        )
    context.log.info(query)
    df = spark.sql(query)
    context.log.info(df)
    return df


@asset(
    name="stg_active_licenses",
    owners=["team:DataEngineering"],
    description=stg_active_licenses_description,
    compute_kind="sql",
    metadata={
        "database": databases["database_bronze"],
        "owners": ["team:DataEngineering"],
        "write_mode": WarehouseWriteMode.overwrite,
        "schema": STG_ACTIVE_LICENSES_SCHEMA,
        "code_location": get_code_location(),
        "description": stg_active_licenses_description,
    },
    required_resource_keys=REQUIRED_RESOURCE_KEYS_CA,
    group_name=GROUP_NAME + "_ca",
    partitions_def=DAILY_PARTITIONS_DEF_CA,
    key_prefix=[AWSRegions.CA_CENTRAL_1.value, databases["database_bronze"]],
)
def stg_active_licenses_ca(context) -> DataFrame:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    partition_date_str = context.partition_key
    query = stg_active_licenses_query.format(DATEID=partition_date_str, **databases)
    context.log.info(query)
    df = spark.sql(query)
    context.log.info(df)
    return df


@asset(
    name="dim_licenses",
    owners=["team:DataEngineering"],
    description=dim_licenses_description,
    compute_kind="sql",
    metadata={
        "database": databases["database_gold"],
        "owners": ["team:DataEngineering"],
        "write_mode": WarehouseWriteMode.overwrite,
        "schema": DIM_LICENSES_SCHEMA,
        "code_location": get_code_location(),
        "description": dim_licenses_description,
    },
    required_resource_keys=REQUIRED_RESOURCE_KEYS_CA,
    group_name=GROUP_NAME + "_ca",
    partitions_def=DAILY_PARTITIONS_DEF_CA,
    key_prefix=[AWSRegions.CA_CENTRAL_1.value, databases["database_gold"]],
    deps=[
        AssetDep(
            AssetKey(
                [
                    AWSRegions.CA_CENTRAL_1.value,
                    databases["database_bronze"],
                    "stg_active_licenses",
                ]
            )
        ),
        AssetDep(
            AssetKey(
                [
                    AWSRegions.CA_CENTRAL_1.value,
                    databases["database_core_bronze"],
                    "raw_clouddb_organizations",
                ]
            ),
            partition_mapping=IdentityPartitionMapping(),
        ),
    ],
)
def dim_licenses_ca(context) -> DataFrame:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    partition_date_str = context.partition_key
    if partition_date_str == DAILY_PARTITIONS_DEF_CA.start.strftime("%Y-%m-%d"):
        query = build_dim_licenses_query.format(
            DATEID=partition_date_str,
            PRODUCT_HIERARCHY_PATH=product_hierarchy_path["CA"],
            **databases
        )
    else:
        query = insert_dim_licenses_query.format(
            DATEID=partition_date_str,
            PRODUCT_HIERARCHY_PATH=product_hierarchy_path["CA"],
            **databases
        )
    context.log.info(query)
    df = spark.sql(query).toPandas()
    df["tier"] = ""
    df["platform_tier"] = ""
    df["device"] = ""
    df["is_platform"] = False
    for index, row in df.iterrows():
        tier = ""
        platform_tier = ""
        device = ""
        is_platform = False
        sku = row["sku"].lower()
        product_family = ""
        if row["product_family"] is not None:
            product_family = row["product_family"].lower()

        # sort keys so longest key is matched first, as premier-ps contains prem, ps tiers
        tier_keys = [
            re.escape(key) for key in sorted(tier_map.keys(), key=len, reverse=True)
        ]
        tier_regex_pattern = r"(?:^|-)" + r"(" + "|".join(tier_keys) + r")" + r"(?:$|-)"
        tier_regex = re.compile(tier_regex_pattern)
        tier_matches = tier_regex.findall(sku)

        device_keys = [
            re.escape(key) for key in sorted(device_map.keys(), key=len, reverse=True)
        ]
        device_regex_pattern = (
            r"(?:^|-)" + r"(" + "|".join(device_keys) + r")" + r"(?:$|-)"
        )
        device_regex = re.compile(device_regex_pattern)
        device_matches = device_regex.findall(sku)

        if len(device_matches) == 1:
            device = device_matches[0]

        if "pltfm" in sku:
            is_platform = True
        else:
            is_platform = False
        # if there is at least one tier match, it can be platform or not platform or both
        if len(tier_matches) == 1:
            # if is_platform is True, then only contains a platform tier
            if is_platform == True:
                platform_tier = tier_map[tier_matches[0]]
            # if is_platform is False, then only contains a non platform tier
            else:
                tier = tier_map[tier_matches[0]]
        # if there are two tier matches, platform must be the second one
        if len(tier_matches) == 2 and is_platform == True:
            tier = tier_map[tier_matches[0]]
            platform_tier = tier_map[tier_matches[1]]

        # these businesses have non tiered SKUs as Add-On tier SKUs
        if product_family in [
            "safety",
            "telematics",
            "other",
            "smart trailers & connected equipment",
        ]:
            if tier == "" and device == "":
                tier = "Add-On"
            if "express" in sku and device == "":
                tier = "Add-On"

        df.at[index, "tier"] = tier
        df.at[index, "platform_tier"] = platform_tier
        df.at[index, "device"] = device
        df.at[index, "is_platform"] = is_platform

    df["date_expired"] = pd.to_datetime(df["date_expired"])
    df["quantity"] = df["quantity"].astype("Int64")
    df["internal_type"] = df["internal_type"].astype("Int64")
    df = spark.createDataFrame(df)
    df = df.withColumn("date_expired", df.date_expired.cast(DateType()))
    context.log.info(df)
    return df


dq_assets = dqs.generate()
