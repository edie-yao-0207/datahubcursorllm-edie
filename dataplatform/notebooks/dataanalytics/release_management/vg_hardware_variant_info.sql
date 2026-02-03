-- Databricks notebook source
-- Date Created: 09/01/21

-- Description: This notebook gets the reported build phase and product version for VG54 devices.

-- Referenced Tables:
-- - kinesisstats.osDModiDeviceInfo

-- Original Author: Christopher Fiore

-- Engineering Reviewer: Brendan Donecker


-- COMMAND ----------

--Add in gateway history table so we can make sure to get the last reported product version for each asssociated gateway a device has been with
CREATE OR REPLACE TEMP VIEW device_history AS (
SELECT
  hist.device_id
  ,hist.gateway_id
  ,d.org_id
  ,gw.product_id
  ,TO_DATE(hist.`timestamp`) AS first_assoc_date
  ,LEAD(TO_DATE(hist.`timestamp`), 1, CURRENT_DATE+1) OVER (PARTITION BY hist.device_id ORDER BY hist.`timestamp`) AS last_assoc_date
FROM productsdb.gateway_device_history AS hist
LEFT JOIN productsdb.gateways AS gw
  ON hist.gateway_id = gw.`id`
LEFT JOIN productsdb.devices AS d
  ON hist.device_id = d.`id`
WHERE TO_DATE(hist.`timestamp`) >= DATE_SUB(CURRENT_DATE(), ${lookback_days})
ORDER BY hist.device_id, hist.`timestamp`
);

-- COMMAND ----------

-- Grab raw data from objectStat and convert the ASCII code into a char.
CREATE OR REPLACE TEMP VIEW osDModiDeviceInfo AS (
  SELECT
    TO_DATE(`date`) AS `date`
    ,`time`
    ,org_id
    ,object_id AS device_id
    ,value.proto_value.modi_device_info.manufacturing_data.bom_version
    ,CASE
      WHEN value.proto_value.modi_device_info.manufacturing_data.build_phase < 256 THEN CHAR(value.proto_value.modi_device_info.manufacturing_data.build_phase)
      ELSE CONCAT(CHAR(value.proto_value.modi_device_info.manufacturing_data.build_phase), CHAR(INT(value.proto_value.modi_device_info.manufacturing_data.build_phase / 256)))
    END AS build_phase
    ,CASE
      WHEN value.proto_value.modi_device_info.manufacturing_data.product_version < 256 THEN CHAR(value.proto_value.modi_device_info.manufacturing_data.product_version)
      ELSE CONCAT(CHAR(value.proto_value.modi_device_info.manufacturing_data.product_version), CHAR(INT(value.proto_value.modi_device_info.manufacturing_data.product_version / 256)))
    END AS product_version
  FROM kinesisstats.osDModiDeviceInfo
  WHERE (value.proto_value.modi_device_info.manufacturing_data.product_version < 65536
    OR value.proto_value.modi_device_info.manufacturing_data.product_version IS NULL)
    AND `date` >= DATE_SUB(CURRENT_DATE(), ${lookback_days})
);

-- COMMAND ----------

-- Set bin size for range join optimization based on the query results below (2023-04-27):
-- SELECT APPROX_PERCENTILE(CAST(last_assoc_date - first_assoc_date AS BIGINT), ARRAY(0.5, 0.9, 0.99, 0.999, 0.9999)) percentiles
-- FROM device_history;
-- ----
-- | percentiles |
-- | [360, 905, 1983, 2445, 2964] |
SET spark.databricks.optimizer.rangeJoin.binSize=900

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW device_info AS (
  SELECT
    hist.device_id
    ,hist.product_id
    ,hist.org_id
    ,MAX((`mod`.`time`, `mod`.bom_version)).bom_version AS bom_version
    ,MAX((`mod`.`time`, `mod`.build_phase)).build_phase AS build_phase
    ,MAX((`mod`.`time`, `mod`.product_version)).product_version AS product_version
  FROM device_history AS hist
  LEFT JOIN osDModiDeviceInfo AS `mod`
    ON hist.device_id = `mod`.device_id
    AND hist.org_id = `mod`.org_id
    AND `mod`.`date` >= hist.first_assoc_date
    AND `mod`.`date` < hist.last_assoc_date
  WHERE (`mod`.build_phase IS NOT NULL
    OR `mod`.product_version IS NOT NULL)
  GROUP BY
    hist.device_id
    ,hist.product_id
    ,hist.org_id
);

-- COMMAND ----------

-- All original component products won't report a product version value. They will be considered product version 0
CREATE OR REPLACE TEMP VIEW device_info_clean AS (
  SELECT
    org_id
    ,device_id
    ,product_id
    ,bom_version
    ,CONCAT('Phase ', STRING(build_phase)) AS build_phase
    ,CONCAT('Version ', STRING(product_version)) AS product_version
  FROM device_info
);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS data_analytics.vg_hardware_variant_info
USING DELTA
AS SELECT * FROM device_info_clean
;


-- COMMAND ----------

MERGE INTO data_analytics.vg_hardware_variant_info AS target
USING device_info_clean AS `updates`
ON
  target.org_id = `updates`.org_id
  AND target.device_id = `updates`.device_id
  AND target.product_id = `updates`.product_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
;
