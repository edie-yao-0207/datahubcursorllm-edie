-- Databricks notebook source
CREATE OR REPLACE TEMP VIEW rollout_stage_firmwares AS (
  SELECT
    CAST(CURRENT_DATE() AS DATE) as date,
    rollout_stage,
    product_id,
    product_firmware_id,
    product_program_id
  FROM firmwaredb.rollout_stage_firmwares
);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS dataprep.firmwares_rollout_stage USING DELTA PARTITIONED BY (date) (
  SELECT * FROM rollout_stage_firmwares
);

-- COMMAND ----------

MERGE INTO dataprep.firmwares_rollout_stage AS target
USING rollout_stage_firmwares AS updates
-- (`rollout_stage`,`product_id`,`product_program_id`) is the enforced PRIMARY KEY in the original table
ON target.date = updates.date
  AND target.rollout_stage = updates.rollout_stage
  AND target.product_id = updates.product_id
  AND target.product_program_id = updates.product_program_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
