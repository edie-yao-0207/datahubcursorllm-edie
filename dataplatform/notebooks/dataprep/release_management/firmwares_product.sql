-- Databricks notebook source
CREATE OR REPLACE TEMP VIEW product_firmwares AS (
  SELECT
    CAST(CURRENT_DATE() AS DATE) as date,
    `id`,
    product_id,
    name,
    version_major,
    version_minor,
    build,
    description,
    approval_state
  FROM firmwaredb.product_firmwares
);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS dataprep.firmwares_product USING DELTA PARTITIONED BY (date) (
  SELECT * FROM product_firmwares
);

-- COMMAND ----------

MERGE INTO dataprep.firmwares_product AS target
USING product_firmwares AS updates
-- `id` is the enforced PRIMARY KEY in the original table
ON target.date = updates.date
  AND target.id = updates.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
