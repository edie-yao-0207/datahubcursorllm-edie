-- Databricks notebook source
CREATE OR REPLACE TEMP VIEW speed_alert_two AS (
  WITH base AS (
    SELECT a.date,
          a.org_id,
          org.org_name,
          org.account_size_segment_name,
          org.account_industry,
          dev.device_id,
          a.occurred_at_ms,
          a.name AS alert_name,
          dev.product_name,
          a.workflow_id
    FROM workflowsdb_shards.workflow_incidents a
    INNER JOIN datamodel_core.dim_organizations org
      ON a.org_id = org.org_id
      AND a.date = org.date
    INNER JOIN datamodel_core.dim_devices dev
      ON BIGINT(replace(a.object_ids, 'device#', '')) = dev.device_id
      AND a.date = dev.date
    WHERE a.date BETWEEN COALESCE(NULLIF('$start_date',''),DATE_SUB(CURRENT_DATE(),3)) AND COALESCE(NULLIF('$end_date',''),DATE_SUB(CURRENT_DATE(),1))
      AND org.date BETWEEN COALESCE(NULLIF('$start_date',''),DATE_SUB(CURRENT_DATE(),3)) AND COALESCE(NULLIF('$end_date',''),DATE_SUB(CURRENT_DATE(),1))
      AND dev.date BETWEEN COALESCE(NULLIF('$start_date',''),DATE_SUB(CURRENT_DATE(),3)) AND COALESCE(NULLIF('$end_date',''),DATE_SUB(CURRENT_DATE(),1))
      AND ( -- https://github.com/samsara-dev/backend/blob/master/go/src/samsaradev.io/platform/workflows/triggertypes/trigger_types.go
          array_contains(a.proto.trigger_matches.trigger_type, 1000)
          OR array_contains(a.proto.trigger_matches.trigger_type, 1001)
          OR array_contains(a.proto.trigger_matches.trigger_type, 1002)
          OR array_contains(a.proto.trigger_matches.trigger_type, 1028))
      AND org.is_paid_safety_customer
    GROUP BY 1,2,3,4,5,6,7,8,9,10
  )

  SELECT b.date,
        b.org_id,
        b.org_name,
        b.account_size_segment_name,
        b.account_industry,
        b.device_id,
        b.occurred_at_ms,
        b.workflow_id,
        b.alert_name,
        b.product_name,
        loc.value.revgeo_state,
        loc.value.revgeo_country
  FROM base b
  LEFT JOIN kinesisstats.location loc
    ON b.date = loc.date
    AND b.device_id = loc.device_id
    AND b.occurred_at_ms = loc.time
);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS data_analytics.speed_alert_two
USING delta
PARTITIONED BY (date)
AS
SELECT * FROM speed_alert_two

-- COMMAND ----------

CREATE OR REPLACE TEMPORARY VIEW speed_alert_two_updates AS (
  SELECT *
  FROM speed_alert_two
  WHERE date BETWEEN COALESCE(NULLIF('$start_date',''),DATE_SUB(CURRENT_DATE(),3)) AND COALESCE(NULLIF('$end_date',''),DATE_SUB(CURRENT_DATE(),1))
);

MERGE INTO data_analytics.speed_alert_two AS target
USING speed_alert_two_updates AS updates
ON target.date = updates.date
AND target.org_id = updates.org_id
AND target.device_id = updates.device_id
AND target.occurred_at_ms = updates.occurred_at_ms
AND target.workflow_id = updates.workflow_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT * ;

-- COMMAND ----------

