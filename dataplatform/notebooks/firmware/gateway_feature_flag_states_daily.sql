-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW current_day_gateway_feature_flag_states AS (
  SELECT
    current_date() AS date,
    ff.org_id,
    ff.gateway_id,
    array_distinct(ff.enabled_feature_flag_values.values) as enabled_feature_flag_values,
    ff.updated_at
  FROM releasemanagementdb_shards.gateway_feature_flag_states as ff
  JOIN clouddb.gateways as g
    ON ff.org_id = g.org_id
    AND ff.gateway_id = g.id
);

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS dataprep_firmware.gateway_feature_flag_states_daily
USING DELTA
PARTITIONED BY (date)
SELECT * FROM current_day_gateway_feature_flag_states
;

-- COMMAND ----------

merge into dataprep_firmware.gateway_feature_flag_states_daily as target
using current_day_gateway_feature_flag_states as updates
on target.org_id = updates.org_id
and target.gateway_id = updates.gateway_id
and target.date = updates.date
when matched then update set *
when not matched then insert * ;
