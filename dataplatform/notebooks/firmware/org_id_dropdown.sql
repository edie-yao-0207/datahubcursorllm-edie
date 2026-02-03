-- COMMAND ----------

create or replace table dataprep_firmware.all_org_ids as (
  select distinct(org_id) as org_id
  from dataprep_firmware.cm_device_daily_metadata
  order by org_id desc
)
