-- COMMAND ----------

create or replace table dataprep_firmware.all_cm_product_program_ids as (
  select distinct(cm_product_program_id) as cm_product_program_id
  from dataprep_firmware.cm_device_daily_metadata
  order by cm_product_program_id desc
)
