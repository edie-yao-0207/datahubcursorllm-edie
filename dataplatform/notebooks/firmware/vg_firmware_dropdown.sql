-- COMMAND ----------

create or replace table dataprep_firmware.all_vg_firmware_builds as (
  select distinct(last_reported_vg_build) as firmware_build
  from dataprep_firmware.cm_device_daily_metadata
  order by firmware_build desc
)
