create or replace table dataprep_firmware.cm_octo_vg_daily_associations_unique using delta partitioned by (date) as (
(
  select
    date,
    vg_gateway_id,
    vg_device_id,
    cm_gateway_id,
    cm_device_id
  from dataprep_firmware.cm_vg_daily_associations_unique
) union (
  select
    date,
    vg_gateway_id,
    vg_device_id,
    octo_gateway_id as cm_gateway_id,
    octo_device_id as cm_device_id
  from dataprep_firmware.octo_vg_daily_associations_unique
  )
);
