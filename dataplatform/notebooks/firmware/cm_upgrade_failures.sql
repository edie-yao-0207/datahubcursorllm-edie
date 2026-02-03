-- Databricks notebook source
create or replace temp view upgrade_failures as (
  select
    a.date,
    a.org_id,
    a.device_id,
    sum(
      case
        when b.value.proto_value.anomaly_event.description like "ERROR: Failed to download%" then 1
        else 0
      end
    ) as upgrade_download_failure_count,
    sum(
      case
        when b.value.proto_value.anomaly_event.description like "ERROR: Failed to run%" then 1
        else 0
      end
    ) as upgrade_install_failure_count
  from dataprep.active_devices as a
  join kinesisstats.osdanomalyevent as b
    on a.device_id = b.object_id
    and a.org_id = b.org_id
  join dataprep_firmware.data_ingestion_high_water_mark as c
  where a.date between coalesce(nullif(getArgument("start_date"), ''), date_sub(current_date(),8))
    and coalesce(nullif(getArgument("end_date"), ''),  current_date())
    and product_id in (43, 44, 155, 167, 126)
    and b.time <= c.time_ms
  group by
    a.date,
    a.org_id,
    a.device_id
);

-- COMMAND ----------

create table if not exists dataprep_firmware.cm_device_daily_upgrade_failures
using delta
partitioned by (date)
as
select * from upgrade_failures

-- COMMAND ----------

merge into dataprep_firmware.cm_device_daily_upgrade_failures as target
using upgrade_failures as updates
on target.org_id = updates.org_id
and target.device_id = updates.device_id
and target.date = updates.date
when matched then update set *
when not matched then insert * ;

-- COMMAND ----------



