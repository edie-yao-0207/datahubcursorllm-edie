-- Databricks notebook source
create or replace temp view full_upgrades as (
  select
    a.date,
    a.org_id,
    a.device_id,
    sum(
      case
        when b.value.proto_value.upgrade_durations.attempted_to_use_delta = false then 1
        else 0
      end
    ) as successful_full_upgrades_count
  from dataprep.active_devices as a
  left join kinesisstats.osdupgradedurations as b
    on a.device_id = b.object_id
    and a.org_id = b.org_id
  left join dataprep_firmware.data_ingestion_high_water_mark as c
  where a.date between coalesce(nullif(getArgument("start_date"), ''), date_sub(current_date(),8))
    and coalesce(nullif(getArgument("end_date"), ''),  current_date())
    and product_id in (43, 44, 155, 167, 126)
    and (b.object_id is null or b.time <= c.time_ms)
  group by
    a.date,
    a.org_id,
    a.device_id
);

-- COMMAND ----------

create table if not exists dataprep_firmware.cm_device_daily_full_upgrades
using delta
partitioned by (date)
as
select * from full_upgrades

-- COMMAND ----------

merge into dataprep_firmware.cm_device_daily_full_upgrades as target
using full_upgrades as updates
on target.org_id = updates.org_id
and target.device_id = updates.device_id
and target.date = updates.date
when matched then update set *
when not matched then insert * ;

-- COMMAND ----------

create or replace temp view max_device_upgrade_stats as (
  select
    a.date,
    a.org_id,
    a.device_id,
    max(b.value.proto_value.upgrade_durations.time_from_first_push_to_reboot_ms) as max_time_from_first_push_to_reboot_ms,
    max(b.value.proto_value.upgrade_durations.delta_download_duration_ms) as max_delta_download_duration_ms,
    max(b.value.proto_value.upgrade_durations.delta_apply_duration_ms) as max_delta_apply_duration_ms,
    max(b.value.proto_value.upgrade_durations.full_script_download_duration_ms) as max_full_script_download_duration_ms,
    max(b.value.proto_value.upgrade_durations.upgrade_script_duration_ms) as max_upgrade_script_duration_ms,
    max(b.value.proto_value.upgrade_durations.wait_duration_ms) as max_wait_duration_ms
  from dataprep.active_devices as a
  join kinesisstats.osdupgradedurations as b
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

create table if not exists dataprep_firmware.cm_device_daily_upgrade_stats
using delta
partitioned by (date)
as
select * from max_device_upgrade_stats

-- COMMAND ----------

merge into dataprep_firmware.cm_device_daily_upgrade_stats as target
using max_device_upgrade_stats as updates
on target.org_id = updates.org_id
and target.device_id = updates.device_id
and target.date = updates.date
when matched then update set *
when not matched then insert * ;

-- COMMAND ----------
