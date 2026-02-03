create or replace temp view hearbeat_on_day as (
  select
    org_id,
    object_id as device_id,
    date
  from kinesisstats_history.osdhubserverdeviceheartbeat as a
  join dataprep_firmware.data_ingestion_high_water_mark as b
  where date between coalesce(nullif(getArgument("start_date"), ''), date_sub(current_date(),14))
    and coalesce(nullif(getArgument("end_date"), ''),  current_date())
    and a.time <= b.time_ms
  group by
    org_id,
    object_id,
    date
);

create or replace temp view lead as (
  select
    org_id,
    device_id,
    date,
    lead(date) over(partition by org_id, device_id order by date asc) as next_date,
    least(date_add(date, 14), current_date()) as last_active_date
  from hearbeat_on_day
);

create or replace temp view exploded as (
  select
    org_id,
    device_id,
    explode(sequence(date(date), date(least(date_sub(next_date, 1), date(last_active_date))), interval 1 day)) as date
  from lead
);

-- COMMAND ----------

create table if not exists dataprep_firmware.active_devices_based_on_heartbeat_in_last_14_days using delta
partitioned by (date)
select * from exploded

-- COMMAND ----------

merge into dataprep_firmware.active_devices_based_on_heartbeat_in_last_14_days as target
using exploded as updates
on target.org_id = updates.org_id
and target.device_id = updates.device_id
and target.date = updates.date
when matched then update set *
when not matched then insert * ;

-- COMMAND ----------
