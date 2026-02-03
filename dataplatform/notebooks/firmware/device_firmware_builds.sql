create or replace temp view hearbeat_on_day as (
  select
    date,
    value.proto_value.hub_server_device_heartbeat.connection.device_hello.gateway_id,
    max_by(value.proto_value.hub_server_device_heartbeat.connection.device_hello.build, time) as latest_build_on_day
  from kinesisstats_history.osdhubserverdeviceheartbeat as a
  join dataprep_firmware.data_ingestion_high_water_mark as b
  where date between coalesce(nullif(getArgument("start_date"), ''), date_sub(current_date(),14))
    and coalesce(nullif(getArgument("end_date"), ''),  current_date())
    and value.proto_value.hub_server_device_heartbeat.connection.device_hello.build is not null
    and a.time <= b.time_ms
  group by
    date,
    value.proto_value.hub_server_device_heartbeat.connection.device_hello.gateway_id
);

create or replace temp view lead as (
  select
    date,
    gateway_id,
    latest_build_on_day,
    lead(date) over(partition by gateway_id order by date asc) as next_date
  from hearbeat_on_day
);

create or replace temp view exploded as (
  select
    gateway_id,
    latest_build_on_day,
    explode(
      case
        when next_date is null and date = current_date() then array(date(date))
        when next_date is null and date < current_date() then sequence(date(date), current_date(), interval 1 day)
        else sequence(date(date), date_sub(next_date, 1), interval 1 day)
      end
    ) as date
  from lead
);

-- COMMAND ----------

create table if not exists dataprep_firmware.device_daily_firmware_builds using delta
partitioned by (date)
select * from exploded

-- COMMAND ----------

merge into dataprep_firmware.device_daily_firmware_builds as target
using exploded as updates
on target.gateway_id = updates.gateway_id
and target.date = updates.date
when matched then update set *
when not matched then insert * ;

-- COMMAND ----------
