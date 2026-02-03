create or replace temporary view cm_livestream_connections as (
  select
    date,
    org_id,
    device_id,
    connected_at_unix_ms
  from datastreams.safety_webrtc_livestream_metrics
  where not user_id = 1
    and not (
      stop_reason = "user_intentional_stop"
      and connected_at_unix_ms = -1
    )
    and date between coalesce(nullif(getArgument("start_date"), ''), date_sub(current_date(), 7))
    and coalesce(nullif(getArgument("end_date"), ''),  current_date())
);

create or replace temporary view cm_device_daily_livestream_attempts as (
  select
    date,
    org_id,
    device_id,
    count(*) as total_connection_attempts,
    sum(
      case
        when connected_at_unix_ms = -1 then 0
        else 1
      end
    ) as successful_connection_attempts
  from cm_livestream_connections
  group by
    date,
    org_id,
    device_id
);

-- COMMAND ----------

create table if not exists dataprep_firmware.cm_device_daily_livestream_attempts using delta
partitioned by (date)
select * from cm_device_daily_livestream_attempts

-- COMMAND ----------

merge into dataprep_firmware.cm_device_daily_livestream_attempts as target
using cm_device_daily_livestream_attempts as updates
on target.org_id = updates.org_id
and target.device_id = updates.device_id
and target.date = updates.date
when matched then update set *
when not matched then insert * ;

-- COMMAND ----------

