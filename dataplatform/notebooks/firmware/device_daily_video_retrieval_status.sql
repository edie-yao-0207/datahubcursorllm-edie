create or replace temporary view cm_video_retrieval_state as (
  select
    date,
    device_id,
    count(*) as num_requests,
    case
      when state = 0 then "Pending"
      when state = 1 then "Failed"
      when state = 2 then "Completed"
      when state = 3 then "TimedOut"
      when state = 4 then "Cancelled"
    end as state
  from clouddb.historical_video_requests
  where is_incognito = 0
    and date between coalesce(nullif(getArgument("start_date"), ''), date_sub(current_date(), 7))
    and coalesce(nullif(getArgument("end_date"), ''),  current_date())
  group by
    date,
    device_id,
    state
);

-- COMMAND ----------

create table if not exists dataprep_firmware.device_daily_video_retrievals_by_state using delta
partitioned by (date)
select * from cm_video_retrieval_state

-- COMMAND ----------

merge into dataprep_firmware.device_daily_video_retrievals_by_state as target
using cm_video_retrieval_state as updates
on target.device_id = updates.device_id
and target.date = updates.date
and target.state = updates.state
when matched then update set *
when not matched then insert * ;

-- COMMAND ----------
