create or replace temp view video_upload_errors_parsed as (
  select
    date,
    org_id,
    object_id as device_id,
    left(value.proto_value.uploaded_file_set.error_message, 28) as error_message_short
  from kinesisstats_history.osduploadedfileset as a
  join dataprep_firmware.data_ingestion_high_water_mark as b
  where value.proto_value.uploaded_file_set.error_message is not null
    and value.proto_value.uploaded_file_set.triggering_report.report_type is null -- VIDEO_REPORT
    and value.proto_value.uploaded_file_set.triggering_report.trigger_reason = 4 -- RECALL
    and date between coalesce(nullif(getArgument("start_date"), ''), date_sub(current_date(), 7))
    and coalesce(nullif(getArgument("end_date"), ''),  current_date())
    and a.time <= b.time_ms
);

create or replace temp view device_daily_video_upload_errors as (
  select
    date,
    org_id,
    device_id,
    error_message_short,
    count(*) num_errors
  from video_upload_errors_parsed
  group by
    date,
    org_id,
    device_id,
    error_message_short
);

-- COMMAND ----------

create table if not exists dataprep_firmware.device_daily_video_errors_by_error using delta
partitioned by (date)
select * from device_daily_video_upload_errors

-- COMMAND ----------

merge into dataprep_firmware.device_daily_video_errors_by_error as target
using device_daily_video_upload_errors as updates
on target.org_id = updates.org_id
and target.device_id = updates.device_id
and target.date = updates.date
and target.error_message_short = updates.error_message_short
when matched then update set *
when not matched then insert * ;

-- COMMAND ----------

