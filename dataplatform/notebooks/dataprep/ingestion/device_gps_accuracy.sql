create or replace temp view device_gps_accuracy as (
  select
    date,
    device_id,
    org_id,
    count(device_id) as gps_count,
    max(value.accuracy_millimeters) as max_acc_milli,
    min(value.accuracy_millimeters) as min_acc_milli,
    mean(value.accuracy_millimeters) as mean_acc_milli
  from kinesisstats.location
  group by
    date,
    device_id,
    org_id
);

create table if not exists dataprep.device_gps_accuracy using delta partitioned by (date) as (
  select *
  from device_gps_accuracy
);

create or replace temp view device_gps_accuracy_updates as (
  select *
  from device_gps_accuracy
  where date between COALESCE(NULLIF(getArgument("start_date"), ''), date_sub(current_date(),3))
    and COALESCE(NULLIF(getArgument("end_date"), ''),  CURRENT_DATE())
);

merge into dataprep.device_gps_accuracy as target
using device_gps_accuracy_updates as updates
on target.date = updates.date
and target.device_id = updates.device_id
and target.org_id = updates.org_id
when matched then update set *
when not matched then insert *;

alter table dataprep.device_gps_accuracy set tblproperties ('comment' = 'A table containing a daily summary of GPS accuracy statistics for devices');
alter table dataprep.device_gps_accuracy change date comment "The calendar date in 'YYYY-mm-dd' format";
alter table dataprep.device_gps_accuracy change device_id comment 'The Samsara ID of the attached device';
alter table dataprep.device_gps_accuracy change org_id comment 'The Samsara cloud dashboard ID that the data belongs to';
alter table dataprep.device_gps_accuracy change gps_count comment 'The count of gps readings from the device for the day';
alter table dataprep.device_gps_accuracy change max_acc_milli comment 'The maximum gps accuracy reading from the device for the day (in millimeters)';
alter table dataprep.device_gps_accuracy change min_acc_milli comment 'The minimum gps accuracy reading from the device for the day (in millimeters)';
alter table dataprep.device_gps_accuracy change mean_acc_milli comment 'The average gps accuracy reading from the device for the day (in millimeters';
