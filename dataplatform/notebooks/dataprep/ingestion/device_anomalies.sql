create or replace temporary view device_anomalies as (
  select
    date,
    object_id as device_id,
    org_id,
    substring_index(value.proto_value.anomaly_event.service_name, ':', 2) as anomaly_event_service,
    count(substring_index(value.proto_value.anomaly_event.service_name, ':', 2)) as ae_count
  from kinesisstats.osdanomalyevent
  where
    value.is_databreak = 'false' and
    value.is_end = 'false'
  group by
    date,
    device_id,
    org_id,
    substring_index(value.proto_value.anomaly_event.service_name, ':', 2)
);

create table if not exists dataprep.device_anomalies using delta partitioned by (date) as (
  select *
  from device_anomalies
);

create or replace temp view device_anomalies_updates as (
  select *
  from device_anomalies
  where date between COALESCE(NULLIF(getArgument("start_date"), ''), date_sub(current_date(),3))
    and COALESCE(NULLIF(getArgument("end_date"), ''),  CURRENT_DATE())
);

merge into dataprep.device_anomalies as target
using device_anomalies_updates as updates
on target.date = updates.date
and target.device_id = updates.device_id
and target.org_id = updates.org_id
and target.anomaly_event_service = updates.anomaly_event_service
when matched then update set *
when not matched then insert *;
