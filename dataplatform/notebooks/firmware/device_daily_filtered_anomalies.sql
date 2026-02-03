create or replace temp view anomalies_parsed as (
  select
    date,
    time,
    org_id,
    object_id as device_id,
    case
      when charindex(':', value.proto_value.anomaly_event.service_name) == 0 THEN value.proto_value.anomaly_event.service_name
      else left(value.proto_value.anomaly_event.service_name, charindex(':', value.proto_value.anomaly_event.service_name) - 1)
    end as anomaly_service
  from kinesisstats.osdanomalyevent as a
  join dataprep_firmware.data_ingestion_high_water_mark as b
  where date between coalesce(nullif(getArgument("start_date"), ''), date_sub(current_date(), 7))
    and coalesce(nullif(getArgument("end_date"), ''),  current_date())
    and a.time <= b.time_ms
    -- These are noisy anomalies that don't have a customer impact that we want to filter out.
    and NOT value.proto_value.anomaly_event.description RLIKE r'Timed out waiting for firmware metrics client to close. Metrics may have been lost.'
    and NOT value.proto_value.anomaly_event.description RLIKE r'StreamFirmwareMetric client Failed to connect firmware metric stream: rpc error: code = Canceled'
    and NOT value.proto_value.anomaly_event.description RLIKE r'StreamFirmwareMetric dropped metrics at shutdown'
    and NOT value.proto_value.anomaly_event.description RLIKE r'FirmwareMetricsClient is dropping metrics'
    and NOT value.proto_value.anomaly_event.description RLIKE r'StreamFirmwareMetric client send call failed: EOF'
    and NOT value.proto_value.anomaly_event.description RLIKE r'StreamFirmwareMetric client stream closure'
);

create or replace temp view device_daily_filtered_anomalies as (
  select
    date,
    org_id,
    device_id,
    anomaly_service,
    count(*) as num_anomalies
  from anomalies_parsed
  group by
    date,
    org_id,
    device_id,
    anomaly_service
);

create or replace temp view daily_active_device_count as (
  select
    date,
    org_id,
    object_id as device_id
  from kinesisstats.osdhubserverdeviceheartbeat as a
  join dataprep_firmware.data_ingestion_high_water_mark as b
  where date between coalesce(nullif(getArgument("start_date"), ''), date_sub(current_date(), 7))
    and coalesce(nullif(getArgument("end_date"), ''),  current_date())
    and a.time <= b.time_ms
  group by
    date,
    org_id,
    object_id
);

create or replace temp view trip_duration_per_day as (
  select
    a.date,
    a.org_id,
    b.cm_device_id,
    sum(a.end_ms - a.start_ms) as trip_duration_ms
  from trips2db_shards.trips as a
  join dataprep_firmware.cm_octo_vg_daily_associations_unique as b
    on a.date = b.date
    and a.device_id = b.vg_device_id
  where a.date between coalesce(nullif(getArgument("start_date"), ''), date_sub(current_date(), 7))
    and coalesce(nullif(getArgument("end_date"), ''),  current_date())
  group by
    a.date,
    a.org_id,
    b.cm_device_id
);

create or replace temp view joined as (
  select
    a.date,
    a.org_id,
    a.device_id,
    c.anomaly_service,
    sum(b.trip_duration_ms) as total_trip_duration_ms,
    sum(c.num_anomalies) as num_anomalies
  from daily_active_device_count as a
  left join trip_duration_per_day as b
    on a.date = b.date
    and a.org_id = b.org_id
    and a.device_id = b.cm_device_id
  left join device_daily_filtered_anomalies as c
    on a.date = c.date
    and a.org_id = c.org_id
    and a.device_id = c.device_id
  group by
    a.date,
    a.org_id,
    a.device_id,
    c.anomaly_service
);

-- COMMAND ----------

create table if not exists dataprep_firmware.device_daily_filtered_anomalies_by_service using delta
partitioned by (date)
select * from joined

-- COMMAND ----------

merge into dataprep_firmware.device_daily_filtered_anomalies_by_service as target
using joined as updates
on target.org_id = updates.org_id
and target.device_id = updates.device_id
and target.date = updates.date
and target.anomaly_service = updates.anomaly_service
when matched then update set *
when not matched then insert * ;

-- COMMAND ----------

