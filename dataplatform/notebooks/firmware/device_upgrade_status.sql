-- Databricks notebook source
-- Reasons for not upgrading
-- download in progress
-- installation in progress
-- offline
-- waiting to install (still moving)
-- download failed
-- installation failed
-- firmware override
-- firmware not pushed
-- device inactive

-- COMMAND ----------

create or replace temp view upgrade_installation_failures as (
  select
    date,
    org_id,
    object_id as device_id,
    time
  from kinesisstats.osdanomalyevent as a
  join dataprep_firmware.data_ingestion_high_water_mark as b
  where date >= date_sub(current_date(), 14)
    and value.proto_value.anomaly_event.description like "ERROR: Failed to run%"
    and a.time <= b.time_ms
);

create or replace temp view upgrade_installation_failure as (
  select
    max(date) as date,
    max(time) as time,
    org_id,
    device_id
  from upgrade_installation_failures
  group by
    device_id,
    org_id
);

create or replace temp view upgrade_download_failures as (
  select
    date,
    org_id,
    object_id as device_id,
    time
  from kinesisstats.osdanomalyevent as a
  join dataprep_firmware.data_ingestion_high_water_mark as b
  where date >= date_sub(current_date(), 14)
    and value.proto_value.anomaly_event.description like "ERROR: Failed to download%"
    and a.time <= b.time_ms
);

create or replace temp view upgrade_download_failure as (
  select
    max(date) as date,
    max(time) as time,
    org_id,
    device_id
  from upgrade_download_failures
  group by
    device_id,
    org_id
);

create or replace temp view upgrade_too_many_tries_failures as (
  select
    date,
    org_id,
    object_id as device_id,
    time
  from kinesisstats.osdanomalyevent as a
  join dataprep_firmware.data_ingestion_high_water_mark as b
  where date >= date_sub(current_date(), 14)
    and value.proto_value.anomaly_event.description like "panic: too many tries%"
    and a.time <= b.time_ms
);

create or replace temp view upgrade_too_many_tries_failure as (
  select
    max(date) as date,
    max(time) as time,
    org_id,
    device_id
  from upgrade_too_many_tries_failures
  group by
    device_id,
    org_id
);

create or replace temp view upgrade_download_corruption_failures as (
  select
    date,
    org_id,
    object_id as device_id,
    time
  from kinesisstats.osdanomalyevent as a
  join dataprep_firmware.data_ingestion_high_water_mark as b
  where date >= date_sub(current_date(), 14)
    and value.proto_value.anomaly_event.description like "%exceeded 2 corrupt tries%"
    and a.time <= b.time_ms
);

create or replace temp view upgrade_download_corruption_failure as (
  select
    max(date) as date,
    max(time) as time,
    org_id,
    device_id
  from upgrade_download_corruption_failures
  group by
    device_id,
    org_id
);

-- COMMAND ----------

create or replace temporary view upgrade_installing as (
  select
    org_id,
    object_id as device_id,
    max(date) as date,
    max(time) as time
  from kinesisstats.osdgatewayupgradeattempt as a
  join dataprep_firmware.data_ingestion_high_water_mark as b
  where date >= date_sub(current_date(), 14)
    and a.time <= b.time_ms
  group by
    org_id,
    object_id
);


-- COMMAND ----------

create or replace temporary view exploded_cell_usage as (
  select
    date,
    time,
    explode(
      concat(
        value.proto_value.incremental_cellular_usage.attached_device_usage,
        ARRAY(
          named_struct(
            "object_id", object_id,
            "bucket", value.proto_value.incremental_cellular_usage.bucket,
            "total_upload_bytes", value.proto_value.incremental_cellular_usage.total_upload_bytes,
            "total_download_bytes", value.proto_value.incremental_cellular_usage.total_download_bytes,
            "hub_client_usage", cast(null as array<struct<usage_type:int, upload_bytes:bigint, download_bytes:bigint>>)
          )
        )
      )
    ) usage
  from kinesisstats.osdincrementalcellularusage as a
  join dataprep_firmware.data_ingestion_high_water_mark as b
  where date >= date_sub(current_date(), 14)
    and a.time <= b.time_ms
);


create or replace temporary view exploded_exploded_cell_usage as (
  select
    date,
    usage.object_id as device_id,
    usage.total_download_bytes,
    time,
    explode(usage.bucket) as bucket
  from exploded_cell_usage
);

create or replace temporary view upgrade_download_cell_usage as (
  select
    date,
    device_id,
    time
  from exploded_exploded_cell_usage
  where bucket.bucket_type in (2, 3)
    and total_download_bytes > 0
);

create or replace temporary view upgrade_downloading as (
  select
    device_id,
    max(time) as time,
    max(date) as date
  from upgrade_download_cell_usage
  group by
    device_id
);


-- COMMAND ----------

create or replace temp view latest_hb as (
  select
    max(date) as date,
    org_id,
    object_id as device_id,
    max(time) as time
  from kinesisstats.osdhubserverdeviceheartbeat as a
  join dataprep_firmware.data_ingestion_high_water_mark as b
  where date >= date_sub(current_date(), 14)
    and a.time <= b.time_ms
  group by
    org_id,
    object_id
);

create or replace temp view device_hello_builds as (
  select
    date,
    org_id,
    object_id as device_id,
    time,
    value.proto_value.hub_server_device_heartbeat.connection.device_hello.build
  from kinesisstats.osdhubserverdeviceheartbeat as a
  join dataprep_firmware.data_ingestion_high_water_mark as b
  where date >= date_sub(current_date(), 14)
    and value.proto_value.hub_server_device_heartbeat.connection.device_hello.build is not null
    and a.time <= b.time_ms
);

create or replace temp view most_recent_builds as (
  select
    max(date) as date,
    org_id,
    device_id,
    max(time) as time,
    max_by(build, time) as latest_build
  from device_hello_builds
  group by
    org_id,
    device_id
);

create or replace temp view active_devices as (
    select
      a.org_id,
      a.device_id,
      a.time as last_heartbeat_ms,
      a.date as last_heartbeat_date,
      b.latest_build
    from latest_hb as a
    join most_recent_builds as b
      on a.org_id = b.org_id
      and a.device_id = b.device_id
);

-- COMMAND ----------

create or replace temp view default_gateway_enrollment as (
   select
    a.org_id,
    a.id as gateway_id,
    a.device_id,
    b.product_group_id,
    a.product_id,
    0 as product_program_id,
    coalesce(c.rollout_stage_id, d.rollout_stage_id, 600) as rollout_stage_id
  from productsdb.gateways as a
  join definitions.products as b
    on a.product_id = b.product_id
  left join firmwaredb.gateway_rollout_stages as c
    on a.product_id = c.product_id
    and a.org_id = c.org_id
    and a.id = c.gateway_id
  left join firmwaredb.product_group_rollout_stage_by_org as d
    on b.product_group_id = d.product_group_id
    and a.org_id = d.org_id
);

create or replace temp view desired_default_firmware as (
  select
    c.id,
    c.name,
    c.build,
    a.org_id,
    a.gateway_id
  from default_gateway_enrollment as a
  join firmwaredb.rollout_stage_firmwares as b
    on a.product_id = b.product_id
    and a.product_program_id = b.product_program_id
    and a.rollout_stage_id = b.rollout_stage
  join firmwaredb.product_firmwares as c
    on b.product_firmware_id = c.id
    and b.product_id = c.product_id
);


create or replace temp view gateway_enrollment as (
  select *
  from dataprep_firmware.gateway_daily_rollout_stages
  where date = date_sub(current_date(), 2)
);

create or replace temp view desired_non_default_firmware as (
  select
    c.id,
    c.name,
    c.build,
    a.org_id,
    a.gateway_id
  from gateway_enrollment as a
  join firmwaredb.rollout_stage_firmwares as b
    on a.product_id = b.product_id
    and a.product_program_id = b.product_program_id
    and a.rollout_stage_id = b.rollout_stage
  join firmwaredb.product_firmwares as c
    on b.product_firmware_id = c.id
    and b.product_id = c.product_id
);

create or replace temp view firmware_upgrade_status_by_reason as (
  select
    d.org_id,
    d.id as gateway_id,
    d.device_id,
    d.product_id,
    a.build as desired_build,
    c.latest_build as current_build,
    e.time as etime,
    f.time as ftime,
    g.time as gtime,
    h.time as htime,
    j.time as jtime,
    k.time as ktime,
    case
      when a.build = c.latest_build then "upgrade complete"
      when b.build != a.build then "custom firmware program or override"
      when e.time is not null and e.time > coalesce(greatest(f.time, g.time, h.time, j.time, k.time), 0) then "upgrade downloading or waiting for movement"
      when f.time is not null and f.time > coalesce(greatest(e.time, g.time, h.time, j.time, k.time), 0) then "upgrade installing"
      when g.time is not null and g.time > coalesce(greatest(f.time, e.time, h.time, j.time, k.time), 0) then "upgrade download failed"
      when h.time is not null and h.time > coalesce(greatest(f.time, g.time, e.time, j.time, k.time), 0) then "upgrade install failed"
      when j.time is not null and j.time > coalesce(greatest(f.time, g.time, e.time, h.time, k.time), 0) then "upgrade too many tries downloading"
      when k.time is not null and k.time > coalesce(greatest(f.time, g.time, e.time, h.time, j.time), 0) then "upgrade download corruption"
      when c.device_id is null then "inactive"
      when c.last_heartbeat_ms < (unix_timestamp(current_timestamp()) * 1000) - (6 * 60 * 60 * 1000) then "offline"
      when e.time is null then "upgrade not pushed"
      else "unknown reason"
    end as reason
  from desired_default_firmware as a
  join desired_non_default_firmware as b
    on a.org_id = b.org_id
    and a.gateway_id = b.gateway_id
  join clouddb.gateways as d
    on a.org_id = d.org_id
    and a.gateway_id = d.id
  left join active_devices as c
    on a.org_id = c.org_id
    and d.device_id = c.device_id
  left join upgrade_downloading as e
    on d.device_id = e.device_id
  left join upgrade_installing as f
    on a.org_id = f.org_id
    and d.device_id = f.device_id
  left join upgrade_download_failure as g
    on a.org_id = g.org_id
    and d.device_id = g.device_id
  left join upgrade_installation_failure as h
    on a.org_id = h.org_id
    and d.device_id = h.device_id
  left join internaldb.simulated_orgs as i
    on a.org_id = i.org_id
  left join upgrade_too_many_tries_failure as j
    on a.org_id = j.org_id
    and d.device_id = j.device_id
  left join upgrade_download_corruption_failures as k
    on a.org_id = k.org_id
    and d.device_id = k.device_id
  where i.org_id is null
);

-- COMMAND ----------

create or replace table dataprep_firmware.firmware_upgrade_status_by_reason as (
  select * from firmware_upgrade_status_by_reason
);

-- COMMAND ----------
