-- Databricks notebook source
create or replace temporary view data as (
  with metrics as (
    select
      `date`,
      org_id,
      object_id as device_id,
      metric.name as metric_name,
      coalesce(sum(metric.value), 0) as `value`

    from
      kinesisstats.osDFirmwareMetrics

    lateral view
      explode(value.proto_value.firmware_metrics.metrics) as metric

    where
      date between date_sub(current_date(), 8) and current_date()
      and org_id != 1
      and metric.metric_type = 1 -- 1: COUNT
      and metric.name like "cableid.%"

    group by
      `date`,
      org_id,
      object_id,
      metric.name
  )

  , metrics_counts as (
    select
      `date`,
      org_id,
      device_id,
      map_from_entries(
        collect_list(named_struct('key', metric_name, 'value', `value`))
      ) as metric_counts

    from
      metrics

    group by
      `date`,
      org_id,
      device_id
  )

  select
    a.date,
    a.org_id,
    b.org_type,
    a.device_id,
    b.product_type,
    b.latest_build_on_day as build,
    b.rollout_stage_id,
    b.product_program_id,
    coalesce(a.metric_counts.`cableid.override.used`, 0) as `cableid.override.used`,
    coalesce(a.metric_counts.`cableid.persisted.changed`, 0) as `cableid.persisted.changed`,
    coalesce(a.metric_counts.`cableid.persisted.created`, 0) as `cableid.persisted.created`,
    coalesce(a.metric_counts.`cableid.persisted.used_on_error`, 0) as `cableid.persisted.used_on_error`,
    coalesce(a.metric_counts.`cableid.persisted.used_on_zero`, 0) as `cableid.persisted.used_on_zero`,
    coalesce(a.metric_counts.`cableid.read.error`, 0) as `cableid.read.error`,
    coalesce(a.metric_counts.`cableid.read.ok_nonzero`, 0) as `cableid.read.ok_nonzero`,
    coalesce(a.metric_counts.`cableid.read.zero`, 0) as `cableid.read.zero`

  from metrics_counts as a

  join data_analytics.dataprep_vg3x_daily_health_metrics as b
    on a.date = b.date
    and a.org_id = b.org_id
    and a.device_id = b.device_id
)

-- COMMAND ----------

create table if not exists firmware.vg_cable_id_cache_metrics
using delta
partitioned by (date)
select * from data

-- COMMAND ----------

merge into firmware.vg_cable_id_cache_metrics as target
using data as updates
on target.date = updates.date
and target.org_id = updates.org_id
and target.device_id = updates.device_id
and target.product_type = updates.product_type
when matched then update set *
when not matched then insert * ;

ALTER TABLE firmware.vg_cable_id_cache_metrics
SET TBLPROPERTIES ('comment' = 'This table contains metrics from the cable ID cache feature');

ALTER TABLE firmware.vg_cable_id_cache_metrics
CHANGE date
COMMENT 'The date that we are reporting cable ID cache information for.';

ALTER TABLE firmware.vg_cable_id_cache_metrics
CHANGE org_id
COMMENT 'The Samsara Cloud dashboard ID that the data belongs to';

ALTER TABLE firmware.vg_cable_id_cache_metrics
CHANGE org_type
COMMENT 'The organization type that these devices belong to. One of Internal, or Customer.';

ALTER TABLE firmware.vg_cable_id_cache_metrics
CHANGE device_id
COMMENT 'The device ID of the VG that we are reporting metrics for';

ALTER TABLE firmware.vg_cable_id_cache_metrics
CHANGE product_type
COMMENT 'The product type of the VG';

ALTER TABLE firmware.vg_cable_id_cache_metrics
CHANGE build
COMMENT 'The firmware build running on the VG';

ALTER TABLE firmware.vg_cable_id_cache_metrics
CHANGE rollout_stage_id
COMMENT 'The firmware rollout stage of the VG';

ALTER TABLE firmware.vg_cable_id_cache_metrics
CHANGE product_program_id
COMMENT 'The product program ID for the firmware rollout for the VG';

ALTER TABLE firmware.vg_cable_id_cache_metrics
CHANGE `cableid.override.used`
COMMENT 'Count of the number of times the cable override was used instead of reading the hardware cable ID';

ALTER TABLE firmware.vg_cable_id_cache_metrics
CHANGE `cableid.persisted.changed`
COMMENT 'Count of the number of times the persisted cable ID cache value was changed due to a new, nonzero value being read from the hardware';

ALTER TABLE firmware.vg_cable_id_cache_metrics
CHANGE `cableid.persisted.created`
COMMENT 'Count of the number of times the persisted cable ID cache value was created';

ALTER TABLE firmware.vg_cable_id_cache_metrics
CHANGE `cableid.persisted.used_on_error`
COMMENT 'Count of the number of times the persisted cable ID was used because an error ocurred reading the hardware ID';

ALTER TABLE firmware.vg_cable_id_cache_metrics
CHANGE `cableid.persisted.used_on_zero`
COMMENT 'Count of the number of times the persisted cable ID was used because the hardware ID was read as 0';

ALTER TABLE firmware.vg_cable_id_cache_metrics
CHANGE `cableid.read.error`
COMMENT 'Count of the number of times an error ocurred reading the hardware cable ID';

ALTER TABLE firmware.vg_cable_id_cache_metrics
CHANGE `cableid.read.ok_nonzero`
COMMENT 'Count of the number of times a valid, nonzero value was read for the hardware cable ID';

ALTER TABLE firmware.vg_cable_id_cache_metrics
CHANGE `cableid.read.zero`
COMMENT 'Count of the number of times a zero value was received when reading the hardware cable ID';
