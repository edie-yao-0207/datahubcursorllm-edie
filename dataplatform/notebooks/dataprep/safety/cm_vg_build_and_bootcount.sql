-- Databricks notebook source
create or replace temporary view osdhubserverdeviceheartbeat as (
  select
    org_id,
    object_id,
    time,
    date,
    value.proto_value.hub_server_device_heartbeat.connection.device_hello.boot_count,
    value.proto_value.hub_server_device_heartbeat.connection.device_hello.build
  from kinesisstats.osdhubserverdeviceheartbeat
  WHERE date between COALESCE(NULLIF(getArgument("start_date"), ''), date_sub(current_date(),3))
                 and COALESCE(NULLIF(getArgument("end_date"), ''),  CURRENT_DATE())
)

-- COMMAND ----------

create or replace temporary view device_bootcount_vg as
(
  select
    cm_vg_intervals.org_id,
    cm_vg_intervals.device_id,
    cm_vg_intervals.cm_device_id,
    cm_vg_intervals.product_id,
    cm_vg_intervals.cm_product_id,
    cm_vg_intervals.date,
    cm_vg_intervals.start_ms,
    cm_vg_intervals.end_ms,
    max(osdhubserverdeviceheartbeat.boot_count) -
    min(osdhubserverdeviceheartbeat.boot_count) as vg_bootcount
  from (SELECT * FROM dataprep_safety.cm_vg_intervals
        WHERE date between COALESCE(NULLIF(getArgument("start_date"), ''), date_sub(current_date(),3))
                       and COALESCE(NULLIF(getArgument("end_date"), ''),  CURRENT_DATE())
       )  cm_vg_intervals
  left join osdhubserverdeviceheartbeat on
    osdhubserverdeviceheartbeat.object_id = cm_vg_intervals.device_id and
    osdhubserverdeviceheartbeat.org_id = cm_vg_intervals.org_id and
    osdhubserverdeviceheartbeat.time >= cm_vg_intervals.start_ms - 5*60*1000 and
    osdhubserverdeviceheartbeat.time <= cm_vg_intervals.end_ms and
    osdhubserverdeviceheartbeat.date = cm_vg_intervals.date
  group by
    cm_vg_intervals.org_id,
    cm_vg_intervals.device_id,
    cm_vg_intervals.cm_device_id,
    cm_vg_intervals.product_id,
    cm_vg_intervals.cm_product_id,
    cm_vg_intervals.start_ms,
    cm_vg_intervals.end_ms,
    cm_vg_intervals.date
);


-- COMMAND ----------

create or replace temporary view device_build_and_bootcount_vg as
(
  select
    device_bootcount_vg.org_id,
    device_bootcount_vg.device_id,
    device_bootcount_vg.cm_device_id,
    device_bootcount_vg.product_id,
    device_bootcount_vg.cm_product_id,
    device_bootcount_vg.date,
    device_bootcount_vg.start_ms,
    device_bootcount_vg.end_ms,
    device_bootcount_vg.vg_bootcount,
    min((osdhubserverdeviceheartbeat.time, osdhubserverdeviceheartbeat.build)).build as vg_build
    from device_bootcount_vg
    left join osdhubserverdeviceheartbeat on
      osdhubserverdeviceheartbeat.object_id = device_bootcount_vg.device_id and
      osdhubserverdeviceheartbeat.org_id = device_bootcount_vg.org_id and
      osdhubserverdeviceheartbeat.date = device_bootcount_vg.date
    group by
      device_bootcount_vg.org_id,
      device_bootcount_vg.device_id,
      device_bootcount_vg.cm_device_id,
      device_bootcount_vg.product_id,
      device_bootcount_vg.cm_product_id,
      device_bootcount_vg.start_ms,
      device_bootcount_vg.end_ms,
      device_bootcount_vg.date,
      device_bootcount_vg.vg_bootcount
)

-- COMMAND ----------

create or replace temporary view cm_bootcount as
(
  select
    device_build_and_bootcount_vg.*,
    max(osdhubserverdeviceheartbeat.boot_count) -
    min(osdhubserverdeviceheartbeat.boot_count) as cm_bootcount
  from device_build_and_bootcount_vg
  left join osdhubserverdeviceheartbeat on
    osdhubserverdeviceheartbeat.object_id = device_build_and_bootcount_vg.cm_device_id and
    osdhubserverdeviceheartbeat.org_id = device_build_and_bootcount_vg.org_id and
    osdhubserverdeviceheartbeat.time >= device_build_and_bootcount_vg.start_ms - 5*60*1000 and
    osdhubserverdeviceheartbeat.time <= device_build_and_bootcount_vg.end_ms and
    osdhubserverdeviceheartbeat.date = device_build_and_bootcount_vg.date
  group by
    device_build_and_bootcount_vg.org_id,
    device_build_and_bootcount_vg.device_id,
    device_build_and_bootcount_vg.cm_device_id,
    device_build_and_bootcount_vg.product_id,
    device_build_and_bootcount_vg.cm_product_id,
    device_build_and_bootcount_vg.start_ms,
    device_build_and_bootcount_vg.end_ms,
    device_build_and_bootcount_vg.date,
    device_build_and_bootcount_vg.vg_build,
    device_build_and_bootcount_vg.vg_bootcount
);

-- COMMAND ----------

create or replace temporary view device_build_and_bootcount as
(
  select
    cm_bootcount.*,
    min((osdhubserverdeviceheartbeat.time, osdhubserverdeviceheartbeat.build)).build as cm_build
  from cm_bootcount
  left join osdhubserverdeviceheartbeat on
    osdhubserverdeviceheartbeat.object_id = cm_bootcount.cm_device_id and
    osdhubserverdeviceheartbeat.org_id = cm_bootcount.org_id and
    osdhubserverdeviceheartbeat.date = cm_bootcount.date
  group by
    cm_bootcount.org_id,
    cm_bootcount.device_id,
    cm_bootcount.cm_device_id,
    cm_bootcount.product_id,
    cm_bootcount.cm_product_id,
    cm_bootcount.start_ms,
    cm_bootcount.end_ms,
    cm_bootcount.date,
    cm_bootcount.vg_build,
    cm_bootcount.vg_bootcount,
    cm_bootcount.cm_bootcount
);
-- COMMAND ----------

create table if not exists dataprep_safety.device_build_and_bootcount
using delta
partitioned by (date)
as
select * from device_build_and_bootcount

-- COMMAND ----------

create or replace temporary view device_build_and_bootcount_updates as
(
  select
    *
  from device_build_and_bootcount
  where date between COALESCE(NULLIF(getArgument("start_date"), ''), date_sub(current_date(),3))
    and COALESCE(NULLIF(getArgument("end_date"), ''),  CURRENT_DATE())
);

merge into dataprep_safety.device_build_and_bootcount as target
using device_build_and_bootcount_updates as updates
on target.org_id = updates.org_id
and target.device_id = updates.device_id
and target.cm_device_id = updates.cm_device_id
and target.start_ms = updates.start_ms
and target.end_ms = updates.end_ms
and target.date = updates.date
when matched then update set *
when not matched then insert * ;

ALTER TABLE dataprep_safety.device_build_and_bootcount
SET TBLPROPERTIES ('comment' = 'This table contains build and bootcount for each cm_vg interval. We look at start_ms to end_ms + 24 hours for the build because sometimes we do not report a heartbeat object stat for a long amount of time and we want to ensure we have a build for each interval.');

ALTER TABLE dataprep_safety.device_build_and_bootcount
CHANGE org_id
COMMENT 'The Samsara Cloud dashboard ID that the data belongs to';

ALTER TABLE dataprep_safety.device_build_and_bootcount
CHANGE cm_device_id
COMMENT 'The device ID of the CM we are reporting metrics for';

ALTER TABLE dataprep_safety.device_build_and_bootcount
CHANGE product_id
COMMENT 'The product ID of the VG the CM is paired to. The product ID mapping can be found in the products.go file in the codebase';

ALTER TABLE dataprep_safety.device_build_and_bootcount
CHANGE cm_product_id
COMMENT 'The product ID of the CM product we are reporting daily metrics for';

ALTER TABLE dataprep_safety.device_build_and_bootcount
CHANGE device_id
COMMENT 'The device ID of the VG that the CM is paired to';

ALTER TABLE dataprep_safety.device_build_and_bootcount
CHANGE date
COMMENT 'The date that we are reporting the metrics for.';

ALTER TABLE dataprep_safety.device_build_and_bootcount
CHANGE start_ms
COMMENT 'The start timestamp (ms) of the CM VG interval that we are reporting the metrics for.';

ALTER TABLE dataprep_safety.device_build_and_bootcount
CHANGE end_ms
COMMENT 'The end timestamp (ms) of the CM VG interval that we are reporting the metrics for.';
