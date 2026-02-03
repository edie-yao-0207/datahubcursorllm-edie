-- Databricks notebook source
-- Find when CM31, CM32, CM33 or CM34 devices are connected to a VG
create or replace temp view vg_cm_serial_associations_base as (
  SELECT date,
        time,
        org_id,
        object_id as vg_device_id,
        coalesce(value.proto_value.attached_usb_devices.serial[array_position(value.proto_value.attached_usb_devices.usb_id, 1668105037) - 1],
        value.proto_value.attached_usb_devices.serial[array_position(value.proto_value.attached_usb_devices.usb_id, 185272842) - 1]) as cm_device_serial
  FROM kinesisstats_history.osdattachedusbdevices as a
  JOIN dataprep_firmware.data_ingestion_high_water_mark as b
  WHERE ((exists(value.proto_value.attached_usb_devices.usb_id, x -> x == 1668105037) -- CM31 & CM32
    and value.proto_value.attached_usb_devices.serial[array_position(value.proto_value.attached_usb_devices.usb_id, 1668105037) - 1] is not null) or
    (exists(value.proto_value.attached_usb_devices.usb_id, x -> x == 185272842) -- CM33 & CM34
    and value.proto_value.attached_usb_devices.serial[array_position(value.proto_value.attached_usb_devices.usb_id, 185272842) - 1] is not null))
    and a.time <= b.time_ms

  UNION

  SELECT date,
        time,
        org_id,
        object_id as vg_device_id,
        upper(replace(value.proto_value.dashcam_connection_state.camera_serial,'-','')) as cm_device_serial
  FROM kinesisstats_history.osddashcamconnected as a
  JOIN dataprep_firmware.data_ingestion_high_water_mark as b
  WHERE value.int_value = 1 --connected
    and value.is_end = FALSE
    and value.is_databreak = FALSE
    and value.proto_value.dashcam_connection_state.camera_product_type in (4,5,9,11) --CM31, CM32, CM33, CM34
    and value.proto_value.dashcam_connection_state.camera_serial is not null
    and a.time <= b.time_ms
);

-- COMMAND ----------

create or replace temp view gateway_device_intervals as (
  select
    device_id,
    gateway_id,
    unix_millis(start_interval) as start_ms,
    coalesce(unix_millis(end_interval), unix_millis(now())) as end_ms
  from datamodel_core.fct_gateway_device_intervals
);

-- COMMAND ----------

-- Goal: Find when CM31, CM32, CM33 or CM34 devices are connected to a VG
create or replace temp view vg_cm_serial_associations_gateways as (
  SELECT
        a.time,
        a.vg_device_id,
        b.gateway_id as vg_gateway_id,
        a.cm_device_serial,
        c.id as cm_gateway_id,
        d.device_id as cm_device_id
  FROM vg_cm_serial_associations_base a
  INNER JOIN gateway_device_intervals b
    ON a.vg_device_id = b.device_id
    and a.time between b.start_ms and b.end_ms
  LEFT JOIN clouddb.gateways c
    ON upper(replace(a.cm_device_serial, '-', '')) = upper(replace(c.serial, '-', ''))
  INNER JOIN gateway_device_intervals d
    ON c.id = d.gateway_id
    and a.time between d.start_ms and d.end_ms
  WHERE b.gateway_id IS NOT NULL
    and c.id IS NOT NULL
);

-- COMMAND ----------

-- 1. Find all instances of a CM being connected to a VG
-- 2. For each one of those rows in 1, find the next time that the CM has a different VG.
-- 3. For each one of those rows in 1, find the next time that the VG has a different CM.
-- 4. Use the smaller of the two times in 4+5 to use as the end time of the associations in 3.
-- 5. If CM or VG doesn't ever change again, coalesce with now()
-- 6. Find the actual start of the intervals

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW transitions_only AS (
  with find_transitions as (
    SELECT time as cur_time,
          vg_gateway_id as cur_vg_gateway_id,
          cm_gateway_id as cur_cm_gateway_id,
          cm_device_id,
          vg_device_id,
          lag(time) over(partition by vg_gateway_id order by time) as prev_vg_time,
          lag(time) over(partition by cm_gateway_id order by time) as prev_cm_time,
          lag(cm_gateway_id) over(partition by vg_gateway_id order by time) as vg_prev_cm_gateway_id,
          lag(vg_gateway_id) over(partition by cm_gateway_id order by time) as cm_prev_vg_gateway_id,
          LAG(vg_device_id) OVER(PARTITION BY vg_gateway_id ORDER BY time) AS vg_prev_vg_device_id,
          case
            WHEN coalesce(lag(cm_gateway_id) over(partition by vg_gateway_id order by time),'null') != cm_gateway_id
            THEN 'prev_vg_time'
            WHEN coalesce(lag(vg_gateway_id) over(partition by cm_gateway_id order by time),'null') != vg_gateway_id
            THEN 'prev_cm_time'
            WHEN COALESCE(LAG(vg_device_id) OVER(PARTITION BY vg_gateway_id ORDER BY time),'null') != vg_device_id
            THEN 'prev_vg_time'
            ELSE null
          end as transition
    FROM vg_cm_serial_associations_gateways
  )

  SELECT *
  FROM find_transitions
  WHERE transition is not null -- transitions
    OR (prev_vg_time is null and prev_cm_time is null) -- first occurence
);

-- COMMAND ----------

create or replace temp view final_associations as (
  WITH intervals AS (
    SELECT cur_vg_gateway_id as vg_gateway_id,
          cur_cm_gateway_id as cm_gateway_id,
          cm_device_id,
          vg_device_id,
          cur_time as start_ms,
          CASE
            WHEN coalesce(lead(cur_cm_gateway_id) over(partition by cur_vg_gateway_id order by cur_time),'null') != cur_cm_gateway_id
            THEN lead(cur_time) over(partition by cur_vg_gateway_id order by cur_time) - 1
            WHEN COALESCE(LEAD(vg_device_id) OVER(PARTITION BY cur_vg_gateway_id ORDER BY cur_time),'null') != vg_device_id
            THEN LEAD(cur_time) OVER(PARTITION BY cur_vg_gateway_id ORDER BY cur_time) - 1
            ELSE NULL
          end as vg_end_ms,
          CASE
            WHEN coalesce(lead(cur_vg_gateway_id) over(partition by cur_cm_gateway_id order by cur_time),'null') != cur_vg_gateway_id
            THEN lead(cur_time) over(partition by cur_cm_gateway_id order by cur_time) -1
            ELSE NULL
          END as cm_end_ms
    FROM transitions_only
  ),

  intervals_final AS (
    SELECT vg_gateway_id,
        cm_gateway_id,
        cm_device_id,
        vg_device_id,
        date(timestamp_millis(start_ms)) as start_date,
        start_ms,
        LEAST(coalesce(vg_end_ms,unix_millis(now())), coalesce(cm_end_ms,unix_millis(now()))) as end_ms
    FROM intervals
  )

  SELECT
    *,
    date(timestamp_millis(end_ms)) as end_date
  FROM intervals_final
);

-- COMMAND ----------

create or replace table dataprep_firmware.cm_vg_gateway_associations_from_usb_enumeration
using delta
partitioned by (start_date)
as
select * from final_associations

-- COMMAND ----------

create or replace temp view exploded as (
  select
    start_ms,
    end_ms,
    explode(sequence(date(start_date), date(end_date), interval 1 day)) as date,
    cm_gateway_id,
    vg_gateway_id,
    cm_device_id,
    vg_device_id
  from final_associations
);

create or replace temp view grouped_by_cm as (
  select
    date,
    count(*) as vg_count,
    cm_gateway_id
  from exploded
  group by
    date,
    cm_gateway_id
);

create or replace temp view grouped_by_vg as (
  select
    date,
    count(*) as cm_count,
    vg_gateway_id
  from exploded
  group by
    date,
    vg_gateway_id
);

create or replace temp view joined as (
  select
    a.date,
    a.vg_gateway_id,
    a.cm_gateway_id,
    a.cm_device_id,
    a.vg_device_id,
    b.vg_count,
    c.cm_count
  from exploded as a
  join grouped_by_cm as b
    on a.date = b.date
    and a.cm_gateway_id = b.cm_gateway_id
  join grouped_by_vg as c
    on a.date = c.date
    and a.vg_gateway_id = c.vg_gateway_id
);

create or replace temp view cm_vg_gateway_daily_associations_unique as (
  select *
  from joined
  where vg_count = 1
    and cm_count = 1
);

-- COMMAND ----------

create or replace table dataprep_firmware.cm_vg_daily_associations_unique
using delta
partitioned by (date)
as
select * from cm_vg_gateway_daily_associations_unique
