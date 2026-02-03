-- Databricks notebook source
create or replace temporary view attached_usb_devices as (
  select
    cm_linked_vgs.org_id,
    cm_linked_vgs.vg_device_id as device_id,
    cm_linked_vgs.linked_cm_id,
    osdattachedusbdevices.time as timestamp,
    exists(osdattachedusbdevices.value.proto_value.attached_usb_devices.usb_id, x -> x IN (1668105037, 185272842)) as attached_to_cm --1668105037 is the GAIA id
  from dataprep_safety.cm_linked_vgs as cm_linked_vgs
  left join kinesisstats.osdattachedusbdevices as osdattachedusbdevices on
    cm_linked_vgs.org_id = osdattachedusbdevices.org_id and
    cm_linked_vgs.vg_device_id = osdattachedusbdevices.object_id
  where
    osdattachedusbdevices.value.is_databreak = false and
    osdattachedusbdevices.value.is_end = false and
    osdattachedusbdevices.date <= date_sub(current_date(), 1)
);

-- COMMAND ----------

-- In order to build our transitions, we need to construct the start and end time.
-- Grab prior state and prior time for each row.
create or replace temporary view cm_attached_lag as (
  select
    linked_cm_id,
    device_id,
    org_id,
    COALESCE(lag(timestamp) over (partition by org_id, linked_cm_id, device_id order by timestamp), 0) as prev_time,
    COALESCE(lag(attached_to_cm) over (partition by org_id, linked_cm_id, device_id order by timestamp), false) as prev_state,
    timestamp as cur_time,
    attached_to_cm as cur_state
  from attached_usb_devices
);

-- Look ahead to grab the next state and time.
create or replace temporary view cm_attached_lead as (
  select
    linked_cm_id,
    device_id,
    org_id,
    timestamp as prev_time,
    attached_to_cm as prev_state,
    COALESCE(lead(timestamp) over (partition by org_id, linked_cm_id, device_id order by timestamp), 0) as cur_time,
    COALESCE(lead(attached_to_cm) over (partition by org_id, linked_cm_id, device_id order by timestamp), false) as cur_state
  from attached_usb_devices
);

--Create table that is a union of the transitions between each state
create or replace temporary view attached_hist as(
  (select *
  from cm_attached_lag lag
  where lag.prev_state != lag.cur_state
  )
  union
  (select *
  from cm_attached_lead lead
  where lead.prev_state != lead.cur_state
  )
);

-- COMMAND ----------

-- Since we only grabbed the transitions, we don't see
-- the timestamps for consecutive reported objectStat values. To
-- create accurate intervals, we need to grab the next rows cur_time
-- since that timestamp is the end of the consecutive values (i.e. state transition).
create or replace temporary view attached_states as (
  select
    linked_cm_id,
    device_id,
    org_id,
    cur_time as start_ms,
    cur_state as attached_to_cm,
    lead(cur_time) over (partition by org_id, linked_cm_id, device_id order by cur_time) as end_ms
  from attached_hist
);

create or replace temporary view cm_attached_intervals as (
  select org_id,
    linked_cm_id,
    device_id,
    attached_to_cm,
    start_ms,
    COALESCE(end_ms, unix_timestamp(date_sub(current_date(), 1)) * 1000) as end_ms
  from attached_states
  where start_ms != 0
);

-- COMMAND ----------

create table if not exists dataprep_safety.attached_cm_usb_ranges
using delta
as
select * from cm_attached_intervals

-- COMMAND ----------

insert overwrite table dataprep_safety.attached_cm_usb_ranges (
  select * from cm_attached_intervals
);
