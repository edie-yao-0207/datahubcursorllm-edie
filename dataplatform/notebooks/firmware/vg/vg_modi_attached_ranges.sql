-- Databricks notebook source
-- MAGIC %python
-- MAGIC spark.conf.set("spark.sql.broadcastTimeout", 36000)

-- COMMAND ----------

--Get device data for all VGs
create or replace temporary view devices as
(
  select a.id as gateway_id, 
    a.device_id, 
    a.org_id
  from clouddb.gateways as a
  where a.product_id in (24,35,53,89,178)
)

-- COMMAND ----------

--Modi only gets reported on change, so we'll need to look past date range that had a modi installed prior to week of interest
create or replace temporary view attached_modi_devices as (
  select
    dev.org_id,
    dev.device_id,
    osdattachedusbdevices.time,
    exists(osdattachedusbdevices.value.proto_value.attached_usb_devices.usb_id, x -> x == 281315232) as attached_to_modi --281315232 is the modi id
  from devices as dev
  left join kinesisstats.osdattachedusbdevices as osdattachedusbdevices on
    dev.org_id = osdattachedusbdevices.org_id and
    dev.device_id = osdattachedusbdevices.object_id
  where osdattachedusbdevices.date >= '2020-04-01'
    and osdattachedusbdevices.value.is_databreak = false
    and osdattachedusbdevices.value.is_end = false
)

-- COMMAND ----------

create or replace temporary view modi_attached_lag as (
  select
    device_id,
    org_id,
    COALESCE(lag(time) over (partition by org_id, device_id order by time), 0) as prev_time,
    COALESCE(lag(attached_to_modi) over (partition by org_id, device_id order by time), false) as prev_state,
    time as cur_time,
    attached_to_modi as cur_state
  from attached_modi_devices
);
 
-- Look ahead to grab the next state and time.
create or replace temporary view modi_attached_lead as (
  select
    device_id,
    org_id,
    time as prev_time,
    attached_to_modi as prev_state,
    COALESCE(lead(time) over (partition by org_id, device_id order by time), 0) as cur_time,
    COALESCE(lead(attached_to_modi) over (partition by org_id, device_id order by time), false) as cur_state
  from attached_modi_devices
);
 
--Create table that is a union of the transitions between each state
create or replace temporary view attached_hist as(
  (select *
  from modi_attached_lag lag
  where lag.prev_state != lag.cur_state
  )
  union
  (select *
  from modi_attached_lead lead
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
    device_id,
    org_id,
    cur_time as start_ms,
    cur_state as attached_to_modi,
    lead(cur_time) over (partition by org_id, device_id order by cur_time) as end_ms
  from attached_hist
);
 
create or replace temporary view modi_attached_intervals as (
  select org_id,
    device_id,
    attached_to_modi,
    start_ms,
    COALESCE(end_ms, unix_timestamp(date_sub(current_date(), 1)) * 1000) as end_ms --fill in end time
  from attached_states
  where start_ms != 0
)

-- COMMAND ----------

create table if not exists data_analytics.vg_modi_attached_ranges
using delta
as
select * from modi_attached_intervals

-- COMMAND ----------

insert overwrite table data_analytics.vg_modi_attached_ranges
select * from modi_attached_intervals

-- COMMAND ----------

ALTER TABLE data_analytics.vg_modi_attached_ranges SET TBLPROPERTIES ('comment' = '');
ALTER TABLE data_analytics.vg_modi_attached_ranges CHANGE org_id COMMENT 'ID of the Samsara organization';
ALTER TABLE data_analytics.vg_modi_attached_ranges CHANGE device_id COMMENT 'ID of the Samsara device';
ALTER TABLE data_analytics.vg_modi_attached_ranges CHANGE attached_to_modi COMMENT 'TRUE if the device had a Modi attached during the range from start_ms to end_ms';
ALTER TABLE data_analytics.vg_modi_attached_ranges CHANGE start_ms COMMENT 'Millisecond Unix timestamp when the range started';
ALTER TABLE data_analytics.vg_modi_attached_ranges CHANGE end_ms COMMENT 'Millisecond Unix timestamp when the range ended';
