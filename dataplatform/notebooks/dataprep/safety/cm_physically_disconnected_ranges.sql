-- Databricks notebook source
create or replace temporary view cm_physically_disconnected_ranges as (
  select cm_power_ranges.org_id,
  cm_power_ranges.linked_cm_id,
  cm_power_ranges.device_id,
  greatest(cm_power_ranges.start_ms, attached_cm_usb_ranges.start_ms) as start_ms,
  least(cm_power_ranges.end_ms, attached_cm_usb_ranges.end_ms) as end_ms
  from dataprep_safety.cm_power_ranges as cm_power_ranges
  join dataprep_safety.attached_cm_usb_ranges as attached_cm_usb_ranges on
    cm_power_ranges.org_id = attached_cm_usb_ranges.org_id and
    cm_power_ranges.linked_cm_id = attached_cm_usb_ranges.linked_cm_id and
    cm_power_ranges.device_id = attached_cm_usb_ranges.device_id and
    not (attached_cm_usb_ranges.end_ms < cm_power_ranges.start_ms or attached_cm_usb_ranges.start_ms > cm_power_ranges.end_ms)
    and attached_cm_usb_ranges.attached_to_cm = 0
  where
    cm_power_ranges.has_power = 0
);

-- COMMAND ----------

create table if not exists dataprep_safety.cm_physically_disconnected_ranges
using delta
as
select * from cm_physically_disconnected_ranges;

-- COMMAND ----------

insert overwrite table dataprep_safety.cm_physically_disconnected_ranges (
  select * from cm_physically_disconnected_ranges
);

-- METADATA ---------

ALTER TABLE dataprep_safety.cm_physically_disconnected_ranges
SET TBLPROPERTIES ('comment' = 'This table contains the time ranges where the CM was physically disconnected from the VG.
We consider a CM to be physically
disconnected from the VG if the VG is not enumerating the CM as an
attached usb device and the CM also does not have power. We will use
this notebook in calculating the recording uptime for cameras. For the
recording uptime calculations, we want to filter out trips where the CM was physically
disconnected.');

ALTER TABLE dataprep_safety.cm_physically_disconnected_ranges
CHANGE org_id
COMMENT 'The Samsara Cloud dashboard ID that the data belongs to';

ALTER TABLE dataprep_safety.cm_physically_disconnected_ranges
CHANGE device_id
COMMENT 'The device ID of the VG that the CM is paired to';

ALTER TABLE dataprep_safety.cm_physically_disconnected_ranges
CHANGE linked_cm_id
COMMENT 'The device ID of the CM we are reporting metrics for';

ALTER TABLE dataprep_safety.cm_physically_disconnected_ranges
CHANGE start_ms
COMMENT 'The start timestamp (ms) of when the CM was physically disconnected.';

ALTER TABLE dataprep_safety.cm_physically_disconnected_ranges
CHANGE end_ms
COMMENT 'The end timestamp (ms) of when the CM was physically disconnected.';
