-- Databricks notebook source
create or replace temporary view safety_harsh_events_raw as
(
select
  cm_linked_vgs.org_id,
  cm_linked_vgs.org_name,
  cm_linked_vgs.org_type,
  cm_linked_vgs.vg_device_id as device_id,
  cm_linked_vgs.product_id,
  vg_gtwy.variant_id AS vg_variant_id,
  COALESCE(vg_var.product_version, 'Version 1') AS vg_product_version,
  cm_linked_vgs.linked_cm_id,
  cm_linked_vgs.cm_product_id,
  cm_gtwy.variant_id AS cm_variant_id,
  safety_events.date,
  safety_events.event_ms,
  safety_events.detail_proto.event_id,
  safety_events.detail_proto.accel_type as accel_type_raw
from dataprep_safety.cm_linked_vgs as cm_linked_vgs
left join safetydb_shards.safety_events as safety_events on
  cm_linked_vgs.org_id = safety_events.org_id and
  cm_linked_vgs.vg_device_id = safety_events.device_id
left join productsdb.gateways as cm_gtwy on -- this can be improved to account for historic variant_id changes
    cm_linked_vgs.org_id = cm_gtwy.org_id and
    cm_linked_vgs.linked_cm_id = cm_gtwy.device_id
left join productsdb.gateways as vg_gtwy on -- this can be improved to account for historic variant_id changes
    cm_linked_vgs.org_id = vg_gtwy.org_id and
    cm_linked_vgs.vg_device_id = vg_gtwy.device_id
left join data_analytics.vg_hardware_variant_info as vg_var on
    cm_linked_vgs.org_id = vg_var.org_id and
    cm_linked_vgs.vg_device_id = vg_var.device_id and
    cm_linked_vgs.product_id = vg_var.product_id
where
  cm_linked_vgs.linked_cm_id is not null and
  safety_events.date <= date_sub(current_date(), 1) and
  safety_events.date >= date_sub(current_date(), 30) and
  safety_events.detail_proto.start.speed_milliknots >= 4344.8812095 and --Filter out events less than 5 MPH
  safety_events.detail_proto.stop.speed_milliknots >= 4344.8812095 and
  safety_events.detail_proto.accel_type is not null and
  safety_events.detail_proto.hidden_to_customer != true
);

-- COMMAND ----------

create or replace temporary view safety_harsh_events_individual as
(
select *,
  coalesce(enums.event_type, cast(accel_type_raw as string)) as harsh_event_type
from safety_harsh_events_raw as events
  left join definitions.harsh_accel_type_enums as enums on
  events.accel_type_raw = enums.enum
);

-- COMMAND ----------

create or replace temporary view safety_harsh_events_coaching_filtered as
(
select safety_harsh_events_individual.*
from safety_harsh_events_individual as safety_harsh_events_individual
  LEFT JOIN safetydb_shards.safety_event_metadata as safety_event_metadata
  ON safety_harsh_events_individual.org_id = safety_event_metadata.org_id
    AND safety_harsh_events_individual.device_id = safety_event_metadata.device_id
    AND safety_harsh_events_individual.event_ms = safety_event_metadata.event_ms
    AND safety_event_metadata.date <= date_sub(current_date(), 1)
    AND safety_event_metadata.date >= date_sub(current_date(), 30)
where
((safety_event_metadata.coaching_state IS NULL AND safety_event_metadata.inbox_state IS NULL) OR
(safety_event_metadata.inbox_state != 2 AND safety_event_metadata.coaching_state NOT IN
(3, 6, 8, 11))) --Filter out events that are not DISMISSED (2) inbox state AND in one of DISMISSED (3) MANUAL_REVIEW (6), AUTO_DISMISSED (8), BETA (11) coaching state
);

-- COMMAND ----------

create or replace temporary view safety_harsh_events_files as
(
select safety_harsh_events_coaching_filtered.*,
  case when osDUploadedFileSet.value.proto_value.uploaded_file_set is not null then 1 else 0 end as successful_file_upload
from safety_harsh_events_coaching_filtered as safety_harsh_events_coaching_filtered
left join kinesisstats.osDUploadedFileSet as osDUploadedFileSet on
  safety_harsh_events_coaching_filtered.org_id = osDUploadedFileSet.org_id and
  safety_harsh_events_coaching_filtered.linked_cm_id = osDUploadedFileSet.object_id and
  safety_harsh_events_coaching_filtered.event_id = osDUploadedFileSet.value.proto_value.uploaded_file_set.event_id and
  osDUploadedFileSet.date <= date_sub(current_date(), 1) and
  osDUploadedFileSet.date >= date_sub(current_date(), 30)
)

-- COMMAND ----------

create or replace temporary view safety_harsh_events_heartbeats as
(
select safety_harsh_events_files.*,
  device_heartbeats_extended.first_heartbeat_date as first_heartbeat_date,
  device_heartbeats_extended.last_heartbeat_date as last_heartbeat_date
from safety_harsh_events_files as safety_harsh_events_files
left join dataprep.device_heartbeats_extended as device_heartbeats_extended
  on safety_harsh_events_files.org_id = device_heartbeats_extended.org_id
  and safety_harsh_events_files.linked_cm_id = device_heartbeats_extended.device_id
);

-- COMMAND ----------

create or replace temporary view safety_events_day as
(
select
  date,
  org_id,
  org_name,
  org_type,
  device_id,
  product_id,
  vg_variant_id,
  vg_product_version,
  linked_cm_id,
  cm_product_id,
  cm_variant_id,
  first_heartbeat_date,
  last_heartbeat_date,
  harsh_event_type,
  count(harsh_event_type) as total_event_occurence,
  sum(successful_file_upload) as total_successful_file_upload
from safety_harsh_events_heartbeats
group by
  date,
  org_id,
  org_name,
  org_type,
  device_id,
  product_id,
  vg_variant_id,
  vg_product_version,
  linked_cm_id,
  cm_product_id,
  cm_variant_id,
  first_heartbeat_date,
  last_heartbeat_date,
  harsh_event_type
);

-- COMMAND ----------

create table
if not exists dataprep_safety.safety_harsh_events_upload_events
using delta
partitioned by (date)
as
select *
from safety_events_day

-- COMMAND ----------

create or replace temporary view safety_harsh_events_individual_updates as
(
select *
from safety_events_day
where date >= date_sub(current_date(), 30) or last_heartbeat_date >= date_sub(current_date(), 7)
);

merge into dataprep_safety.safety_harsh_events_upload_events as target
using safety_harsh_events_individual_updates as updates
on target.date = updates.date
and target.org_id = updates.org_id
and target.device_id = updates.device_id
and target.linked_cm_id = updates.linked_cm_id
and target.harsh_event_type = updates.harsh_event_type
when matched then update set *
when not matched then insert * ;
