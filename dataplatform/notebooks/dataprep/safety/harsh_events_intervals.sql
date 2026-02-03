-- Databricks notebook source
create or replace temporary view safety_harsh_events_individual as
(
select
  cm_linked_vgs.vg_device_id as device_id,
  cm_linked_vgs.org_id,
  cm_linked_vgs.linked_cm_id,
  cm_linked_vgs.cm_product_id,
  safety_events.date,
  safety_events.event_ms,
  safety_events.detail_proto.event_id as event_id,
  safety_events.detail_proto.accel_type as accel_type
from dataprep_safety.cm_linked_vgs as cm_linked_vgs
left join safetydb_shards.safety_events as safety_events on
  cm_linked_vgs.org_id = safety_events.org_id and
  cm_linked_vgs.vg_device_id = safety_events.device_id
where
  cm_linked_vgs.linked_cm_id is not null and
  safety_events.detail_proto.start.speed_milliknots >= 4344.8812095 and --Filter out events less than 5 MPH
  safety_events.detail_proto.stop.speed_milliknots >= 4344.8812095 and
  safety_events.detail_proto.accel_type not in (0, 6, 15, 16) and
  (safety_events.date between COALESCE(NULLIF(getArgument("start_date"), ''),  date_sub(CURRENT_DATE(),5))
                          and COALESCE(NULLIF(getArgument("end_date"), ''),  CURRENT_DATE()))
  and safety_events.detail_proto.hidden_to_customer != true
);

create or replace temporary view safety_harsh_events_coaching_filtered as
(
select safety_harsh_events_individual.*
from safety_harsh_events_individual as safety_harsh_events_individual
LEFT JOIN safetydb_shards.safety_event_metadata as safety_event_metadata ON
  safety_harsh_events_individual.org_id = safety_event_metadata.org_id
  AND safety_harsh_events_individual.device_id = safety_event_metadata.device_id
  AND safety_harsh_events_individual.event_ms = safety_event_metadata.event_ms
  and safety_harsh_events_individual.date = safety_event_metadata.date
where
(safety_event_metadata.date between COALESCE(NULLIF(getArgument("start_date"), ''),  date_sub(CURRENT_DATE(),5))
                                and COALESCE(NULLIF(getArgument("end_date"), ''),  CURRENT_DATE()))
and ((safety_event_metadata.coaching_state IS NULL AND safety_event_metadata.inbox_state IS NULL) OR
(safety_event_metadata.inbox_state != 2 AND safety_event_metadata.coaching_state NOT IN
(3, 6, 8, 11))) --Filter out events that are not DISMISSED (2) inbox state AND in one of DISMISSED (3) MANUAL_REVIEW (6), AUTO_DISMISSED (8), BETA (11) coaching state
);

-- COMMAND ----------

create or replace temporary view safety_harsh_events_files as
(
select safety_harsh_events_coaching_filtered.*,
  osDUploadedFileSet.value.proto_value.uploaded_file_set as uploaded_file_set,
  case when osDUploadedFileSet.value.proto_value.uploaded_file_set is not null then 1 else 0 end as successful_file_upload
from safety_harsh_events_coaching_filtered as safety_harsh_events_coaching_filtered
left join kinesisstats.osDUploadedFileSet as osDUploadedFileSet on
  safety_harsh_events_coaching_filtered.org_id = osDUploadedFileSet.org_id and
  safety_harsh_events_coaching_filtered.linked_cm_id = osDUploadedFileSet.object_id and
  safety_harsh_events_coaching_filtered.event_id = osDUploadedFileSet.value.proto_value.uploaded_file_set.event_id and
  osDUploadedFileSet.date = safety_harsh_events_coaching_filtered.date
WHERE
osDUploadedFileSet.date between COALESCE(NULLIF(getArgument("start_date"), ''),  date_sub(CURRENT_DATE(),5))
                            and COALESCE(NULLIF(getArgument("end_date"), ''),  CURRENT_DATE())
);

-- COMMAND ----------

create or replace temporary view safety_harsh_events_intervals as
(
select cm_vg_intervals.org_id,
  cm_vg_intervals.device_id,
  cm_vg_intervals.cm_device_id,
  cm_vg_intervals.cm_product_id,
  cm_vg_intervals.start_ms,
  cm_vg_intervals.end_ms,
  cm_vg_intervals.date,
  count(safety_harsh_events_files.accel_type) as harsh_event_count,
  sum(safety_harsh_events_files.successful_file_upload) as successful_file_upload_count
from dataprep_safety.cm_vg_intervals as cm_vg_intervals
left join safety_harsh_events_files as safety_harsh_events_files on
  cm_vg_intervals.org_id = safety_harsh_events_files.org_id and
  cm_vg_intervals.cm_device_id = safety_harsh_events_files.linked_cm_id and
  cm_vg_intervals.device_id = safety_harsh_events_files.device_id and
  safety_harsh_events_files.event_ms >= cm_vg_intervals.start_ms and
  safety_harsh_events_files.event_ms <= cm_vg_intervals.end_ms
WHERE
cm_vg_intervals.date between COALESCE(NULLIF(getArgument("start_date"), ''),  date_sub(CURRENT_DATE(),5))
                         and COALESCE(NULLIF(getArgument("end_date"), ''),  CURRENT_DATE())
group by
      cm_vg_intervals.org_id,
      cm_vg_intervals.cm_device_id,
      cm_vg_intervals.cm_product_id,
      cm_vg_intervals.start_ms,
      cm_vg_intervals.end_ms,
      cm_vg_intervals.device_id,
      cm_vg_intervals.date
);

-- COMMAND ----------

create table
if not exists dataprep_safety.safety_harsh_events_intervals
using delta
partitioned by (date)
as
(
select *
from safety_harsh_events_intervals
)

-- COMMAND ----------

create or replace temporary view safety_harsh_events_intervals_updates as
(
  select *
from safety_harsh_events_intervals
where date between COALESCE(NULLIF(getArgument("start_date"), ''),  date_sub(CURRENT_DATE(),5))
               and COALESCE(NULLIF(getArgument("end_date"), ''),  CURRENT_DATE())
);

merge into dataprep_safety.safety_harsh_events_intervals as target
using safety_harsh_events_intervals_updates as updates
on target.org_id = updates.org_id
and target.device_id = updates.device_id
and target.cm_device_id = updates.cm_device_id
and target.start_ms = updates.start_ms
and target.end_ms = updates.end_ms
when matched then update set *
when not matched then insert * ;
