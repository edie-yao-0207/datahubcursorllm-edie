-- Databricks notebook source
create or replace temporary view video_requests_individual as
(
select
  historical_video_requests.date,
  cm_linked_vgs.org_id,
  cm_linked_vgs.org_name,
  cm_linked_vgs.org_type,
  cm_linked_vgs.vg_device_id as device_id,
  cm_linked_vgs.product_id,
  vg_gtwy.variant_id as vg_variant_id,
  COALESCE(vg_var.product_version, 'Version 1') as vg_product_version,
  cm_linked_vgs.linked_cm_id,
  cm_linked_vgs.cm_product_id,
  cm_gtwy.variant_id as cm_variant_id,
  (historical_video_requests.asset_ready_at_ms - historical_video_requests.created_at_ms) as prepare_asset_for_upload_duration_ms,
  (historical_video_requests.completed_at_ms - historical_video_requests.created_at_ms) as upload_and_processing_duration_ms,
  case when historical_video_requests.state = 2 then 1 else 0 end as successful,
  (historical_video_requests.end_ms - historical_video_requests.start_ms) as requested_duration_ms
from dataprep_safety.cm_linked_vgs as cm_linked_vgs
join clouddb.groups as groups on
  cm_linked_vgs.org_id = groups.organization_id
left join clouddb.historical_video_requests as historical_video_requests on
  cm_linked_vgs.vg_device_id = historical_video_requests.device_id and
  historical_video_requests.group_id = groups.id
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
  historical_video_requests.is_multicam = 0 and
  historical_video_requests.created_at_ms != 0
);

-- COMMAND ----------

create or replace temporary view video_requests_day as
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
  sum(prepare_asset_for_upload_duration_ms) as total_prepare_asset_for_upload_duration_ms,
  sum(upload_and_processing_duration_ms) as total_upload_and_processing_duration_ms,
  count(successful) as total_requested,
  sum(successful) as total_succesful,
  sum(requested_duration_ms) as total_requested_duration_ms,
  max(requested_duration_ms) as max_requested_duration_ms
from video_requests_individual
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
  cm_variant_id
);

-- COMMAND ----------

create or replace temporary view video_requests_heartbeats as (
  select
    video_requests_day.*,
    device_heartbeats_extended.first_heartbeat_date,
    device_heartbeats_extended.last_heartbeat_date,
    cm_build.latest_build_on_day as last_reported_cm_build,
    vg_build.latest_build_on_day as last_reported_vg_build
  from video_requests_day video_requests_day
  left join dataprep.device_heartbeats_extended as device_heartbeats_extended on
    video_requests_day.org_id = device_heartbeats_extended.org_id
    and video_requests_day.linked_cm_id = device_heartbeats_extended.device_id
  left join dataprep.device_builds as vg_build on
    video_requests_day.date = vg_build.date
    and video_requests_day.org_id = vg_build.org_id
    and video_requests_day.device_id = vg_build.device_id
  left join dataprep.device_builds as cm_build on
    video_requests_day.date = cm_build.date
     and video_requests_day.org_id = cm_build.org_id
     and video_requests_day.linked_cm_id = cm_build.device_id
);

-- COMMAND ----------

create table if not exists dataprep_safety.video_request_data
using delta
partitioned by
(date)
as
select *
from video_requests_heartbeats;

-- COMMAND ----------

create or replace temporary view video_request_data_updates as
(
  select *
from video_requests_heartbeats
where date >= date_sub(current_date(), 7) or last_heartbeat_date >= date_sub(current_date(), 3)
);

merge into dataprep_safety.video_request_data as target
using video_request_data_updates as updates
on target.date = updates.date
and target.org_id = updates.org_id
and target.device_id = updates.device_id
and target.linked_cm_id = updates.linked_cm_id
when matched then update set *
when not matched then insert * ;
