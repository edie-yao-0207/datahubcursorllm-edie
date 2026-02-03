-- Databricks notebook source
create or replace temporary view anomalies_dates as
(
  select
    explode(sequence(date_sub(current_date(),7), current_date())) as date, --if we do more than that we run into broadcast issues
    cm_linked_vgs.org_id,
    cm_linked_vgs.org_type,
    cm_linked_vgs.org_name,
    cm_linked_vgs.vg_device_id as device_id,
    cm_linked_vgs.product_id,
    vg_gtwy.variant_id AS vg_variant_id,
    COALESCE(vg_var.product_version, 'Version 1') AS vg_product_version,
    cm_gtwy.variant_id AS cm_variant_id,
    cm_linked_vgs.linked_cm_id,
    cm_linked_vgs.cm_product_id
  from dataprep_safety.cm_linked_vgs as cm_linked_vgs
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
);

create or replace temporary view anomalies_by_service as
(
  select a.*,
    b.anomaly_event_service,
    b.ae_count as total_count
  from anomalies_dates a
  left join dataprep.device_anomalies b on
    b.device_id = a.linked_cm_id and
    b.org_id = a.org_id and
    b.date = a.date
);

-- COMMAND ----------

create or replace temporary view anomalies_by_service_with_heartbeat as
(
  select a.*,
    device_heartbeats_extended.first_heartbeat_date as first_heartbeat_date,
    device_heartbeats_extended.first_heartbeat_ms as first_heartbeat_ms,
    device_heartbeats_extended.last_heartbeat_ms as last_heartbeat_ms,
    device_heartbeats_extended.last_heartbeat_date as last_heartbeat_date
  from anomalies_by_service as a
  left join dataprep.device_heartbeats_extended
    on a.linked_cm_id = device_heartbeats_extended.device_id
    and a.org_id = device_heartbeats_extended.org_id
  where device_heartbeats_extended.first_heartbeat_date <= a.date
);

-- COMMAND ----------

create table
if not exists dataprep_safety.anomalies_by_service
using delta
select *
from anomalies_by_service_with_heartbeat

-- COMMAND ----------
--Updated date stop to backfill missing information from sequential job failues in November
-- Something to investigate, this merge upsert results in double the rows being added to the table daily versus prior to upsert
create or replace temporary view anomalies_by_service_updates as (
  select *
  from anomalies_by_service_with_heartbeat
  where date >= date_sub(current_date() , 3) or last_heartbeat_date >= date_sub(current_date(), 3)
);

merge into dataprep_safety.anomalies_by_service as target
using anomalies_by_service_updates as updates
on target.date = updates.date
  and target.org_id = updates.org_id
  and target.device_id = updates.device_id
  and target.linked_cm_id = updates.linked_cm_id
  and target.anomaly_event_service = updates.anomaly_event_service
when matched then update set *
when not matched then insert * ;
