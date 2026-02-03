-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Notes
-- MAGIC - End time should be over four hours before current time to account for DBX replication delay
-- MAGIC - Count only events in safetydb.safety_events and TEv2 backed by SEv1.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Visible Triage Events
-- MAGIC Each row in triage_events_v2 is considered visible in the aggregated inbox with the following exceptions:
-- MAGIC - Only internal users can see dark-launched events
-- MAGIC - Events from non-vehicle devices are filtered out

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### [Query] Visible Triage Events

-- COMMAND ----------

-- tev2data are all v2 triage events backed by a v1 safety event that use a 'vehicle' device and are in our legacy persist feature flag, after July 15th
create or replace temp view visible_non_speeding_triage_events as (
    select t.*
    from safetyeventtriagedb_shards.triage_events_v2 t
    inner join productsdb.devices d on t.device_id = d.id
    where t.org_id in (${org_ids})
    and t.start_ms > unix_millis(timestamp(date_format(date_sub(now(), 8), 'yyyy-MM-dd'))) + 7 * 60 * 60 * 1000
    and t.start_ms < unix_millis(timestamp(date_format(date_sub(now(), 1), 'yyyy-MM-dd'))) + 7 * 60 * 60 * 1000
    and t.version = 1 -- non-speeding only
    AND t.release_stage >= 2 -- filter out UNSET and DARK_LAUNCHED
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Visible Safety Events
-- MAGIC Safety events are visible with multiple exceptions:
-- MAGIC - Do not show coaching states beta, manual_review, manual_review_dismissed, and auto_dismissed
-- MAGIC - Do not show events on non-vehicle device types
-- MAGIC - Do not show low-speed events
-- MAGIC - Do not show off-trip events
-- MAGIC - Do not show weird accel types (INVALID, FALSE_POSITIVE)
-- MAGIC - Do not show events missing dashcam assets
-- MAGIC - Do not show release stage UNSET
-- MAGIC - Currently, we also only receive events from Konmari (not safetyeventsbuilder)

-- COMMAND ----------

create or replace temp view safety_events as (
  select
    *,
    safety_events.detail_proto.event_id as event_id,
    safety_events.detail_proto.accel_type as accel_type
  from
    safetydb_shards.safety_events
  WHERE
    org_id in (${org_ids})
    AND event_ms > unix_millis(timestamp(date_format(date_sub(now(), 8), 'yyyy-MM-dd'))) + 7 * 60 * 60 * 1000
    AND event_ms < unix_millis(timestamp(date_format(date_sub(now(), 1), 'yyyy-MM-dd'))) + 7 * 60 * 60 * 1000
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### [Query] SEv1 Filter Step 1

-- COMMAND ----------

-- Apply release stage filter: Filter out UNSET + DARK_LAUNCHED
create or replace temp view safety_events_filtered_step1 as (
  select * from safety_events where release_stage >= 2
)

-- COMMAND ----------

-- Apply coaching state filter: Filter out beta, manual_review, manual_review_dismissed, and auto_dismissed
create or replace temp view safety_events_filtered_step2 as (
  select
    se.*
  from
    safety_events_filtered_step1 se
    join safetydb_shards.safety_event_metadata sem on se.org_id = sem.org_id
    and se.device_id = sem.device_id
    and se.event_ms = sem.event_ms
  where
    sem.coaching_state not in (6, 8, 11, 12)
    or (se.org_id in (14083) and sem.coaching_state in (6, 12)) -- Pike shows manual review/manual review dismissed events
)

-- COMMAND ----------

-- Apply behavior label filter: Filter out FalsePositive and Invalid
create or replace temp view safety_events_filtered_step3 as (
  select * from safety_events_filtered_step2 where accel_type not in (0, 6)
)

-- COMMAND ----------

-- Apply low speed filter with exceptions (accel type or HEv2)
create or replace temp view safety_events_filtered_step4 as (
  select
    *
  from
    safety_events_filtered_step3 s
  where
    (
      s.detail_proto.accel_type in (5, 11, 18, 17, 28) -- exempt from speed threshold
      OR s.detail_proto.ingestion_tag = 1 -- hev2
      OR (
        s.detail_proto.start.speed_milliknots > 4345 -- 5mph start
        and s.detail_proto.stop.speed_milliknots > 4345 -- 5mph start
      )
    )
)

-- COMMAND ----------

-- Apply vehicle-type device filter
create or replace temp view safety_events_filtered_step5 as (
  select
    s.*
  from
    safety_events_filtered_step4 s
    inner join productsdb.devices d on s.device_id = d.id
  where
    d.product_id in (141, 56, 58, 7, 17, 24, 35, 53, 89, 90, 123, 178)
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### [Query] SEv1 Filter Step 6

-- COMMAND ----------

-- Apply off-trip filter (note: subject to change if trip_start_ms is unreliable)
create or replace temp view safety_events_filtered_step6 as (
  select * from safety_events_filtered_step5 s where s.trip_start_ms > 0
)


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### [Query] Visible Safety Events

-- COMMAND ----------

create
or replace temp view visible_safety_events as (
  with safety_events_to_matching_asset as (
    select
      s.org_id,
      s.device_id,
      s.event_ms,
      s.additional_labels,
      s.detail_proto.accel_type as accel_type,
      s.trip_start_ms,
      asset.asset_ms as matching_asset_ms,
      min(abs(asset.asset_ms - s.event_ms)) over (partition by s.org_id, s.device_id, s.event_ms) as min_asset_ms_diff
    from
      safety_events_filtered_step6 s
      inner join (
        -- could also be left join
        select
          *
        from
          cmassetsdb_shards.dashcam_assets
        where
          org_id in (${org_ids})
          and asset_ms > unix_millis(
            timestamp(date_format(date_sub(now(), 8), 'yyyy-MM-dd'))
          ) + 7 * 60 * 60 * 1000
          and asset_ms < unix_millis(
            timestamp(date_format(date_sub(now(), 1), 'yyyy-MM-dd'))
          ) + 7 * 60 * 60 * 1000
          and isnotnull(
            uploaded_file_set_proto.uploaded_file_set.event_id
          )
          and isnotnull(uploaded_file_set_proto.uploaded_file_set.s3urls)
      ) asset on s.device_id = asset.device_id
      and s.org_id = asset.org_id
    where
      (
        s.event_id = asset.event_id -- First, try matching on event_id.
        or (
          s.event_ms = asset.dashcam_report_proto.dashcam_report.unix_trigger_time_ms
        ) -- Fallback on unix trigger time.
        or (
          asset.asset_ms < s.event_ms + 120000
          and asset.asset_ms > s.event_ms - 120000
        ) -- Fallback on asset occurred within two minutes of event.)
      )
      and (
        asset.dashcam_report_proto.dashcam_report.report_type in (0, 3) -- video
        or (
          asset.dashcam_report_proto.dashcam_report.report_type in (1) -- image
          and s.accel_type in (23, 24, 25, 29)
        )
      )
  )
  select
    a.*,
    da.dashcam_report_proto.dashcam_report.report_type as report_type
  from
    safety_events_to_matching_asset a
    join cmassetsdb_shards.dashcam_assets da on a.org_id = da.org_id
    and a.device_id = da.device_id
    and a.matching_asset_ms = da.asset_ms
  where
    abs(a.matching_asset_ms - a.event_ms) = a.min_asset_ms_diff
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Missing SEv1
-- MAGIC Missing SEv1 for Visible TEv2

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### [Query] Missing SEv1

-- COMMAND ----------

create
or replace temp view missing_sev1 as (
  -- missing sev1
  (
    select
      te.org_id as triage_org_id,
      te.device_id as triage_device_id,
      devices.name as triage_device_name,
      te.start_ms as triage_start_ms,
      from_unixtime(te.start_ms/1000 - 6*60*60, 'yyyy-MM-dd HH:mm:ss') as triage_start_time,
      te.trigger_label as triage_behavior_label,
      te.trip_start_ms as triage_trip_start_ms,
      se.org_id as safety_org_id,
      se.device_id as safety_device_id,
      from_unixtime(se.event_ms/1000 - 6*60*60, 'yyyy-MM-dd HH:mm:ss') as safety_event_ms,
      se.additional_labels.additional_labels [0].label_type as safety_behavior_label,
      se.trip_start_ms as safety_trip_start_ms
    from
      visible_non_speeding_triage_events te
      left join visible_safety_events se on te.start_ms = se.event_ms
      and te.org_id = se.org_id
      and te.device_id = se.device_id
      join productsdb.devices on te.device_id = devices.id
    where isnull(se.org_id)
  )
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### [Query] Missing SEv1 with metadata

-- COMMAND ----------

create
or replace temp view missing_sev1_with_safety_event_metadata as (
  select
    se.org_id,
    se.device_id,
    se.event_ms,
    se.trip_start_ms,
    se.additional_labels.additional_labels[0].label_type as behavior_label,
    se.detail_proto.accel_type,
    se.detail_proto.ingestion_tag,
    sem.coaching_state
  from
    missing_sev1 missing
    join safetydb_shards.safety_events se on missing.triage_org_id = se.org_id
    and missing.triage_device_id = se.device_id
    and missing.triage_start_ms = se.event_ms
    join safetydb_shards.safety_event_metadata sem on missing.triage_org_id = sem.org_id
    and missing.triage_device_id = sem.device_id
    and missing.triage_start_ms = sem.event_ms
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### [Query] Post-trip events

-- COMMAND ----------

create or replace temp view missing_sev1_filtered_by_post_trip as (
  select * from missing_sev1_with_safety_event_metadata where trip_start_ms <= 0 and behavior_label = 19 and ingestion_tag = 1
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### [Query] Remaining Missing SEv1
-- MAGIC Excluding all of the root causes we've already identified.

-- COMMAND ----------

create
or replace temp view unexplained_missing_sev1 as (
  select
    missing.org_id,
    devices.name as device_name,
    missing.device_id,
    missing.event_ms,
    from_unixtime(
      missing.event_ms / 1000 - 6 * 60 * 60,
      'yyyy-MM-dd HH:mm:ss'
    ) as event_time,
    missing.coaching_state,
    missing.behavior_label,
    missing.trip_start_ms
  from
    missing_sev1_with_safety_event_metadata missing
    left join (
      select
        *
      from
        missing_sev1_filtered_by_post_trip
    ) filtered on missing.org_id = filtered.org_id
    and missing.device_id = filtered.device_id
    and missing.event_ms = filtered.event_ms
    join productsdb.devices on devices.id = missing.device_id
  where
    isnull(filtered.org_id)
)

-- COMMAND ----------

select * from unexplained_missing_sev1;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### [Query] Total Org Breakdown of Unexplained Missing SEv1

-- COMMAND ----------

create
or replace temp view unexplained_missing_sev1_org_breakdown as (
  with missing_sev1_org_to_count as (
    select
      org_id,
      count(*) as count
    from
      unexplained_missing_sev1
    group by
      org_id
  )
  select
    missing.org_id,
    missing.count as missing_sev1_count,
    total_events.count as total_event_count,
    round(missing.count * 100 / total_events.count, 2) as pct_discrepancy
  from
    missing_sev1_org_to_count missing
    left join (
      select
        org_id,
        count(*) as count
      from
        visible_non_speeding_triage_events visible
      group by
        org_id
    ) as total_events on total_events.org_id = missing.org_id
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### [Create Table] Total Org Breakdown

-- COMMAND ----------

create or replace table
dataprep_safety.aggregated_inbox_missing_safety_events
as (
  select * from unexplained_missing_sev1_org_breakdown
)
