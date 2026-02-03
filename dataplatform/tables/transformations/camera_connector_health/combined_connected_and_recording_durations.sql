-- Find any associated cm_ids, if any
with vgs_with_cm3xs AS (
  SELECT
    devices.id AS vg_id,
    gateways.device_id AS cm_id
  FROM
    productsdb.gateways gateways
    JOIN productsdb.devices devices ON upper(replace(gateways.serial, '-', '')) = upper(replace(devices.camera_serial, '-', ''))
    AND devices.org_id = gateways.org_id
  WHERE
    gateways.product_id IN (
      43,  -- CM32
      44,  -- CM31
      155, -- CM34 BrigidDual-256GB
      167  -- CM33 BrigidSingle
    )
),
-- Populate cm_id into auxcam connected and recording durations
auxcam_connected_and_recording_durations_with_cm_id AS (
  select
    org_id,
    date,
    a.vg_id,
    cm_id,
    trip_start_ms,
    trip_duration_ms,
    connected_ms,
    recording_ms
  from
    camera_connector_health.auxcam_connected_and_recording_durations a
    left join vgs_with_cm3xs v on a.vg_id = v.vg_id
  where
    trip_start_ms >= unix_timestamp(date_sub(${end_date}, 30)) * 1000
),
-- Auxcams that are currently associated with a VG
collections_with_auxcams as (
  select
    device_id as auxcam_id,
    collection_uuid
  from
    deviceassociationsdb_shards.device_collection_associations
  where
    product_id = 126
    and end_at is null
),
-- Devices that are currently associated with an auxcam
non_auxcams_in_auxcam_collection as (
  select
    device_id,
    collection_uuid
  from
    deviceassociationsdb_shards.device_collection_associations
  where
    collection_uuid in (
      select
        collection_uuid
      from
        collections_with_auxcams
    )
    and product_id != 126
    and end_at is null
),
-- Auxcams and the devices in the same conllection
auxcams_and_associated_vgs as (
  select
    distinct device_id
  from
    non_auxcams_in_auxcam_collection n
    inner join collections_with_auxcams c on n.collection_uuid = c.collection_uuid
)
-- Only include auxcams data if auxcams are currently connected
-- Otherwise, use only Baxter data
select
  org_id,
  CASE
    WHEN date_format(date, "yyyy-MM-dd") < ${start_date} THEN ${start_date}
    WHEN date_format(date, "yyyy-MM-dd") > ${end_date} THEN ${end_date}
    ELSE date
  END AS date,
  vg_id,
  cm_id,
  trip_start_ms,
  trip_duration_ms,
  connected_ms,
  recording_ms
from
  auxcam_connected_and_recording_durations_with_cm_id
where
  vg_id in (
    select
      *
    from
      auxcams_and_associated_vgs
  )
  -- INC-1441: Temporarily exclude problematic VG to mitigate incident
  -- Additional VGs subsequently excluded to address similar failures
  and vg_id NOT IN (281474988296557, 281474985935519, 281474995514677)
union
select
  org_id,
  CASE
    WHEN date_format(date, "yyyy-MM-dd") < ${start_date} THEN ${start_date}
    WHEN date_format(date, "yyyy-MM-dd") > ${end_date} THEN ${end_date}
    ELSE date
  END AS date,
  vg_id,
  cm_id,
  trip_start_ms,
  trip_duration_ms,
  connected_ms,
  recording_ms
from
  camera_connector_health.cc_connected_and_recording_durations
where
  vg_id not in (
    select
      *
    from
      auxcams_and_associated_vgs
  )
order by
  vg_id,
  trip_start_ms asc
