-- Databricks notebook source
-- Find when Octo devices are connected to a VG
-- NOTE: A VG can have multiple Octo devices connected (e.g. dual AIM4 setups)
CREATE OR REPLACE TEMP VIEW vg_octo_serial_associations_base AS (
  WITH raw_observations AS (
    SELECT
      date,
      time,
      org_id,
      object_id AS vg_device_id,
      usb_device.serial AS octo_device_serial
    FROM kinesisstats_history.osdattachedusbdevices AS a
    JOIN dataprep_firmware.data_ingestion_high_water_mark AS b
    LATERAL VIEW EXPLODE(value.proto_value.attached_usb_devices) AS usb_device
    WHERE usb_device.usb_id IN (1096968297, 1096968298) -- Octo AHD1, Octo AHD4
      AND usb_device.serial IS NOT NULL
      AND a.time <= b.time_ms
  ),
  -- Filter out transient connections: require at least 2 observations of a
  -- (vg_device_id, octo_device_serial) pair to consider it a valid association.
  -- This eliminates single-observation noise from brief/glitchy connections.
  valid_pairs AS (
    SELECT vg_device_id, octo_device_serial
    FROM raw_observations
    GROUP BY vg_device_id, octo_device_serial
    HAVING COUNT(*) >= 2
  )
  SELECT r.*
  FROM raw_observations r
  INNER JOIN valid_pairs v
    ON r.vg_device_id = v.vg_device_id
    AND r.octo_device_serial = v.octo_device_serial
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

-- Goal: Find when Octo devices are connected to a VG
create or replace temp view vg_octo_serial_associations_gateways as (
  SELECT
        a.time,
        a.vg_device_id,
        b.gateway_id as vg_gateway_id,
        a.octo_device_serial,
        c.id as octo_gateway_id,
        d.device_id as octo_device_id
  FROM vg_octo_serial_associations_base a
  INNER JOIN gateway_device_intervals b
    ON a.vg_device_id = b.device_id
    and a.time between b.start_ms and b.end_ms
  LEFT JOIN clouddb.gateways c
    ON upper(replace(a.octo_device_serial, '-', '')) = upper(replace(c.serial, '-', ''))
  INNER JOIN gateway_device_intervals d
    ON c.id = d.gateway_id
    and a.time between d.start_ms and d.end_ms
  WHERE b.gateway_id IS NOT NULL
    and c.id IS NOT NULL
);

-- COMMAND ----------

-- For each (VG, Octo) pair, track when the association starts and ends:
-- 1. A new association starts when we first see this (VG, Octo) pair, or when
--    the Octo moves from a different VG, or when the VG device_id changes.
-- 2. An association ends when the Octo moves to a different VG, or when the
--    VG device_id changes, or at the end of available data.
-- NOTE: For dual AIM4, a VG can have multiple Octos simultaneously, so we
-- track each (VG, Octo) pair independently.

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW transitions_only AS (
  with find_transitions as (
    SELECT time as cur_time,
          vg_gateway_id as cur_vg_gateway_id,
          octo_gateway_id as cur_octo_gateway_id,
          octo_device_id,
          vg_device_id,
          -- Previous time for this specific (VG, Octo) pair
          LAG(time) over(partition by vg_gateway_id, octo_gateway_id order by time) as prev_pair_time,
          -- Previous time for this Octo (any VG)
          lag(time) over(partition by octo_gateway_id order by time) as prev_octo_time,
          -- Previous VG for this Octo
          lag(vg_gateway_id) over(partition by octo_gateway_id order by time) as octo_prev_vg_gateway_id,
          -- Previous vg_device_id for this (VG, Octo) pair
          LAG(vg_device_id) OVER(PARTITION BY vg_gateway_id, octo_gateway_id ORDER BY time) AS pair_prev_vg_device_id,
          case
            -- Octo moved from a different VG to this VG
            WHEN coalesce(lag(vg_gateway_id) over(partition by octo_gateway_id order by time),'null') != vg_gateway_id
            THEN 'octo_changed_vg'
            -- VG device reassigned while this (VG, Octo) pair exists
            WHEN COALESCE(LAG(vg_device_id) OVER(PARTITION BY vg_gateway_id, octo_gateway_id ORDER BY time),'null') != vg_device_id
            THEN 'vg_device_changed'
            ELSE null
          end as transition
    FROM vg_octo_serial_associations_gateways
  )

  SELECT *
  FROM find_transitions
  WHERE transition is not null -- transitions
    OR prev_pair_time is null -- first occurrence of this (VG, Octo) pair
);

-- COMMAND ----------

create or replace temp view final_associations as (
  -- Get the last observation time for each (VG, Octo) pair
  WITH last_observation_per_pair AS (
    SELECT vg_gateway_id, octo_gateway_id, MAX(time) as last_pair_time
    FROM vg_octo_serial_associations_gateways
    GROUP BY vg_gateway_id, octo_gateway_id
  ),

  -- Get the last observation time for each VG (any Octo)
  last_observation_per_vg AS (
    SELECT vg_gateway_id, MAX(time) as last_vg_time
    FROM vg_octo_serial_associations_gateways
    GROUP BY vg_gateway_id
  ),

  intervals AS (
    SELECT t.cur_vg_gateway_id as vg_gateway_id,
          t.cur_octo_gateway_id as octo_gateway_id,
          t.octo_device_id,
          t.vg_device_id,
          t.cur_time as start_ms,
          -- End time based on VG device changing (for this specific VG-Octo pair)
          -- NOTE: We partition by BOTH VG and Octo to track each pair independently.
          -- We do NOT end an association just because the VG has a different Octo
          -- (that's allowed for dual AIM4).
          CASE
            WHEN COALESCE(LEAD(t.vg_device_id) OVER(PARTITION BY t.cur_vg_gateway_id, t.cur_octo_gateway_id ORDER BY t.cur_time),'null') != t.vg_device_id
            THEN LEAD(t.cur_time) OVER(PARTITION BY t.cur_vg_gateway_id, t.cur_octo_gateway_id ORDER BY t.cur_time) - 1
            ELSE NULL
          end as vg_end_ms,
          -- End time based on Octo moving to a different VG
          CASE
            WHEN coalesce(lead(t.cur_vg_gateway_id) OVER(PARTITION BY t.cur_octo_gateway_id ORDER BY t.cur_time),'null') != t.cur_vg_gateway_id
            THEN lead(t.cur_time) OVER(PARTITION BY t.cur_octo_gateway_id ORDER BY t.cur_time) - 1
            ELSE NULL
          END AS octo_end_ms,
          -- End time based on VG continuing without this Octo (replacement detection).
          -- If the VG has observations after this (VG, Octo) pair's last observation,
          -- the Octo was likely disconnected/replaced. End the association at the
          -- last observation of this pair.
          CASE
            WHEN lp.last_pair_time < lv.last_vg_time
            THEN lp.last_pair_time
            ELSE NULL
          END AS pair_inactive_end_ms
    FROM transitions_only t
    JOIN last_observation_per_pair lp
      ON t.cur_vg_gateway_id = lp.vg_gateway_id
      AND t.cur_octo_gateway_id = lp.octo_gateway_id
    JOIN last_observation_per_vg lv
      ON t.cur_vg_gateway_id = lv.vg_gateway_id
  ),

  intervals_final AS (
    SELECT vg_gateway_id,
        octo_gateway_id,
        octo_device_id,
        vg_device_id,
        date(timestamp_millis(start_ms)) as start_date,
        start_ms,
        LEAST(
          COALESCE(vg_end_ms, unix_millis(NOW())),
          COALESCE(octo_end_ms, unix_millis(NOW())),
          COALESCE(pair_inactive_end_ms, unix_millis(NOW()))
        ) AS end_ms
    FROM intervals
  )

  SELECT
    *,
    date(timestamp_millis(end_ms)) as end_date
  FROM intervals_final
);

-- COMMAND ----------

create or replace table dataprep_firmware.octo_vg_gateway_associations_from_usb_enumeration
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
    octo_gateway_id,
    vg_gateway_id,
    octo_device_id,
    vg_device_id
  from final_associations
);

create or replace temp view grouped_by_octo as (
  select
    date,
    count(distinct vg_gateway_id) as vg_count,
    octo_gateway_id
  from exploded
  group by
    date,
    octo_gateway_id
);

create or replace temp view grouped_by_vg as (
  select
    date,
    count(distinct octo_gateway_id) as octo_count,
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
    a.octo_gateway_id,
    a.octo_device_id,
    a.vg_device_id,
    b.vg_count,
    c.octo_count
  from exploded as a
  join grouped_by_octo as b
    on a.date = b.date
    and a.octo_gateway_id = b.octo_gateway_id
  join grouped_by_vg as c
    on a.date = c.date
    and a.vg_gateway_id = c.vg_gateway_id
);

-- NOTE: Allow multiple Octos per VG (e.g. dual AIM4 setups) by not filtering on octo_count.
-- Still filter vg_count = 1 to ensure each Octo is associated with only one VG at a time
-- (handles device swaps correctly).
create or replace temp view octo_vg_gateway_daily_associations_unique as (
  select *
  from joined
  where vg_count = 1
);

-- COMMAND ----------

create or replace table dataprep_firmware.octo_vg_daily_associations_unique
using delta
partitioned by (date)
as
select * from octo_vg_gateway_daily_associations_unique
