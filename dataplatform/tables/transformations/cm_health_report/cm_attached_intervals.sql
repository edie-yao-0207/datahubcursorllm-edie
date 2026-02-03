WITH attached_usb_devices AS (
  SELECT
    cm_linked_vgs.org_id,
    cm_linked_vgs.vg_device_id as device_id,
    cm_linked_vgs.linked_cm_id,
    osdattachedusbdevices.time as timestamp,
    -- 1668105037 is the GAIA id
    -- 205874022 is CMX2 diclops
    -- 205874024 is CMX2 Camera (Sonix)
    -- 94606370 is CM11 Camera (ARC International)
    COALESCE(EXISTS(
      osdattachedusbdevices.value.proto_value.attached_usb_devices.usb_id,
      x -> x == 1668105037
      OR x == 205874022
      OR x == 205874024
      OR x == 94606370
    ), false) AS attached_to_cm,
    date
  FROM
    cm_health_report.cm_2x_3x_linked_vgs as cm_linked_vgs
    LEFT JOIN kinesisstats.osdattachedusbdevices as osdattachedusbdevices on cm_linked_vgs.org_id = osdattachedusbdevices.org_id
    AND cm_linked_vgs.vg_device_id = osdattachedusbdevices.object_id
  WHERE
    osdattachedusbdevices.value.is_databreak = false
    AND osdattachedusbdevices.value.is_end = false
    -- Always use the most recent 30 days of data.
    AND osdattachedusbdevices.date < ${end_date}
    AND osdattachedusbdevices.date >= ${start_date}
    -- Exclude data in the past 4 hours due to ingestion delays.
    AND osdattachedusbdevices.time <= unix_timestamp(to_timestamp(${pipeline_execution_time})) * 1000 - 4 * 60 * 60 * 1000
),
cm_attached_lag AS (
  -- In order to build our transitions, we need to construct the start and end time.
  -- Grab prior state and prior time for each row.
  SELECT
    linked_cm_id,
    device_id,
    org_id,
    lag(timestamp) OVER (
      PARTITION BY org_id,
      linked_cm_id,
      device_id
      ORDER BY
        timestamp
    ) AS prev_time,
    lag(attached_to_cm) OVER (
      PARTITION BY org_id,
      linked_cm_id,
      device_id
      ORDER BY
        timestamp
    ) AS prev_state,
    timestamp AS cur_time,
    attached_to_cm AS cur_state,
    date
  FROM
    attached_usb_devices
),
attached_hist AS (
  -- Create table that contains the transitions between each state
  -- The null check includes the first state transition, but not
  -- the open bound.
  SELECT
    *
  FROM
    cm_attached_lag lag
  WHERE
    lag.prev_state != lag.cur_state OR prev_time IS NULL
),
attached_states as (
  -- Generate the correct start/end intervals from state transitions
  SELECT
    org_id,
    device_id,
    linked_cm_id,
    cur_time AS start_ms,
    cur_state AS attached_to_cm,
    lead(cur_time) OVER (
      PARTITION BY org_id,
      linked_cm_id,
      device_id
      ORDER BY
        cur_time
    ) AS end_ms,
    date
  FROM
    attached_hist
) -- exclude the open interval at the end of the lead series.
SELECT
  *
FROM
  attached_states
WHERE
  end_ms IS NOT NULL
