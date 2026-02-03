WITH osdhubserverdeviceheartbeat AS (
  SELECT
    osdhubserverdeviceheartbeat.org_id,
    object_id,
    time,
    date,
    value.proto_value.hub_server_device_heartbeat.connection.device_hello.boot_count,
    value.proto_value.hub_server_device_heartbeat.connection.device_hello.build
  FROM
    kinesisstats.osdhubserverdeviceheartbeat
    JOIN cm_health_report.cm_2x_3x_linked_vgs AS vg_cm ON osdhubserverdeviceheartbeat.org_id = vg_cm.org_id
    AND (
      osdhubserverdeviceheartbeat.object_id = vg_cm.vg_device_id
      OR osdhubserverdeviceheartbeat.object_id = vg_cm.linked_cm_id
    )
  WHERE
    osdhubserverdeviceheartbeat.value.is_databreak = 'false'
    AND osdhubserverdeviceheartbeat.value.is_end = 'false'
    AND osdhubserverdeviceheartbeat.date >= ${start_date}
    AND osdhubserverdeviceheartbeat.date < ${end_date}
    -- Exclude data in the past 4 hours due to ingestion delays.
    AND osdhubserverdeviceheartbeat.time <= unix_timestamp(to_timestamp(${pipeline_execution_time})) * 1000 - 4 * 60 * 60 * 1000
),
-- vg and cm over intervals
device_bootcount_vg AS (
  SELECT
    cm_target_intervals.org_id,
    cm_target_intervals.vg_device_id,
    cm_target_intervals.cm_device_id,
    cm_target_intervals.vg_product_id,
    cm_target_intervals.cm_product_id,
    cm_target_intervals.date,
    cm_target_intervals.start_ms,
    cm_target_intervals.end_ms,
    max(osdhubserverdeviceheartbeat.boot_count) - min(osdhubserverdeviceheartbeat.boot_count) AS vg_bootcount
  FROM
    cm_health_report.cm_target_intervals
    JOIN osdhubserverdeviceheartbeat ON osdhubserverdeviceheartbeat.object_id = cm_target_intervals.vg_device_id
    AND osdhubserverdeviceheartbeat.org_id = cm_target_intervals.org_id
    AND osdhubserverdeviceheartbeat.time >= cm_target_intervals.start_ms - 5 * 60 * 1000
    AND osdhubserverdeviceheartbeat.time <= cm_target_intervals.end_ms
    AND osdhubserverdeviceheartbeat.date = cm_target_intervals.date
  GROUP BY
    cm_target_intervals.org_id,
    cm_target_intervals.vg_device_id,
    cm_target_intervals.cm_device_id,
    cm_target_intervals.vg_product_id,
    cm_target_intervals.cm_product_id,
    cm_target_intervals.start_ms,
    cm_target_intervals.end_ms,
    cm_target_intervals.date
),
device_build_and_bootcount_vg AS (
  SELECT
    device_bootcount_vg.org_id,
    device_bootcount_vg.vg_device_id,
    device_bootcount_vg.cm_device_id,
    device_bootcount_vg.vg_product_id,
    device_bootcount_vg.cm_product_id,
    device_bootcount_vg.date,
    device_bootcount_vg.start_ms,
    device_bootcount_vg.end_ms,
    device_bootcount_vg.vg_bootcount,
    -- take earliest build
    min(
      (
        osdhubserverdeviceheartbeat.time,
        osdhubserverdeviceheartbeat.build
      )
    ).build AS vg_build
  FROM
    device_bootcount_vg
    JOIN osdhubserverdeviceheartbeat ON osdhubserverdeviceheartbeat.object_id = device_bootcount_vg.vg_device_id
    AND osdhubserverdeviceheartbeat.org_id = device_bootcount_vg.org_id
    AND osdhubserverdeviceheartbeat.date = device_bootcount_vg.date
  GROUP BY
    device_bootcount_vg.org_id,
    device_bootcount_vg.vg_device_id,
    device_bootcount_vg.cm_device_id,
    device_bootcount_vg.vg_product_id,
    device_bootcount_vg.cm_product_id,
    device_bootcount_vg.start_ms,
    device_bootcount_vg.end_ms,
    device_bootcount_vg.date,
    device_bootcount_vg.vg_bootcount
),
cm_bootcount AS (
  SELECT
    device_build_and_bootcount_vg.*,
    max(osdhubserverdeviceheartbeat.boot_count) - min(osdhubserverdeviceheartbeat.boot_count) AS cm_bootcount
  FROM
    device_build_and_bootcount_vg
    JOIN osdhubserverdeviceheartbeat ON osdhubserverdeviceheartbeat.object_id = device_build_and_bootcount_vg.cm_device_id
    AND osdhubserverdeviceheartbeat.org_id = device_build_and_bootcount_vg.org_id
    -- we consider heartbeat build+bootcount data received up to 5 min grace period before start of trip
    AND osdhubserverdeviceheartbeat.time >= device_build_and_bootcount_vg.start_ms - 5 * 60 * 1000
    AND osdhubserverdeviceheartbeat.time <= device_build_and_bootcount_vg.end_ms
    AND osdhubserverdeviceheartbeat.date = device_build_and_bootcount_vg.date
  GROUP BY
    device_build_and_bootcount_vg.org_id,
    device_build_and_bootcount_vg.vg_device_id,
    device_build_and_bootcount_vg.cm_device_id,
    device_build_and_bootcount_vg.vg_product_id,
    device_build_and_bootcount_vg.cm_product_id,
    device_build_and_bootcount_vg.start_ms,
    device_build_and_bootcount_vg.end_ms,
    device_build_and_bootcount_vg.date,
    device_build_and_bootcount_vg.vg_build,
    device_build_and_bootcount_vg.vg_bootcount
),
device_build_and_bootcount AS (
  SELECT
    cm_bootcount.*,
    min(
      (
        osdhubserverdeviceheartbeat.time,
        osdhubserverdeviceheartbeat.build
      )
    ).build AS cm_build
  FROM
    cm_bootcount
    JOIN osdhubserverdeviceheartbeat ON osdhubserverdeviceheartbeat.object_id = cm_bootcount.cm_device_id
    AND osdhubserverdeviceheartbeat.org_id = cm_bootcount.org_id
    AND osdhubserverdeviceheartbeat.date = cm_bootcount.date
  GROUP BY
    cm_bootcount.org_id,
    cm_bootcount.vg_device_id,
    cm_bootcount.cm_device_id,
    cm_bootcount.vg_product_id,
    cm_bootcount.cm_product_id,
    cm_bootcount.start_ms,
    cm_bootcount.end_ms,
    cm_bootcount.date,
    cm_bootcount.vg_build,
    cm_bootcount.vg_bootcount,
    cm_bootcount.cm_bootcount
)
SELECT
  *
FROM
  device_build_and_bootcount
