SELECT
  edh.date,
  COALESCE(aetolbh.org_id, abebh.org_id, bebh.org_id, cbh.org_id, ccbh.org_id, laetolbh.org_id, lcbh.org_id, rgbbh.org_id, edh.org_id, esfbh.org_id) AS org_id,
  COALESCE(aetolbh.device_id, abebh.device_id, bebh.device_id, cbh.device_id, ccbh.device_id, laetolbh.device_id, lcbh.device_id, rgbbh.device_id, edh.object_id, esfbh.object_id) AS device_id,
  COALESCE(aetolbh.driver_id, abebh.driver_id, bebh.driver_id, cbh.driver_id, ccbh.driver_id, laetolbh.driver_id, lcbh.driver_id, rgbbh.driver_id, edh.driver_id, esfbh.driver_id, 0) AS driver_id,
  COALESCE(aetolbh.interval_start, abebh.interval_start, bebh.interval_start, cbh.interval_start, ccbh.interval_start, laetolbh.interval_start, lcbh.interval_start, rgbbh.interval_start, edh.interval_start, esfbh.interval_start) AS interval_start,
  COALESCE(aetolbh.interval_start, abebh.interval_start, bebh.interval_start, cbh.interval_start, ccbh.interval_start, laetolbh.interval_start, lcbh.interval_start, rgbbh.interval_start, edh.interval_start, esfbh.interval_start) + INTERVAL 1 HOURS as interval_end,
  ccbh.cruise_control_ms AS cruise_control_ms,
  COALESCE(lcbh.limited_coasting_time_ms, cbh.coasting_time_ms) AS coasting_ms, -- Some firmware versions do not report the limited coasting stat, COALESCE with regular stat
  rgbbh.rpm_green_band_ms AS green_band_ms,
  COALESCE(laetolbh.limited_accel_engine_torque_over_limit_ms, aetolbh.accel_engine_torque_over_limit_ms) AS accelerator_engine_torque_over_ms, -- Some firmware versions do not report the limited accel torque stat, COALESCE with regular stat
  abebh.anticipation_brake_events AS quick_brake_event_count,
  bebh.brake_events AS total_brake_event_count,
  esfbh.on_duration_ms AS on_duration_ms,
  esfbh.idle_duration_ms AS idle_duration_ms,
  esfbh.aux_during_idle_ms AS aux_during_idle_ms,
  soptbh.over_speed_ms AS over_speed_ms,
  wfbd.duration_ms AS wear_free_braking_duration_ms,
  bd.duration_ms AS braking_duration_ms,

  -- Maintain fuel efficiency filtering consistency with FEER. This was adapted from `aggregated_vehicle_driver_rows_v3` logic.
  -- We have seen unrealistic fuel efficiencies in the past, so we want this logic for the Driver Efficiency API as well for consistency.
  -- If the mpg is greater than 200, we set the fuel_consumption and distance fields to 0.
  -- unit conversions -> 0.00026417 ml/gal, 0.0006213711922 meters/mile
  CASE
    WHEN esfbh.fuel_consumed_ml > 0 AND dbh.distance_traveled_m * 0.0006213711922 / (esfbh.fuel_consumed_ml * 0.000264172) > 200 THEN 0
    ELSE esfbh.fuel_consumed_ml
  END fuel_consumed_ml,
  -- distance_meters_gps historically represented just GPS distance, it now represents canonical distance that
  -- includes ECU distance. The column name hasn't been changed to reflect this as this change comes with
  -- significant work and cost, a ticket has been created to carry out this work at the next suitable opportunity
  -- where we intend to run a backfill for all time. https://samsaradev.atlassian.net/browse/EVEC-355
  CASE
    WHEN esfbh.fuel_consumed_ml > 0 AND dbh.distance_traveled_m * 0.0006213711922 / (esfbh.fuel_consumed_ml * 0.000264172) > 200 THEN 0
    ELSE dbh.distance_traveled_m
  END distance_meters_gps,
  COALESCE(wwbh.weighted_weight, 0) AS weighted_weight,
  COALESCE(wwbh.weight_time, 0) AS weight_time,
  COALESCE(uaddbh.uphill_duration_ms, 0) AS uphill_duration_ms,
  COALESCE(uaddbh.downhill_duration_ms, 0) AS downhill_duration_ms
FROM
  fuel_energy_efficiency_report.engine_duration_by_hour_v2 AS edh
FULL OUTER JOIN ecodriving_report.anticipation_brake_events_by_hour AS abebh
  ON edh.date = abebh.date
  AND edh.org_id = abebh.org_id
  AND edh.object_id = abebh.device_id
  AND edh.driver_id = abebh.driver_id
  AND edh.interval_start = abebh.interval_start
FULL OUTER JOIN ecodriving_report.brake_events_by_hour AS bebh
  ON edh.date = bebh.date
  AND edh.org_id = bebh.org_id
  AND edh.object_id = bebh.device_id
  AND edh.driver_id = bebh.driver_id
  AND edh.interval_start = bebh.interval_start
FULL OUTER JOIN ecodriving_report.coasting_by_hour AS cbh
  ON edh.date = cbh.date
  AND edh.org_id = cbh.org_id
  AND edh.object_id = cbh.device_id
  AND edh.driver_id = cbh.driver_id
  AND edh.interval_start = cbh.interval_start
FULL OUTER JOIN ecodriving_report.cruise_control_by_hour AS ccbh
  ON edh.date = ccbh.date
  AND edh.org_id = ccbh.org_id
  AND edh.object_id = ccbh.device_id
  AND edh.driver_id = ccbh.driver_id
  AND edh.interval_start = ccbh.interval_start
FULL OUTER JOIN ecodriving_report.limited_accel_engine_torque_over_limit_by_hour AS laetolbh
  ON edh.date = laetolbh.date
  AND edh.org_id = laetolbh.org_id
  AND edh.object_id = laetolbh.device_id
  AND edh.driver_id = laetolbh.driver_id
  AND edh.interval_start = laetolbh.interval_start
FULL OUTER JOIN ecodriving_report.limited_coasting_by_hour AS lcbh
  ON edh.date = lcbh.date
  AND edh.org_id = lcbh.org_id
  AND edh.object_id = lcbh.device_id
  AND edh.driver_id = lcbh.driver_id
  AND edh.interval_start = lcbh.interval_start
FULL OUTER JOIN ecodriving_report.rpm_green_band_by_hour AS rgbbh
  ON edh.date = rgbbh.date
  AND edh.org_id = rgbbh.org_id
  AND edh.object_id = rgbbh.device_id
  AND edh.driver_id = rgbbh.driver_id
  AND edh.interval_start = rgbbh.interval_start
FULL OUTER JOIN ecodriving_report.accel_engine_torque_over_limit_by_hour AS aetolbh
  ON edh.date = aetolbh.date
  AND edh.org_id = aetolbh.org_id
  AND edh.object_id = aetolbh.device_id
  AND edh.driver_id = aetolbh.driver_id
  AND edh.interval_start = aetolbh.interval_start
FULL OUTER JOIN fuel_energy_efficiency_report.engine_state_fuel_by_hour_v3 as esfbh
  ON edh.date = esfbh.date
  AND edh.org_id = esfbh.org_id
  AND edh.driver_id = esfbh.driver_id
  AND edh.object_id = esfbh.object_id
  AND edh.interval_start = esfbh.interval_start
FULL OUTER JOIN fuel_energy_efficiency_report.distance_by_hour_v3 as dbh
  ON edh.date = dbh.date
  AND edh.org_id = dbh.org_id
  AND edh.driver_id = dbh.driver_id
  AND edh.object_id = dbh.object_id
  AND edh.interval_start = dbh.interval_start
FULL OUTER JOIN ecodriving_report.speed_over_profile_threshold_by_hour as soptbh
  ON edh.date = soptbh.date
  AND edh.org_id = soptbh.org_id
  AND edh.driver_id = soptbh.driver_id
  AND edh.object_id = soptbh.device_id
  AND edh.interval_start = soptbh.interval_start
FULL OUTER JOIN ecodriving_report.weighted_weight_by_hour as wwbh
  ON edh.date = wwbh.date
  AND edh.org_id = wwbh.org_id
  AND edh.object_id = wwbh.device_id
  AND edh.interval_start = wwbh.interval_start
FULL OUTER JOIN ecodriving_report.uphill_and_downhill_duration_by_hour as uaddbh
  ON edh.date = uaddbh.date
  AND edh.org_id = uaddbh.org_id
  AND edh.object_id = uaddbh.device_id
  AND edh.driver_id = uaddbh.driver_id
  AND edh.interval_start = uaddbh.interval_start
FULL OUTER JOIN ecodriving_report.wear_free_braking_duration_ms as wfbd
  ON edh.date = wfbd.date
  AND edh.org_id = wfbd.org_id
  AND edh.object_id = wfbd.device_id
  AND edh.driver_id = wfbd.driver_id
  AND edh.interval_start = wfbd.interval_start
FULL OUTER JOIN ecodriving_report.braking_duration_ms as bd
  ON edh.date = bd.date
  AND edh.org_id = bd.org_id
  AND edh.object_id = bd.device_id
  AND edh.driver_id = bd.driver_id
  AND edh.interval_start = bd.interval_start
WHERE edh.date >= ${start_date}
AND edh.date < ${end_date}
AND edh.on_duration_ms != 0
