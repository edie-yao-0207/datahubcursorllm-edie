WITH location_speed AS (
  SELECT
    DATE(loc.date) AS date,
    loc.org_id,
    loc.device_id,
    loc.time,
    previous.time AS previous_time,
    COALESCE(loc.value.ecu_speed_meters_per_second, loc.value.gps_speed_meters_per_second, 0) AS speed_meters_per_second
  FROM kinesisstats_window.location as loc
  WHERE loc.date >= ${start_date}
  AND loc.date < ${end_date}
), location_speed_and_profile as (
  -- Obtain the most suitable profile for each org/device combination.
  -- If an org/device matches then use that profile
  -- Otherwise fall back on the default org profile
  SELECT
    ls.date,
    ls.org_id,
    ls.device_id,
    ls.time,
    -- Speeding cannot take longer than time while the engine is ON or IDLE
    -- Ignore speeding when the engine state interval is not found
    -- and not joined to the locations
    IF (esi.start_ms IS NOT NULL,
      ls.time - GREATEST(ls.previous_time, esi.start_ms),
      0
    ) AS time_at_speed_ms,
    ls.speed_meters_per_second,
    COALESCE(pf.profile_id, sp.profile_uuid, 0) as profile_id
  FROM location_speed AS ls
  LEFT JOIN fueldb_shards.driver_efficiency_profiles_devices as pf
    ON pf.org_id = ls.org_id
    AND pf.device_id = ls.device_id
    AND ls.speed_meters_per_second > 0
  LEFT JOIN fueldb_shards.driver_efficiency_score_profiles as sp
    ON sp.org_id = ls.org_id
    AND sp.is_default = 1
    AND ls.speed_meters_per_second > 0
  LEFT JOIN engine_state.intervals AS esi
    -- esi.date is an index, so it will speedup the query
    ON ls.date = esi.date
    AND ls.org_id = esi.org_id
    AND ls.device_id = esi.object_id
    AND esi.start_ms < ls.time AND ls.time <= esi.end_ms
    -- Speeding can be done when the engine is ON or IDLE
    AND (esi.state = "ON" OR esi.state = "IDLE")
)

SELECT
  lsp.date,
  lsp.org_id,
  lsp.device_id AS device_id,
  -- Combine NULL and ZERO driver assignments to be equivalent.
  -- This ensures that we don't lose data for unassigned periods.
  -- This also ensures that we don't run into duplicate rows on merge downstream.
  COALESCE(da.driver_id, 0) AS driver_id,
  WINDOW(from_unixtime(time / 1000), "1 hour").start AS interval_start,
  WINDOW(from_unixtime(time / 1000), "1 hour").end AS interval_end,
  -- Compare meters_per_second against custom_speeding_threshold. If it does not exist fall back to 50 mph (22.352 mps)
  -- as it is the configured value for when orgs don't have a speed_threshold configured.
  -- 5 mph padding to both speeds to match Safety's implementation
  -- https://github.com/samsara-dev/backend/blob/master/go/src/samsaradev.io/stats/trips/tripslogic/speeding.go#L55
  -- We apply this condition here rather than as a restriction in the data considered to ensure that we retain a value of
  -- zero where speed data exists, this ensures that we can differentiate between ZERO and MISSING over-speed time.
  SUM(CASE WHEN lsp.speed_meters_per_second > 0 AND lsp.speed_meters_per_second > COALESCE(spthi.custom_speed_profile_threshold_meters_per_second, 22.352) + 2.2352 THEN time_at_speed_ms ELSE 0 END) AS over_speed_ms
FROM location_speed_and_profile AS lsp
LEFT JOIN fuel_energy_efficiency_report.driver_assignments AS da -- Use same driver_assignment as FEER for consistency
  ON da.org_id = lsp.org_id
  AND da.device_id = lsp.device_id
  AND da.end_time >= CAST(unix_timestamp(CAST (${start_date} AS TIMESTAMP)) * 1000 AS BIGINT)
  AND lsp.time BETWEEN da.start_time AND da.end_time
LEFT JOIN ecodriving_report.speed_profile_threshold_intervals spthi
  ON spthi.org_id = lsp.org_id
  AND spthi.profile_id = lsp.profile_id
  AND spthi.interval_end >= CAST (${start_date} AS TIMESTAMP)
  AND CAST(lsp.time / 1000 AS TIMESTAMP) BETWEEN spthi.interval_start AND spthi.interval_end
WHERE lsp.date >= ${start_date}
  AND lsp.date < ${end_date}
GROUP BY lsp.date, lsp.org_id, lsp.device_id, COALESCE(da.driver_id, 0), WINDOW

