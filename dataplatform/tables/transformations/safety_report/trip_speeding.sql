WITH us_orgs AS (
  SELECT
    id
  FROM
    clouddb.organizations
  where
    locale = 'us'
),
non_us_orgs AS (
  SELECT
    id
  FROM
    clouddb.organizations
  where
    locale != 'us'
),
us_trips_statistics AS (
  SELECT
    trips.org_id as org_id,
    trips.device_id as device_id,
    trips.driver_id_exclude_0_drivers as driver_id,
    trips.start_ms as start_ms,
    trips.end_ms as end_ms,
    trips.version as version,
    COALESCE(
      TO_DATE(
        from_unixtime(
          trips.end_ms / CAST(1e3 AS DECIMAL(4, 0))
        )
      ),
      TO_DATE(date)
    ) as date,
    COALESCE(
      trips.proto.trip_speeding_count_custom.not_speeding_count,
      COALESCE(
        trips.proto.trip_speeding_count_mph.not_speeding_count,
        0
      )
    ) AS not_speeding_count,
    COALESCE(
      trips.proto.trip_speeding_count_custom.light_speeding_count,
      COALESCE(
        trips.proto.trip_speeding_count_mph.light_speeding_count,
        0
      )
    ) AS light_speeding_count,
    COALESCE(
      trips.proto.trip_speeding_count_custom.moderate_speeding_count,
      COALESCE(
        trips.proto.trip_speeding_count_mph.moderate_speeding_count,
        0
      )
    ) AS moderate_speeding_count,
    COALESCE(
      trips.proto.trip_speeding_count_custom.heavy_speeding_count,
      COALESCE(
        trips.proto.trip_speeding_count_mph.heavy_speeding_count,
        0
      )
    ) AS heavy_speeding_count,
    COALESCE(
      trips.proto.trip_speeding_count_custom.severe_speeding_count,
      COALESCE(
        trips.proto.trip_speeding_count_mph.severe_speeding_count,
        0
      )
    ) AS severe_speeding_count,
    COALESCE(
      trips.proto.trip_speeding_custom.not_speeding_ms,
      COALESCE(
        trips.proto.trip_speeding_mph.not_speeding_ms,
        0
      )
    ) AS not_speeding_ms,
    COALESCE(
      trips.proto.trip_speeding_custom.light_speeding_ms,
      COALESCE(
        trips.proto.trip_speeding_mph.light_speeding_ms,
        0
      )
    ) AS light_speeding_ms,
    COALESCE(
      trips.proto.trip_speeding_custom.moderate_speeding_ms,
      COALESCE(
        trips.proto.trip_speeding_mph.moderate_speeding_ms,
        0
      )
    ) AS moderate_speeding_ms,
    COALESCE(
      trips.proto.trip_speeding_custom.heavy_speeding_ms,
      COALESCE(
        trips.proto.trip_speeding_mph.heavy_speeding_ms,
        0
      )
    ) AS heavy_speeding_ms,
    COALESCE(
      trips.proto.trip_speeding_custom.severe_speeding_ms,
      COALESCE(
        trips.proto.trip_speeding_mph.severe_speeding_ms,
        0
      )
    ) AS severe_speeding_ms,
    COALESCE(
      trips.proto.trip_engine.engine_idle_ms,
      0
    ) AS engine_idle_ms,
    COALESCE(
      trips.proto.trip_distance.distance_meters,
      0
    ) AS distance_meters,
    CAST(
      CASE
        -- CM31 (product id: 44), CM33 (product_id: 167)
        WHEN devices.camera_product_id IN (44, 167) THEN 1
        ELSE 0
      END AS BYTE
    ) AS has_outward_camera,
    CAST(
      CASE
        -- CM32 (product id: 43), CM34 (product_id: 155)
        WHEN devices.camera_product_id IN (43, 155) THEN 1
        ELSE 0
      END AS BYTE
    ) AS has_inward_outward_camera,
    trips.end_ms - trips.start_ms AS driving_time_ms,
    IF(
      trips.proto.trip_seatbelt.reporting_present,
      trips.end_ms - trips.start_ms,
      0
    ) AS seatbelt_reporting_present_ms,
    COALESCE(
      trips.proto.trip_seatbelt.unbuckled_ms,
      0
    ) AS seatbelt_unbuckled_ms
  FROM
    trips2db_shards.trips trips
    JOIN productsdb.devices devices ON devices.id = trips.device_id
    JOIN us_orgs ON us_orgs.id = trips.org_id
    JOIN clouddb.groups groups ON groups.organization_id = trips.org_id
  WHERE
    date >= DATE_ADD(${start_date}, -2)
    AND date < ${end_date}
    AND TO_DATE(
      from_unixtime(
        trips.end_ms / CAST(1e3 AS DECIMAL(4, 0))
      )
    ) >= ${start_date}
    AND TO_DATE(
      from_unixtime(
        trips.end_ms / CAST(1e3 AS DECIMAL(4, 0))
      )
    ) < ${end_date} -- This filters out all assets that are not vehicles
    AND devices.asset_type = 4 -- This filters out trips corresponding to unassigned orgs
    AND trips.org_id != 1 -- This filters out trips with invalid trip version
    AND trips.version = 101
),
non_us_trips_statistics AS (
  SELECT
    trips.org_id as org_id,
    trips.device_id as device_id,
    trips.driver_id_exclude_0_drivers as driver_id,
    trips.start_ms as start_ms,
    trips.end_ms as end_ms,
    trips.version as version,
    COALESCE(
      TO_DATE(
        from_unixtime(
          trips.end_ms / CAST(1e3 AS DECIMAL(4, 0))
        )
      ),
      TO_DATE(date)
    ) as date,
    COALESCE(
      trips.proto.trip_speeding_count_custom.not_speeding_count,
      -- `trip_speeding_count_percent` is not implemented, so we fallback to 0
      0
    ) AS not_speeding_count,
    COALESCE(
      trips.proto.trip_speeding_count_custom.light_speeding_count,
      0
    ) AS light_speeding_count,
    COALESCE(
      trips.proto.trip_speeding_count_custom.moderate_speeding_count,
      0
    ) AS moderate_speeding_count,
    COALESCE(
      trips.proto.trip_speeding_count_custom.heavy_speeding_count,
      0
    ) AS heavy_speeding_count,
    COALESCE(
      trips.proto.trip_speeding_count_custom.severe_speeding_count,
      0
    ) AS severe_speeding_count,
    COALESCE(
      trips.proto.trip_speeding_custom.not_speeding_ms,
      COALESCE(
        trips.proto.trip_speeding_percent.not_speeding_ms,
        0
      )
    ) AS not_speeding_ms,
    COALESCE(
      trips.proto.trip_speeding_custom.light_speeding_ms,
      COALESCE(
        trips.proto.trip_speeding_percent.light_speeding_ms,
        0
      )
    ) AS light_speeding_ms,
    COALESCE(
      trips.proto.trip_speeding_custom.moderate_speeding_ms,
      COALESCE(
        trips.proto.trip_speeding_percent.moderate_speeding_ms,
        0
      )
    ) AS moderate_speeding_ms,
    COALESCE(
      trips.proto.trip_speeding_custom.heavy_speeding_ms,
      COALESCE(
        trips.proto.trip_speeding_percent.heavy_speeding_ms,
        0
      )
    ) AS heavy_speeding_ms,
    COALESCE(
      trips.proto.trip_speeding_custom.severe_speeding_ms,
      COALESCE(
        trips.proto.trip_speeding_percent.severe_speeding_ms,
        0
      )
    ) AS severe_speeding_ms,
    COALESCE(
      trips.proto.trip_engine.engine_idle_ms,
      0
    ) AS engine_idle_ms,
    COALESCE(
      trips.proto.trip_distance.distance_meters,
      0
    ) AS distance_meters,
    CAST(
      CASE
        -- CM31 (product id: 44), CM33 (product_id: 167)
        WHEN devices.camera_product_id IN (44, 167) THEN 1
        ELSE 0
      END AS BYTE
    ) AS has_outward_camera,
    CAST(
      CASE
        -- CM32 (product id: 43), CM34 (product_id: 155)
        WHEN devices.camera_product_id IN (43, 155) THEN 1
        ELSE 0
      END AS BYTE
    ) AS has_inward_outward_camera,
    trips.end_ms - trips.start_ms AS driving_time_ms,
    IF(
      trips.proto.trip_seatbelt.reporting_present,
      trips.end_ms - trips.start_ms,
      0
    ) AS seatbelt_reporting_present_ms,
    COALESCE(
      trips.proto.trip_seatbelt.unbuckled_ms,
      0
    ) AS seatbelt_unbuckled_ms
  FROM
    trips2db_shards.trips trips
    JOIN productsdb.devices devices ON devices.id = trips.device_id
    JOIN non_us_orgs ON non_us_orgs.id = trips.org_id
    JOIN clouddb.groups groups ON groups.organization_id = trips.org_id
  WHERE
    date >= DATE_ADD(${start_date}, -2)
    AND date < ${end_date}
    AND TO_DATE(
      from_unixtime(
        trips.end_ms / CAST(1e3 AS DECIMAL(4, 0))
      )
    ) >= ${start_date}
    AND TO_DATE(
      from_unixtime(
        trips.end_ms / CAST(1e3 AS DECIMAL(4, 0))
      )
    ) < ${end_date} -- This filters out all assets that are not vehicles
    AND devices.asset_type = 4 -- This filters out trips corresponding to unassigned orgs
    AND trips.org_id NOT IN (1, 562949953421343) -- This filters out trips with invalid trip version
    AND trips.version = 101
),
trips_statistics as (
  SELECT
    *
  FROM
    us_trips_statistics
  UNION ALL
  SELECT
    *
  FROM
    non_us_trips_statistics
)
SELECT
  *
FROM
  trips_statistics
