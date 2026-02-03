-- `operating_authority_updates` extracts the operating authority and driver ID
-- from driver carrier entries (which are populated when driver selects new carrier)
-- and vehicle assignments (which are populated when driver is assigned to a vehicle)
WITH carrier_updates AS (
  SELECT
    uuid AS id,
    DATE(FROM_UNIXTIME(log_at_ms / 1000)) AS date,
    org_id,
    CAST(FROM_UNIXTIME(log_at_ms / 1000) AS TIMESTAMP) AS log_at,
    log_at_ms,
    driver_id,
    CAST(dot_number AS INT) AS operating_authority
  FROM compliancedb_shards.driver_carrier_entries
  WHERE DATE(FROM_UNIXTIME(log_at_ms / 1000)) >= ${start_date}
    AND DATE(FROM_UNIXTIME(log_at_ms / 1000)) <  ${end_date}
    AND org_id NOT IN (SELECT * FROM helpers.ignored_org_ids)
),

vehicle_assignments AS (
  SELECT
    org_id,
    driver_id,
    vehicle_id,
    CAST(UNIX_TIMESTAMP(start_at) * 1000 AS BIGINT) AS start_time,
    CAST(
      COALESCE(
        UNIX_TIMESTAMP(end_at),
        UNIX_TIMESTAMP(${end_date}, 'yyyy-MM-dd')
      ) * 1000 AS BIGINT
    ) AS end_time
  FROM compliancedb_shards.driver_hos_vehicle_assignments
  WHERE start_at < ${end_date}
    AND (end_at IS NULL OR end_at >= ${start_date})
    AND org_id NOT IN (SELECT * FROM helpers.ignored_org_ids)
),

operating_authority_updates AS (
  SELECT *
  FROM (
    SELECT
      cu.id,
      cu.date,
      cu.org_id,
      COALESCE(va.vehicle_id, 0) AS vehicle_id,
      cu.log_at,
      cu.driver_id,
      cu.operating_authority,
      ROW_NUMBER() OVER (PARTITION BY cu.id ORDER BY va.start_time DESC, va.end_time DESC) AS rn
    FROM carrier_updates cu
    LEFT JOIN vehicle_assignments va
      ON  cu.org_id    = va.org_id
      AND cu.driver_id = va.driver_id
      AND cu.log_at_ms >= va.start_time
      AND cu.log_at_ms <  va.end_time
  ) WHERE rn = 1
),

-- It's a rare occurrence to have dupes, but ensure that we have
-- distinct rows for our primary key. This can happen since the
-- driver_hos_logs table is keyed on (org_id, id).
distinct_rows AS (
  SELECT
    date,
    org_id,
    vehicle_id,
    log_at,
    MIN_BY(driver_id, id)           AS driver_id,
    MIN_BY(operating_authority, id) AS operating_authority
  FROM operating_authority_updates
  GROUP BY date, org_id, vehicle_id, log_at
)

SELECT *, COALESCE(TO_TIMESTAMP(${pipeline_execution_time}),CURRENT_TIMESTAMP()) as updated_at FROM distinct_rows
