WITH hos_violations AS (
  -- For violations that are still ongoing, take current time for end_at
  SELECT
    hos.org_id,
    hos.driver_id,
    start_at,
    COALESCE(end_at, current_timestamp()) as end_at,
    DATE(start_at) AS start_date,
    COALESCE(DATE(end_at), DATE(current_timestamp())) AS end_date,
    type
  FROM eldhosdb_shards.driver_hos_violations hos
  JOIN datamodel_platform.dim_drivers dd -- Restrict to only eligible drivers
    ON hos.org_id = dd.org_id
    AND hos.driver_id = dd.driver_id
  WHERE
    hos.deleted_at = TIMESTAMP '+0101'
    AND NOT dd.is_deleted
    AND dd.eld_exempt = 0
    AND dd.date = (SELECT MAX(date) FROM datamodel_platform.dim_drivers)
    AND type in (
    -- allowlisted types from https://samsaur.us/hos-allowed-violation-types
        1, -- SHIFT_HOURS
        2, -- SHIFT_DRIVING_HOURS
        100, -- RESTBREAK_MISSED
        101, -- CALIFORNIA_MEALBREAK_MISSED
        200, -- CYCLE_HOURS_ON
        201, -- CYCLE_OFF_HOURS_AFTER_ON_DUTY_HOURS
        1000, -- SHIFT_ON_DUTY_HOURS
        1001, -- DAILY_ON_DUTY_HOURS
        1002, -- DAILY_DRIVING_HOURS
        1009 -- MANDATORY_24_HOURS_OFF_DUTY
    )
),
ignored_violations AS (
  SELECT
    hos.org_id,
    hos.driver_id,
    start_at,
    COALESCE(end_at, current_timestamp()) as end_at,
    DATE(start_at) AS start_date,
    COALESCE(DATE(end_at), DATE(current_timestamp())) AS end_date,
    type
  FROM eldhosdb_shards.driver_hos_ignored_violations hos
  JOIN datamodel_platform.dim_drivers dd
    ON hos.org_id = dd.org_id
  AND hos.driver_id = dd.driver_id
  WHERE
    hos.deleted_at = TIMESTAMP '+0101'
    AND NOT dd.is_deleted
    AND dd.eld_exempt = 0
    AND dd.date = (SELECT MAX(date) FROM datamodel_platform.dim_drivers)
    AND type in (
    -- allowlisted types from https://samsaur.us/hos-allowed-violation-types
        1, -- SHIFT_HOURS
        2, -- SHIFT_DRIVING_HOURS
        100, -- RESTBREAK_MISSED
        101, -- CALIFORNIA_MEALBREAK_MISSED
        200, -- CYCLE_HOURS_ON
        201, -- CYCLE_OFF_HOURS_AFTER_ON_DUTY_HOURS
        1000, -- SHIFT_ON_DUTY_HOURS
        1001, -- DAILY_ON_DUTY_HOURS
        1002, -- DAILY_DRIVING_HOURS
        1009 -- MANDATORY_24_HOURS_OFF_DUTY
    )
),
exploded_violations AS (
-- Explode violations into daily buckets
  SELECT
    org_id,
    driver_id,
    type,
    date_series AS date,
    CASE
      WHEN DATE(start_at) = date_series THEN start_at
      ELSE CAST(date_series AS TIMESTAMP)
    END AS adjusted_start,
    CASE
      WHEN DATE(end_at) = date_series THEN end_at
      ELSE CAST(date_series + INTERVAL 1 DAY AS TIMESTAMP) - INTERVAL 1 MILLISECOND -- Go until end of day
    END AS adjusted_end
  FROM hos_violations
  LATERAL VIEW EXPLODE(SEQUENCE(DATE(start_at), DATE(end_at), INTERVAL 1 DAY)) AS date_series
),
exploded_ignored_violations AS (
    SELECT
      org_id,
      driver_id,
      type,
      date_series AS date,
      CASE
        WHEN DATE(start_at) = date_series THEN start_at
        ELSE CAST(date_series AS TIMESTAMP)
      END AS adjusted_start,
      CASE
        WHEN DATE(end_at) = date_series THEN end_at
        ELSE CAST(date_series + INTERVAL 1 DAY AS TIMESTAMP) - INTERVAL 1 MILLISECOND -- Go until end of day
      END AS adjusted_end
    FROM ignored_violations
    LATERAL VIEW EXPLODE(SEQUENCE(DATE(start_at), DATE(end_at), INTERVAL 1 DAY)) AS date_series
),
split_violations AS (
  -- Split violations into multiple segments if they overlap with ignored violations (for the same driver/type)
  SELECT
    v.org_id,
    v.driver_id,
    v.date,
    -- First segment: start when violation starts, end when ignore begins
    CASE
      WHEN v.adjusted_start < i.adjusted_start AND v.adjusted_end > i.adjusted_start
      THEN v.adjusted_start
      ELSE NULL
    END AS first_segment_start,
    CASE
      WHEN v.adjusted_start < i.adjusted_start AND v.adjusted_end > i.adjusted_start
      THEN i.adjusted_start
      ELSE NULL
    END AS first_segment_end,
    -- Second segment: start when ignored ends, end when violation ends
    CASE
      WHEN v.adjusted_start < i.adjusted_end AND v.adjusted_end > i.adjusted_end
      THEN i.adjusted_end
      ELSE NULL
    END AS second_segment_start,
    CASE
      WHEN v.adjusted_start < i.adjusted_end AND v.adjusted_end > i.adjusted_end
      THEN v.adjusted_end
      ELSE NULL
    END AS second_segment_end,
    -- No overlap at all, take original violation
    CASE
      WHEN (i.adjusted_start IS NULL AND i.adjusted_end IS NULL) OR (v.adjusted_end <= i.adjusted_start OR v.adjusted_start >= i.adjusted_end)
      THEN v.adjusted_start
      ELSE NULL
    END AS no_overlap_start,
    CASE
      WHEN (i.adjusted_start IS NULL AND i.adjusted_end IS NULL) OR (v.adjusted_end <= i.adjusted_start OR v.adjusted_start >= i.adjusted_end)
      THEN v.adjusted_end
      ELSE NULL
    END AS no_overlap_end
  FROM exploded_violations v
  LEFT JOIN exploded_ignored_violations i
    ON v.org_id = i.org_id
    AND v.driver_id = i.driver_id
    AND v.date = i.date
    AND v.type = i.type
),
flattened_split_violations AS (
  -- Merge all the splits into a common structure
  SELECT org_id, driver_id, date, first_segment_start AS adjusted_start, first_segment_end AS adjusted_end
  FROM split_violations WHERE first_segment_start IS NOT NULL

  UNION ALL

  SELECT org_id, driver_id, date, second_segment_start AS adjusted_start, second_segment_end AS adjusted_end
  FROM split_violations WHERE second_segment_start IS NOT NULL

  UNION ALL

  SELECT org_id, driver_id, date, no_overlap_start AS adjusted_start, no_overlap_end AS adjusted_end
  FROM split_violations WHERE no_overlap_start IS NOT NULL
),
assign_group AS (
  -- Assign a unique group ID to overlapping records
  SELECT
    org_id,
    driver_id,
    date,
    adjusted_start,
    adjusted_end,
    SUM(
        CASE
          WHEN adjusted_start > LAG(adjusted_end) OVER (
            PARTITION BY org_id, driver_id, date
            ORDER BY adjusted_start
          ) THEN 1
          ELSE 0
        END
    ) OVER (
        PARTITION BY org_id, driver_id, date
        ORDER BY adjusted_start
    ) AS group_id
  FROM flattened_split_violations
),
merged_violations AS (
  -- Merge overlapping violations within each assigned group
  SELECT
    org_id,
    driver_id,
    date,
    MIN(adjusted_start) AS merged_start,
    MAX(adjusted_end) AS merged_end
  FROM assign_group
  GROUP BY org_id, driver_id, date, group_id
),
final_daily_totals AS (
  -- Compute final daily violation time per driver
  -- Cap at 24 hours per driver per day to handle edge cases where violations might exceed a day
  SELECT
    org_id,
    driver_id,
    date,
    LEAST(
      SUM(TIMESTAMPDIFF(MILLISECOND, merged_start, merged_end)),
      24 * 60 * 60 * 1000  -- 24 hours in milliseconds
    ) AS total_violation_time_ms
  FROM merged_violations
  GROUP BY org_id, driver_id, date
),
base AS (
  -- Aggregate all durations for an org per day
  SELECT
    date,
    org_id,
    SUM(total_violation_time_ms) AS violation_duration_ms
  FROM final_daily_totals
  GROUP BY date, org_id
),
total_drivers AS (
-- Use latest date for all eligible drivers
  SELECT
    org_id,
    COUNT(driver_id) as count_drivers
  FROM datamodel_platform.dim_drivers
  WHERE
    NOT is_deleted
    AND eld_exempt = 0
    AND date = (SELECT MAX(date) FROM datamodel_platform.dim_drivers)
  GROUP BY 1
)
SELECT
  /*+ BROADCAST(o) */
  v.date,
  v.org_id,
  sam_map.sam_number,
  o.internal_type,
  o.account_size_segment,
  o.account_industry,
  o.account_arr_segment,
  o.is_paid_customer,
  o.is_paid_safety_customer,
  o.is_paid_stce_customer,
  o.is_paid_telematics_customer,
  violation_duration_ms,
  COALESCE(count_drivers, 0) * 24 * 60 * 60 * 1000 AS possible_driver_ms,
  c.region AS subregion,
  c.avg_mileage,
  c.fleet_size,
  c.industry_vertical,
  c.fuel_category,
  c.primary_driving_environment,
  c.fleet_composition
FROM base v
LEFT JOIN total_drivers d
  ON v.org_id = d.org_id
JOIN datamodel_core.dim_organizations o
  ON v.date = o.date
  AND v.org_id = o.org_id
LEFT JOIN product_analytics.map_org_sam_number_latest sam_map
  ON sam_map.org_id = o.org_id
LEFT OUTER JOIN product_analytics_staging.stg_organization_categories c
  ON v.org_id = c.org_id
  AND c.date = (SELECT MAX(date) FROM product_analytics_staging.stg_organization_categories)
