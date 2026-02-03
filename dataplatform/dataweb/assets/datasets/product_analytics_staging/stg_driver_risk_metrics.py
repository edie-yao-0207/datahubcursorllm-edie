import math

from dagster import AssetExecutionContext, DailyPartitionsDefinition
from dataweb import (
    NonEmptyDQCheck,
    NonNullDQCheck,
    PrimaryKeyDQCheck,
    table,
)
from dataweb.userpkgs.constants import (
    DATAENGINEERING,
    FRESHNESS_SLO_9AM_PST,
    MEMORY_OPTIMIZED_INSTANCE_POOL_KEY,
    AWSRegion,
    ColumnDescription,
    Database,
    InstanceType,
    TableType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.query import create_run_config_overrides
from dataweb.userpkgs.utils import (
    build_table_description,
    partition_key_ranges_from_context,
)
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

PRIMARY_KEYS = ["date", "metric"]

SCHEMA = [
    {
        "name": "date",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": ColumnDescription.DATE,
        },
    },
    {
        "name": "metric",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Name of the metric being tracked",
        },
    },
    {
        "name": "value",
        "type": {
            "type": "map",
            "keyType": "string",
            "valueType": "double",
            "valueContainsNull": True,
        },
        "nullable": False,
        "metadata": {
            "comment": "Map of metric-specific key-value pairs where values are numeric",
        },
    },
    {
        "name": "created_at",
        "type": "timestamp",
        "nullable": False,
        "metadata": {
            "comment": "Timestamp when the row was created",
        },
    },
]

QUERY = """
WITH feature_values AS (
  SELECT
    CAST(date AS STRING) AS date,
    org_id,
    driver_id,
    risk_score,
    classification,
    CAST(get_json_object(trip_duration_weighted, '$.value') AS DOUBLE) AS trip_duration_weighted,
    CAST(get_json_object(n_org_vg_devices, '$.value') AS DOUBLE) AS n_org_vg_devices,
    -- over_speed_limit_mins_60d,
    CAST(get_json_object(driver_tenure_days, '$.value') AS DOUBLE) AS driver_tenure_days,
    CAST(get_json_object(distracted_driving_60d, '$.value') AS DOUBLE) AS distracted_driving_60d,
    CAST(get_json_object(trip_count_below_freezing_60d, '$.value') AS DOUBLE) AS trip_count_below_freezing_60d,
    CAST(get_json_object(trip_count_over_35_60d, '$.value') AS DOUBLE) AS trip_count_over_35_60d,
    CAST(get_json_object(proportion_urban_driving_environment_locations_60d, '$.value') AS DOUBLE) AS proportion_urban_driving_environment_locations_60d,
    CAST(get_json_object(primary_max_weight_lbs, '$.value') AS DOUBLE) AS primary_max_weight_lbs,
    CAST(get_json_object(hos_violation_count_60d, '$.value') AS DOUBLE) AS hos_violation_count_60d,
    CAST(get_json_object(moderate_speeding_mins_60d, '$.value') AS DOUBLE) AS moderate_speeding_mins_60d,
    CAST(get_json_object(mobile_usage_violations_60d, '$.value') AS DOUBLE) AS mobile_usage_violations_60d,
    CAST(get_json_object(max_speed_weighted, '$.value') AS DOUBLE) AS max_speed_weighted,
    CAST(get_json_object(proportion_rural_driving_environment_locations_60d, '$.value') AS DOUBLE) AS proportion_rural_driving_environment_locations_60d,
    CAST(get_json_object(proportion_roadway_service_60d, '$.value') AS DOUBLE) AS proportion_roadway_service_60d,
    CAST(get_json_object(harsh_braking_60d, '$.value') AS DOUBLE) AS harsh_braking_60d,
    CAST(get_json_object(proportion_mixed_driving_environment_locations_60d, '$.value') AS DOUBLE) AS proportion_mixed_driving_environment_locations_60d,
    CAST(get_json_object(proportion_roadway_residential_60d, '$.value') AS DOUBLE) AS proportion_roadway_residential_60d,
    CAST(get_json_object(light_speeding_mins_60d, '$.value') AS DOUBLE) AS light_speeding_mins_60d,
    CAST(get_json_object(proportion_roadway_motorway_60d, '$.value') AS DOUBLE) AS proportion_roadway_motorway_60d,
    CAST(get_json_object(trip_ended_at_night_weighted, '$.value') AS DOUBLE) AS trip_ended_at_night_weighted,
    CAST(get_json_object(is_eld_relevant_device, '$.value') AS DOUBLE) AS is_eld_relevant_device,
    CAST(get_json_object(trip_count_60d, '$.value') AS DOUBLE) AS trip_count_60d,
    CAST(get_json_object(proportion_roadway_trunk_60d, '$.value') AS DOUBLE) AS proportion_roadway_trunk_60d,
    CAST(get_json_object(proportion_roadway_secondary_60d, '$.value') AS DOUBLE) AS proportion_roadway_secondary_60d,
    CAST(get_json_object(proportion_roadway_null_60d, '$.value') AS DOUBLE) AS proportion_roadway_null_60d,
    CAST(get_json_object(trip_ended_in_morning_60d, '$.value') AS DOUBLE) AS trip_ended_in_morning_60d,
    CAST(get_json_object(severe_speeding_mins_60d, '$.value') AS DOUBLE) AS severe_speeding_mins_60d,
    CAST(get_json_object(coached_events_60d, '$.value') AS DOUBLE) AS coached_events_60d,
    CAST(get_json_object(total_events_weighted, '$.value') AS DOUBLE) AS total_events_weighted,
    CAST(get_json_object(speeding_60d, '$.value') AS DOUBLE) AS speeding_60d,
    CAST(get_json_object(labelled_crash_count_prev_1_year, '$.value') AS DOUBLE) AS labelled_crash_count_prev_1_year,
    CAST(get_json_object(proportion_roadway_tertiary_60d, '$.value') AS DOUBLE) AS proportion_roadway_tertiary_60d,
    CAST(get_json_object(proportion_roadway_unclassified_60d, '$.value') AS DOUBLE) AS proportion_roadway_unclassified_60d,
    CAST(get_json_object(rolling_stop_60d, '$.value') AS DOUBLE) AS rolling_stop_60d,
    CAST(get_json_object(proportion_roadway_primary_60d, '$.value') AS DOUBLE) AS proportion_roadway_primary_60d,
    CAST(get_json_object(drowsiness_60d, '$.value') AS DOUBLE) AS drowsiness_60d,
    CAST(get_json_object(harsh_turns_60d, '$.value') AS DOUBLE) AS harsh_turns_60d,
    CAST(get_json_object(proportion_roadway_living_street_60d, '$.value') AS DOUBLE) AS proportion_roadway_living_street_60d,
    CAST(get_json_object(following_distance_60d, '$.value') AS DOUBLE) AS following_distance_60d,
    CAST(get_json_object(speeding_ratio_weighted, '$.value') AS DOUBLE) AS speeding_ratio_weighted,
    CAST(get_json_object(heavy_speeding_mins_60d, '$.value') AS DOUBLE) AS heavy_speeding_mins_60d,
    CAST(get_json_object(crashes_60d, '$.value') AS DOUBLE) AS crashes_60d,
    CAST(get_json_object(trip_distance_miles_60d, '$.value') AS DOUBLE) AS trip_distance_miles_60d,
    CAST(get_json_object(lane_departure_60d, '$.value') AS DOUBLE) AS lane_departure_60d,
    CAST(get_json_object(virtual_coaching_60d, '$.value') AS DOUBLE) AS virtual_coaching_60d,
    CAST(get_json_object(harsh_accelerations_60d, '$.value') AS DOUBLE) AS harsh_accelerations_60d
  FROM product_analytics.driver_risk_classification
  WHERE date BETWEEN DATEADD(DAY, -40, '{PARTITION_START}') AND '{PARTITION_START}'
),

scores AS (
  SELECT
    CAST(date AS STRING) AS date,
    timestamp,
    org_id,
    driver_id,
    process_uuid,
    risk_score,
    classification
  FROM product_analytics.driver_risk_classification
  WHERE date BETWEEN DATEADD(DAY, -40, '{PARTITION_START}') AND '{PARTITION_START}'
),

date_spine AS (
  SELECT '{PARTITION_START}' AS date
),

-- empty map<string, double> for COALESCE fallback when LEFT JOIN has no row
empty_map AS (
  SELECT map_from_entries(filter(array(named_struct('key', 'x', 'value', 0.0)), 1=0)) AS value
),

-- risk class sizes
class_pcts AS (
  SELECT
    date,
    classification,
    COUNT(*) * 1.0 / SUM(COUNT(*)) OVER (PARTITION BY date) AS pct_in_class
  FROM scores
  WHERE date = '{PARTITION_START}'
  GROUP BY 1, 2
),
metric_risk_class_sizes AS (
  SELECT
    d.date,
    'risk_class_sizes' AS metric,
    COALESCE(m.value, e.value) AS value
  FROM date_spine d
  CROSS JOIN empty_map e
  LEFT JOIN (
    SELECT
      date,
      map_from_entries(
        collect_list(
          named_struct('key', classification, 'value', pct_in_class)
        )
      ) AS value
    FROM class_pcts
    GROUP BY 1
  ) m ON d.date = m.date
),

-- top K lift and crash concentration & number of crashes per bucket
scores_crashes AS (
  SELECT
    s.*,
    c.timestamp AS crash_timestamp,
    COALESCE(c.is_tp, 0) AS is_tp,
    c.crash_types
  FROM scores s
  LEFT JOIN product_analytics_staging.stg_crash_trips c
    ON s.driver_id = c.driver_id
    AND s.org_id = c.org_id
    AND CAST(s.date AS DATE) = c.date
  WHERE CAST(s.date AS DATE) BETWEEN DATEADD(DAY, -40, '{PARTITION_START}') AND DATEADD(DAY, -13, '{PARTITION_START}')
),
crashes_per_class AS (
  SELECT
    classification,
    ROUND(AVG(is_tp), 8) AS avg_crash_rate,
    SUM(is_tp) AS ct_crashes
  FROM scores_crashes
  GROUP BY 1

  UNION ALL

  SELECT
    'overall' AS classification,
    ROUND(AVG(is_tp), 8) AS avg_crash_rate,
    SUM(is_tp) AS ct_crashes
  FROM scores_crashes
),
-- top 10% riskiest drivers per day (by risk_score) for num_crashes_28d and avg_crash_rate_28d
percentile_90_per_date AS (
  SELECT
    date,
    PERCENTILE_APPROX(risk_score, 0.9) AS p90_risk_score
  FROM scores_crashes
  GROUP BY 1
),
top10pct_riskiest_crashes AS (
  SELECT
    'top10pct_riskiest' AS classification,
    ROUND(AVG(sc.is_tp), 8) AS avg_crash_rate,
    SUM(sc.is_tp) AS ct_crashes
  FROM scores_crashes sc
  INNER JOIN percentile_90_per_date p
    ON sc.date = p.date
    AND sc.risk_score >= p.p90_risk_score
),
metric_num_crashes_28d AS (
  SELECT
    -- date partition for dash, but metric based on earlier dates
    '{PARTITION_START}' AS date,
    'num_crashes_28d'                 AS metric,
    map_from_entries(
      collect_list(
        named_struct(
          'key',   classification,
          'value', CAST(ct_crashes AS DOUBLE)
        )
      )
    ) AS value
  FROM (
    SELECT classification, ct_crashes FROM crashes_per_class
    UNION ALL
    SELECT classification, ct_crashes FROM top10pct_riskiest_crashes
  ) combined
),

metric_avg_crash_rate_28d AS (
  SELECT
    -- date partition for dash, but metric based on earlier dates
    '{PARTITION_START}' AS date,
    'avg_crash_rate_28d'                AS metric,
    map_from_entries(
      collect_list(
        named_struct(
          'key',   classification,
          'value', CAST(avg_crash_rate AS DOUBLE)
        )
      )
    ) AS value
  FROM (
    SELECT classification, avg_crash_rate FROM crashes_per_class
    UNION ALL
    SELECT classification, avg_crash_rate FROM top10pct_riskiest_crashes
  ) combined
),

-- median risk score per class
median_risk_scores AS (
  SELECT
    date,
    classification,
    ROUND(MEDIAN(risk_score), 4) AS median_risk_score
  FROM scores
  WHERE date = '{PARTITION_START}'
  GROUP BY 1, 2

  UNION ALL

  SELECT
    date,
    'overall' AS classification,
    ROUND(MEDIAN(risk_score), 4) AS median_risk_score
  FROM scores
  WHERE date = '{PARTITION_START}'
  GROUP BY 1
),
metric_median_risk_scores AS (
  SELECT
    d.date,
    'median_risk_score'                AS metric,
    COALESCE(m.value, e.value) AS value
  FROM date_spine d
  CROSS JOIN empty_map e
  LEFT JOIN (
    SELECT
      date,
      map_from_entries(
        collect_list(
          named_struct(
            'key',   classification,
            'value', CAST(median_risk_score AS DOUBLE)
          )
        )
      ) AS value
    FROM median_risk_scores
    GROUP BY 1
  ) m ON d.date = m.date
),

-- driver risk class changes
prev_day AS (
  SELECT
    driver_id,
    org_id,
    classification AS class_prev
  FROM scores
  WHERE date = DATEADD(DAY, -1, '{PARTITION_START}')
),
next_day AS (
  SELECT
    driver_id,
    org_id,
    classification AS class_next
  FROM scores
  WHERE date = '{PARTITION_START}'
),
transitions AS (
  SELECT
    p.driver_id,
    p.org_id,
    p.class_prev,
    n.class_next
  FROM prev_day p
  INNER JOIN next_day n
    ON p.driver_id = n.driver_id
    AND p.org_id   = n.org_id
),
transition_counts AS (
  SELECT
    class_prev,
    class_next,
    COUNT(DISTINCT driver_id) AS ct_drivers
  FROM transitions
  GROUP BY 1, 2
),
prev_class_totals AS (
  SELECT
    class_prev,
    COUNT(DISTINCT driver_id) AS ct_prev_class_drivers
  FROM transitions
  GROUP BY 1
),
transition_props AS (
  SELECT
    CONCAT(tc.class_prev, '_', tc.class_next) AS transition_key,
    tc.class_prev,
    1.0 * tc.ct_drivers / NULLIF(pct.ct_prev_class_drivers, 0) AS prop_drivers
  FROM transition_counts tc
  INNER JOIN prev_class_totals pct
    ON tc.class_prev = pct.class_prev
),
metric_driver_risk_class_changes AS (
  SELECT
    '{PARTITION_START}' AS date,
    'driver_class_changes'    AS metric,
    map_from_entries(
      collect_list(
        named_struct(
          'key',   transition_key,
          'value', CAST(prop_drivers AS DECIMAL(38,16))
        )
      )
    ) AS value
  FROM transition_props
),

-- expected score range
metric_expected_score_range AS (
  SELECT
    d.date,
    'expected_score_range'        AS metric,
    COALESCE(m.value, e.value) AS value
  FROM date_spine d
  CROSS JOIN empty_map e
  LEFT JOIN (
    SELECT
      date,
      map_from_entries(
        array(
          named_struct(
            'key',   'within_expected_range',
            'value',
            CASE
              WHEN MIN(risk_score) >= 0 AND MAX(risk_score) <= 1 THEN 1.0
              ELSE 0.0
            END
          )
        )
      ) AS value
    FROM scores
    WHERE date = '{PARTITION_START}'
      AND risk_score IS NOT NULL
    GROUP BY 1
  ) m ON d.date = m.date
),

-- extreme risk scores
score_cutoffs AS (
  SELECT
    PERCENTILE_APPROX(risk_score, 0.95) AS p95,
    PERCENTILE_APPROX(risk_score, 0.05) AS p05
  FROM scores
  WHERE date = '{PARTITION_START}'
    AND risk_score IS NOT NULL
),
extreme_avgs AS (
  SELECT
    AVG(CASE WHEN s.risk_score >= c.p95 THEN s.risk_score END) AS avg_top5pct,
    AVG(CASE WHEN s.risk_score <= c.p05 THEN s.risk_score END) AS avg_bottom5pct
  FROM scores s
  CROSS JOIN score_cutoffs c
  WHERE s.date = '{PARTITION_START}'
    AND s.risk_score IS NOT NULL
),
metric_avg_risk_scores_extremes AS (
  SELECT
    '{PARTITION_START}' AS date,
    'avg_risk_scores_extremes'       AS metric,
    map_from_entries(
      array(
        named_struct(
          'key',   'top5_pct',
          'value', CAST(avg_top5pct AS DECIMAL(38,16))
        ),
        named_struct(
          'key',   'bottom5_pct',
          'value', CAST(avg_bottom5pct AS DECIMAL(38,16))
        )
      )
    ) AS value
  FROM extreme_avgs
),

-- e2e pipeline health metric
drivers_with_trips_prevday AS (
  SELECT DISTINCT
    last_trip_date,
    driver_id
  FROM feature_store.driver_risk_insights_features
  WHERE last_trip_date = DATEADD(DAY,-1, '{PARTITION_START}')
    AND last_trip_date = date
),
drivers_with_new_scores AS (
  SELECT DISTINCT
    date,
    driver_id
  FROM product_analytics.driver_risk_classification
  WHERE date = '{PARTITION_START}'
),
pipeline_counts AS (
  SELECT
    dt.last_trip_date,
    COUNT(DISTINCT dt.driver_id) AS ct_drivers_with_trips_prevday,
    COUNT(DISTINCT ds.driver_id) AS ct_drivers_with_scores_nextday,
    1.0000 * COUNT(DISTINCT ds.driver_id) / NULLIF(COUNT(DISTINCT dt.driver_id), 0) AS execution_success_rate
  FROM drivers_with_trips_prevday dt
  LEFT JOIN drivers_with_new_scores ds
    ON dt.last_trip_date = DATEADD(DAY, -1, ds.date)
   AND dt.driver_id = ds.driver_id
  GROUP BY 1
),
metric_e2e_pipeline_health AS (
  SELECT
    '{PARTITION_START}'                        AS date,
    'e2e_pipeline_health'          AS metric,
    map_from_entries(
      array(
        named_struct(
          'key', 'ct_drivers_with_trips_prevday',
          'value', CAST(ct_drivers_with_trips_prevday AS DOUBLE)
        ),
        named_struct(
          'key', 'ct_drivers_with_scores_nextday',
          'value', CAST(ct_drivers_with_scores_nextday AS DOUBLE)
        ),
        named_struct(
          'key', 'execution_success_rate',
          'value', CAST(execution_success_rate AS DOUBLE)
        )
      )
    ) AS value
  FROM pipeline_counts
),

-- feature data health
agg AS (
  SELECT
      COUNT(*) AS total_rows,

      -- trip_duration_weighted: >= 0
      SUM(CASE WHEN trip_duration_weighted IS NULL THEN 1 ELSE 0 END) AS missing_trip_duration_weighted,
      SUM(CASE WHEN trip_duration_weighted IS NOT NULL AND trip_duration_weighted < 0 THEN 1 ELSE 0 END) AS oor_trip_duration_weighted,

      -- n_org_vg_devices: > 0
      SUM(CASE WHEN n_org_vg_devices IS NULL THEN 1 ELSE 0 END) AS missing_n_org_vg_devices,
      SUM(CASE WHEN n_org_vg_devices IS NOT NULL AND n_org_vg_devices <= 0 THEN 1 ELSE 0 END) AS oor_n_org_vg_devices,

      -- driver_tenure_days: >= 0
      SUM(CASE WHEN driver_tenure_days IS NULL THEN 1 ELSE 0 END) AS missing_driver_tenure_days,
      SUM(CASE WHEN driver_tenure_days IS NOT NULL AND driver_tenure_days < 0 THEN 1 ELSE 0 END) AS oor_driver_tenure_days,

      -- distracted_driving_60d: -1 or >= 0
      SUM(CASE WHEN distracted_driving_60d IS NULL THEN 1 ELSE 0 END) AS missing_distracted_driving_60d,
      SUM(
          CASE
              WHEN distracted_driving_60d IS NOT NULL
                    AND distracted_driving_60d < 0
                    AND distracted_driving_60d <> -1
              THEN 1 ELSE 0
          END
      ) AS oor_distracted_driving_60d,

      -- trip_count_below_freezing_60d: >= 0
      SUM(CASE WHEN trip_count_below_freezing_60d IS NULL THEN 1 ELSE 0 END) AS missing_trip_count_below_freezing_60d,
      SUM(CASE WHEN trip_count_below_freezing_60d IS NOT NULL AND trip_count_below_freezing_60d < 0 THEN 1 ELSE 0 END) AS oor_trip_count_below_freezing_60d,

      -- trip_count_over_35_60d: >= 0
      SUM(CASE WHEN trip_count_over_35_60d IS NULL THEN 1 ELSE 0 END) AS missing_trip_count_over_35_60d,
      SUM(CASE WHEN trip_count_over_35_60d IS NOT NULL AND trip_count_over_35_60d < 0 THEN 1 ELSE 0 END) AS oor_trip_count_over_35_60d,

      -- proportion_urban_driving_environment_locations_60d: [0, 1]
      SUM(CASE WHEN proportion_urban_driving_environment_locations_60d IS NULL THEN 1 ELSE 0 END) AS missing_proportion_urban_driving_environment_locations_60d,
      SUM(
          CASE
              WHEN proportion_urban_driving_environment_locations_60d IS NOT NULL
                    AND (proportion_urban_driving_environment_locations_60d < 0
                        OR proportion_urban_driving_environment_locations_60d > 1)
              THEN 1 ELSE 0
          END
      ) AS oor_proportion_urban_driving_environment_locations_60d,

      -- primary_max_weight_lbs: > 0 (but you treat -1 specially elsewhere; keeping your logic as-is)
      SUM(CASE WHEN primary_max_weight_lbs IS NULL THEN 1 ELSE 0 END) AS missing_primary_max_weight_lbs,
      SUM(CASE 
              WHEN primary_max_weight_lbs IS NOT NULL 
                  AND primary_max_weight_lbs < 0
                  AND primary_max_weight_lbs <> -1
              THEN 1 ELSE 0
          END
      ) AS oor_primary_max_weight_lbs,

      -- hos_violation_count_60d: -1 or >= 0
      SUM(CASE WHEN hos_violation_count_60d IS NULL THEN 1 ELSE 0 END) AS missing_hos_violation_count_60d,
      SUM(
          CASE
              WHEN hos_violation_count_60d IS NOT NULL
                    AND hos_violation_count_60d < 0
                    AND hos_violation_count_60d <> -1
              THEN 1 ELSE 0
          END
      ) AS oor_hos_violation_count_60d,

      -- moderate_speeding_mins_60d: >= 0
      SUM(CASE WHEN moderate_speeding_mins_60d IS NULL THEN 1 ELSE 0 END) AS missing_moderate_speeding_mins_60d,
      SUM(CASE WHEN moderate_speeding_mins_60d IS NOT NULL AND moderate_speeding_mins_60d < 0 THEN 1 ELSE 0 END) AS oor_moderate_speeding_mins_60d,

      -- mobile_usage_violations_60d: -1 or >= 0
      SUM(CASE WHEN mobile_usage_violations_60d IS NULL THEN 1 ELSE 0 END) AS missing_mobile_usage_violations_60d,
      SUM(
          CASE
              WHEN mobile_usage_violations_60d IS NOT NULL
                    AND mobile_usage_violations_60d < 0
                    AND mobile_usage_violations_60d <> -1
              THEN 1 ELSE 0
          END
      ) AS oor_mobile_usage_violations_60d,

      -- max_speed_weighted: >= 0
      SUM(CASE WHEN max_speed_weighted IS NULL THEN 1 ELSE 0 END) AS missing_max_speed_weighted,
      SUM(CASE WHEN max_speed_weighted IS NOT NULL AND max_speed_weighted < 0 THEN 1 ELSE 0 END) AS oor_max_speed_weighted,

      -- proportion_rural_driving_environment_locations_60d: [0, 1]
      SUM(CASE WHEN proportion_rural_driving_environment_locations_60d IS NULL THEN 1 ELSE 0 END) AS missing_proportion_rural_driving_environment_locations_60d,
      SUM(
          CASE
              WHEN proportion_rural_driving_environment_locations_60d IS NOT NULL
                    AND (proportion_rural_driving_environment_locations_60d < 0
                        OR proportion_rural_driving_environment_locations_60d > 1)
              THEN 1 ELSE 0
          END
      ) AS oor_proportion_rural_driving_environment_locations_60d,

      -- proportion_roadway_service_60d: [0, 1]
      SUM(CASE WHEN proportion_roadway_service_60d IS NULL THEN 1 ELSE 0 END) AS missing_proportion_roadway_service_60d,
      SUM(
          CASE
              WHEN proportion_roadway_service_60d IS NOT NULL
                    AND (proportion_roadway_service_60d < 0
                        OR proportion_roadway_service_60d > 1)
              THEN 1 ELSE 0
          END
      ) AS oor_proportion_roadway_service_60d,

      -- harsh_braking_60d: >= 0
      SUM(CASE WHEN harsh_braking_60d IS NULL THEN 1 ELSE 0 END) AS missing_harsh_braking_60d,
      SUM(CASE WHEN harsh_braking_60d IS NOT NULL AND harsh_braking_60d < 0 THEN 1 ELSE 0 END) AS oor_harsh_braking_60d,

      -- proportion_mixed_driving_environment_locations_60d: [0, 1]
      SUM(CASE WHEN proportion_mixed_driving_environment_locations_60d IS NULL THEN 1 ELSE 0 END) AS missing_proportion_mixed_driving_environment_locations_60d,
      SUM(
          CASE
              WHEN proportion_mixed_driving_environment_locations_60d IS NOT NULL
                    AND (proportion_mixed_driving_environment_locations_60d < 0
                        OR proportion_mixed_driving_environment_locations_60d > 1)
              THEN 1 ELSE 0
          END
      ) AS oor_proportion_mixed_driving_environment_locations_60d,

      -- proportion_roadway_residential_60d: [0, 1]
      SUM(CASE WHEN proportion_roadway_residential_60d IS NULL THEN 1 ELSE 0 END) AS missing_proportion_roadway_residential_60d,
      SUM(
          CASE
              WHEN proportion_roadway_residential_60d IS NOT NULL
                    AND (proportion_roadway_residential_60d < 0
                        OR proportion_roadway_residential_60d > 1)
              THEN 1 ELSE 0
          END
      ) AS oor_proportion_roadway_residential_60d,

      -- light_speeding_mins_60d: >= 0
      SUM(CASE WHEN light_speeding_mins_60d IS NULL THEN 1 ELSE 0 END) AS missing_light_speeding_mins_60d,
      SUM(CASE WHEN light_speeding_mins_60d IS NOT NULL AND light_speeding_mins_60d < 0 THEN 1 ELSE 0 END) AS oor_light_speeding_mins_60d,

      -- proportion_roadway_motorway_60d: [0, 1]
      SUM(CASE WHEN proportion_roadway_motorway_60d IS NULL THEN 1 ELSE 0 END) AS missing_proportion_roadway_motorway_60d,
      SUM(
          CASE
              WHEN proportion_roadway_motorway_60d IS NOT NULL
                    AND (proportion_roadway_motorway_60d < 0
                        OR proportion_roadway_motorway_60d > 1)
              THEN 1 ELSE 0
          END
      ) AS oor_proportion_roadway_motorway_60d,

      -- trip_ended_at_night_weighted: >= 0
      SUM(CASE WHEN trip_ended_at_night_weighted IS NULL THEN 1 ELSE 0 END) AS missing_trip_ended_at_night_weighted,
      SUM(CASE WHEN trip_ended_at_night_weighted IS NOT NULL AND trip_ended_at_night_weighted < 0 THEN 1 ELSE 0 END) AS oor_trip_ended_at_night_weighted,

      -- is_eld_relevant_device: 0 or 1
      SUM(CASE WHEN is_eld_relevant_device IS NULL THEN 1 ELSE 0 END) AS missing_is_eld_relevant_device,
      SUM(
          CASE
              WHEN is_eld_relevant_device IS NOT NULL
                    AND is_eld_relevant_device NOT IN (0.0, 1.0)
              THEN 1 ELSE 0
          END
      ) AS oor_is_eld_relevant_device,

      -- trip_count_60d: >= 0
      SUM(CASE WHEN trip_count_60d IS NULL THEN 1 ELSE 0 END) AS missing_trip_count_60d,
      SUM(CASE WHEN trip_count_60d IS NOT NULL AND trip_count_60d < 0 THEN 1 ELSE 0 END) AS oor_trip_count_60d,

      -- proportion_roadway_trunk_60d: [0, 1]
      SUM(CASE WHEN proportion_roadway_trunk_60d IS NULL THEN 1 ELSE 0 END) AS missing_proportion_roadway_trunk_60d,
      SUM(
          CASE
              WHEN proportion_roadway_trunk_60d IS NOT NULL
                    AND (proportion_roadway_trunk_60d < 0
                        OR proportion_roadway_trunk_60d > 1)
              THEN 1 ELSE 0
          END
      ) AS oor_proportion_roadway_trunk_60d,

      -- proportion_roadway_secondary_60d: [0, 1]
      SUM(CASE WHEN proportion_roadway_secondary_60d IS NULL THEN 1 ELSE 0 END) AS missing_proportion_roadway_secondary_60d,
      SUM(
          CASE
              WHEN proportion_roadway_secondary_60d IS NOT NULL
                    AND (proportion_roadway_secondary_60d < 0
                        OR proportion_roadway_secondary_60d > 1)
              THEN 1 ELSE 0
          END
      ) AS oor_proportion_roadway_secondary_60d,

      -- proportion_roadway_null_60d: [0, 1]
      SUM(CASE WHEN proportion_roadway_null_60d IS NULL THEN 1 ELSE 0 END) AS missing_proportion_roadway_null_60d,
      SUM(
          CASE
              WHEN proportion_roadway_null_60d IS NOT NULL
                    AND (proportion_roadway_null_60d < 0
                        OR proportion_roadway_null_60d > 1)
              THEN 1 ELSE 0
          END
      ) AS oor_proportion_roadway_null_60d,

      -- trip_ended_in_morning_60d: >= 0
      SUM(CASE WHEN trip_ended_in_morning_60d IS NULL THEN 1 ELSE 0 END) AS missing_trip_ended_in_morning_60d,
      SUM(CASE WHEN trip_ended_in_morning_60d IS NOT NULL AND trip_ended_in_morning_60d < 0 THEN 1 ELSE 0 END) AS oor_trip_ended_in_morning_60d,

      -- severe_speeding_mins_60d: >= 0
      SUM(CASE WHEN severe_speeding_mins_60d IS NULL THEN 1 ELSE 0 END) AS missing_severe_speeding_mins_60d,
      SUM(CASE WHEN severe_speeding_mins_60d IS NOT NULL AND severe_speeding_mins_60d < 0 THEN 1 ELSE 0 END) AS oor_severe_speeding_mins_60d,

      -- coached_events_60d: -1 or >= 0
      SUM(CASE WHEN coached_events_60d IS NULL THEN 1 ELSE 0 END) AS missing_coached_events_60d,
      SUM(
          CASE
              WHEN coached_events_60d IS NOT NULL
                    AND coached_events_60d < 0
                    AND coached_events_60d <> -1
              THEN 1 ELSE 0
          END
      ) AS oor_coached_events_60d,

      -- total_events_weighted: >= 0
      SUM(CASE WHEN total_events_weighted IS NULL THEN 1 ELSE 0 END) AS missing_total_events_weighted,
      SUM(CASE WHEN total_events_weighted IS NOT NULL AND total_events_weighted < 0 THEN 1 ELSE 0 END) AS oor_total_events_weighted,

      -- speeding_60d: >= 0
      SUM(CASE WHEN speeding_60d IS NULL THEN 1 ELSE 0 END) AS missing_speeding_60d,
      SUM(CASE WHEN speeding_60d IS NOT NULL AND speeding_60d < 0 THEN 1 ELSE 0 END) AS oor_speeding_60d,

      -- labelled_crash_count_prev_1_year: >= 0
      SUM(CASE WHEN labelled_crash_count_prev_1_year IS NULL THEN 1 ELSE 0 END) AS missing_labelled_crash_count_prev_1_year,
      SUM(CASE WHEN labelled_crash_count_prev_1_year IS NOT NULL AND labelled_crash_count_prev_1_year < 0 THEN 1 ELSE 0 END) AS oor_labelled_crash_count_prev_1_year,

      -- proportion_roadway_tertiary_60d: [0, 1]
      SUM(CASE WHEN proportion_roadway_tertiary_60d IS NULL THEN 1 ELSE 0 END) AS missing_proportion_roadway_tertiary_60d,
      SUM(
          CASE
              WHEN proportion_roadway_tertiary_60d IS NOT NULL
                    AND (proportion_roadway_tertiary_60d < 0
                        OR proportion_roadway_tertiary_60d > 1)
              THEN 1 ELSE 0
          END
      ) AS oor_proportion_roadway_tertiary_60d,

      -- proportion_roadway_unclassified_60d: [0, 1]
      SUM(CASE WHEN proportion_roadway_unclassified_60d IS NULL THEN 1 ELSE 0 END) AS missing_proportion_roadway_unclassified_60d,
      SUM(
          CASE
              WHEN proportion_roadway_unclassified_60d IS NOT NULL
                    AND (proportion_roadway_unclassified_60d < 0
                        OR proportion_roadway_unclassified_60d > 1)
              THEN 1 ELSE 0
          END
      ) AS oor_proportion_roadway_unclassified_60d,

      -- rolling_stop_60d: -1 or >= 0
      SUM(CASE WHEN rolling_stop_60d IS NULL THEN 1 ELSE 0 END) AS missing_rolling_stop_60d,
      SUM(
          CASE
              WHEN rolling_stop_60d IS NOT NULL
                    AND rolling_stop_60d < 0
                    AND rolling_stop_60d <> -1
              THEN 1 ELSE 0
          END
      ) AS oor_rolling_stop_60d,

      -- proportion_roadway_primary_60d: [0, 1]
      SUM(CASE WHEN proportion_roadway_primary_60d IS NULL THEN 1 ELSE 0 END) AS missing_proportion_roadway_primary_60d,
      SUM(
          CASE
              WHEN proportion_roadway_primary_60d IS NOT NULL
                    AND (proportion_roadway_primary_60d < 0
                        OR proportion_roadway_primary_60d > 1)
              THEN 1 ELSE 0
          END
      ) AS oor_proportion_roadway_primary_60d,

      -- drowsiness_60d: -1 or >= 0
      SUM(CASE WHEN drowsiness_60d IS NULL THEN 1 ELSE 0 END) AS missing_drowsiness_60d,
      SUM(
          CASE
              WHEN drowsiness_60d IS NOT NULL
                    AND drowsiness_60d < 0
                    AND drowsiness_60d <> -1
              THEN 1 ELSE 0
          END
      ) AS oor_drowsiness_60d,

      -- harsh_turns_60d: >= 0
      SUM(CASE WHEN harsh_turns_60d IS NULL THEN 1 ELSE 0 END) AS missing_harsh_turns_60d,
      SUM(CASE WHEN harsh_turns_60d IS NOT NULL AND harsh_turns_60d < 0 THEN 1 ELSE 0 END) AS oor_harsh_turns_60d,

      -- proportion_roadway_living_street_60d: [0, 1]
      SUM(CASE WHEN proportion_roadway_living_street_60d IS NULL THEN 1 ELSE 0 END) AS missing_proportion_roadway_living_street_60d,
      SUM(
          CASE
              WHEN proportion_roadway_living_street_60d IS NOT NULL
                    AND (proportion_roadway_living_street_60d < 0
                        OR proportion_roadway_living_street_60d > 1)
              THEN 1 ELSE 0
          END
      ) AS oor_proportion_roadway_living_street_60d,

      -- following_distance_60d: -1 or >= 0
      SUM(CASE WHEN following_distance_60d IS NULL THEN 1 ELSE 0 END) AS missing_following_distance_60d,
      SUM(
          CASE
              WHEN following_distance_60d IS NOT NULL
                    AND following_distance_60d < 0
                    AND following_distance_60d <> -1
              THEN 1 ELSE 0
          END
      ) AS oor_following_distance_60d,

      -- speeding_ratio_weighted: [0, 1.75]
      SUM(CASE WHEN speeding_ratio_weighted IS NULL THEN 1 ELSE 0 END) AS missing_speeding_ratio_weighted,
      SUM(
          CASE
              WHEN speeding_ratio_weighted IS NOT NULL
                    AND (speeding_ratio_weighted < 0
                        OR speeding_ratio_weighted > 1.75)
              THEN 1 ELSE 0
          END
      ) AS oor_speeding_ratio_weighted,

      -- heavy_speeding_mins_60d: >= 0
      SUM(CASE WHEN heavy_speeding_mins_60d IS NULL THEN 1 ELSE 0 END) AS missing_heavy_speeding_mins_60d,
      SUM(CASE WHEN heavy_speeding_mins_60d IS NOT NULL AND heavy_speeding_mins_60d < 0 THEN 1 ELSE 0 END) AS oor_heavy_speeding_mins_60d,

      -- crashes_60d: >= 0
      SUM(CASE WHEN crashes_60d IS NULL THEN 1 ELSE 0 END) AS missing_crashes_60d,
      SUM(CASE WHEN crashes_60d IS NOT NULL AND crashes_60d < 0 THEN 1 ELSE 0 END) AS oor_crashes_60d,

      -- trip_distance_miles_60d: >= 0
      SUM(CASE WHEN trip_distance_miles_60d IS NULL THEN 1 ELSE 0 END) AS missing_trip_distance_miles_60d,
      SUM(CASE WHEN trip_distance_miles_60d IS NOT NULL AND trip_distance_miles_60d < 0 THEN 1 ELSE 0 END) AS oor_trip_distance_miles_60d,

      -- lane_departure_60d: >= 0
      SUM(CASE WHEN lane_departure_60d IS NULL THEN 1 ELSE 0 END) AS missing_lane_departure_60d,
      SUM(CASE WHEN lane_departure_60d IS NOT NULL AND lane_departure_60d < 0 THEN 1 ELSE 0 END) AS oor_lane_departure_60d,

      -- virtual_coaching_60d: -1 or >= 0
      SUM(CASE WHEN virtual_coaching_60d IS NULL THEN 1 ELSE 0 END) AS missing_virtual_coaching_60d,
      SUM(
          CASE
              WHEN virtual_coaching_60d IS NOT NULL
                    AND virtual_coaching_60d < 0
                    AND virtual_coaching_60d <> -1
              THEN 1 ELSE 0
          END
      ) AS oor_virtual_coaching_60d,

      -- harsh_accelerations_60d: >= 0
      SUM(CASE WHEN harsh_accelerations_60d IS NULL THEN 1 ELSE 0 END) AS missing_harsh_accelerations_60d,
      SUM(CASE WHEN harsh_accelerations_60d IS NOT NULL AND harsh_accelerations_60d < 0 THEN 1 ELSE 0 END) AS oor_harsh_accelerations_60d

  FROM feature_values
  WHERE date = '{PARTITION_START}'
),
long AS (
  SELECT
    total_rows,
    feature,
    missing_count,
    out_of_range_count
  FROM agg
  LATERAL VIEW STACK(
    47,
    'trip_duration_weighted', missing_trip_duration_weighted, oor_trip_duration_weighted,
    'n_org_vg_devices', missing_n_org_vg_devices, oor_n_org_vg_devices,
    'driver_tenure_days', missing_driver_tenure_days, oor_driver_tenure_days,
    'distracted_driving_60d', missing_distracted_driving_60d, oor_distracted_driving_60d,
    'trip_count_below_freezing_60d', missing_trip_count_below_freezing_60d, oor_trip_count_below_freezing_60d,
    'trip_count_over_35_60d', missing_trip_count_over_35_60d, oor_trip_count_over_35_60d,
    'proportion_urban_driving_environment_locations_60d', missing_proportion_urban_driving_environment_locations_60d, oor_proportion_urban_driving_environment_locations_60d,
    'primary_max_weight_lbs', missing_primary_max_weight_lbs, oor_primary_max_weight_lbs,
    'hos_violation_count_60d', missing_hos_violation_count_60d, oor_hos_violation_count_60d,
    'moderate_speeding_mins_60d', missing_moderate_speeding_mins_60d, oor_moderate_speeding_mins_60d,
    'mobile_usage_violations_60d', missing_mobile_usage_violations_60d, oor_mobile_usage_violations_60d,
    'max_speed_weighted', missing_max_speed_weighted, oor_max_speed_weighted,
    'proportion_rural_driving_environment_locations_60d', missing_proportion_rural_driving_environment_locations_60d, oor_proportion_rural_driving_environment_locations_60d,
    'proportion_roadway_service_60d', missing_proportion_roadway_service_60d, oor_proportion_roadway_service_60d,
    'harsh_braking_60d', missing_harsh_braking_60d, oor_harsh_braking_60d,
    'proportion_mixed_driving_environment_locations_60d', missing_proportion_mixed_driving_environment_locations_60d, oor_proportion_mixed_driving_environment_locations_60d,
    'proportion_roadway_residential_60d', missing_proportion_roadway_residential_60d, oor_proportion_roadway_residential_60d,
    'light_speeding_mins_60d', missing_light_speeding_mins_60d, oor_light_speeding_mins_60d,
    'proportion_roadway_motorway_60d', missing_proportion_roadway_motorway_60d, oor_proportion_roadway_motorway_60d,
    'trip_ended_at_night_weighted', missing_trip_ended_at_night_weighted, oor_trip_ended_at_night_weighted,
    'is_eld_relevant_device', missing_is_eld_relevant_device, oor_is_eld_relevant_device,
    'trip_count_60d', missing_trip_count_60d, oor_trip_count_60d,
    'proportion_roadway_trunk_60d', missing_proportion_roadway_trunk_60d, oor_proportion_roadway_trunk_60d,
    'proportion_roadway_secondary_60d', missing_proportion_roadway_secondary_60d, oor_proportion_roadway_secondary_60d,
    'proportion_roadway_null_60d', missing_proportion_roadway_null_60d, oor_proportion_roadway_null_60d,
    'trip_ended_in_morning_60d', missing_trip_ended_in_morning_60d, oor_trip_ended_in_morning_60d,
    'severe_speeding_mins_60d', missing_severe_speeding_mins_60d, oor_severe_speeding_mins_60d,
    'coached_events_60d', missing_coached_events_60d, oor_coached_events_60d,
    'total_events_weighted', missing_total_events_weighted, oor_total_events_weighted,
    'speeding_60d', missing_speeding_60d, oor_speeding_60d,
    'labelled_crash_count_prev_1_year', missing_labelled_crash_count_prev_1_year, oor_labelled_crash_count_prev_1_year,
    'proportion_roadway_tertiary_60d', missing_proportion_roadway_tertiary_60d, oor_proportion_roadway_tertiary_60d,
    'proportion_roadway_unclassified_60d', missing_proportion_roadway_unclassified_60d, oor_proportion_roadway_unclassified_60d,
    'rolling_stop_60d', missing_rolling_stop_60d, oor_rolling_stop_60d,
    'proportion_roadway_primary_60d', missing_proportion_roadway_primary_60d, oor_proportion_roadway_primary_60d,
    'drowsiness_60d', missing_drowsiness_60d, oor_drowsiness_60d,
    'harsh_turns_60d', missing_harsh_turns_60d, oor_harsh_turns_60d,
    'proportion_roadway_living_street_60d', missing_proportion_roadway_living_street_60d, oor_proportion_roadway_living_street_60d,
    'following_distance_60d', missing_following_distance_60d, oor_following_distance_60d,
    'speeding_ratio_weighted', missing_speeding_ratio_weighted, oor_speeding_ratio_weighted,
    'heavy_speeding_mins_60d', missing_heavy_speeding_mins_60d, oor_heavy_speeding_mins_60d,
    'crashes_60d', missing_crashes_60d, oor_crashes_60d,
    'trip_distance_miles_60d', missing_trip_distance_miles_60d, oor_trip_distance_miles_60d,
    'lane_departure_60d', missing_lane_departure_60d, oor_lane_departure_60d,
    'virtual_coaching_60d', missing_virtual_coaching_60d, oor_virtual_coaching_60d,
    'harsh_accelerations_60d', missing_harsh_accelerations_60d, oor_harsh_accelerations_60d
  ) s AS feature, missing_count, out_of_range_count
),
feature_missing AS ( -- filter out null feature keys (prevents NULL_MAP_KEY)
  SELECT
    feature,
    CAST(missing_count * 1.0 / NULLIF(total_rows, 0) AS DECIMAL(38,16)) AS missing_prop
  FROM long
  WHERE feature IS NOT NULL
),
feature_oor AS (
  SELECT
    feature,
    CAST(out_of_range_count * 1.0 / NULLIF(total_rows, 0) AS DECIMAL(38,16)) AS oor_prop
  FROM long
  WHERE feature IS NOT NULL
),
metric_feature_missing_prop AS (
  SELECT
    '{PARTITION_START}' AS date,
    'feature_missing_prop'           AS metric,
    map_from_entries(
      collect_list(
        named_struct('key', feature, 'value', missing_prop)
      )
    ) AS value
  FROM feature_missing
),
metric_feature_oor_prop AS (
  SELECT
    '{PARTITION_START}' AS date,
    'feature_oor_prop'               AS metric,
    map_from_entries(
      collect_list(
        named_struct('key', feature, 'value', oor_prop)
      )
    ) AS value
  FROM feature_oor
),

-- performance overview page views
metric_page_views_1d AS (
  SELECT
    '{PARTITION_START}' AS date,
    'perfoverview_page_views_1d'        AS metric,
    map_from_entries(
      array(
        named_struct(
          'key', 'total_views',
          'value', CAST(COUNT(*) AS DOUBLE)
        ),
        named_struct(
          'key', 'users_viewed',
          'value', CAST(COUNT(DISTINCT user_id) AS DOUBLE)
        ),
        named_struct(
          'key', 'orgs_viewed',
          'value', CAST(COUNT(DISTINCT org_id) AS DOUBLE)
        )
      )
    ) AS value
  FROM datamodel_platform_silver.stg_cloud_routes
  WHERE date = '{PARTITION_START}'
    AND routename = 'fleet_supervisor_insights'
    AND is_customer_email = TRUE
),
metric_page_views_7d AS (
  SELECT
    '{PARTITION_START}' AS date,
    'perfoverview_page_views_7d'           AS metric,
    map_from_entries(
      array(
        named_struct(
          'key', 'total_views',
          'value', CAST(COUNT(*) AS DOUBLE)
        ),
        named_struct(
          'key', 'users_viewed',
          'value', CAST(COUNT(DISTINCT user_id) AS DOUBLE)
        ),
        named_struct(
          'key', 'orgs_viewed',
          'value', CAST(COUNT(DISTINCT org_id) AS DOUBLE)
        )
      )
    ) AS value
  FROM datamodel_platform_silver.stg_cloud_routes
  WHERE date BETWEEN DATEADD(DAY, -6, '{PARTITION_START}') AND '{PARTITION_START}'
    AND routename = 'fleet_supervisor_insights'
    AND is_customer_email = TRUE
),
metric_page_views_28d AS (
  SELECT
    '{PARTITION_START}' AS date,
    'perfoverview_page_views_28d'           AS metric,
    map_from_entries(
      array(
        named_struct(
          'key', 'total_views',
          'value', CAST(COUNT(*) AS DOUBLE)
        ),
        named_struct(
          'key', 'users_viewed',
          'value', CAST(COUNT(DISTINCT user_id) AS DOUBLE)
        ),
        named_struct(
          'key', 'orgs_viewed',
          'value', CAST(COUNT(DISTINCT org_id) AS DOUBLE)
        )
      )
    ) AS value
  FROM datamodel_platform_silver.stg_cloud_routes
  WHERE date BETWEEN DATEADD(DAY, -27, '{PARTITION_START}') AND '{PARTITION_START}'
    AND routename = 'fleet_supervisor_insights'
    AND is_customer_email = TRUE
),

-- risk explainability modal clicks
all_clicks AS (
  SELECT
    CAST(mp_date AS STRING) AS mp_date,
    mp_user_id,
    orgid,
    'risk_pill_click' AS action_type
  FROM mixpanel_samsara.supervisor_insights_drivers_requiring_attention_safety_score_entry_risk_pill_click
  WHERE mp_date BETWEEN DATEADD(DAY, -27, '{PARTITION_START}') AND '{PARTITION_START}'
    AND mp_user_id NOT LIKE '%samsara%'

  UNION ALL

  SELECT
    CAST(mp_date AS STRING) AS mp_date,
    mp_user_id,
    orgid,
    'send_to_coaching_click' AS action_type
  FROM mixpanel_samsara.supervisor_insights_drivers_requiring_attention_safety_score_entry_send_to_coaching_click
  WHERE mp_date BETWEEN DATEADD(DAY, -27, '{PARTITION_START}') AND '{PARTITION_START}'
    AND mp_user_id NOT LIKE '%samsara%'
),
metric_risk_explainability_clicks_1d AS (
  SELECT
    '{PARTITION_START}' AS date,
    'risk_explainability_clicks_1d' AS metric,
    map_from_entries(
      array(
        named_struct(
          'key', 'total_clicks',
          'value', CAST(COUNT(*) AS DOUBLE)
        ),
        named_struct(
          'key', 'users_clicked',
          'value', CAST(COUNT(DISTINCT mp_user_id) AS DOUBLE)
        ),
        named_struct(
          'key', 'orgs_clicked',
          'value', CAST(COUNT(DISTINCT orgid) AS DOUBLE)
        )
      )
    ) AS value
  FROM all_clicks
  WHERE mp_date = '{PARTITION_START}'
),
metric_risk_explainability_clicks_7d AS (
  SELECT
    '{PARTITION_START}' AS date,
    'risk_explainability_clicks_7d' AS metric,
    map_from_entries(
      array(
        named_struct(
          'key', 'total_clicks',
          'value', CAST(COUNT(*) AS DOUBLE)
        ),
        named_struct(
          'key', 'users_clicked',
          'value', CAST(COUNT(DISTINCT mp_user_id) AS DOUBLE)
        ),
        named_struct(
          'key', 'orgs_clicked',
          'value', CAST(COUNT(DISTINCT orgid) AS DOUBLE)
        )
      )
    ) AS value
  FROM all_clicks
  WHERE mp_date BETWEEN DATEADD(DAY, -6, '{PARTITION_START}') AND '{PARTITION_START}'
),
metric_risk_explainability_clicks_28d AS (
  SELECT
    '{PARTITION_START}' AS date,
    'risk_explainability_clicks_28d' AS metric,
    map_from_entries(
      array(
        named_struct(
          'key', 'total_clicks',
          'value', CAST(COUNT(*) AS DOUBLE)
        ),
        named_struct(
          'key', 'users_clicked',
          'value', CAST(COUNT(DISTINCT mp_user_id) AS DOUBLE)
        ),
        named_struct(
          'key', 'orgs_clicked',
          'value', CAST(COUNT(DISTINCT orgid) AS DOUBLE)
        )
      )
    ) AS value
  FROM all_clicks
  WHERE mp_date BETWEEN DATEADD(DAY, -27, '{PARTITION_START}') AND '{PARTITION_START}'
),
metric_send_to_coaching_clicks_1d AS (
  SELECT
    '{PARTITION_START}' AS date,
    'send_to_coaching_clicks_1d' AS metric,
    map_from_entries(
      array(
        named_struct(
          'key', 'total_clicks',
          'value', CAST(COUNT_IF(action_type = 'send_to_coaching_click') AS DOUBLE)
        ),
        named_struct(
          'key', 'users_clicked',
          'value', CAST(COUNT(DISTINCT CASE WHEN action_type = 'send_to_coaching_click' THEN mp_user_id ELSE NULL END) AS DOUBLE)
        ),
        named_struct(
          'key', 'orgs_clicked',
          'value', CAST(COUNT(DISTINCT CASE WHEN action_type = 'send_to_coaching_click' THEN orgid ELSE NULL END) AS DOUBLE)
        )
      )
    ) AS value
  FROM all_clicks
  WHERE mp_date = '{PARTITION_START}'
),
metric_send_to_coaching_clicks_7d AS (
  SELECT
    '{PARTITION_START}' AS date,
    'send_to_coaching_clicks_7d' AS metric,
    map_from_entries(
      array(
        named_struct(
          'key', 'total_clicks',
          'value', CAST(COUNT_IF(action_type = 'send_to_coaching_click') AS DOUBLE)
        ),
        named_struct(
          'key', 'users_clicked',
          'value', CAST(COUNT(DISTINCT CASE WHEN action_type = 'send_to_coaching_click' THEN mp_user_id ELSE NULL END) AS DOUBLE)
        ),
        named_struct(
          'key', 'orgs_clicked',
          'value', CAST(COUNT(DISTINCT CASE WHEN action_type = 'send_to_coaching_click' THEN orgid ELSE NULL END) AS DOUBLE)
        )
      )
    ) AS value
  FROM all_clicks
  WHERE mp_date BETWEEN DATEADD(DAY, -6, '{PARTITION_START}') AND '{PARTITION_START}'
),
metric_send_to_coaching_clicks_28d AS (
  SELECT
    '{PARTITION_START}' AS date,
    'send_to_coaching_clicks_28d' AS metric,
    map_from_entries(
      array(
        named_struct(
          'key', 'total_clicks',
          'value', CAST(COUNT_IF(action_type = 'send_to_coaching_click') AS DOUBLE)
        ),
        named_struct(
          'key', 'users_clicked',
          'value', CAST(COUNT(DISTINCT CASE WHEN action_type = 'send_to_coaching_click' THEN mp_user_id ELSE NULL END) AS DOUBLE)
        ),
        named_struct(
          'key', 'orgs_clicked',
          'value', CAST(COUNT(DISTINCT CASE WHEN action_type = 'send_to_coaching_click' THEN orgid ELSE NULL END) AS DOUBLE)
        )
      )
    ) AS value
  FROM all_clicks
  WHERE mp_date BETWEEN DATEADD(DAY, -27, '{PARTITION_START}') AND '{PARTITION_START}'
),

-- coaching submitted clicks
all_coaching_submitted_clicks AS (
  SELECT
    CAST(mp_date AS STRING) AS mp_date,
    mp_user_id,
    orgid
  FROM mixpanel_samsara.fleet_reports_safety_supervisor_insights_send_to_coaching_modal_send_to_coaching_submitted
  WHERE mp_date BETWEEN DATEADD(DAY, -27, '{PARTITION_START}') AND '{PARTITION_START}'
    AND mp_user_id NOT LIKE '%samsara%'
),
metric_coaching_submitted_clicks_1d AS (
  SELECT
    '{PARTITION_START}' AS date,
    'coaching_submitted_clicks_1d' AS metric,
    map_from_entries(
      array(
        named_struct(
          'key', 'total_clicks',
          'value', CAST(COUNT(*) AS DOUBLE)
        ),
        named_struct(
          'key', 'users_clicked',
          'value', CAST(COUNT(DISTINCT mp_user_id) AS DOUBLE)
        ),
        named_struct(
          'key', 'orgs_clicked',
          'value', CAST(COUNT(DISTINCT orgid) AS DOUBLE)
        )
      )
    ) AS value
  FROM all_coaching_submitted_clicks
  WHERE mp_date = '{PARTITION_START}'
),
metric_coaching_submitted_clicks_7d AS (
  SELECT
    '{PARTITION_START}' AS date,
    'coaching_submitted_clicks_7d' AS metric,
    map_from_entries(
      array(
        named_struct(
          'key', 'total_clicks',
          'value', CAST(COUNT(*) AS DOUBLE)
        ),
        named_struct(
          'key', 'users_clicked',
          'value', CAST(COUNT(DISTINCT mp_user_id) AS DOUBLE)
        ),
        named_struct(
          'key', 'orgs_clicked',
          'value', CAST(COUNT(DISTINCT orgid) AS DOUBLE)
        )
      )
    ) AS value
  FROM all_coaching_submitted_clicks
  WHERE mp_date BETWEEN DATEADD(DAY, -6, '{PARTITION_START}') AND '{PARTITION_START}'
),
metric_coaching_submitted_clicks_28d AS (
  SELECT
    '{PARTITION_START}' AS date,
    'coaching_submitted_clicks_28d' AS metric,
    map_from_entries(
      array(
        named_struct(
          'key', 'total_clicks',
          'value', CAST(COUNT(*) AS DOUBLE)
        ),
        named_struct(
          'key', 'users_clicked',
          'value', CAST(COUNT(DISTINCT mp_user_id) AS DOUBLE)
        ),
        named_struct(
          'key', 'orgs_clicked',
          'value', CAST(COUNT(DISTINCT orgid) AS DOUBLE)
        )
      )
    ) AS value
  FROM all_coaching_submitted_clicks
  WHERE mp_date BETWEEN DATEADD(DAY, -27, '{PARTITION_START}') AND '{PARTITION_START}'
),

-- hit rate at K: 1d, 7d, 28d
all_clicks_28d AS (
  SELECT
    CAST(mp_date AS STRING) AS mp_date,
    time,
    mp_user_id,
    orgid,
    rank
  FROM mixpanel_samsara.supervisor_insights_drivers_requiring_attention_safety_score_entry_risk_pill_click
  WHERE mp_date BETWEEN DATEADD(DAY, -27, '{PARTITION_START}') AND '{PARTITION_START}'
    AND mp_user_id NOT LIKE '%samsara%'

  UNION ALL

  SELECT
    CAST(mp_date AS STRING) AS mp_date,
    time,
    mp_user_id,
    orgid,
    rank
  FROM mixpanel_samsara.supervisor_insights_drivers_requiring_attention_safety_score_entry_send_to_coaching_click
  WHERE mp_date BETWEEN DATEADD(DAY, -27, '{PARTITION_START}') AND '{PARTITION_START}'
    AND mp_user_id NOT LIKE '%samsara%'
),
highest_rank_1d AS (
  SELECT
    mp_date,
    mp_user_id,
    MIN(rank) AS highest_rank
  FROM all_clicks_28d
  WHERE mp_date = '{PARTITION_START}'
  GROUP BY 1,2
),
highest_rank_7d AS (
  SELECT
    mp_date,
    mp_user_id,
    MIN(rank) AS highest_rank
  FROM all_clicks_28d
  WHERE mp_date BETWEEN DATEADD(DAY, -6, '{PARTITION_START}') AND '{PARTITION_START}'
  GROUP BY 1,2
),
highest_rank_28d AS (
  SELECT
    mp_date,
    mp_user_id,
    MIN(rank) AS highest_rank
  FROM all_clicks_28d
  WHERE mp_date BETWEEN DATEADD(DAY, -27, '{PARTITION_START}') AND '{PARTITION_START}'
  GROUP BY 1,2
),
performance_overview_views_1d AS (
  SELECT DISTINCT CAST(date AS STRING) AS date, user_id
  FROM datamodel_platform_silver.stg_cloud_routes
  WHERE date = '{PARTITION_START}'
    AND routename = 'fleet_supervisor_insights'
    AND is_customer_email = TRUE
),
performance_overview_views_7d AS (
  SELECT DISTINCT CAST(date AS STRING) AS date, user_id
  FROM datamodel_platform_silver.stg_cloud_routes
  WHERE date BETWEEN DATEADD(DAY, -6, '{PARTITION_START}') AND '{PARTITION_START}'
    AND routename = 'fleet_supervisor_insights'
    AND is_customer_email = TRUE
),
performance_overview_views_28d AS (
  SELECT DISTINCT CAST(date AS STRING) AS date, user_id
  FROM datamodel_platform_silver.stg_cloud_routes
  WHERE date BETWEEN DATEADD(DAY, -27, '{PARTITION_START}') AND '{PARTITION_START}'
    AND routename = 'fleet_supervisor_insights'
    AND is_customer_email = TRUE
),
metric_hit_rate_1d AS (
  SELECT
    '{PARTITION_START}' AS date,
    'hit_rate_1d' AS metric,
    map_from_entries(
      array(
        named_struct('key','hit_rate_k_10', 'value', CAST(100.0 * SUM(CASE WHEN highest_rank <= 10 THEN 1 ELSE 0 END) / NULLIF((SELECT COUNT(*) FROM performance_overview_views_1d),0) AS DOUBLE)),
        named_struct('key','hit_rate_k_5',  'value', CAST(100.0 * SUM(CASE WHEN highest_rank <= 5  THEN 1 ELSE 0 END) / NULLIF((SELECT COUNT(*) FROM performance_overview_views_1d),0) AS DOUBLE)),
        named_struct('key','hit_rate_k_3',  'value', CAST(100.0 * SUM(CASE WHEN highest_rank <= 3  THEN 1 ELSE 0 END) / NULLIF((SELECT COUNT(*) FROM performance_overview_views_1d),0) AS DOUBLE)),
        named_struct('key','total_user_days','value', CAST((SELECT COUNT(*) FROM performance_overview_views_1d) AS DOUBLE))
      )
    ) AS value
  FROM highest_rank_1d
),
metric_hit_rate_7d AS (
  SELECT
    '{PARTITION_START}' AS date,
    'hit_rate_7d' AS metric,
    map_from_entries(
      array(
        named_struct('key','hit_rate_k_10', 'value', CAST(100.0 * SUM(CASE WHEN highest_rank <= 10 THEN 1 ELSE 0 END) / NULLIF((SELECT COUNT(*) FROM performance_overview_views_7d),0) AS DOUBLE)),
        named_struct('key','hit_rate_k_5',  'value', CAST(100.0 * SUM(CASE WHEN highest_rank <= 5  THEN 1 ELSE 0 END) / NULLIF((SELECT COUNT(*) FROM performance_overview_views_7d),0) AS DOUBLE)),
        named_struct('key','hit_rate_k_3',  'value', CAST(100.0 * SUM(CASE WHEN highest_rank <= 3  THEN 1 ELSE 0 END) / NULLIF((SELECT COUNT(*) FROM performance_overview_views_7d),0) AS DOUBLE)),
        named_struct('key','total_user_days','value', CAST((SELECT COUNT(*) FROM performance_overview_views_7d) AS DOUBLE))
      )
    ) AS value
  FROM highest_rank_7d
),
metric_hit_rate_28d AS (
  SELECT
    '{PARTITION_START}' AS date,
    'hit_rate_28d' AS metric,
    map_from_entries(
      array(
        named_struct('key','hit_rate_k_10', 'value', CAST(100.0 * SUM(CASE WHEN highest_rank <= 10 THEN 1 ELSE 0 END) / NULLIF((SELECT COUNT(*) FROM performance_overview_views_28d),0) AS DOUBLE)),
        named_struct('key','hit_rate_k_5',  'value', CAST(100.0 * SUM(CASE WHEN highest_rank <= 5  THEN 1 ELSE 0 END) / NULLIF((SELECT COUNT(*) FROM performance_overview_views_28d),0) AS DOUBLE)),
        named_struct('key','hit_rate_k_3',  'value', CAST(100.0 * SUM(CASE WHEN highest_rank <= 3  THEN 1 ELSE 0 END) / NULLIF((SELECT COUNT(*) FROM performance_overview_views_28d),0) AS DOUBLE)),
        named_struct('key','total_user_days','value', CAST((SELECT COUNT(*) FROM performance_overview_views_28d) AS DOUBLE))
      )
    ) AS value
  FROM highest_rank_28d
),

-- mean reciprocal rank (MRR) metrics: 1d, 7d, 28d
all_risk_explainability_clicks_28d AS (
  SELECT
    CAST(mp_date AS STRING) AS mp_date,
    time,
    mp_user_id,
    orgid,
    rank
  FROM mixpanel_samsara.supervisor_insights_drivers_requiring_attention_safety_score_entry_risk_pill_click
  WHERE mp_date BETWEEN DATEADD(DAY, -27, '{PARTITION_START}') AND '{PARTITION_START}'
    AND mp_user_id NOT LIKE '%samsara%'

  UNION ALL

  SELECT
    CAST(mp_date AS STRING) AS mp_date,
    time,
    mp_user_id,
    orgid,
    rank
  FROM mixpanel_samsara.supervisor_insights_drivers_requiring_attention_safety_score_entry_send_to_coaching_click
  WHERE mp_date BETWEEN DATEADD(DAY, -27, '{PARTITION_START}') AND '{PARTITION_START}'
    AND mp_user_id NOT LIKE '%samsara%'
),
first_event_1d AS (
  SELECT
    mp_date,
    mp_user_id,
    rank
  FROM all_risk_explainability_clicks_28d
  WHERE mp_date = '{PARTITION_START}'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY mp_date, mp_user_id ORDER BY time) = 1
),
first_event_7d AS (
  SELECT
    mp_date,
    mp_user_id,
    rank
  FROM all_risk_explainability_clicks_28d
  WHERE mp_date BETWEEN DATEADD(DAY, -6, '{PARTITION_START}') AND '{PARTITION_START}'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY mp_date, mp_user_id ORDER BY time) = 1
),
first_event_28d AS (
  SELECT
    mp_date,
    mp_user_id,
    rank
  FROM all_risk_explainability_clicks_28d
  WHERE mp_date BETWEEN DATEADD(DAY, -27, '{PARTITION_START}') AND '{PARTITION_START}'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY mp_date, mp_user_id ORDER BY time) = 1
),
metric_mean_recip_rank_1d AS (
  SELECT
    '{PARTITION_START}' AS date,
    'mean_recip_rank_1d' AS metric,
    map_from_entries(
      array(
        named_struct('key','mrr_k_3',  'value', CAST(AVG(CASE WHEN rank <= 3  THEN 1.0 / rank ELSE 0.0 END) AS DOUBLE)),
        named_struct('key','mrr_k_5',  'value', CAST(AVG(CASE WHEN rank <= 5  THEN 1.0 / rank ELSE 0.0 END) AS DOUBLE)),
        named_struct('key','mrr_k_10', 'value', CAST(AVG(CASE WHEN rank <= 10 THEN 1.0 / rank ELSE 0.0 END) AS DOUBLE))
      )
    ) AS value
  FROM first_event_1d
),
metric_mean_recip_rank_7d AS (
  SELECT
    '{PARTITION_START}' AS date,
    'mean_recip_rank_7d' AS metric,
    map_from_entries(
      array(
        named_struct('key','mrr_k_3',  'value', CAST(AVG(CASE WHEN rank <= 3  THEN 1.0 / rank ELSE 0.0 END) AS DOUBLE)),
        named_struct('key','mrr_k_5',  'value', CAST(AVG(CASE WHEN rank <= 5  THEN 1.0 / rank ELSE 0.0 END) AS DOUBLE)),
        named_struct('key','mrr_k_10', 'value', CAST(AVG(CASE WHEN rank <= 10 THEN 1.0 / rank ELSE 0.0 END) AS DOUBLE))
      )
    ) AS value
  FROM first_event_7d
),
metric_mean_recip_rank_28d AS (
  SELECT
    '{PARTITION_START}' AS date,
    'mean_recip_rank_28d' AS metric,
    map_from_entries(
      array(
        named_struct('key','mrr_k_3',  'value', CAST(AVG(CASE WHEN rank <= 3  THEN 1.0 / rank ELSE 0.0 END) AS DOUBLE)),
        named_struct('key','mrr_k_5',  'value', CAST(AVG(CASE WHEN rank <= 5  THEN 1.0 / rank ELSE 0.0 END) AS DOUBLE)),
        named_struct('key','mrr_k_10', 'value', CAST(AVG(CASE WHEN rank <= 10 THEN 1.0 / rank ELSE 0.0 END) AS DOUBLE))
      )
    ) AS value
  FROM first_event_28d
)

-- union all metrics together
SELECT *,
  NOW() AS created_at
FROM metric_risk_class_sizes

UNION ALL

SELECT *,
  NOW() AS created_at
FROM metric_num_crashes_28d

UNION ALL

SELECT *,
  NOW() AS created_at
FROM metric_avg_crash_rate_28d

UNION ALL

SELECT *,
  NOW() AS created_at
FROM metric_median_risk_scores

UNION ALL

SELECT *,
  NOW() AS created_at
FROM metric_driver_risk_class_changes

UNION ALL

SELECT *,
  NOW() AS created_at
FROM metric_expected_score_range

UNION ALL

SELECT *,
  NOW() AS created_at
FROM metric_avg_risk_scores_extremes

UNION ALL

SELECT *,
  NOW() AS created_at
FROM metric_e2e_pipeline_health

UNION ALL

SELECT *,
  NOW() AS created_at
FROM metric_feature_missing_prop

UNION ALL

SELECT *,
  NOW() AS created_at
FROM metric_feature_oor_prop

UNION ALL

SELECT *,
  NOW() AS created_at
FROM metric_page_views_1d

UNION ALL

SELECT *,
  NOW() AS created_at
FROM metric_page_views_7d

UNION ALL

SELECT *,
  NOW() AS created_at
FROM metric_page_views_28d

UNION ALL

SELECT *,
  NOW() AS created_at
FROM metric_risk_explainability_clicks_1d

UNION ALL

SELECT *,
  NOW() AS created_at
FROM metric_risk_explainability_clicks_7d

UNION ALL

SELECT *,
  NOW() AS created_at
FROM metric_risk_explainability_clicks_28d

UNION ALL

SELECT *,
  NOW() AS created_at
FROM metric_send_to_coaching_clicks_1d

UNION ALL

SELECT *,
  NOW() AS created_at
FROM metric_send_to_coaching_clicks_7d

UNION ALL

SELECT *,
  NOW() AS created_at
FROM metric_send_to_coaching_clicks_28d

UNION ALL

SELECT *,
  NOW() AS created_at
FROM metric_coaching_submitted_clicks_1d

UNION ALL

SELECT *,
  NOW() AS created_at
FROM metric_coaching_submitted_clicks_7d

UNION ALL

SELECT *,
  NOW() AS created_at
FROM metric_coaching_submitted_clicks_28d

UNION ALL

SELECT *,
  NOW() AS created_at
FROM metric_hit_rate_1d

UNION ALL

SELECT *,
  NOW() AS created_at
FROM metric_hit_rate_7d

UNION ALL

SELECT *,
  NOW() AS created_at
FROM metric_hit_rate_28d

UNION ALL

SELECT *,
  NOW() AS created_at
FROM metric_mean_recip_rank_1d

UNION ALL

SELECT *,
  NOW() AS created_at
FROM metric_mean_recip_rank_7d

UNION ALL

SELECT *,
  NOW() AS created_at
FROM metric_mean_recip_rank_28d
"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="""Daily metrics for driver risk classification system including risk class sizes, crash rates, feature health, and pipeline metrics.""",
        row_meaning="""Each row represents a metric calculation for a specific date, with metric-specific key-value pairs stored in the value map.""",
        related_table_info={},
        table_type=TableType.STAGING,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=[AWSRegion.US_WEST_2],
    owners=[DATAENGINEERING],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DailyPartitionsDefinition(start_date="2023-09-01"),
    write_mode=WarehouseWriteMode.OVERWRITE,
    run_config_overrides=create_run_config_overrides(
        driver_instance_type=InstanceType.RD_FLEET_2XLARGE,
        worker_instance_type=InstanceType.RD_FLEET_2XLARGE,
        max_workers=32,
    ),
    dq_checks=[
        NonEmptyDQCheck(name="dq_non_empty_stg_driver_risk_metrics"),
        PrimaryKeyDQCheck(name="dq_pk_stg_driver_risk_metrics", primary_keys=PRIMARY_KEYS),
        NonNullDQCheck(
            name="dq_non_null_stg_driver_risk_metrics",
            non_null_columns=PRIMARY_KEYS,
        ),
    ],
    backfill_batch_size=1,
    upstreams=[
        "product_analytics.driver_risk_classification",
        "product_analytics_staging.stg_crash_trips",
        "feature_store.driver_risk_insights_features",
        "datamodel_platform_silver.stg_cloud_routes",
        "mixpanel_samsara.supervisor_insights_drivers_requiring_attention_safety_score_entry_risk_pill_click",
        "mixpanel_samsara.supervisor_insights_drivers_requiring_attention_safety_score_entry_send_to_coaching_click",
        "mixpanel_samsara.fleet_reports_safety_supervisor_insights_send_to_coaching_modal_send_to_coaching_submitted",
    ],
)
def stg_driver_risk_metrics(context: AssetExecutionContext) -> DataFrame:
    context.log.info("Updating stg_driver_risk_metrics")
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    
    partition_keys = partition_key_ranges_from_context(context)[0]
    PARTITION_START = partition_keys[0]
    
    # Create a comprehensive cached DataFrame that covers both main query and PSI needs
    # Main query needs: -40 to PARTITION_START
    # PSI needs: -55 to PARTITION_START
    # So we load -55 to PARTITION_START to cover both, eliminating redundant table scans
    context.log.info("Creating cached driver_risk_classification DataFrame for main query and PSI")
    main_query_base_df_query = f"""
      SELECT
        CAST(date AS STRING) AS date,
        timestamp,
        org_id,
        driver_id,
        process_uuid,
        risk_score,
        classification,
        base_value,
        -- Feature values (for main query and feature_value_psi)
        CAST(get_json_object(trip_duration_weighted, '$.value') AS DOUBLE) AS trip_duration_weighted,
        CAST(get_json_object(n_org_vg_devices, '$.value') AS DOUBLE) AS n_org_vg_devices,
        CAST(get_json_object(driver_tenure_days, '$.value') AS DOUBLE) AS driver_tenure_days,
        CAST(get_json_object(distracted_driving_60d, '$.value') AS DOUBLE) AS distracted_driving_60d,
        CAST(get_json_object(trip_count_below_freezing_60d, '$.value') AS DOUBLE) AS trip_count_below_freezing_60d,
        CAST(get_json_object(trip_count_over_35_60d, '$.value') AS DOUBLE) AS trip_count_over_35_60d,
        CAST(get_json_object(proportion_urban_driving_environment_locations_60d, '$.value') AS DOUBLE)
            AS proportion_urban_driving_environment_locations_60d,
        CAST(get_json_object(primary_max_weight_lbs, '$.value') AS DOUBLE) AS primary_max_weight_lbs,
        CAST(get_json_object(hos_violation_count_60d, '$.value') AS DOUBLE) AS hos_violation_count_60d,
        CAST(get_json_object(moderate_speeding_mins_60d, '$.value') AS DOUBLE) AS moderate_speeding_mins_60d,
        CAST(get_json_object(mobile_usage_violations_60d, '$.value') AS DOUBLE) AS mobile_usage_violations_60d,
        CAST(get_json_object(max_speed_weighted, '$.value') AS DOUBLE) AS max_speed_weighted,
        CAST(get_json_object(proportion_rural_driving_environment_locations_60d, '$.value') AS DOUBLE)
            AS proportion_rural_driving_environment_locations_60d,
        CAST(get_json_object(proportion_roadway_service_60d, '$.value') AS DOUBLE) AS proportion_roadway_service_60d,
        CAST(get_json_object(harsh_braking_60d, '$.value') AS DOUBLE) AS harsh_braking_60d,
        CAST(get_json_object(proportion_mixed_driving_environment_locations_60d, '$.value') AS DOUBLE)
            AS proportion_mixed_driving_environment_locations_60d,
        CAST(get_json_object(proportion_roadway_residential_60d, '$.value') AS DOUBLE) AS proportion_roadway_residential_60d,
        CAST(get_json_object(light_speeding_mins_60d, '$.value') AS DOUBLE) AS light_speeding_mins_60d,
        CAST(get_json_object(proportion_roadway_motorway_60d, '$.value') AS DOUBLE) AS proportion_roadway_motorway_60d,
        CAST(get_json_object(trip_ended_at_night_weighted, '$.value') AS DOUBLE) AS trip_ended_at_night_weighted,
        CAST(get_json_object(is_eld_relevant_device, '$.value') AS DOUBLE) AS is_eld_relevant_device,
        CAST(get_json_object(trip_count_60d, '$.value') AS DOUBLE) AS trip_count_60d,
        CAST(get_json_object(proportion_roadway_trunk_60d, '$.value') AS DOUBLE) AS proportion_roadway_trunk_60d,
        CAST(get_json_object(proportion_roadway_secondary_60d, '$.value') AS DOUBLE) AS proportion_roadway_secondary_60d,
        CAST(get_json_object(proportion_roadway_null_60d, '$.value') AS DOUBLE) AS proportion_roadway_null_60d,
        CAST(get_json_object(trip_ended_in_morning_60d, '$.value') AS DOUBLE) AS trip_ended_in_morning_60d,
        CAST(get_json_object(severe_speeding_mins_60d, '$.value') AS DOUBLE) AS severe_speeding_mins_60d,
        CAST(get_json_object(coached_events_60d, '$.value') AS DOUBLE) AS coached_events_60d,
        CAST(get_json_object(total_events_weighted, '$.value') AS DOUBLE) AS total_events_weighted,
        CAST(get_json_object(speeding_60d, '$.value') AS DOUBLE) AS speeding_60d,
        CAST(get_json_object(labelled_crash_count_prev_1_year, '$.value') AS DOUBLE) AS labelled_crash_count_prev_1_year,
        CAST(get_json_object(proportion_roadway_tertiary_60d, '$.value') AS DOUBLE) AS proportion_roadway_tertiary_60d,
        CAST(get_json_object(proportion_roadway_unclassified_60d, '$.value') AS DOUBLE) AS proportion_roadway_unclassified_60d,
        CAST(get_json_object(rolling_stop_60d, '$.value') AS DOUBLE) AS rolling_stop_60d,
        CAST(get_json_object(proportion_roadway_primary_60d, '$.value') AS DOUBLE) AS proportion_roadway_primary_60d,
        CAST(get_json_object(drowsiness_60d, '$.value') AS DOUBLE) AS drowsiness_60d,
        CAST(get_json_object(harsh_turns_60d, '$.value') AS DOUBLE) AS harsh_turns_60d,
        CAST(get_json_object(proportion_roadway_living_street_60d, '$.value') AS DOUBLE)
            AS proportion_roadway_living_street_60d,
        CAST(get_json_object(following_distance_60d, '$.value') AS DOUBLE) AS following_distance_60d,
        CAST(get_json_object(speeding_ratio_weighted, '$.value') AS DOUBLE) AS speeding_ratio_weighted,
        CAST(get_json_object(heavy_speeding_mins_60d, '$.value') AS DOUBLE) AS heavy_speeding_mins_60d,
        CAST(get_json_object(crashes_60d, '$.value') AS DOUBLE) AS crashes_60d,
        CAST(get_json_object(trip_distance_miles_60d, '$.value') AS DOUBLE) AS trip_distance_miles_60d,
        CAST(get_json_object(lane_departure_60d, '$.value') AS DOUBLE) AS lane_departure_60d,
        CAST(get_json_object(virtual_coaching_60d, '$.value') AS DOUBLE) AS virtual_coaching_60d,
        CAST(get_json_object(harsh_accelerations_60d, '$.value') AS DOUBLE) AS harsh_accelerations_60d,
        -- SHAP values (for feature_attribution_psi)
        CAST(get_json_object(trip_duration_weighted, '$.shap_value') AS DOUBLE) AS trip_duration_weighted_shap,
        CAST(get_json_object(driver_tenure_days, '$.shap_value') AS DOUBLE) AS driver_tenure_days_shap,
        CAST(get_json_object(primary_max_weight_lbs, '$.shap_value') AS DOUBLE) AS primary_max_weight_lbs_shap,
        CAST(get_json_object(max_speed_weighted, '$.shap_value') AS DOUBLE) AS max_speed_weighted_shap,
        CAST(get_json_object(trip_count_60d, '$.shap_value') AS DOUBLE) AS trip_count_60d_shap,
        CAST(get_json_object(total_events_weighted, '$.shap_value') AS DOUBLE) AS total_events_weighted_shap,
        CAST(get_json_object(coached_events_60d, '$.shap_value') AS DOUBLE) AS coached_events_60d_shap,
        CAST(get_json_object(mobile_usage_violations_60d, '$.shap_value') AS DOUBLE) AS mobile_usage_violations_60d_shap,
        CAST(get_json_object(harsh_braking_60d, '$.shap_value') AS DOUBLE) AS harsh_braking_60d_shap,
        CAST(get_json_object(trip_ended_at_night_weighted, '$.shap_value') AS DOUBLE) AS trip_ended_at_night_weighted_shap,
        CAST(get_json_object(trip_count_below_freezing_60d, '$.shap_value') AS DOUBLE) AS trip_count_below_freezing_60d_shap,
        CAST(get_json_object(trip_count_over_35_60d, '$.shap_value') AS DOUBLE) AS trip_count_over_35_60d_shap
      FROM product_analytics.driver_risk_classification
      WHERE date BETWEEN DATEADD(DAY, -55, '{PARTITION_START}') AND '{PARTITION_START}'
    """
    # Step 1: Execute QUERY directly to get SQL-only metrics (no temp view needed)
    # QUERY references product_analytics.driver_risk_classification directly
    context.log.info(f"Formatting QUERY with PARTITION_START={PARTITION_START}")
    try:
        main_query = QUERY.format(
            PARTITION_START=PARTITION_START,
        )
        context.log.info("QUERY formatted successfully, executing main QUERY for SQL-only metrics")
        main_metrics_df = spark.sql(main_query)
        context.log.info("Main QUERY executed successfully")
    except Exception as e:
        context.log.error(f"Error executing main QUERY: {e}")
        raise
    
    # Step 2: Create cached DataFrame for PSI calculations (56-day window)
    # This DataFrame contains the columns needed for risk_score_psi, feature_value_psi, and feature_attribution_psi
    main_query_base_df = spark.sql(main_query_base_df_query)
    main_query_base_df.cache()
    # Materialize the cache to ensure it's populated before use
    _ = main_query_base_df.count()  # Trigger cache materialization
    context.log.info("Cached unified driver_risk_classification DataFrame created (contains all columns: risk_score, feature values, SHAP values) for PSI calculations")
    
    def _compute_psi_core(
        spark: SparkSession,
        context: AssetExecutionContext,
        source_column: str,
        feature_name: str,
        PARTITION_START: str,
        cached_df: DataFrame,
        quantile_edges: list,
        bin_expr_sql: str,
        prev_month_start: str,
        prev_month_end: str,
        current_month_start: str,
        current_month_end: str,
        log_suffix: str = ""
    ) -> float:
        """
        Core PSI computation logic shared between compute_feature_psi and compute_feature_attribution_psi.
        
        Args:
            spark: SparkSession
            context: AssetExecutionContext
            source_column: Column name in cached_df to use for PSI calculation
            feature_name: Name to use for aliasing and logging
            PARTITION_START: Partition start date string
            cached_df: Cached DataFrame with data
            quantile_edges: List of quantile edge values for binning (pre-computed)
            bin_expr_sql: SQL expression for computing bin_index (should use {feature} as placeholder, will be replaced with feature_name)
            prev_month_start: Start date of previous month period (pre-computed)
            prev_month_end: End date of previous month period (pre-computed)
            current_month_start: Start date of current month period (pre-computed)
            current_month_end: End date of current month period (pre-computed)
            log_suffix: Suffix for log messages (e.g., " (mode=quantile)" or " SHAP")
        
        Returns:
            PSI value as float, or None on error
        """
        from pyspark.sql.functions import array, expr, lit
        
        # Filter and prepare prev_month and current_month DataFrames
        prev_month_df = cached_df.filter(
            (col("date") >= prev_month_start) & 
            (col("date") <= prev_month_end) &
            col(source_column).isNotNull()
        ).select(
            col(source_column).cast("double").alias(feature_name),
            lit("prev_month").alias("dataset_type")
        )
        
        current_month_df = cached_df.filter(
            (col("date") >= current_month_start) & 
            (col("date") <= current_month_end) &
            col(source_column).isNotNull()
        ).select(
            col(source_column).cast("double").alias(feature_name),
            lit("current_month").alias("dataset_type")
        )
        
        # Union prev and current month
        union_df = prev_month_df.unionAll(current_month_df)
        
        # Bin the data using DataFrame operations with expr for complex SQL
        # Create edges as a single-row DataFrame with array column for cross join
        edges_df = spark.range(1).select(array([lit(e) for e in quantile_edges]).alias("edges"))
        
        # Cross join union_df with edges_df and apply binning logic
        binned_data_df = union_df.crossJoin(edges_df).filter(
            col("dataset_type").isNotNull()
        ).withColumn(
            "bin_index",
            expr(bin_expr_sql.strip().format(feature=feature_name))
        )
        
        # Count by bin and dataset_type using DataFrame operations
        psi_data_df = binned_data_df.filter(col("bin_index").isNotNull()).groupBy("dataset_type", "bin_index").count().withColumnRenamed("count", "ct").orderBy("dataset_type", "bin_index")
        
        try:
            # Compute PSI using PySpark operations
            psi_pivot_df = psi_data_df.groupBy("bin_index").pivot("dataset_type").sum("ct").fillna(0)
            
            # Check if both columns exist
            if "prev_month" not in psi_pivot_df.columns or "current_month" not in psi_pivot_df.columns:
                context.log.warning(f"Missing prev_month or current_month for {feature_name}{log_suffix}, PSI set to NaN")
                return None
            
            # Calculate total counts for normalization
            totals = psi_pivot_df.agg(
                F.sum("prev_month").alias("prev_month_total"),
                F.sum("current_month").alias("current_month_total")
            ).collect()[0]
            prev_month_total = totals["prev_month_total"] or 0.0
            current_month_total = totals["current_month_total"] or 0.0
            
            if prev_month_total == 0.0 or current_month_total == 0.0:
                context.log.warning(f"Zero totals for {feature_name}{log_suffix}, PSI set to NaN")
                return None
            
            # Normalize counts within each dataset_type to get proportion per bin
            epsilon = 1e-6
            psi_normalized_df = psi_pivot_df.withColumn(
                "prev_month_prop",
                (F.col("prev_month") / F.lit(prev_month_total)).cast(DoubleType())
            ).withColumn(
                "current_month_prop",
                (F.col("current_month") / F.lit(current_month_total)).cast(DoubleType())
            ).withColumn(
                "prev_month_clipped",
                F.greatest(F.col("prev_month_prop"), F.lit(epsilon))
            ).withColumn(
                "current_month_clipped",
                F.greatest(F.col("current_month_prop"), F.lit(epsilon))
            ).withColumn(
                "psi_component",
                (F.col("prev_month_clipped") - F.col("current_month_clipped")) *
                F.log(F.col("prev_month_clipped") / F.col("current_month_clipped"))
            )
            
            # Sum over all bins to get total PSI
            psi_result = psi_normalized_df.agg(F.sum("psi_component").alias("total_psi")).collect()[0]
            psi_val = float(psi_result["total_psi"] or 0.0)
            
            context.log.info(f"PSI for {feature_name}{log_suffix}: {psi_val:.4f}")
            return psi_val
        except Exception as e:
            context.log.error(f"Error computing PSI for {feature_name}{log_suffix}: {e}")
            return None
    
    # Compute risk_score_psi metric using _compute_psi_core
    from datetime import datetime, timedelta

    from pyspark.sql.functions import col
    
    # Calculate date boundaries once (used by all PSI calculations)
    context.log.info("Computing date boundaries for PSI calculations")
    partition_date = datetime.strptime(PARTITION_START, '%Y-%m-%d')
    prev_month_start = (partition_date - timedelta(days=55)).strftime('%Y-%m-%d')
    prev_month_end = (partition_date - timedelta(days=28)).strftime('%Y-%m-%d')
    current_month_start = (partition_date - timedelta(days=27)).strftime('%Y-%m-%d')
    current_month_end = PARTITION_START
    
    context.log.info("Computing risk_score_psi using _compute_psi_core")
    
    # Calculate quantile edges for risk_score (10 bins: 0.00, 0.10, ..., 1.00)
    prev_month_base = main_query_base_df.filter(
        (col("date") >= prev_month_start) & 
        (col("date") <= prev_month_end) &
        col("risk_score").isNotNull()
    )
    
    quantiles = [0.00, 0.10, 0.20, 0.30, 0.40, 0.50, 0.60, 0.70, 0.80, 0.90, 1.00]
    quantile_edges_result = prev_month_base.approxQuantile("risk_score", quantiles, 0.0)
    
    # Build bin_expr_sql using quantile mode pattern (consistent with compute_feature_attribution_psi)
    # Wrap with GREATEST(0, ...) and LEAST(..., size(edges) - 1) to prevent negative or out-of-range bin indices
    # For 10 bins (11 edges), valid bin indices are 1-10 (1-indexed)
    bin_expr_sql = """
      LEAST(size(edges) - 1,
        GREATEST(0,
          CASE
            WHEN ARRAY_POSITION(
                  TRANSFORM(
                    edges,
                    e -> e >= LEAST(
                              GREATEST({feature}, element_at(edges, 1)),
                              element_at(edges, -1)
                            )
                  ),
                  true
                ) = 0
              THEN size(edges) - 2 + 1
            ELSE ARRAY_POSITION(
                  TRANSFORM(
                    edges,
                    e -> e >= LEAST(
                              GREATEST({feature}, element_at(edges, 1)),
                              element_at(edges, -1)
                            )
                  ),
                  true
                ) - 1 + 1
          END
        )
      )
    """
    
    # Call _compute_psi_core to compute PSI
    total_psi = _compute_psi_core(
        spark=spark,
        context=context,
        source_column="risk_score",
        feature_name="risk_score",
        PARTITION_START=PARTITION_START,
        cached_df=main_query_base_df,
        quantile_edges=quantile_edges_result,
        bin_expr_sql=bin_expr_sql,
        prev_month_start=prev_month_start,
        prev_month_end=prev_month_end,
        current_month_start=current_month_start,
        current_month_end=current_month_end,
        log_suffix=""
    )
    
    # Handle None return value (convert to NaN)
    if total_psi is None:
        total_psi = float('nan')
    
    # Format as metric row matching the existing format
    # Handle NaN by using NULL in SQL
    psi_value_sql = "NULL" if (isinstance(total_psi, float) and math.isnan(total_psi)) else str(total_psi)
    psi_metric_df = spark.sql(f"""
        SELECT
            '{PARTITION_START}' AS date,
            'risk_score_psi' AS metric,
            map_from_entries(
                array(
                    named_struct('key', 'risk_score_psi', 'value', CAST({psi_value_sql} AS DOUBLE))
                )
            ) AS value,
            NOW() AS created_at
    """)
    
    # Compute feature_value_psi metric for 12 features
    FEATURE_BIN_CONFIG = {
        "trip_duration_weighted":        {"mode": "quantile",              "bins": 10},
        "driver_tenure_days":            {"mode": "quantile",              "bins": 10},
        "primary_max_weight_lbs":        {"mode": "quantile",              "bins": 10},
        "max_speed_weighted":            {"mode": "quantile",              "bins": 10},
        "trip_count_60d":                {"mode": "quantile",              "bins": 10},
        "total_events_weighted":         {"mode": "zero_plus_quantile",    "bins": 10},
        "coached_events_60d":            {"mode": "na_zero_plus_quantile", "bins": 10},
        "mobile_usage_violations_60d":   {"mode": "na_zero_plus_quantile", "bins": 10},
        "harsh_braking_60d":             {"mode": "zero_plus_quantile",    "bins": 10},
        "trip_ended_at_night_weighted":  {"mode": "zero_plus_quantile",    "bins": 10},
        "trip_count_below_freezing_60d": {"mode": "zero_plus_quantile",    "bins": 10},
        "trip_count_over_35_60d":        {"mode": "quantile",              "bins": 10},
    }
    
    feature_ls = list(FEATURE_BIN_CONFIG.keys())
    
    def compute_feature_psi(spark: SparkSession, context: AssetExecutionContext, feature: str, PARTITION_START: str, cached_df: DataFrame, prev_month_start: str, prev_month_end: str, current_month_start: str, current_month_end: str) -> float:
        """Compute PSI for a single feature using feature-specific binning"""
        
        if feature not in FEATURE_BIN_CONFIG:
            context.log.warning(f"No bin config found for feature '{feature}', skipping")
            return None
        
        cfg = FEATURE_BIN_CONFIG[feature]
        mode = cfg["mode"]
        n_bins = cfg["bins"]
        
        # Build quantile_edges based on mode
        def get_prev_month_for_quantiles(prev_month_base, source_column):
            if mode == "quantile":
                return prev_month_base
            elif mode == "zero_plus_quantile":
                return prev_month_base.filter(col(source_column) > 0)
            elif mode == "na_zero_plus_quantile":
                return prev_month_base.filter((col(source_column) > 0) & (col(source_column) != -1))
            else:
                return prev_month_base
        
        # Calculate quantile edges using DataFrame operations
        prev_month_base = cached_df.filter(
            (col("date") >= prev_month_start) & 
            (col("date") <= prev_month_end) &
            col(feature).isNotNull()
        )
        
        prev_month_for_quantiles = get_prev_month_for_quantiles(prev_month_base, feature)
        quantiles = [i / n_bins for i in range(n_bins + 1)]
        quantile_edges_result = prev_month_for_quantiles.approxQuantile(feature, quantiles, 0.0)
        
        # Build bin_expr based on mode (using expr for complex SQL expressions)
        # Wrap with GREATEST(0, ...) and LEAST(..., size(edges) - 1) to prevent negative or out-of-range bin indices
        # For 10 bins (11 edges), valid bin indices are 1-10 (1-indexed)
        if mode == "quantile":
            bin_expr_sql = """
              LEAST(size(edges) - 1,
                GREATEST(0,
                  CASE
                    WHEN ARRAY_POSITION(
                          TRANSFORM(
                            edges,
                            e -> e >= LEAST(
                                      GREATEST({feature}, element_at(edges, 1)),
                                      element_at(edges, -1)
                                    )
                          ),
                          true
                        ) = 0
                      THEN size(edges) - 2 + 1
                    ELSE ARRAY_POSITION(
                          TRANSFORM(
                            edges,
                            e -> e >= LEAST(
                                      GREATEST({feature}, element_at(edges, 1)),
                                      element_at(edges, -1)
                                    )
                          ),
                          true
                        ) - 1 + 1
                  END
                )
              )
            """
        elif mode == "zero_plus_quantile":
            bin_expr_sql = """
              CASE
                WHEN {feature} = 0 THEN 0
                ELSE LEAST(size(edges) - 1,
                  GREATEST(0,
                    CASE
                      WHEN ARRAY_POSITION(
                            TRANSFORM(
                              edges,
                              e -> e >= LEAST(
                                        GREATEST({feature}, element_at(edges, 1)),
                                        element_at(edges, -1)
                                      )
                            ),
                            true
                          ) = 0
                        THEN size(edges) - 2 + 1
                      ELSE ARRAY_POSITION(
                            TRANSFORM(
                              edges,
                              e -> e >= LEAST(
                                        GREATEST({feature}, element_at(edges, 1)),
                                        element_at(edges, -1)
                                      )
                            ),
                            true
                          ) - 1 + 1
                    END
                  )
                )
              END
            """
        elif mode == "na_zero_plus_quantile":
            # For na_zero_plus_quantile: special bins 0-1, quantile bins 2-11 (10 bins)
            # Max bin index is size(edges) = 11 (not size(edges) - 1)
            bin_expr_sql = """
              CASE
                WHEN {feature} = -1 THEN 0
                WHEN {feature} = 0 THEN 1
                ELSE LEAST(size(edges),
                  GREATEST(0,
                    CASE
                      WHEN ARRAY_POSITION(
                            TRANSFORM(
                              edges,
                              e -> e >= LEAST(
                                        GREATEST({feature}, element_at(edges, 1)),
                                        element_at(edges, -1)
                                      )
                            ),
                            true
                          ) = 0
                        THEN size(edges) - 2 + 2
                      ELSE ARRAY_POSITION(
                            TRANSFORM(
                              edges,
                              e -> e >= LEAST(
                                        GREATEST({feature}, element_at(edges, 1)),
                                        element_at(edges, -1)
                                      )
                            ),
                            true
                          ) - 1 + 2
                    END
                  )
                )
              END
            """
        else:
            context.log.warning(f"Unknown binning mode '{mode}' for feature '{feature}', skipping")
            return None
        
        return _compute_psi_core(
            spark=spark,
            context=context,
            source_column=feature,
            feature_name=feature,
            PARTITION_START=PARTITION_START,
            cached_df=cached_df,
            quantile_edges=quantile_edges_result,
            bin_expr_sql=bin_expr_sql,
            prev_month_start=prev_month_start,
            prev_month_end=prev_month_end,
            current_month_start=current_month_start,
            current_month_end=current_month_end,
            log_suffix=f" (mode={mode})"
        )
    
    # Compute PSI for each feature using cached DataFrame
    context.log.info("Computing feature_value_psi for 12 features")
    feature_psi_values = {}
    for feature in feature_ls:
        psi_val = compute_feature_psi(spark, context, feature, PARTITION_START, main_query_base_df, prev_month_start, prev_month_end, current_month_start, current_month_end)
        if psi_val is not None:
            feature_psi_values[feature] = psi_val
    
    # Format as metric row with map of all feature PSI values
    if feature_psi_values:
        # Build map entries for all features
        map_entries = ",\n                    ".join(
            f"named_struct('key', '{feat}', 'value', CAST({val} AS DOUBLE))"
            for feat, val in feature_psi_values.items()
        )
        
        feature_psi_metric_df = spark.sql(f"""
            SELECT
                '{PARTITION_START}' AS date,
                'feature_value_psi' AS metric,
                map_from_entries(
                    array(
                        {map_entries}
                    )
                ) AS value,
                NOW() AS created_at
        """)
        
        # Union with existing metrics
        result_df = main_metrics_df.union(psi_metric_df).union(feature_psi_metric_df)
    else:
        context.log.warning("No feature PSI values computed, skipping feature_value_psi metric")
        result_df = main_metrics_df.union(psi_metric_df)
    
    # Compute feature_attribution_psi metric for 12 features (SHAP values)
    # Map feature names to their corresponding _shap column names in the unified cached view
    FEATURE_TO_SHAP_COLUMN = {
        'trip_duration_weighted': 'trip_duration_weighted_shap',
        'driver_tenure_days': 'driver_tenure_days_shap',
        'primary_max_weight_lbs': 'primary_max_weight_lbs_shap',
        'max_speed_weighted': 'max_speed_weighted_shap',
        'trip_count_60d': 'trip_count_60d_shap',
        'total_events_weighted': 'total_events_weighted_shap',
        'coached_events_60d': 'coached_events_60d_shap',
        'mobile_usage_violations_60d': 'mobile_usage_violations_60d_shap',
        'harsh_braking_60d': 'harsh_braking_60d_shap',
        'trip_ended_at_night_weighted': 'trip_ended_at_night_weighted_shap',
        'trip_count_below_freezing_60d': 'trip_count_below_freezing_60d_shap',
        'trip_count_over_35_60d': 'trip_count_over_35_60d_shap',
    }
    
    def compute_feature_attribution_psi(spark: SparkSession, context: AssetExecutionContext, feature: str, PARTITION_START: str, cached_df: DataFrame, prev_month_start: str, prev_month_end: str, current_month_start: str, current_month_end: str) -> float:
        """Compute PSI for a single feature's SHAP values using quantile binning"""
        
        # Get the corresponding SHAP column name
        shap_column = FEATURE_TO_SHAP_COLUMN.get(feature)
        if not shap_column:
            context.log.warning(f"No SHAP column mapping found for feature '{feature}', skipping")
            return None
        
        # Calculate quantile edges using fixed quantile list
        prev_month_base = cached_df.filter(
            (col("date") >= prev_month_start) & 
            (col("date") <= prev_month_end) &
            col(shap_column).isNotNull()
        )
        
        # Calculate quantile edges using DataFrame operations
        quantiles = [0.00, 0.10, 0.20, 0.30, 0.40, 0.50, 0.60, 0.70, 0.80, 0.90, 1.00]
        quantile_edges_result = prev_month_base.select(
            col(shap_column).cast("double").alias(feature)
        ).approxQuantile(feature, quantiles, 0.0)
        
        # Calculate bin_index using ARRAY_POSITION with TRANSFORM (similar to compute_feature_psi)
        # This approach works consistently across PySpark versions
        # Wrap with GREATEST(0, ...) and LEAST(..., size(edges) - 1) to prevent negative or out-of-range bin indices
        # For 10 bins (11 edges), valid bin indices are 1-10 (1-indexed)
        bin_expr_sql = """
          LEAST(size(edges) - 1,
            GREATEST(0,
              CASE
                WHEN ARRAY_POSITION(
                      TRANSFORM(
                        edges,
                        e -> e >= LEAST(
                                  GREATEST({feature}, element_at(edges, 1)),
                                  element_at(edges, -1)
                                )
                      ),
                      true
                    ) = 0
                  THEN size(edges) - 2 + 1
                ELSE ARRAY_POSITION(
                      TRANSFORM(
                        edges,
                        e -> e >= LEAST(
                                  GREATEST({feature}, element_at(edges, 1)),
                                  element_at(edges, -1)
                                )
                      ),
                      true
                    ) - 1 + 1
              END
            )
          )
        """
        
        return _compute_psi_core(
            spark=spark,
            context=context,
            source_column=shap_column,
            feature_name=feature,
            PARTITION_START=PARTITION_START,
            cached_df=cached_df,
            quantile_edges=quantile_edges_result,
            bin_expr_sql=bin_expr_sql,
            prev_month_start=prev_month_start,
            prev_month_end=prev_month_end,
            current_month_start=current_month_start,
            current_month_end=current_month_end,
            log_suffix=" SHAP"
        )
    
    # Compute PSI for each feature's SHAP values using cached DataFrame
    context.log.info("Computing feature_attribution_psi for 12 features")
    feature_attribution_psi_values = {}
    for feature in feature_ls:
        psi_val = compute_feature_attribution_psi(spark, context, feature, PARTITION_START, main_query_base_df, prev_month_start, prev_month_end, current_month_start, current_month_end)
        if psi_val is not None:
            feature_attribution_psi_values[feature] = psi_val
    
    # Unpersist the cached DataFrame to free memory
    main_query_base_df.unpersist()
    
    # Format as metric row with map of all feature attribution PSI values
    if feature_attribution_psi_values:
        # Build map entries for all features
        map_entries = ",\n                    ".join(
            f"named_struct('key', '{feat}', 'value', CAST({val} AS DOUBLE))"
            for feat, val in feature_attribution_psi_values.items()
        )
        
        feature_attribution_psi_metric_df = spark.sql(f"""
            SELECT
                '{PARTITION_START}' AS date,
                'feature_attribution_psi' AS metric,
                map_from_entries(
                    array(
                        {map_entries}
                    )
                ) AS value,
                NOW() AS created_at
        """)
        
        # Union with existing metrics
        context.log.info("Combining all metrics")
        result_df = result_df.union(feature_attribution_psi_metric_df)
    else:
        context.log.warning("No feature attribution PSI values computed, skipping feature_attribution_psi metric")
    
    return result_df

