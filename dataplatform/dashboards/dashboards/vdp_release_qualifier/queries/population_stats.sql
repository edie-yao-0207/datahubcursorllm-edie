-- =============================================================================
-- Dashboard: VDP Release Qualifier
-- Base Query: Population Stats
-- =============================================================================
-- This is the base query used by most widgets in the dashboard.
-- Each widget queries this dataset with different metric_name parameters.
-- =============================================================================

SELECT
  CAST(stats.date AS DATE) AS date,
  stats.metric_name,
  pops.population_type,

  -- Dimensions - used as grouping key for charts
  concat_ws(
    ' ', array_except(
        array(
        stats.metric_name,
        CAST(pops.org_id AS STRING),
        pops.make,
        pops.model,
        pops.year,
        pops.engine_model,
        pops.fuel_type,
        pops.product_name,
        pops.cable_name,
        pops.market,
        pops.build,
        pops.product_program_id,
        pops.rollout_stage_id,
        CASE WHEN entry.x IS NOT NULL THEN format_string("X(%s)", entry.x) ELSE NULL END
      ), array(NULL)
    )
  ) AS key,

  pops.product_name,

  -- Aggregate fields
  stats.count,
  stats.sum,
  stats.min,
  stats.max,
  stats.avg,
  stats.variance,
  stats.stddev,
  stats.mean,
  stats.median,
  stats.mode,
  stats.first,
  stats.last,
  stats.kurtosis,
  entry.x,
  entry.y,
  stats.sum / pops.count_if_include_in_agg_metrics AS coverage

FROM product_analytics.agg_population_metrics stats
JOIN product_analytics.dim_populations pops
  ON pops.population_id = stats.population_id AND pops.date = stats.date

LATERAL VIEW
  explode(
    CASE 
      WHEN :graph_type = 'percentile' THEN stats.percentile
      WHEN :graph_type = 'histogram' THEN stats.histogram 
      ELSE ARRAY(NULL) 
    END
  ) 
AS entry

WHERE array_contains(split(:metric_name, ','), stats.metric_name)
  AND stats.date BETWEEN :date_range.min AND :date_range.max
  AND pops.population_type = :population_type

  AND (entry.x >= CAST(:x_min AS DOUBLE) OR :x_min = 'null')
  AND (entry.x <= CAST(:x_max AS DOUBLE) OR :x_max = 'null')

  AND (
    array_contains(:org_id, CAST(pops.org_id AS STRING))
    OR NOT pops.is_grouped_org_id
  )
  AND (
    array_contains(:make, pops.make)
    OR NOT pops.is_grouped_make
  )
  AND (
    array_contains(:model, pops.model)
    OR NOT pops.is_grouped_model
  )
  AND (
    array_contains(:year, CAST(pops.year AS STRING))
    OR NOT pops.is_grouped_year
  )
  AND (
    array_contains(:engine_model, pops.engine_model)
    OR NOT pops.is_grouped_engine_model
  )
  AND (
    array_contains(:engine_type, pops.engine_type)
    OR NOT pops.is_grouped_engine_type
  )
  AND (
    array_contains(:fuel_type, pops.fuel_type)
    OR NOT pops.is_grouped_fuel_type
  )
  AND (
    array_contains(:product_name, CAST(pops.product_name AS STRING))
    OR NOT pops.is_grouped_product_name
  )
  AND (
    array_contains(:cable_name, pops.cable_name)
    OR NOT pops.is_grouped_cable_name
  )
  AND (
    array_contains(:build, pops.build)
    OR NOT pops.is_grouped_build
  )
  AND (
    array_contains(:product_program_id, pops.product_program_id)
    OR NOT pops.is_grouped_product_program_id
  )
  AND (
    array_contains(:rollout_stage_id, CAST(pops.rollout_stage_id AS STRING))
    OR NOT pops.is_grouped_rollout_stage_id
  )
  AND (
    array_contains(:market, pops.market)
    OR NOT pops.is_grouped_market
  )

ORDER BY stats.date

