-- Rollout Stats - specialized query for rollout charts
-- Shows device counts by product/build combination
SELECT
    CAST(stats.date AS DATE) AS date,
    format_string('%s B(%s)', pops.product_name, pops.build) AS key,
    stats.count
FROM product_analytics.agg_population_metrics stats
JOIN product_analytics.dim_populations pops
    ON stats.date = pops.date AND stats.population_id = pops.population_id
WHERE stats.metric_name = 'hub_server_device_heartbeat_build_count'
    AND pops.population_type = 'build product_name'
    AND pops.product_name = :product_name
    AND stats.count >= 100
    AND stats.date BETWEEN :date_range.min AND :date_range.max
ORDER BY stats.date

