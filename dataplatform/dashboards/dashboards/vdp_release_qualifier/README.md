# VDP Release Qualifier Dashboard

This dashboard configuration is generated from the original `VDP Release Qualifier.lvdash.json` file.

## Overview

The VDP Release Qualifier dashboard monitors critical diagnostics (P0) across different product areas:

- **Rollout Status**: Device deployment tracking by product
- **P0 Overview**: Core diagnostic metrics (engine state, fuel, odometer, etc.)
- **Fuel**: Fuel consumption and related metrics
- **ELD**: Electronic Logging Device compliance metrics
- **EcoDriving**: Driver behavior metrics (braking, coasting, etc.)
- **EV**: Electric vehicle specific metrics
- **Internal**: CAN decode coverage, fault rates, and system health
- **Consistency**: Data consistency checks

## Architecture

This dashboard uses a **parameterized central dataset pattern** where:

1. A single `product_analytics.agg_population_metrics` table stores all metrics
2. Each SQL query filters by specific `metric_name` values
3. Results are grouped by population dimensions (product, make, model, year, etc.)

### Global Filters

| Filter | Description |
|--------|-------------|
| `date_range` | Date range for data (default: -60d to today) |
| `population_type` | Grouping dimension (e.g., `product_name`, `make model year`) |
| `market` | Geographic market filter (NA, EMEA, etc.) |
| `product_name` | Hardware product filter (VG54-NA, VG34, etc.) |
| `make`, `model`, `year` | Vehicle filters |
| `engine_model`, `engine_type`, `fuel_type` | Engine specification filters |

## Query Structure

All queries follow a similar pattern:

```sql
SELECT
  CAST(stats.date AS DATE) AS date,
  concat_ws(' ', ...) AS key,  -- Grouping key for charts
  stats.<metric_field>         -- avg, count, coverage, etc.
FROM product_analytics.agg_population_metrics stats
JOIN product_analytics.dim_populations pops
  ON pops.population_id = stats.population_id AND pops.date = stats.date
WHERE array_contains(split(:metric_names, ','), stats.metric_name)
  AND stats.date BETWEEN :date_range.min AND :date_range.max
  AND pops.population_type = :population_type
  -- Additional population filters...
```

## Metric Categories

### P0 Diagnostics
- `percent_invalid_*` - Percentage of invalid stat values
- `count_invalid_*` - Count of invalid stat values
- `coverage_*` - Device coverage for each stat

### Ratio Metrics (Percentiles)
- `ratio_odometer_to_gps_distance` - Odometer accuracy
- `ratio_delta_fuel_consumed_to_gps_distance` - Fuel efficiency
- `ratio_time_coasting_to_gps_distance` - Coasting behavior
- `ratio_harsh_brake_count_to_gps_distance` - Braking events

## Building the Dashboard

```bash
cd dataplatform/dashboards
python3 generate_from_config.py vdp_release_qualifier/dashboard_config.json > output/VDP\ Release\ Qualifier.lvdash.json
```

## Notes

- The original dashboard uses complex multi-select parameter bindings that may need adjustment
- Some widgets reference percentile/histogram data via `LATERAL VIEW explode()`
- The `key` field is constructed dynamically to support chart grouping

