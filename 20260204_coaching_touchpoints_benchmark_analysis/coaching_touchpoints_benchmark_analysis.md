# Coaching Touchpoints Benchmark Analysis

**Date:** February 4, 2026  
**Author:** Data Analytics  
**Status:** Draft

---

## Goal

Determine what a "healthy" level of coaching touchpoints looks like by comparing distributions across pre-assigned driver risk categories and comparing customers with ARR < 100k vs ARR ≥ 100k.

---

## Definitions

| Term | Definition |
|------|------------|
| **Coaching touchpoints** | Count of distinct `coachable_item_uuid` where `share_type IN (1, 2)` — Self-Review OR Manager-Led Coaching. Excludes training. |
| **Weekly period** | Jan 26 – Feb 1, 2026 |
| **Monthly period** | Jan 1 – Jan 31, 2026 |
| **Driver risk category** | From `product_analytics.driver_risk_classification` as of Feb 1, 2026 (LOW, MEDIUM, HIGH) |
| **ARR segment** | Derived from `account_arr_segment` in `datamodel_core.dim_organizations`: `ARR < 100k` or `ARR >= 100k` |

---

## Data Sources

| Source | Purpose |
|--------|---------|
| `coachingdb_shards.coaching_sessions` | Coaching session metadata |
| `coachingdb_shards.coachable_item` | Individual coaching items per session |
| `coachingdb_shards.coachable_item_share` | Share type (1=Self-Review, 2=Manager-Led) |
| `product_analytics.driver_risk_classification` | Driver risk category (LOW/MEDIUM/HIGH) |
| `datamodel_core.dim_organizations` | Account ARR segment |

---

## Summary Results

**Total drivers with coaching touchpoints:** 45,008

### ARR < 100k (15,501 drivers)

| Risk Level | N Drivers | % of Segment | p50 Monthly | p90 Monthly | Mean Monthly |
|------------|-----------|--------------|-------------|-------------|--------------|
| LOW | 304 | 2.0% | 1.0 | 8 | 4.9 |
| MEDIUM | 2,103 | 13.6% | 2.0 | 10 | 5.1 |
| HIGH | 13,094 | 84.5% | 2.0 | 21 | 9.4 |

### ARR >= 100k (29,507 drivers)

| Risk Level | N Drivers | % of Segment | p50 Monthly | p90 Monthly | Mean Monthly |
|------------|-----------|--------------|-------------|-------------|--------------|
| LOW | 464 | 1.6% | 1.0 | 4 | 2.2 |
| MEDIUM | 3,976 | 13.5% | 1.0 | 7 | 4.0 |
| HIGH | 25,067 | 85.0% | 2.0 | 14 | 7.9 |

---

## Visualizations

![Coaching Analysis Charts](coaching_analysis_charts.png)

**Left chart:** Monthly coaching touchpoints by risk level and ARR segment, with mean (blue diamond) and p90 (purple triangle) markers.

**Right chart:** Distribution of drivers by risk level within each ARR segment, with driver counts labeled.

---

## Key Takeaways

### 1. Typical vs High Coaching by Risk Group

- **HIGH-risk drivers** receive the most coaching: p50 = 2 monthly, p90 = 14-21 monthly
- **LOW-risk drivers** have minimal coaching: p50 = 1 monthly, p90 = 4-8 monthly
- **Benchmark for HIGH-risk:** 2+ monthly touchpoints at median, 14-21+ at p90 indicates a high-performing coaching program

### 2. ARR Segment Differences

- **ARR < 100k customers coach more intensively:** HIGH-risk drivers receive ~1.2x more monthly touchpoints (mean 9.4 vs 7.9) compared to ARR >= 100k
- This may indicate smaller customers have more concentrated driver pools or more hands-on coaching programs

### 3. Risk Distribution is Consistent

- ~85% HIGH, ~13.5% MEDIUM, ~2% LOW across both ARR segments
- The risk classification model applies uniformly regardless of customer size

### 4. Coaching is Risk-Aligned

- HIGH-risk drivers receive 2-3x more coaching touchpoints than LOW-risk drivers
- This is a healthy signal that coaching efforts are appropriately targeting higher-risk populations

---

## SQL Query

```sql
WITH coaching_touchpoints AS (
  SELECT 
    cs.org_id, 
    ci.driver_id,
    COUNT(DISTINCT CASE 
      WHEN ci.date BETWEEN '2026-01-26' AND '2026-02-01' 
      THEN ci.uuid 
    END) AS touchpoints_weekly,
    COUNT(DISTINCT CASE 
      WHEN ci.date BETWEEN '2026-01-01' AND '2026-01-31' 
      THEN ci.uuid 
    END) AS touchpoints_monthly
  FROM coachingdb_shards.coaching_sessions cs
  JOIN coachingdb_shards.coachable_item ci 
    ON cs.uuid = ci.coaching_session_uuid
  JOIN coachingdb_shards.coachable_item_share cis 
    ON ci.uuid = cis.coachable_item_uuid
  WHERE cis.share_type IN (1, 2)  -- Self-Review or Manager-Led
    AND ci.date BETWEEN '2026-01-01' AND '2026-02-01'
    AND ci.driver_id IS NOT NULL 
    AND ci.driver_id != 0
  GROUP BY cs.org_id, ci.driver_id
),
risk_classification AS (
  SELECT org_id, driver_id, classification AS driver_risk_category
  FROM product_analytics.driver_risk_classification
  WHERE date = '2026-02-01'
),
org_arr AS (
  SELECT 
    org_id,
    CASE 
      WHEN account_arr_segment IN ('100k - 500K', '500K - 1M', '1M+') 
      THEN 'ARR >= 100k' 
      ELSE 'ARR < 100k' 
    END AS org_arr_segment
  FROM datamodel_core.dim_organizations
  WHERE date = '2026-02-01' 
    AND is_paid_customer = TRUE
)
SELECT 
  rc.driver_risk_category,
  oa.org_arr_segment,
  COUNT(*) AS n_drivers,
  ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY oa.org_arr_segment), 2) AS pct_within_arr_segment,
  ROUND(PERCENTILE(ct.touchpoints_monthly, 0.50), 2) AS p50_monthly,
  ROUND(PERCENTILE(ct.touchpoints_monthly, 0.90), 2) AS p90_monthly,
  ROUND(AVG(ct.touchpoints_monthly), 2) AS mean_monthly
FROM coaching_touchpoints ct
LEFT JOIN risk_classification rc 
  ON ct.org_id = rc.org_id AND ct.driver_id = rc.driver_id
LEFT JOIN org_arr oa 
  ON ct.org_id = oa.org_id
WHERE rc.driver_risk_category IS NOT NULL 
  AND oa.org_arr_segment IS NOT NULL
GROUP BY rc.driver_risk_category, oa.org_arr_segment
ORDER BY oa.org_arr_segment, rc.driver_risk_category
```

---

## Files

| File | Description |
|------|-------------|
| `coaching_analysis_data.csv` | Driver-level data (45,008 rows) |
| `coaching_analysis_charts.png` | Visualization charts |
| `coaching_touchpoints_benchmark_analysis.md` | This document |

---

## Appendix: Coaching Touchpoint Definitions

From `backend/dataplatform/dataweb/userpkgs/safety_constants.py`:

| share_type | Coaching Type | Description |
|------------|---------------|-------------|
| 1 | Self-Review (Self-Coaching) | Driver reviews the safety event on their own via the app |
| 2 | Manager-Led Coaching | A coach/manager reviews the event with the driver |

**Note:** Training (`product_type = 3` from `formsdb`) is tracked separately and is NOT included in coaching touchpoints.
