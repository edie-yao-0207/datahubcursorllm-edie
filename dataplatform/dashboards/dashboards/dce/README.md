# DCE Program Metrics Dashboard

High-level program dashboard for Data Cost & Efficiency stakeholders, organized around the four DCE goal areas.

## Program Goal Areas

| Goal Type | Focus | Key Metrics |
|-----------|-------|-------------|
| **Diagnostic Depth** | P0 Coverage Rate - attain & maintain max coverage | % eligible vehicles reporting full P0 signal set |
| **Diagnostic Breadth** | Grow distinct signal count MoM/QoQ/YoY | Total distinct signals, # of 3P integrations |
| **Workflow Velocity** | Accelerate request → production throughput | Median time to graduation, graduation rate |
| **Resource Cost** | Reduce pipeline & manual AE cost | $ per signal decoded, pipeline efficiency |

## Dashboard Tabs

### 1. Diagnostic Depth
**Goal**: First attain, then maintain coverage targets by region/population

| Metric | Data Source | Status |
|--------|-------------|--------|
| P0 Coverage Rate by Signal | `agg_telematics_coverage_qoq_normalized` | ✅ Implemented |
| Coverage QoQ Change | `agg_telematics_coverage_qoq_normalized` | ✅ Implemented |
| Coverage by Region/Market | `agg_telematics_coverage_qoq_normalized` | ✅ Implemented |
| Coverage by Population (EVs, School Buses) | Need population-level coverage data | ⏳ Future |

### 2. Diagnostic Breadth
**Goal**: Grow unique signal count and integrations

| Metric | Data Source | Status |
|--------|-------------|--------|
| Daily Unique Signals | `agg_signal_promotion_daily_metrics` | ✅ Implemented |
| Monthly Graduations | `agg_signal_promotion_daily_metrics` | ✅ Implemented |
| Graduations by Data Source | `agg_signal_promotion_daily_metrics` by data_source | ✅ Implemented |
| # of 3P Integrations (Teknet, Aptiv) | Need integration-level tagging | ⏳ Future |
| Cumulative Distinct Signals | Need cumulative aggregation | ⏳ Future |

### 3. Workflow Velocity
**Goal**: Reduce time from request to production

| Metric | Data Source | Status |
|--------|-------------|--------|
| Avg Days to Graduation | `agg_signal_promotion_daily_metrics.avg_time_to_graduation_seconds` | ✅ Implemented |
| Avg Days in Stage | `agg_signal_promotion_daily_metrics.avg_time_in_current_stage_seconds` | ✅ Implemented |
| Graduation Rate | `agg_signal_promotion_daily_metrics.graduation_rate` | ✅ Implemented |
| Time from Request → Dashboard | Need request tracking data | ⏳ Future |

### 4. Resource Cost
**Goal**: Reduce cost per signal decoded

| Metric | Data Source | Status |
|--------|-------------|--------|
| Monthly Platform Cost | `agg_costs_rolling` + `dim_cost_hierarchies` | ✅ Implemented |
| Budget Status (vs $15K/mo) | Calculated from monthly cost | ✅ Implemented |
| MoM Cost Change | Calculated from monthly cost | ✅ Implemented |
| $ per Graduation | monthly_cost / graduations | ✅ Implemented |
| Pipeline Runtime Efficiency | Need job runtime data | ⏳ Future |

### 5. ML Model Metrics
**Goal**: Track ML-based signal reverse engineering performance

This tab has a **tab-level signal filter** (not global) to focus on specific signals.

| Metric | Data Source | Status |
|--------|-------------|--------|
| Prediction Accuracy (correctly predicted MMYEF) | `dojo.signal_reverse_engineering_metrics_v0` | ✅ Implemented |
| Promoted Device Coverage | `dojo.signal_reverse_engineering_metrics_v0` | ✅ Implemented |

**Notes**:
- Many predictions aren't evaluated yet, so prediction_count - correct_prediction_count does **not** mean incorrect predictions
- Many correct predictions are not (yet) promoted because we already have a request-based decoding

## Data Sources

| Tab | Primary Tables |
|-----|----------------|
| Diagnostic Depth | `agg_telematics_coverage_qoq_normalized`, `fct_telematics_stat_metadata` |
| Diagnostic Breadth | `agg_signal_promotion_daily_metrics` |
| Workflow Velocity | `agg_signal_promotion_daily_metrics` |
| Resource Cost | `agg_costs_rolling`, `dim_cost_hierarchies`, `agg_signal_promotion_daily_metrics` |
| ML Model Metrics | `dojo.signal_reverse_engineering_metrics_v0` |

## Related Dashboards

- **Platform Analytics Dashboard** (`platform_analytics/`): Detailed operational metrics for cost, reliability, and usage
- **Signal Promotion Metrics** (`signal_promotion/`): Detailed SPS operational metrics

## Generating the Dashboard

```bash
cd dataplatform/dashboards
make dce
```

## SLOs

| Goal Area | SLO | Target |
|-----------|-----|--------|
| Resource Cost | Monthly Platform Cost | < $15,000/month |
| Resource Cost | MoM Cost Growth | < $2,000/month |
| Workflow Velocity | Graduation Rate | > 50% |
| Workflow Velocity | Time to Graduation | < 14 days |

## Future Enhancements

1. **Diagnostic Depth**: Add population-level coverage (EVs, School Buses, etc.)
2. **Diagnostic Breadth**: Add cumulative distinct signal tracking, 3P integration counts
3. **Workflow Velocity**: Add request-to-dashboard tracking if data becomes available
4. **Resource Cost**: Add pipeline runtime efficiency metrics
