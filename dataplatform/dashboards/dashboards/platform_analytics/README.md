# Data Cost & Efficiency (DCE) Dashboards

SQL queries for monitoring data platform costs, usage, reliability, and efficiency metrics.

**Note**: Dashboard queries point to `product_analytics_staging` for testing. Update to production database when ready.

## 📊 Dashboard Structure

```
dce/
├── cost/           # Cost monitoring and optimization
│   ├── overview/   # High-level cost snapshot
│   ├── anomalies/  # Cost spike detection
│   ├── attribution/# Cost breakdown by team/service
│   └── efficiency/ # Cost per user, idle tables
│
├── usage/          # User adoption and engagement
│   ├── overview/   # High-level usage snapshot
│   └── engagement/ # Power users, engagement scores, growth trends
│
└── reliability/    # Data freshness and SLA compliance
    ├── overview/   # High-level reliability snapshot
    ├── trends/     # SLA compliance over time
    └── tables/     # Per-table freshness details
```

---

### Cost Dashboard
Monitor and optimize data platform spending

**Overview Tab** - High-level snapshot
- `monthly_cost_trend.sql` - Monthly cost with MoM growth and budget status
- `cost_by_time_window.sql` - Compare costs across different time horizons
- `top_cost_summary.sql` - Executive summary metrics

**Anomalies Tab** - Cost spike detection
- `critical_table_increases.sql` - Services with critical cost spikes
- `anomaly_summary.sql` - Daily rollup of all anomalies
- `persistent_anomalies.sql` - Services anomalous for multiple days

**Attribution Tab** - Cost breakdown
- `cost_by_team.sql` - Cost breakdown by owning team
- `cost_by_product_feature.sql` - Cost by product area and dataplatform feature
- `top_tables_by_cost.sql` - Top 50 most expensive services

**Efficiency Tab** - Optimization opportunities
- `cost_per_user.sql` - Cost per active user metrics
- `idle_table_cost.sql` - Services with cost but no usage
- `cost_efficiency_trends.sql` - Services where cost rises but usage falls

---

### Usage Dashboard
Track user adoption and service utilization

**Overview Tab** - High-level snapshot
- `usage_snapshot.sql` - Scorecards: MAU, queries, tables, growth
- `usage_summary.sql` - DAU/WAU/MAU and query volume trends
- `usage_growth.sql` - User and query growth rates

**Engagement Tab** - User behavior deep dive
- `platform_power_users.sql` - Power users by total platform interactions (SQL + dashboards)
- `platform_engagement_scores.sql` - Engagement scoring matrix with SQL/dashboard scores
- `platform_adoption_breakdown.sql` - Platform adoption by department/job family
- `platform_usage_trends.sql` - DAU/WAU/MAU trends with cross-platform adoption
- `most_accessed_tables.sql` - Top services by user count
- `dashboard_usage.sql` - FirmwareVDP dashboard activity
- `dashboard_usage_summary.sql` - Dashboard engagement summary

---

### Reliability Dashboard
Monitor data freshness and SLA compliance

**Overview Tab** - High-level snapshot
- `reliability_snapshot.sql` - Scorecards: Fresh/Warning/Stale counts, SLA %

**Trends Tab** - SLA compliance over time
- `sla_trends.sql` - SLA compliance and latency trends with charts

**Tables Tab** - Per-table details
- `table_freshness.sql` - Current freshness status for all tables
- `sla_breaches.sql` - Tables that have breached 2-day SLA

---

## 🔗 Data Sources

All queries use tables from `product_analytics_staging` (testing) / `product_analytics_staging` (production):

| Table | Description |
|-------|-------------|
| `dim_billing_services` | Date-partitioned billing service combinations with team/region/product_group |
| `dim_cost_hierarchies` | Date-partitioned aggregation populations with cost_hierarchy_id and dimension details |
| `dim_user_hierarchies` | User/employee hierarchy dimension - join on date + user_hierarchy_id |
| `dim_tables` | Table registry with team ownership - join on date + table_id |
| `fct_table_cost_daily` | Daily costs by service from billing |
| `fct_table_user_queries` | Atomic SQL usage: (date, table_id, employee_id) → query_count |
| `fct_dashboard_usage_daily` | Daily dashboard usage per employee + dashboard |
| `agg_costs_rolling` | Cost metrics with rolling windows - join on date + cost_hierarchy_id |
| `fct_table_cost_anomalies` | Cost anomaly detection - join on date + cost_hierarchy_id |
| `agg_table_usage_rolling` | Service usage metrics - join on date + cost_hierarchy_id |
| `agg_platform_usage_rolling` | Unified SQL + dashboard usage - join on date + user_hierarchy_id |
| `fct_partition_landings` | Partition landing events with SLA metrics |
| `agg_landing_reliability_rolling` | Rolling reliability metrics per table |

### Key Columns

**Cost hierarchy columns** (in `dim_cost_hierarchies`):
- `cost_hierarchy_id` - xxhash64 for efficient joining (primary key)
- `grouping_columns` - Dot-separated column names (e.g., `region.team.service`)
- `hierarchy_label` - Dot-separated values (e.g., `us-west-2.firmwarevdp.dataweb`)
- Dimension columns: `region`, `team`, `product_group`, `dataplatform_feature`, `service`

**User hierarchy columns** (in `dim_user_hierarchies`):
- `user_hierarchy_id` - xxhash64 for efficient joining (primary key)
- `grouping_columns` - Dot-separated column names (e.g., `job_family_group.job_family.department.employee_email`)
- `hierarchy_label` - Dot-separated values (e.g., `Software.Firmware.100000 - Engineering`)
- Dimension columns: `job_family_group`, `job_family`, `department`, `employee_email`

**Cost aggregate tables** (`agg_costs_rolling`, `agg_table_usage_rolling`, `fct_table_cost_anomalies`):
- `date` - Primary key
- `cost_hierarchy_id` - Primary key, join to `dim_cost_hierarchies` for dimension details

**User aggregate tables** (`agg_platform_usage_rolling`):
- `date` - Primary key
- `user_hierarchy_id` - Primary key, join to `dim_user_hierarchies` for user details
- Nested struct by domain:
  - `monthly.users.*` - User segmentation: `total`, `sql_users`, `dashboard_users`, `sql_only`, `dashboard_only`, `both`
  - `monthly.sql.*` - SQL metrics: `queries`, `unique_tables`
  - `monthly.dashboard.*` - Dashboard metrics: `events`, `views`, `clones`, `edits`, `unique_dashboards`
  - `monthly.combined.*` - Combined: `total_interactions`, `active_days`, `delta_users`
- Includes `top_dashboards_daily` and `top_dashboards_monthly` arrays

**Hierarchy dimension population counts** (new):
- All hierarchy tables (`dim_cost_hierarchies`, `dim_user_hierarchies`, `dim_dashboard_hierarchies`) now include population counts
- These help understand hierarchy cardinality (e.g., `employee_count`, `service_count`, `dashboard_count`)
- Example: `distinct_teams`, `distinct_departments`, `distinct_workspaces`

**Reliability tables**:
- `fct_partition_landings` - Raw landing events with `meets_1d_sla`, `meets_2d_sla`
- `agg_landing_reliability_rolling` - Per-table reliability with `daily`, `weekly`, `monthly` struct

---

## 📖 Query Pattern

All dashboard queries follow this pattern:

```sql
-- Join cost aggregate table to dim_cost_hierarchies for dimension details
SELECT 
  c.date,
  h.team,
  h.service,
  c.monthly.cost AS monthly_cost
FROM product_analytics_staging.agg_costs_rolling c
JOIN product_analytics_staging.dim_cost_hierarchies h
  USING (date, cost_hierarchy_id)
WHERE h.service IS NOT NULL  -- Service-level groupings
  AND h.team = 'firmwarevdp'
  AND c.date BETWEEN :date.min AND :date.max
```

### Joining Cost and Usage

```sql
-- Join costs to usage on cost_hierarchy_id + date
SELECT 
  c.date,
  c.monthly.cost / NULLIF(u.monthly.unique_users, 0) AS cost_per_user
FROM product_analytics_staging.agg_costs_rolling c
JOIN product_analytics_staging.agg_table_usage_rolling u
  USING (date, cost_hierarchy_id)
JOIN product_analytics_staging.dim_cost_hierarchies h
  USING (date, cost_hierarchy_id)
WHERE h.grouping_columns = 'team'
  AND h.team = 'firmwarevdp'
```

### Joining Platform Usage

```sql
-- Join platform usage to dim_user_hierarchies for employee details
-- Note: metrics are nested by domain (users/sql/dashboard/combined)
SELECT 
  p.date,
  h.job_family_group,
  h.job_family,
  p.monthly.users.total AS total_active_users,
  p.monthly.users.both AS power_users,
  p.monthly.sql.queries AS sql_queries,
  p.monthly.dashboard.events AS dashboard_events
FROM product_analytics_staging.agg_platform_usage_rolling p
JOIN product_analytics_staging.dim_user_hierarchies h
  USING (date, user_hierarchy_id)
WHERE h.grouping_columns = 'job_family_group.job_family'  -- Job family level
  AND p.date BETWEEN :date.min AND :date.max
```

---

## 🚀 Getting Started

1. **Copy queries** to Databricks SQL
2. **Adjust parameters**:
   - Budget thresholds in cost queries
   - Date ranges as needed
   - Team filters for specific views
3. **Create dashboards** organized by tabs
4. **Set up alerts** for SLO breaches
5. **Schedule refreshes** (recommend daily at 9am)
