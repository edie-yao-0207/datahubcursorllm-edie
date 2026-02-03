# Reliability Dashboard Queries

Data freshness and SLA compliance monitoring for managed tables.

## Structure

### `overview/`
**High-level snapshot** - Quick health check scorecards
- `reliability_snapshot.sql` - Fresh/Warning/Stale counts, SLA %, avg latency

### `trends/`
**Time-series analysis** - How reliability changes over time
- `sla_trends.sql` - SLA compliance and latency trends with charts

### `tables/`
**Per-table details** - Drill down to specific tables
- `table_freshness.sql` - Current freshness status for all tables
- `sla_breaches.sql` - Tables that have breached 2-day SLA

## Key Metrics

| Metric | Target | Description |
|--------|--------|-------------|
| 2-Day SLA % | >95% | Partitions landing within 2 days of partition date |
| 1-Day SLA % | >80% | Partitions landing within 1 day of partition date |
| Avg Latency | <1.0 days | Average days between partition date and landing |
| Fresh Tables | >90% | Tables with recent partition landings |

## Data Sources

- `agg_landing_reliability_rolling` - Rolling reliability metrics per table
- `fct_partition_landings` - Raw partition landing events

