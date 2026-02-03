# Eco Driving Segmented Events Dashboard

This dashboard monitors Eco Driving segmented events from kinesisstats, providing insights into driving behavior patterns and data quality.

## Overview

The dashboard tracks 5 types of eco driving segmented events:

| Event Type | Source Table | Description |
|------------|--------------|-------------|
| **Cruise Control** | `kinesisstats.osDEcoDrivingEventCruiseControl` | Vehicle using cruise control |
| **Hard Acceleration** | `kinesisstats.osDEcoDrivingEventHardAcceleration` | Aggressive acceleration events |
| **Overspeed** | `kinesisstats.osDEcoDrivingEventOverSpeed` | Vehicle exceeding speed thresholds |
| **Coasting** | `kinesisstats.osDEcoDrivingEventCoasting` | Vehicle coasting (fuel-efficient driving) |
| **Wear-Free Braking** | `kinesisstats.osDEcoDrivingEventWearFreeBraking` | Regenerative or engine braking events |

## Tabs

### Overview
High-level summary metrics including:
- Total events and unique devices across all types
- Valid event rate
- Event and device counts by type
- Daily trends

### Data Health
Data quality monitoring:
- Valid vs invalid row counts per event type
- Daily invalid rate trends
- Health summary table

### Duration Analysis
Event duration statistics:
- Duration statistics (min, max, avg, median, p95) by type
- Duration distribution histograms for each event type (0.5s buckets)

### Comparison
Cross-event-type comparison:
- Full comparison table with volume, quality, and duration metrics
- Stacked bar chart of valid vs invalid rows

## Data Quality Definitions

- **Valid Events**: Events where `proto_value IS NOT NULL`
- **Invalid Events**: Events where `proto_value IS NULL` AND (`is_databreak = FALSE` OR `is_databreak IS NULL`)
- **Data Breaks**: Events where `is_databreak = TRUE` (intentional gaps)

## Usage

### Building the Dashboard

```bash
cd dataplatform/dashboards
make eco_driving_segmented_events
```

### Filters

- **Date Range**: Filter events by date (default: last 30 days)
- **Event Type**: Filter to specific event types (default: all)

## Source Notebook

This dashboard was productionized from the exploratory notebook:
`Eco Driving Segmented Events Data (1).ipynb`

## Queries Directory Structure

```
queries/
├── lookups/
│   └── event_types.sql           # Event type dropdown values
├── overview/
│   ├── total_events_by_type.sql  # Total event counter
│   ├── unique_devices_total.sql  # Unique device counter
│   ├── valid_event_rate.sql      # Valid rate counter
│   ├── events_by_type_summary.sql    # Bar chart
│   ├── devices_by_type_summary.sql   # Bar chart
│   ├── daily_device_trend.sql    # Line chart
│   └── daily_event_trend.sql     # Line chart
├── data_health/
│   ├── health_summary_by_type.sql    # Table
│   ├── daily_invalid_rate.sql    # Line chart
│   └── daily_valid_count.sql     # Line chart
├── duration/
│   ├── duration_stats_by_type.sql    # Table
│   ├── duration_histogram_cruise_control.sql
│   ├── duration_histogram_hard_acceleration.sql
│   ├── duration_histogram_overspeed.sql
│   ├── duration_histogram_coasting.sql
│   └── duration_histogram_wear_free_braking.sql
└── comparison/
    ├── full_comparison_summary.sql   # Table
    └── valid_vs_invalid_stacked.sql  # Stacked bar
```
