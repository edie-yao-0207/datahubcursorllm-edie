# CAN Trace Collection Dashboard

Dashboard for monitoring the CAN trace collection pipeline, tracking candidate generation, collection progress, quota enforcement, and diversity metrics.

## Overview

This dashboard provides comprehensive visibility into the CAN trace collection pipeline, including:

- **Candidate Generation**: Daily candidate counts by query type (representative, training, inference, seatbelt, stream_id training)
- **Collection Coverage**: MMYEF and device-level coverage tracking
- **Quota Status**: Quota enforcement and progress monitoring
- **Diversity Metrics**: MMYEF and device diversity tracking
- **Stream ID Coverage**: Per-stream_id training coverage
- **Reverse Engineering**: Coverage gaps and training/inference metrics

## Data Sources

Primary tables used:
- `product_analytics_staging.fct_can_trace_representative_candidates`
- `product_analytics_staging.fct_can_trace_reverse_engineering_candidates`
- `product_analytics_staging.fct_can_trace_training_by_stream_id`
- `product_analytics_staging.fct_can_trace_seatbelt_trip_start_candidates`
- `product_analytics_staging.fct_can_traces_required`
- `product_analytics_staging.agg_tags_per_mmyef`
- `product_analytics_staging.agg_tags_per_device`
- `product_analytics_staging.agg_mmyef_stream_ids`
- `product_analytics_staging.dim_device_vehicle_properties`
- `product_analytics_staging.dim_mmyef_vehicle_characteristics`

## Tabs

### Overview
High-level KPIs and trends including total candidates, collected traces, active MMYEFs, and collection rate trends.

### Candidates
Candidate generation metrics by query type, ranking distribution, and top candidates.

### Collection Coverage
MMYEF coverage trends, snapshot views with tag breakdowns, and top MMYEFs by collection count.

### Quota Status
Quota enforcement metrics, progress per MMYEF, and fulfillment rates by query type.

### Diversity Metrics
MMYEF and device diversity tracking, collection distribution histograms, and underrepresented populations.

### Stream ID Coverage
Stream ID coverage statistics, underrepresented stream IDs, and training candidates by stream ID.

### Reverse Engineering
Coverage gap analysis, training vs inference candidate counts, and gap distribution metrics.

## Key Metrics

- **Quota Thresholds**: Representative quota = 10 traces per MMYEF, Stream ID training quota = 200 traces per stream_id
- **Tag Names**: 
  - `can-set-representative-0`: Representative dataset candidates
  - `can-set-training-0`: Reverse engineering training candidates
  - `can-set-inference-0`: Reverse engineering inference candidates
  - `can-set-training-stream-id-0`: Per-stream_id training candidates
  - `can-set-seatbelt-trip-start-0`: Seatbelt trip start candidates

## Usage

The dashboard uses a default date range of 90 days. Adjust the date range filter to analyze different time periods.

All tables are date-partitioned. Snapshot views use the latest partition date within the selected range.

