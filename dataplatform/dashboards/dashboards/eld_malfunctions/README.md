# ELD Malfunctions Dashboard

This dashboard provides comprehensive analysis of ELD (Electronic Logging Device) malfunctions and diagnostics across various dimensions including cable, product, vehicle population (MMYEF), and malfunction type.

## Overview

The dashboard surfaces all malfunctions from `kinesisstats_history.osDEldEvent` and provides:
- High-level summaries and trends
- Breakdowns by cable, product, and vehicle population
- Comparison views across dimensions
- Time series trends

## Data Source

- **Primary Table**: `kinesisstats_history.osDEldEvent` (using history table for access to older data)
- **Dimension Tables**:
  - `product_analytics.dim_device_dimensions` (for cable_id, cable_name, product_id, product_name)
  - `product_analytics_staging.dim_device_vehicle_properties` (for MMYEF)

## Malfunction Codes

The dashboard translates enumeration codes to human-readable labels:

### Malfunctions
- `P` - Power Malfunction
- `E` - Engine Sync Malfunction
- `T` - Timing Malfunction
- `L` - Positioning Malfunction
- `R` - Data Recording Malfunction
- `S` - Data Transfer Malfunction
- `O` - Other ELD Malfunction

### Diagnostics
- `1` - Power Diagnostic
- `2` - Engine Sync Diagnostic
- `3` - Missing Required Data Diagnostic
- `4` - Data Transfer Diagnostic
- `5` - Unidentified Driving Diagnostic
- `6` - Other ELD Diagnostic

## Filters

The dashboard includes filters for:
- **Date Range**: Default 30 days
- **Cable**: Multi-select filter by cable_id/cable_name
- **Product**: Multi-select filter by product_id/product_name
- **Malfunction Type**: Multi-select filter by malfunction type

## Tabs

1. **Overview**: High-level summary with total counts, breakdown by type, and daily trends
2. **By Dimension**: Detailed breakdowns by cable, product, and MMYEF
3. **Comparison**: Cross-tabulation views for comparing cable and product combinations
4. **Trends**: Time series analysis of malfunctions over time

## Generating the Dashboard

```bash
cd dataplatform/dashboards
python3 generate_from_config.py dashboards/eld_malfunctions/dashboard_config.json > output/ELD_Malfunctions_Dashboard.lvdash.json
```

