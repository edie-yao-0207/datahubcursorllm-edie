# VDP Trace Explorer Dashboard

Dashboard for exploring CAN trace data, signal definitions, and population coverage.

## Tabs

### Traces
Browse and filter trace UUIDs by vehicle properties (Make, Model, Year, Engine, Fuel Group, Powertrain), then visualize decoded signals for a specific trace.

**Data Sources:**
- `product_analytics_staging.dim_trace_characteristics`
- `product_analytics_staging.dim_device_vehicle_properties`
- `product_analytics_staging.fct_can_trace_decoded`

### Signal Catalog
Searchable catalog of all signal definitions across data sources (Global, J1939-DA, J1979-DA, SPS, AI).

**Data Sources:**
- `product_analytics_staging.dim_signal_catalog_definitions`

### ASC Files
Browse and download ASC trace files from S3 with vehicle metadata.

**Data Sources:**
- `product_analytics_staging.fct_can_trace_exports`
- `product_analytics_staging.dim_device_vehicle_properties`

### Trace Diversity
Population coverage analytics - cumulative trace counts by device and MMYEF over time.

**Data Sources:**
- `product_analytics_staging.dim_trace_characteristics`
- `product_analytics_staging.dim_device_vehicle_properties`
- `product_analytics_staging.fct_can_trace_recompiled`

## Generating the Dashboard

```bash
cd dataplatform/dashboards
make trace_explorer
```

## New Widget Types Used

This dashboard uses several widget types that were added to support it:

- **`text`**: Markdown content for documentation and instructions
- **`field_multi_select`**: Multi-select filter based on column values
- **`range_slider`**: Numeric range filter (e.g., year 2010-2024)

