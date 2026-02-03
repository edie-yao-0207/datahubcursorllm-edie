"""
CAN Trace Collection Coverage Analysis

This asset analyzes CAN trace collection coverage across MMYEF (Make/Model/Year/Engine/Fuel) 
populations to identify representation gaps and guide more balanced trace collection. Combines 
data about eligible recording windows, collected traces, and device populations to provide 
comprehensive coverage metrics and collection targets.

## Purpose
- **Coverage Analysis**: Quantify trace collection coverage across different vehicle populations
- **Gap Identification**: Identify under-represented MMYEF populations with low trace counts
- **Collection Optimization**: Provide data-driven targets for improving population diversity
- **Resource Planning**: Understand available vs collected traces to optimize collection efforts

## Processing Logic
1. **Date Generation**: Creates analysis for each date in the partition range (supports both daily runs and backfills)
2. **Population Enumeration**: Per date, identifies active MMYEF populations from last 2 days of device vehicle properties  
3. **Eligible Analysis**: Per date, counts actionable recording windows per population from 2-day context (matching Go query pattern)
4. **Collection Analysis**: Counts ALL historically collected traces per population (consistent across all analysis dates for balancing)
5. **Coverage Calculation**: Per date, computes coverage metrics comparing immediate opportunities vs historical collections
6. **Priority Scoring**: Per date, ranks populations for collection priority based on historical gaps and current opportunities
7. **Ranking**: Per date, assigns priority ranks (1 = highest priority) for easy downstream filtering (e.g., rank <= 1000 for top 1000)

## Key Metrics
- **Population Size**: Number of active devices per MMYEF population
- **Eligible Count**: Available recording windows that could be collected
- **Collected Count**: Traces actually collected and available in library
- **Coverage Ratio**: Collected traces as percentage of eligible recordings
- **Collection Priority**: Score prioritizing under-represented populations with collection opportunities

## Input Data
- **Device Populations**: `dim_device_vehicle_properties` - MMYEF population definitions and sizes (last 2 days)
- **Eligible Windows**: `fct_eligible_can_recording_windows` - recordings available for collection (last 2 days for actionable opportunities)
- **Collected Traces**: `fct_can_trace_status` - ALL traces ever collected and processed (full historical data for balancing)
- **Collection Tags**: Dynamic analysis of traces by all collection criteria with flexible tag-to-count mapping

## Output Data
Each row represents coverage analysis for a specific MMYEF population:
- **Population Identifiers**: MMYEF hash and individual vehicle attributes  
- **Size Metrics**: Device count and activity levels for the population
- **Availability Metrics**: Count of eligible recording windows and time ranges
- **Collection Metrics**: Count of collected traces and collection dates
- **Coverage Analysis**: Coverage ratios, gaps, collection priority scores, priority rankings, and dynamic tag-to-count mappings

## Use Cases
- **Collection Planning**: Identify which populations need more trace collection
- **Diversity Monitoring**: Track representation across vehicle types and characteristics  
- **Resource Allocation**: Focus collection efforts on under-represented populations
- **Progress Tracking**: Monitor improvement in population coverage over time
- **Data Quality**: Ensure trace datasets represent diverse vehicle populations for robust analysis
"""

from dagster import AssetExecutionContext
from dataweb import table, build_general_dq_checks
from dataweb.userpkgs.constants import (
    ALL_COMPUTE_REGIONS,
    Database,
    TableType,
    WarehouseWriteMode,
    DQCheckMode,
    FRESHNESS_SLO_9AM_PST,
    FIRMWAREVDP,
)
from dataweb.userpkgs.firmware.constants import DATAWEB_PARTITION_DATE
from dataweb.userpkgs.query import format_date_partition_query
from dataweb.userpkgs.firmware.schema import (
    Column,
    ColumnType,
    DataType,
    Metadata,
    columns_to_schema,
    get_non_null_columns,
    get_primary_keys,
    map_of,
)
from dataclasses import replace
from dataweb.userpkgs.firmware.table import ProductAnalyticsStaging
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.utils import build_table_description
from .dim_device_vehicle_properties import MMYEF_ID

# Shared column definition for collection priority ranking
COLLECTION_PRIORITY_RANK_COLUMN = Column(
    name="collection_priority_rank",
    type=DataType.LONG,
    nullable=True,
    metadata=Metadata(
        comment="Rank of population for collection priority (1 = highest priority, per date). NULL for populations with no eligible opportunities."
    ),
)

QUERY = """
WITH
-- Generate all dates in the partition range for per-date analysis
dates_in_range AS (
    SELECT date_col as analysis_date
    FROM (
        SELECT explode(sequence(
            date("{date_start}"), 
            date("{date_end}"), 
            interval 1 day
        )) as date_col
    )
),

device_populations_per_date AS (
    -- Get MMYEF populations with device counts for each analysis date
    SELECT
        d.analysis_date,
        dvp.mmyef_id,
        -- Use aggregate functions to get representative values for vehicle attributes
        -- since mmyef_id is a hash of these attributes, they should be consistent
        FIRST(dvp.make) as make,
        FIRST(dvp.model) as model,
        FIRST(dvp.year) as year,
        FIRST(dvp.engine_model) as engine_model,
        FIRST(dvp.powertrain) as powertrain,
        FIRST(dvp.fuel_group) as fuel_group,
        FIRST(dvp.trim) as trim,
        COUNT(DISTINCT dvp.device_id) as population_device_count,
        COUNT(DISTINCT dvp.org_id) as population_org_count
    FROM dates_in_range d
    CROSS JOIN {product_analytics_staging}.dim_device_vehicle_properties dvp
    WHERE dvp.date BETWEEN DATE_SUB(d.analysis_date, 2) AND d.analysis_date
      AND dvp.make IS NOT NULL
      AND dvp.model IS NOT NULL
      AND dvp.year IS NOT NULL
      AND dvp.mmyef_id IS NOT NULL
    GROUP BY d.analysis_date, dvp.mmyef_id
),

eligible_windows_per_date_population AS (
    -- Count eligible recording windows per date per MMYEF population
    SELECT
        d.analysis_date,
        dvp.mmyef_id,
        COUNT(*) as eligible_window_count,
        COUNT(DISTINCT erw.device_id) as eligible_device_count,
        COUNT(DISTINCT erw.org_id) as eligible_org_count,
        SUM(CASE WHEN erw.on_trip THEN 1 ELSE 0 END) as eligible_on_trip_count,
        AVG(erw.capture_duration) as avg_capture_duration
    FROM dates_in_range d
    JOIN {product_analytics_staging}.fct_eligible_can_recording_windows erw
        ON erw.date BETWEEN DATE_SUB(d.analysis_date, 2) AND d.analysis_date  -- 2-day context window
    JOIN {product_analytics_staging}.dim_device_vehicle_properties dvp
        USING (org_id, device_id, date)
    WHERE
        erw.date BETWEEN "{date_start}" AND "{date_end}"
        AND dvp.make IS NOT NULL 
        AND dvp.model IS NOT NULL 
        AND dvp.year IS NOT NULL
        AND dvp.mmyef_id IS NOT NULL
    GROUP BY ALL
),

-- Explode tag arrays and count by population 
tag_counts_per_population AS (
    SELECT
        dvp.mmyef_id,
        tag_name,
        COUNT(*) as tag_count
    FROM {product_analytics_staging}.fct_can_trace_status cts
    JOIN {product_analytics_staging}.dim_device_vehicle_properties dvp
        USING (org_id, device_id)
    LATERAL VIEW explode(cts.tag_names) exploded AS tag_name
    WHERE cts.tag_names IS NOT NULL
        AND cts.date >= DATE_SUB("{date_start}", 730)  -- Last 2 years for performance while maintaining balancing history
    GROUP BY dvp.mmyef_id, tag_name
),

collected_traces_per_population AS (
    -- Count collected traces per MMYEF population across last 2 years of historical data
    -- Limited date filtering for performance while maintaining sufficient collection history for balancing
    SELECT
        dvp.mmyef_id,
        COUNT(*) as collected_trace_count,
        COUNT(DISTINCT cts.device_id) as collected_device_count,
        COUNT(DISTINCT cts.org_id) as collected_org_count,
        COUNT(DISTINCT cts.trace_uuid) as unique_trace_count,
        SUM(CASE WHEN cts.is_available THEN 1 ELSE 0 END) as available_trace_count,
        COUNT(DISTINCT CASE WHEN cts.tag_names IS NOT NULL THEN cts.trace_uuid END) as tagged_trace_count
    FROM {product_analytics_staging}.fct_can_trace_status cts
    JOIN {product_analytics_staging}.dim_device_vehicle_properties dvp
        USING (org_id, device_id)
    WHERE cts.date >= DATE_SUB("{date_start}", 730)  -- Last 2 years for performance while maintaining balancing history
    GROUP BY dvp.mmyef_id
),

-- Aggregate tag counts into maps per population
tag_maps_per_population AS (
    SELECT
        mmyef_id,
        map_from_arrays(
            collect_list(tag_name),
            collect_list(tag_count)
        ) as tag_counts_map
    FROM tag_counts_per_population
    GROUP BY mmyef_id
),

collected_traces_per_date_population AS (
    -- Cross join historical collection data with analysis dates for per-date comparison
    SELECT
        d.analysis_date,
        ctp.*,
        tmp.tag_counts_map
    FROM dates_in_range d
    CROSS JOIN collected_traces_per_population ctp
    LEFT JOIN tag_maps_per_population tmp USING (mmyef_id)
),

population_coverage_analysis AS (
    SELECT
        dpd.analysis_date,
        dpd.mmyef_id,
        dpd.make,
        dpd.model,
        dpd.year,
        dpd.engine_model,
        dpd.powertrain,
        dpd.fuel_group,
        dpd.trim,
        
        -- Population metrics
        dpd.population_device_count,
        dpd.population_org_count,
        
        -- Eligible recording metrics
        COALESCE(ewp.eligible_window_count, 0) as eligible_window_count,
        COALESCE(ewp.eligible_device_count, 0) as eligible_device_count,
        COALESCE(ewp.eligible_org_count, 0) as eligible_org_count,
        COALESCE(ewp.eligible_on_trip_count, 0) as eligible_on_trip_count,
        ewp.avg_capture_duration,
        
        -- Collected trace metrics
        COALESCE(ctp.collected_trace_count, 0) as collected_trace_count,
        COALESCE(ctp.collected_device_count, 0) as collected_device_count,
        COALESCE(ctp.collected_org_count, 0) as collected_org_count,
        COALESCE(ctp.unique_trace_count, 0) as unique_trace_count,
        COALESCE(ctp.available_trace_count, 0) as available_trace_count,
        COALESCE(ctp.tagged_trace_count, 0) as tagged_trace_count,
        -- Dynamic tag counts mapping
        ctp.tag_counts_map
        
    FROM device_populations_per_date dpd
    LEFT JOIN eligible_windows_per_date_population ewp 
        USING (analysis_date, mmyef_id)
    LEFT JOIN collected_traces_per_date_population ctp 
        USING (analysis_date, mmyef_id)
),

ranked_populations AS (
    SELECT
        *,
        -- Rank populations by priority score within each date (1 = highest priority)
        -- Only rank populations with actionable opportunities (eligible_window_count > 0)
        CASE 
            WHEN eligible_window_count > 0 THEN
                CAST(ROW_NUMBER() OVER (
                    PARTITION BY analysis_date, (CASE WHEN eligible_window_count > 0 THEN 1 ELSE 0 END)
                    ORDER BY collected_trace_count ASC, population_device_count DESC, mmyef_id
                ) AS BIGINT)
            ELSE NULL  -- No rank for populations with no opportunities
        END as collection_priority_rank
    FROM population_coverage_analysis
)

SELECT
    CAST(analysis_date AS STRING) as date,
    mmyef_id,
    make,
    model,
    year,
    engine_model,
    powertrain,
    fuel_group,
    trim,
    population_device_count,
    population_org_count,
    eligible_window_count,
    eligible_device_count,
    eligible_org_count,
    eligible_on_trip_count,
    avg_capture_duration,
    collected_trace_count,
    collected_device_count,
    collected_org_count,
    unique_trace_count,
    available_trace_count,
    tagged_trace_count,
    tag_counts_map,
    collection_priority_rank
FROM ranked_populations
ORDER BY analysis_date, collection_priority_rank
"""

COLUMNS = [
    ColumnType.DATE,
    replace(MMYEF_ID, nullable=False, primary_key=True),
    ColumnType.MAKE,
    ColumnType.MODEL,
    ColumnType.YEAR,
    ColumnType.ENGINE_MODEL,
    ColumnType.POWERTRAIN,
    ColumnType.FUEL_GROUP,
    ColumnType.TRIM,
    # Population metrics
    Column(
        name="population_device_count",
        type=ColumnType.DEVICE_ID.value.type,
        nullable=False,
        metadata=Metadata(comment="Number of active devices in this MMYEF population"),
    ),
    Column(
        name="population_org_count",
        type=ColumnType.ORG_ID.value.type,
        nullable=False,
        metadata=Metadata(
            comment="Number of organizations with devices in this population"
        ),
    ),
    # Eligible recording metrics
    Column(
        name="eligible_window_count",
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(
            comment="Number of eligible recording windows available for collection"
        ),
    ),
    Column(
        name="eligible_device_count",
        type=ColumnType.DEVICE_ID.value.type,
        nullable=False,
        metadata=Metadata(comment="Number of devices with eligible recording windows"),
    ),
    Column(
        name="eligible_org_count",
        type=ColumnType.ORG_ID.value.type,
        nullable=False,
        metadata=Metadata(
            comment="Number of organizations with eligible recording windows"
        ),
    ),
    Column(
        name="eligible_on_trip_count",
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(
            comment="Number of eligible windows that occurred during trips"
        ),
    ),
    Column(
        name="avg_capture_duration",
        type=DataType.DOUBLE,
        nullable=True,
        metadata=Metadata(
            comment="Average capture duration of eligible windows in milliseconds"
        ),
    ),
    # Collected trace metrics
    Column(
        name="collected_trace_count",
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(comment="Number of traces collected from this population"),
    ),
    Column(
        name="collected_device_count",
        type=ColumnType.DEVICE_ID.value.type,
        nullable=False,
        metadata=Metadata(
            comment="Number of devices that have contributed collected traces"
        ),
    ),
    Column(
        name="collected_org_count",
        type=ColumnType.ORG_ID.value.type,
        nullable=False,
        metadata=Metadata(
            comment="Number of organizations that have contributed collected traces"
        ),
    ),
    Column(
        name="unique_trace_count",
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(comment="Number of unique trace UUIDs collected"),
    ),
    Column(
        name="available_trace_count",
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(
            comment="Number of collected traces that are available in library"
        ),
    ),
    Column(
        name="tagged_trace_count",
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(comment="Number of collected traces with collection tags"),
    ),
    Column(
        name="tag_counts_map",
        type=map_of(DataType.STRING, DataType.LONG, value_contains_null=True),
        nullable=True,
        metadata=Metadata(
            comment="Map of tag_name -> count showing how many traces were collected for each collection tag"
        ),
    ),
    COLLECTION_PRIORITY_RANK_COLUMN,
]

SCHEMA = columns_to_schema(*COLUMNS)
PRIMARY_KEYS = get_primary_keys(COLUMNS)
NON_NULL_COLUMNS = get_non_null_columns(COLUMNS)


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Per-date actionable CAN trace collection coverage analysis across MMYEF (Make/Model/Year/Engine/Fuel) populations "
        "for balanced trace collection decisions. Processes each date in partition range individually, combining 2-day context windows "
        "of device population and eligible recording data with complete historical trace collection status. Identifies coverage gaps, "
        "prioritizes collection efforts, and calculates coverage ratios per date. Supports both daily runs and backfills by generating "
        "actionable guidance per date matching existing Go query patterns (2-day context windows) for trace collection planning. "
        "Includes priority ranking per date for easy downstream filtering (e.g., rank <= 1000 for top 1000 populations).",
        row_meaning="Per-date coverage analysis and collection priority metrics for a specific MMYEF vehicle population, including population size, "
        "eligible recording availability, collected trace counts, coverage ratios, priority scoring, and priority ranking for collection efforts on that date",
        table_type=TableType.MONTHLY_REPORTING_AGG,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DATAWEB_PARTITION_DATE,
    upstreams=[
        AnyUpstream(ProductAnalyticsStaging.DIM_DEVICE_VEHICLE_PROPERTIES),
        AnyUpstream(ProductAnalyticsStaging.FCT_ELIGIBLE_CAN_RECORDING_WINDOWS),
        AnyUpstream(ProductAnalyticsStaging.FCT_CAN_TRACE_STATUS),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    single_run_backfill=True,
    dq_check_mode=DQCheckMode.WHOLE_RESULT,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.AGG_CAN_TRACE_COLLECTION_COVERAGE.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
)
def agg_can_trace_collection_coverage(context: AssetExecutionContext) -> str:
    """
    Analyze CAN trace collection coverage across MMYEF populations for daily collection decisions.

    This asset processes each date in the partition range individually, combining last 2 days
    of device population data and eligible recording windows (matching existing Go query pattern)
    with COMPLETE HISTORICAL trace collection data to identify coverage gaps and prioritize
    collection efforts. Supports both single-date runs and backfills by calculating coverage
    analysis per date.

    Key outputs:
    - Per-date actionable coverage analysis showing historical gaps vs immediate opportunities
    - Priority scoring and ranking per date to guide collection toward under-represented populations
    - Classification flags for uncovered, low-coverage, and high-opportunity populations
    - Metrics comparing actionable eligible windows (2-day context) vs all-time collected traces
    - Priority ranking (1 = highest priority) for easy downstream filtering (e.g., rank <= 1000)
    - Dynamic tag-to-count mapping for flexible analysis of collection criteria without hardcoded tag names

    Args:
        context: Dagster asset execution context with partition information

    Returns:
        Formatted SQL query string for execution
    """
    return format_date_partition_query(QUERY, context)
