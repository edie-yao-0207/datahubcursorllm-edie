-- =============================================================================
-- Dashboard: DCE Program Metrics
-- Tab: CAN Pipeline Impact
-- View: Data Compression Snapshot
-- =============================================================================
-- Visualization: Counter
-- Description: Current compression ratio and deduplication stats (all-time historical data)
-- Note: This shows all-time totals to match impact metrics document
-- =============================================================================

WITH raw_frames AS (
  SELECT 
    COUNT(*) AS raw_count,
    SUM(LENGTH(payload)) AS raw_bytes
  FROM product_analytics_staging.fct_sampled_can_frames
  WHERE payload IS NOT NULL
    AND LENGTH(payload) > 0
    AND source_interface != 'j1587'
),
recompiled_frames AS (
  SELECT 
    COUNT(*) AS recompiled_count,
    SUM(duplicate_count) AS total_frames_including_duplicates,
    SUM(payload_length) AS recompiled_bytes,
    AVG(duplicate_count) AS avg_duplicates_per_frame
  FROM product_analytics_staging.fct_can_trace_recompiled
)

SELECT 
  rf.raw_count AS raw_frames,
  rc.recompiled_count AS unique_recompiled_frames,
  rc.total_frames_including_duplicates AS recompiled_frames_with_duplicates,
  ROUND(rf.raw_count::FLOAT / NULLIF(rc.recompiled_count, 0), 2) AS compression_ratio,
  ROUND((1.0 - rc.recompiled_count::FLOAT / rf.raw_count::FLOAT), 4) AS deduplication_reduction_pct,
  ROUND(rc.avg_duplicates_per_frame, 2) AS avg_duplicates_per_frame
FROM raw_frames rf
CROSS JOIN recompiled_frames rc

