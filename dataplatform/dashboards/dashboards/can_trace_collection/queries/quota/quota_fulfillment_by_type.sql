-- =============================================================================
-- Dashboard: CAN Trace Collection Dashboard
-- Tab: Quota Status
-- View: Quota Fulfillment by Query Type
-- =============================================================================
-- Visualization: Bar
-- Title: Quota Fulfillment by Query Type
-- Description: Quota fulfillment rates by query type (all-time cumulative)
-- Note: Uses agg_tags_per_mmyef at MAX(date) which contains cumulative counts up to that date (effectively all-time)
-- =============================================================================

-- Note: agg_tags_per_mmyef stores cumulative counts per date partition.
-- MAX(date) partition contains cumulative counts up to that date (effectively all-time).
-- Quota defaults: can-set-main-0: 10, others: 0 (no quota tracking)
WITH latest_snapshot_date AS (
    SELECT MAX(date) AS max_date
    FROM datamodel_dev.agg_tags_per_mmyef
),

-- Explode tag_counts_map to get tag breakdowns
mmyef_tags_exploded AS (
    SELECT
        atpm.mmyef_id,
        exploded.tag_name,
        exploded.tag_count,
        CASE 
            WHEN exploded.tag_name = 'can-set-main-0' THEN 10
            ELSE 0  -- No quota for other tags
        END AS quota
    FROM datamodel_dev.agg_tags_per_mmyef atpm
    CROSS JOIN latest_snapshot_date lsd
    LATERAL VIEW explode(atpm.tag_counts_map) exploded AS tag_name, tag_count
    WHERE atpm.date = lsd.max_date
      AND atpm.tag_counts_map IS NOT NULL
),

-- Aggregate by tag - count MMYEFs meeting quota per tag
tag_aggregates AS (
    SELECT
        tag_name,
        quota,
        COUNT(DISTINCT CASE WHEN tag_count >= quota AND quota > 0 THEN mmyef_id END) AS meeting_quota_count,
        COUNT(DISTINCT mmyef_id) AS total_mmyefs_with_tag
    FROM mmyef_tags_exploded
    GROUP BY tag_name, quota
)

SELECT
    tag_name AS query_type,
    quota,
    COALESCE(meeting_quota_count, 0) AS meeting_quota,
    total_mmyefs_with_tag AS total_mmyefs,
    CASE 
        WHEN quota > 0 THEN
            CAST(COALESCE(meeting_quota_count, 0) AS DOUBLE) / NULLIF(CAST(total_mmyefs_with_tag AS DOUBLE), 0)
        ELSE 0.0
    END AS fulfillment_rate
FROM tag_aggregates
ORDER BY fulfillment_rate DESC, quota DESC
