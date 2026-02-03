queries = {
    "Get all the rules for segments within the smart-maps-show-public-alerts FF": """
    WITH relevant_segments AS (
        SELECT *
        FROM datamodel_launchdarkly_bronze.segment_flag
        WHERE
            key = 'smart-maps-show-public-alerts'
            AND date = (SELECT MAX(date) FROM datamodel_launchdarkly_bronze.segment_flag)
    )
    SELECT
        segment_rule_id AS rule_id,
        attribute,
        op,
        negate,
        FROM_JSON(values, 'array<string>') AS value_array
    FROM datamodel_launchdarkly_bronze.rule_clause
    WHERE
        segment_key IN (SELECT segment_key FROM relevant_segments)
        AND attribute IN ('orgId', 'orgCreatedAt', 'orgLocale', 'orgReleaseType', 'samNumber', 'orgIsInternal') -- can include more here; these are the ones we started with
        AND date = (SELECT MAX(date) FROM datamodel_launchdarkly_bronze.rule_clause)
    """
}
