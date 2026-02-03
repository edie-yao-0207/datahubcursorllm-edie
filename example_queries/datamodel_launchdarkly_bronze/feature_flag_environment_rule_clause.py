queries = {
    "Get all the rules (non-segment) within the smart-maps-show-public-alerts FF": """
    WITH relevant_rules AS (
        -- Get all relevant rules for the FF
        SELECT *
        FROM datamodel_launchdarkly_bronze.feature_flag_environment_rule
        WHERE
            feature_flag_key = 'smart-maps-show-public-alerts'
            AND date = (SELECT MAX(date) FROM datamodel_launchdarkly_bronze.feature_flag_environment_rule)
    )
    SELECT
        rc.feature_flag_environment_rule_id AS rule_id,
        rc.attribute,
        rc.op,
        rc.negate,
        ffr.variation,
        FROM_JSON(rc.values, 'array<string>') AS value_array
    FROM datamodel_launchdarkly_bronze.feature_flag_environment_rule_clause rc
    JOIN relevant_rules ffr
        ON ffr.id = rc.feature_flag_environment_rule_id
    WHERE
        rc.attribute IN ('orgId', 'orgCreatedAt', 'orgLocale', 'orgReleaseType', 'samNumber', 'orgIsInternal') -- Can add more here
        AND rc.date = (SELECT MAX(date) FROM datamodel_launchdarkly_bronze.feature_flag_environment_rule_clause)
    """
}
