queries = {
    "Get all relevant segments for the smart-maps-show-public-alerts FF": """
    SELECT feature_flag_key,
    segments
    FROM datamodel_launchdarkly_silver.feature_flag_details
    WHERE
        feature_flag_key = 'smart-maps-show-public-alerts'
        AND date = (SELECT MAX(date) FROM datamodel_launchdarkly_silver.feature_flag_details)
    """
}
