queries = {
    "Get all relevant segments for the smart-maps-show-public-alerts FF": """
    SELECT *
    FROM datamodel_launchdarkly_bronze.segment_flag
    WHERE
        key = 'smart-maps-show-public-alerts'
        AND date = (SELECT MAX(date) FROM datamodel_launchdarkly_bronze.segment_flag)
    """
}
