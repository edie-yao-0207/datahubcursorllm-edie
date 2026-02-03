queries = {
    "Show per device coverage values for one day before the current date.": """

    SELECT
        date
        , type
        , type_field
        , primary_fields
        , org_id
        , device_id
        , value

    FROM
        product_analytics.agg_device_stats_secondary_coverage

    WHERE
        date = date_sub(current_date(), 1)

        """,
}
