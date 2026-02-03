queries = {
    "Population stats from the latest date": """
            SELECT
                type
                , type_field
                , secondary_field
                , uuid
                , sum
                , count
                , avg
                , stddev
                , variance
                , mean
                , median
                , mode
                , first
                , last
            FROM
                product_analytics.agg_population_metrics
            WHERE
                date = date_sub(current_date(), 1)
            """,
}
