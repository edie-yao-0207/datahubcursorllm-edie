queries = {
    "Promotions applied to a MMYEF yesterday": """

SELECT
    date
    , make
    , model
    , year
    , engine_model
    , fuel_type
    , engine_type
    , signal
    , bus
    , source
    , electrification_level
    , primary_fuel_type
    , secondary_fuel_type
    , population_count
FROM
    product_analytics.agg_mmyef_promotions
WHERE
    date = CURRENT_DATE() - 1

            """,
}
