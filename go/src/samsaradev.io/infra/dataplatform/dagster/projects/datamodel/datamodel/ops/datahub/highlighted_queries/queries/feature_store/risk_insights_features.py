queries = {
    "Get safety event breakdowns per driver for the latest day": """
        SELECT
            org_id,
            driver_id,
            driver_history_crash_30d,
            driver_history_speeding_30d,
            driver_history_braking_30d,
            driver_history_harsh_turn_30d
            FROM feature_store.risk_insights_features
            WHERE date = date_sub(current_date(), 1)
            """,
}
