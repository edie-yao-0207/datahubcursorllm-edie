queries = {
    "Get the count of last 30 days of key safety events per driver for the latest day": """
        SELECT
            org_id,
            driver_id,
            crashes_30d,
            speeding_30d,
            harsh_braking_30d,
            harsh_turns_30d
            FROM feature_store.driver_risk_insights_features
        WHERE date = DATE_SUB(CURRENT_DATE(), 1)
        """,
}
