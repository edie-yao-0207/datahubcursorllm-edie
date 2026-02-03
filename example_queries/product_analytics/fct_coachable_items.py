queries = {
    "Count of severe speeding and max speeding violations by date and org": """
            SELECT
                date,
                org_id,
                COUNT(*) AS num_items
            FROM product_analytics.fct_coachable_items
            WHERE coachable_item_type_enum IN (40, 41)
            GROUP BY date, org_id
            ORDER BY date, org_id
            """,
    "Join coachable items with triage events for triage-backed items": """
            SELECT ci.*, te.*
            FROM product_analytics.fct_coachable_items ci
            LEFT JOIN (
                SELECT *
                FROM product_analytics.fct_safety_triage_events
            ) te
                -- Note: date is not included because dates may not correspond
                -- between coachable items and triage events
                ON ci.org_id = te.org_id
                AND LOWER(REPLACE(ci.source_id, '-', '')) = LOWER(te.uuid)
            -- corresponds to CoachableItemBackingType.TRIAGE_EVENT
            WHERE ci.coachable_item_backing_type_enum = 4
            LIMIT 10
            """,
}
