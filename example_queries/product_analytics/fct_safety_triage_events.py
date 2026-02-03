queries = {
    "Count of safety triage events by behavior label, over the past 7 days, for customers with the new inbox feature enabled": """
            -- What share of customers have the new inbox feature enabled?
            WITH enabled_customers AS
            (
            SELECT DISTINCT fp.org_id
            FROM releasemanagementdb_shards.feature_package_self_serve fp
            INNER JOIN datamodel_core.dim_organizations do ON fp.org_id = do.org_id
            WHERE TRUE
                AND fp.feature_package_uuid = 'A9C0CCC1FA084014A512693E2D2A7AC2'
                AND fp.enabled = 1
                AND fp.scope = 1
                -- feature_package_self_serve does not have historical data by date.
                -- Since feature_package_self_serve only has enablement as of the present time, 
                -- join to the latest data in dim_organizations
                AND do.date = CAST( (SELECT MAX(date) FROM datamodel_core.dim_organizations) AS STRING)
                AND do.internal_type = 0
            )
            , exploded_array AS (
            SELECT * FROM product_analytics.fct_safety_triage_events
            LATERAL VIEW OUTER EXPLODE(behavior_labels_names_array) t AS behavior_label_name
            WHERE date >= STRING(DATE_SUB(CURRENT_DATE(), 7))
            )
            SELECT date, behavior_label_name, COUNT(behavior_label_name)
            FROM exploded_array te
            INNER JOIN enabled_customers ec ON te.org_id = ec.org_id
            GROUP BY 1, 2 ORDER BY 1, 3 DESC;
            """,
}
