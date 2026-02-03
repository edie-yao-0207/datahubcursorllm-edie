queries = {
    "Number of organizations that changed their speeding settings on each date during July 2024": """
        WITH speeding_settings_cte AS (
        SELECT
            `date`,
            speeding_settings,
            LAG(speeding_settings) OVER (
            PARTITION BY org_id
            ORDER BY
                `date`
            ) AS speeding_settings_lag
        FROM
            datamodel_core.dim_organizations_settings
        WHERE
            `date` >= '2024-07-01'
            AND `date` <= '2024-07-31'
        )
        SELECT
        `date`,
        count(*)
        FROM
        speeding_settings_cte
        WHERE
        speeding_settings <> speeding_settings_lag
        GROUP BY
        1
        ORDER BY
        1
            """
}
