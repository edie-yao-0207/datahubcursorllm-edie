queries = {
    "Users who viewed the dashboard (in last 90d)": """
            SELECT
                user_email,
                COUNT(1) as num_views
            FROM auditlog.fct_databricks_dashboard_events
            WHERE
                dashboard_id = '01effa02feab11c7bd16a7519d498302' -- Replace with your dashboard ID
                AND action_name = 'getPublishedDashboard'
                AND date BETWEEN DATE_SUB(current_date(), 89) AND current_date()
                AND user_email LIKE '%@samsara.com' -- Filter out internal Databricks actions (e.g lakeflow) and any service principals
            GROUP BY 1
            ORDER BY 2 DESC
            """,
    "Views over time (month over month) (in last 180d)": """
            SELECT
                DATE_TRUNC('MONTH', date) AS month,
                COUNT(1) AS num_views
            FROM
                auditlog.fct_databricks_dashboard_events
            WHERE
                dashboard_id = '01effa02feab11c7bd16a7519d498302' -- Replace with your dashboard ID
                AND action_name = 'getPublishedDashboard'
                AND date BETWEEN DATE_SUB(current_date(), 179) AND current_date()
                AND user_email LIKE '%@samsara.com' -- Filter out internal Databricks actions (e.g lakeflow) and any service principals
            GROUP BY 1
            ORDER BY 1 DESC
    """,
    "MAUs of your dashboard (in last 180d)": """
            SELECT
                DATE_TRUNC('MONTH', date) AS month,
                COUNT(DISTINCT user_email) AS mau
            FROM
                auditlog.fct_databricks_dashboard_events
            WHERE
                dashboard_id = '01effa02feab11c7bd16a7519d498302' -- Replace with your dashboard ID
                AND action_name = 'getPublishedDashboard'
                AND date BETWEEN DATE_SUB(current_date(), 179) AND current_date()
                AND user_email LIKE '%@samsara.com' -- Filter out internal Databricks actions (e.g lakeflow) and any service principals
            GROUP BY 1
            ORDER BY 1 DESC
    """,
}
