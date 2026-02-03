from datamodel.ops.datahub.highlighted_queries.shared_queries import (
    users_and_their_roles,
)

queries = {
    "Number of active users overall and by domain (web, fleet app)": """
    WITH user_activity AS (
        SELECT
            user_id,
            date_trunc('month', date) AS activity_month,
            MAX(GREATEST(last_web_login_date, last_fleet_app_usage_date)) as last_activity_date,
            MAX(last_web_login_date) AS last_web_login_date,
            MAX(last_fleet_app_usage_date) AS last_fleet_app_usage_date
        FROM
            datamodel_platform.dim_users
        WHERE
            date >= date_trunc('month', add_months(current_date(), -3))

        GROUP BY
            user_id, activity_month
    )

    SELECT
        activity_month,
        COUNT(DISTINCT user_id) AS active_users,
        COUNT(DISTINCT CASE WHEN last_web_login_date IS NOT NULL AND last_web_login_date >= activity_month AND last_web_login_date < add_months(activity_month, 1) THEN user_id END) AS active_web_users,
        COUNT(DISTINCT CASE WHEN last_fleet_app_usage_date IS NOT NULL AND last_fleet_app_usage_date >= activity_month AND last_fleet_app_usage_date < add_months(activity_month, 1) THEN user_id END) AS active_fleet_app_users
    FROM
        user_activity
    WHERE
        last_activity_date IS NOT NULL
        AND last_activity_date >= activity_month
        AND last_activity_date < add_months(activity_month, 1)
    GROUP BY
        activity_month
    ORDER BY
        activity_month DESC;
    """
}

queries[users_and_their_roles.name] = users_and_their_roles.sql
