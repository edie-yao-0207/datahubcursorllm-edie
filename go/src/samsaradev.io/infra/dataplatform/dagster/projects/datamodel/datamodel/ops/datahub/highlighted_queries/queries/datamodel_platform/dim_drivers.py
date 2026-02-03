queries = {
    "Summary of active drivers within in past 90 days per organization at end of month": """
    WITH end_of_month AS (
    SELECT DISTINCT date
    FROM definitions.445_calendar
    WHERE date = eom
    AND date BETWEEN DATE_SUB(CURRENT_DATE(), 90) AND CURRENT_DATE())

    SELECT drivers.date,
        drivers.org_id,
        COUNT(DISTINCT CASE WHEN drivers.last_mobile_login_date IS NOT NULL THEN drivers.driver_id END) as has_used_mobile_app,
        COUNT(DISTINCT CASE WHEN drivers.last_mobile_login_date BETWEEN DATE_SUB(end_of_month.date, 90) AND end_of_month.date THEN drivers.driver_id END) AS active_last_90d
    FROM datamodel_platform.dim_drivers drivers
    INNER JOIN end_of_month
    ON end_of_month.date = drivers.date
    GROUP BY 1,2
    ORDER BY 3 DESC
            """
}
