queries = {
    "Number of users active per activity source per day": """
        select date,
            count(distinct case when source = 'fleet_app' then user_id end) as mobile_users,
            count(distinct case when source = 'web' then user_id end) as web_users,
            count(distinct case when source = 'login' then user_id end) as users_web_login,
            count(distinct user_id) as active_user_across_surface
        from datamodel_platform.fct_user_activity
        where date between date_sub(current_date(), 7) and current_date()
        group by 1
        order by 1
            """
}
