queries = {
    "Number of distinct user sessions per day": """
        select
          date,
          count(1) as count_distinct_sessions
        from (
          select
            date,
            mixpanel_distinct_id,
            session_id
          from datamodel_platform.fct_user_session_dashboard_page_visits
          group by 1, 2, 3
        )
        group by 1
    """,
    "Top five most frequently-visited pages (by route name)": """
        select
          date,
          route_name,
          count(1)
        from datamodel_platform.fct_user_session_dashboard_page_visits
        group by 1, 2
        order by 3 desc
        limit 5
    """,
}
