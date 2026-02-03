queries = {
    "Counts of events by type where users submitted thumbs up/thumbs down feedback": """
                    SELECT detection_label, count(*) AS type_count
                    FROM datamodel_safety.fct_safety_events
                    WHERE org_id = 874 -- Example org is DOHRN
                        AND `date` >= DATE_SUB(CURRENT_DATE(), 28)
                        AND `date` <= CURRENT_DATE()
                        AND harsh_event_surveys IS NOT NULL
                    GROUP BY 1
                    ORDER BY 2 DESC
        """,
}
