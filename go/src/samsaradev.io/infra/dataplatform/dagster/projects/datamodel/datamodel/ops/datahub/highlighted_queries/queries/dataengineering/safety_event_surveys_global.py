queries = {
    "Count of safety event surveys grouped by org, device, detection type, and region for the latest date": """
            WITH surveys AS (
                SELECT
                    org_id,
                    device_id,
                    detection_type,
                    region,
                    EXPLODE(harsh_event_surveys) AS harsh_event_survey
                FROM dataengineering.safety_event_surveys_global
                WHERE date = date_sub(current_date(), 1)
            )
            SELECT
                org_id,
                device_id,
                detection_type,
                region,
                harsh_event_survey.is_useful,
                COUNT(*)
            FROM surveys
            GROUP BY
                org_id,
                device_id,
                detection_type,
                region,
                harsh_event_survey.is_useful
            """,
}
