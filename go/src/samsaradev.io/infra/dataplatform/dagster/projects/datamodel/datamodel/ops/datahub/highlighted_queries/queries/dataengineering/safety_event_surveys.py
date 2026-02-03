queries = {
    "Count of safety event surveys grouped by org, device, and detection type for the latest date": """
            WITH surveys AS (
                SELECT
                    org_id,
                    device_id,
                    detection_type,
                    EXPLODE(harsh_event_surveys) AS harsh_event_survey
                FROM dataengineering.safety_event_surveys
                WHERE date = date_sub(current_date(), 1)
            )
            SELECT
                org_id,
                device_id,
                detection_type,
                harsh_event_survey.is_useful,
                COUNT(*)
            FROM surveys
            GROUP BY
                org_id,
                device_id,
                detection_type,
                harsh_event_survey.is_useful
            """,
}
