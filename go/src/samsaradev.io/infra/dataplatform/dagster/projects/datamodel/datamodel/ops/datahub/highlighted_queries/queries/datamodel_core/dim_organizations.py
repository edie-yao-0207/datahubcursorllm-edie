queries = {
    "Summary of paid organizations by account ARR bucket": """
     SELECT date,
       account_arr_segment,
       COUNT(DISTINCT CASE WHEN is_paid_customer = TRUE THEN org_id END) AS paid_orgs,
       COUNT(DISTINCT CASE WHEN is_paid_customer = TRUE THEN sam_number END) AS paid_sams,
       COUNT(DISTINCT CASE WHEN is_paid_telematics_customer = TRUE THEN org_id END) AS paid_telematics_orgs,
       COUNT(DISTINCT CASE WHEN is_paid_telematics_customer = TRUE THEN sam_number END) AS paid_telematics_sams,
       COUNT(DISTINCT CASE WHEN is_paid_safety_customer = TRUE THEN org_id END) AS paid_safety_orgs,
       COUNT(DISTINCT CASE WHEN is_paid_safety_customer = TRUE THEN sam_number END) AS paid_safety_sams,
       COUNT(DISTINCT CASE WHEN is_paid_stce_customer = TRUE THEN org_id END) AS paid_stce_orgs,
       COUNT(DISTINCT CASE WHEN is_paid_stce_customer = TRUE THEN sam_number END) AS paid_stce_sams
    FROM datamodel_core.dim_organizations org
    WHERE date = (SELECT MAX(date) FROM datamodel_core.dim_organizations)
    GROUP BY 1,2
    ORDER BY 3 DESC
            """
}
