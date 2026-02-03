queries = {
    "Active license summary by sku and date with active licenses deduplicated by account and filtered to non trial and non expired licenses.": """
        SELECT date,
            sku,
            SUM(quantity) AS total_quantity,
            COUNT(DISTINCT sam_number) AS accounts
        FROM dataengineering.dim_licenses_global
        WHERE
        date = (SELECT MAX(date) FROM dataengineering.dim_licenses_global)
        AND quantity > 0
        AND is_trial = FALSE
        AND internal_type = 0
        AND date_expired IS NULL
        GROUP BY 1,2
        ORDER BY 3 DESC
            """
}
