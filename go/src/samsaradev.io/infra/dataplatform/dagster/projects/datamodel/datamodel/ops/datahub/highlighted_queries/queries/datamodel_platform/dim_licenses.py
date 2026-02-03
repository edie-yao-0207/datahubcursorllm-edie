queries = {
    "Active license summary by sku and date with active licenses deduplicated by account and filtered to non trial and non expired licenses.": """
        SELECT date,
            sku,
            SUM(quantity) AS total_quantity,
            COUNT(DISTINCT sam_number) AS accounts
        FROM datamodel_platform.dim_licenses
        WHERE
        date = (SELECT MAX(date) FROM datamodel_platform.dim_licenses)
        AND quantity > 0
        AND is_trial = FALSE
        AND internal_type = 0
        AND date_expired IS NULL
        GROUP BY 1,2
        ORDER BY 3 DESC
            """,
    "License count per SKU per SAM number": """
        --SELECT one SKU per SAM_NUMBER as licenses are duplicated throughout a SAM_NUMBER
        --when computing totals, you must deduplicate by sam_number
        SELECT
            date,
            sam_number,
            sku,
            MIN(date_first_active) AS date_first_active,
            MAX(quantity) -- quantity is the same throughout the sam, take max() to put quantity in the group by
        FROM datamodel_platform.dim_licenses
        WHERE
        date = (SELECT MAX(date) FROM datamodel_platform.dim_licenses)
        AND quantity > 0
        AND is_trial = FALSE
        AND internal_type = 0
        AND date_expired IS NULL
        GROUP BY ALL
        """,
}
