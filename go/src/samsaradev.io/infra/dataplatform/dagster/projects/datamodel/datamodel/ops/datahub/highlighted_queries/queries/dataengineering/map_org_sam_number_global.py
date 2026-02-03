queries = {
    "Find sam numbers that have the largest number of orgs": """
            SELECT
            sam_number,
            region,
            COUNT(DISTINCT org_id) AS n_orgs
            FROM dataengineering.map_org_sam_number_global
            WHERE date = (SELECT MAX(date) FROM dataengineering.map_org_sam_number_global)
            AND sam_number IS NOT NULL
            GROUP BY 1,2
            ORDER BY 3 DESC
            """,
}
