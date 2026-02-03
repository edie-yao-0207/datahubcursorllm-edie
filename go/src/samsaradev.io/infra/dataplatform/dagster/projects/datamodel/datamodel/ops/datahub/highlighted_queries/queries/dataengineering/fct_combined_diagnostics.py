queries = {
    "Find average fuel consumption rate for heavy duty vehicles driving in California in a given org": """
    select
        date,
        org_id,
        avg(osdfuelconsumptionratemlperhour) as avg_osdfuelconsumptionratemlperhour
    from dataengineering.fct_combined_diagnostics
    where date = date_sub(current_date(), 1)
    and org_id = 32663
    -- US state bounding boxes from https://observablehq.com/@rdmurphy/u-s-state-bounding-boxes
    and longitude between -124.41060660766607 and -114.13445790587905
    and latitude between 32.5342307609976 and 42.00965914828148
    """
}
