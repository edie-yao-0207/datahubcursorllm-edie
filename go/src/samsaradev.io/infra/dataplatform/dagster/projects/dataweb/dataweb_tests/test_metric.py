from dataweb.userpkgs.utils import get_final_metric_sql, get_metric_asset_name


def test_metric_asset_name():

    asset_name = "device_licenses_count"
    assert (
        get_metric_asset_name(asset_name) == "device_licenses"
    ), f"Expected 'device_licenses', got {get_metric_asset_name(asset_name)}"


def test_final_metric_sql():

    sql_string = """
    SELECT a.org_id,
    c.fleet_size
    FROM datamodel_core.dim_organizations a
    JOIN product_analytics_staging.stg_organization_categories c
        ON a.org_id = c.org_id
        AND c.date = (SELECT MAX(date) FROM product_analytics_staging.stg_organization_categories)
    """

    sql_string_eu = """
    SELECT a.org_id,
    c.fleet_size
    FROM datamodel_core.dim_organizations a
    JOIN product_analytics_staging.stg_organization_categories_eu c
        ON a.org_id = c.org_id
        AND c.date = (SELECT MAX(date) FROM product_analytics_staging.stg_organization_categories_eu)
    """

    final_sql = get_final_metric_sql(sql_string, "us-west-2")
    assert (
        final_sql.strip() == sql_string.strip()
    ), f"Expected both SQLs to be equal, got {final_sql}"

    final_sql = get_final_metric_sql(sql_string, "eu-west-1")
    assert (
        final_sql.strip() == sql_string_eu.strip()
    ), f"Expected both SQLs to be equal, got {final_sql}"
