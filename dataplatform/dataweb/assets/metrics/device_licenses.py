from dagster import AssetKey, AssetExecutionContext
from dataweb import GlossaryTerm, metric
from dataweb.userpkgs.constants import AWSRegion, Database, DATAENGINEERING
from dataweb.userpkgs.utils import get_metric_sql


@metric(
    description="Active device licenses (licenses with a device type in their SKU).",
    expressions=[
        {
            "expression_type": "sum",
            "expression": "SUM(CASE WHEN device_type IN ('AG Powered', 'AG Unpowered', 'AT', 'CM-S', 'CM-D', 'CM-M', 'VG', 'SG') THEN quantity END)",
            "metadata": {
                "comment": "The total number of device licenses held, can be NULL if no value exists."
            },
        },
        {
            "expression_type": "avg",
            "expression": "AVG(CASE WHEN device_type IN ('AG Powered', 'AG Unpowered', 'AT', 'CM-S', 'CM-D', 'CM-M', 'VG', 'SG') THEN quantity END)",
            "metadata": {
                "comment": "The average quantity of device licenses held, can be NULL if no value exists."
            },
        },
    ],
    dimensions=[
        {
            "name": "date",
            "metadata": {
                "comment": "Date of snapshot of copy of edw.silver.fct_license_orders_daily_snapshot table."
            },
        },
        {
            "name": "sam_number",
            "metadata": {"comment": "Samsara account number of organization."},
        },
        {
            "name": "internal_type",
            "metadata": {
                "comment": "Field representing internal status of organizations. 0 represents customer organizations."
            },
        },
        {
            "name": "sku",
            "metadata": {
                "comment": "License sku, does not represent quantity of licenses only that at least one license is held. "
            },
        },
        {
            "name": "product_family",
            "metadata": {
                "comment": "Most recent family of license sku as of query date from edw.silver.fct_license_orders_daily_snapshot."
            },
        },
        {
            "name": "sub_product_line",
            "metadata": {
                "comment": "Most recent sub_product_line of license sku as of query date from edw.silver.fct_license_orders_daily_snapshot."
            },
        },
        {
            "name": "device_type",
            "metadata": {
                "comment": "Device type for the given SKU as of query date from edw.silver.fct_license_orders_daily_snapshot."
            },
        },
    ],
    regions=[AWSRegion.US_WEST_2],
    owners=[DATAENGINEERING],
    glossary_terms=[GlossaryTerm.PLATFORM, GlossaryTerm.MPR],
    upstreams=[
        AssetKey([Database.PRODUCT_ANALYTICS, "map_org_sam_number_latest_global"]),
        AssetKey([Database.PRODUCT_ANALYTICS_STAGING, "stg_organization_categories_global"])
    ],
)
def device_licenses(context: AssetExecutionContext) -> str:
    return get_metric_sql(context)
