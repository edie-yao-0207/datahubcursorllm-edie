from dagster import AssetExecutionContext, AssetKey
from dataweb import GlossaryTerm, metric
from dataweb.userpkgs.constants import DATAENGINEERING, AWSRegion, Database
from dataweb.userpkgs.utils import get_metric_sql


@metric(
    description="Active customers (those that are a paid customer with least one active device)",
    expressions=[
        {
            "expression_type": "count",
            "expression": "COUNT(DISTINCT IF(active_device_count > 0, sam_number, NULL))",
            "metadata": {
                "comment": "The total number of SAMs that are paying customers with at least one active device"
            },
        },
        {
            "expression_type": "percent",
            "expression": "COUNT(DISTINCT IF(active_device_count > 0, sam_number, NULL)) / COUNT(DISTINCT sam_number)",
            "metadata": {
                "comment": "The percent of SAMs that are paying customers with at least one active device"
            },
        },
    ],
    dimensions=[
        {
            "name": "date",
            "metadata": {
                "comment": "Date of snapshot of copy of dynamodb.cached_active_skus table."
            },
        },
        {
            "name": "org_id",
            "metadata": {"comment": "ID of organization of license SKU."},
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
            "name": "account_size_segment",
            "metadata": {
                "comment": "A bucketing of customer accounts by sizes - Small Business, Mid Market, Enterprise"
            },
        },
        {
            "name": "account_industry",
            "metadata": {
                "comment": "Classifies customer organization by industry designation"
            },
        },
        {
            "name": "account_arr_segment",
            "metadata": {
                "comment": """ARR from the previously publicly reporting quarter in the buckets:
                <0
                0 - 100k
                100k - 500K,
                500K - 1M
                1M+
                Note - ARR information is calculated at the account level, not the SAM Number level
                An account and have multiple SAMs. Additionally a SAM can have multiple accounts, resulting in a single SAM having multiple account_arr_segments
                """
            },
        },
        {
            "name": "is_paid_customer",
            "metadata": {
                "comment": "Whether the customer organization had active licenses that month"
            },
        },
        {
            "name": "is_paid_safety_customer",
            "metadata": {
                "comment": "Whether the customer organization had active licenses within the safety product family"
            },
        },
        {
            "name": "is_paid_stce_customer",
            "metadata": {
                "comment": "Whether the customer organization had active licenses within the STCE product family"
            },
        },
        {
            "name": "is_paid_telematics_customer",
            "metadata": {
                "comment": "Whether the customer organization had active licenses within the Telematics product family"
            },
        },
        {
            "name": "avg_mileage",
            "metadata": {
                "comment": "Average mileage of organization`s fleet."
            },
        },
        {
            "name": "subregion",
            "metadata": {
                "comment": "Primary region of organization`s trips"
            },
        },
        {
            "name": "fleet_size",
            "metadata": {
                "comment": "Fleet size of organization (small, medium, large)."
            },
        },
        {
            "name": "industry_vertical",
            "metadata": {
                "comment": "Vertical that organization belongs to."
            },
        },
        {
            "name": "fuel_category",
            "metadata": {
                "comment": "Fuel category of organization`s fleet."
            },
        },
        {
            "name": "primary_driving_environment",
            "metadata": {
                "comment": "Primary driving environment of organization`s trips."
            },
        },
        {
            "name": "fleet_composition",
            "metadata": {
                "comment": "Composition of organization`s fleet, based on vehicle weights."
            },
        },
        {
            "name": "region",
            "metadata": {"comment": "The Samsara cloud region."},
        },
    ],
    regions=[AWSRegion.US_WEST_2],
    owners=[DATAENGINEERING],
    glossary_terms=[GlossaryTerm.PLATFORM, GlossaryTerm.MPR],
    upstreams=[
        AssetKey([Database.DATAENGINEERING, "dim_organizations_global"]),
        AssetKey([Database.DATAENGINEERING, "device_activity_global"]),
        AssetKey([Database.PRODUCT_ANALYTICS, "map_org_sam_number_latest_global"]),
        AssetKey([Database.PRODUCT_ANALYTICS_STAGING, "stg_organization_categories_global"]),
    ],
)
def active_customers(context: AssetExecutionContext) -> str:
    return get_metric_sql(context)
