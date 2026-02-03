from dagster import AssetKey, AssetExecutionContext
from dataweb import GlossaryTerm, metric
from dataweb.userpkgs.constants import AWSRegion, Database, DATAANALYTICS, DATAENGINEERING
from dataweb.userpkgs.utils import get_metric_sql


@metric(
    description="(Global) The number of unique users from customer accounts that have accessed the cloud dashboard.",
    expressions=[
        {
            "expression_type": "count",
            "expression": "COUNT(DISTINCT user_email)",
            "metadata": {
                "comment": "The number of unique users from customer accounts that have accessed the cloud dashboard."
            },
        }
    ],
    dimensions=[
        {
            "name": "date",
            "metadata": {
                "comment": "The calendar day when the user accessed the dashboard in YYYY-mm-dd format"
            },
        },
        {
            "name": "parent_sam_number",
            "metadata": {"comment": "The parent SAM number associated with the user."},
        },
        {
            "name": "sam_number",
            "metadata": {"comment": "The SAM number associated with the user."},
        },
        {
            "name": "org_id",
            "metadata": {
                "comment": "The Samsara cloud dashboard ID associated with the user"
            },
        },
        {
            "name": "internal_type",
            "metadata": {
                "comment": "Field representing internal status of organizations. 0 represents customer organizations."
            },
        },
        {
            "name": "account_size_segment_name",
            "metadata": {
                "comment": "Classifies customer organization by size - Small Business, Mid Market, Enterprise"
            },
        },
        {
            "name": "account_arr_segment",
            "metadata": {"comment": "Classifies customer organization by ARR size"},
        },
        {
            "name": "account_industry",
            "metadata": {
                "comment": "Classifies customer organization by industry designation"
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
            "name": "region",
            "metadata": {
                "comment": "The Samsara cloud region of the organization"
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
    ],
    regions=[AWSRegion.US_WEST_2],
    owners=[DATAENGINEERING, DATAANALYTICS],
    glossary_terms=[GlossaryTerm.PLATFORM, GlossaryTerm.MPR],
    upstreams=[
        AssetKey([Database.DATAENGINEERING, "cloud_dashboard_user_activity_global"]),
        AssetKey([Database.PRODUCT_ANALYTICS_STAGING, "stg_organization_categories_global"]),
        AssetKey([Database.PRODUCT_ANALYTICS, "map_org_sam_number_latest_global"])
    ],
)
def users_with_cloud_dashboard_activity(context: AssetExecutionContext) -> str:
    return get_metric_sql(context)
