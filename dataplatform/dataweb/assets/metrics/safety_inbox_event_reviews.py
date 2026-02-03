from dagster import AssetKey, AssetExecutionContext
from dataweb import GlossaryTerm, metric
from dataweb.userpkgs.constants import AWSRegion, Database, DATAANALYTICS, DATAENGINEERING
from dataweb.userpkgs.utils import get_metric_sql

@metric(
    description="(Global) The total number of safety inbox event reviews.",
    expressions=[
        {
            "expression_type": "count",
            "expression": "COUNT(event_id)",
            "metadata": {"comment": "The total number of safety inbox event reviews."},
        }
    ],
    dimensions=[
        {
            "name": "date",
            "metadata": {
                "comment": "The calendar date when the coaching session started in YYYY-mm-dd format"
            },
        },
        {
            "name": "org_id",
            "metadata": {
                "comment": "The Samsara cloud dashboard ID that the data belongs to"
            },
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
                "comment": "Classifies accounts by size - Small Business, Mid Market, Enterprise"
            },
        },
        {
            "name": "detection_type",
            "metadata": {"comment": "The cause of the safety event."},
        },
        {
            "name": "sam_number",
            "metadata": {
                "comment": "This is the internal Samsara Customer account ID."
            },
        },
        {
            "name": "account_industry",
            "metadata": {"comment": "Industry classification of the account."},
        },
        {
            "name": "account_arr_segment",
            "metadata": {
                "comment": "A bucketing of customer accounts by sizes - Small Business, Mid Market, Enterprise"
            },
        },
        {
            "name": "has_ser_license",
            "metadata": {
                "comment": "A flag indicating whether the customer is paying for SER."
            },
        },
        {
            "name": "has_safety_event_review",
            "metadata": {
                "comment": "A flag indicating whether the safety inbox event has an associated review job."
            },
        },
        {
            "name": "is_manual_dismissed",
            "metadata": {
                "comment": "A flag indicating whether the safety inbox event with the review was manually dismissed."
            },
        },
        {
            "name": "is_pending",
            "metadata": {
                "comment": "A flag indicating whether the safety inbox event with the review is pending."
            },
        },
        {
            "name": "is_auto_dismissed",
            "metadata": {
                "comment": "A flag indicating whether the safety inbox event with the review was dismissed automatically."
            },
        },
        {
            "name": "is_customer_dismissed",
            "metadata": {
                "comment": "A flag indicating whether the safety inbox event with the review was dismissed by a customer"
            },
        },
        {
            "name": "is_viewed",
            "metadata": {
                "comment": "A flag indicating whether the safety inbox event with the review was viewed."
            },
        },
        {
            "name": "is_coached",
            "metadata": {
                "comment": "A flag indicating whether the safety inbox event with the review was coached."
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
    ],
    regions=[AWSRegion.US_WEST_2],
    owners=[DATAENGINEERING, DATAANALYTICS],
    glossary_terms=[GlossaryTerm.SAFETY, GlossaryTerm.AI],
    upstreams=[
        AssetKey(["safetyeventreviewdb", "review_request_metadata"]),
        AssetKey(["safetyeventreviewdb", "jobs"]),
        AssetKey([Database.DATAMODEL_CORE, "dim_organizations"]),
        AssetKey([Database.DATAENGINEERING, "safety_inbox_events_status_global"]),
        AssetKey([Database.PRODUCT_ANALYTICS_STAGING, "stg_organization_categories_global"]),
        AssetKey([Database.PRODUCT_ANALYTICS, "map_org_sam_number_latest_global"]),
    ],
)
def safety_inbox_event_reviews(context: AssetExecutionContext) -> str:
    return get_metric_sql(context)
