from dagster import AssetKey, AssetExecutionContext
from dataweb import GlossaryTerm, metric
from dataweb.userpkgs.constants import AWSRegion, Database, DATAANALYTICS, DATAENGINEERING
from dataweb.userpkgs.utils import get_metric_sql

@metric(
    description="(Global) The number of safety event reviews.",
    expressions=[
        {
            "expression_type": "count",
            "expression": "COUNT(uuid)",
            "metadata": {"comment": "The number of safety event reviews."},
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
            "name": "sam_number",
            "metadata": {
                "comment": "This is the internal Samsara Customer account ID."
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
            "name": "account_industry",
            "metadata": {"comment": "Industry classification of the account."},
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
            "name": "has_ser_license",
            "metadata": {
                "comment": "A flag indicating whether the customer is paying for SER."
            },
        },
        {
            "name": "detection_type",
            "metadata": {
                "comment": "The detection label that was applied to the safety event"
            },
        },
        {
            "name": "is_review_completed",
            "metadata": {
                "comment": "A flag indicating whether the safety event review was completed by a customer."
            },
        },
        {
            "name": "is_accepted",
            "metadata": {
                "comment": "A flag indicating whether the detection type of the safety event was reviewed as correct."
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
        AssetKey([Database.DATAENGINEERING, "safety_event_reviews_details"]),
        AssetKey(["safetyeventreviewdb", "review_results"]),
        AssetKey([Database.PRODUCT_ANALYTICS_STAGING, "stg_organization_categories_global"]),
        AssetKey([Database.PRODUCT_ANALYTICS, "map_org_sam_number_latest_global"]),
    ],
)
def safety_event_reviews(context: AssetExecutionContext) -> str:
    return get_metric_sql(context)
