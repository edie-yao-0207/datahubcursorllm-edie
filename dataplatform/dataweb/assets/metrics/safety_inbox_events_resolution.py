from dagster import AssetKey, AssetExecutionContext
from dataweb import GlossaryTerm, metric
from dataweb.userpkgs.constants import AWSRegion, Database, DATAENGINEERING
from dataweb.userpkgs.utils import get_metric_sql

@metric(
    description="(Global) The time it takes to resolve a safety inbox event.",
    expressions=[
        {
            "expression_type": "median",
            "expression": "PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY days_to_resolve)",
            "metadata": {
                "comment": "The median number of days for resolving a safety inbox event."
            },
        }
    ],
    dimensions=[
        {
            "name": "date",
            "metadata": {
                "comment": "The field which partitions the table, in YYYY-mm-dd format."
            },
        },
        {
            "name": "org_id",
            "metadata": {
                "comment": "The Samsara cloud dashboard ID that the data belongs to"
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
            "metadata": {"comment": "Industry classification of the account."},
        },
        {
            "name": "account_arr_segment",
            "metadata": {
                "comment": "A bucketing of customer accounts by sizes - Small Business, Mid Market, Enterprise"
            },
        },
        {
            "name": "is_coaching_assigned",
            "metadata": {
                "comment": "A flag indicating whether the safety event has been assigned a coaching event."
            },
        },
        {
            "name": "is_car_viewed",
            "metadata": {"comment": "A flag indicating whether the car has been viewed or not"},
        },
        {
            "name": "is_customer_dismissed",
            "metadata": {
                "comment": "A flag indicating whether the safety event was dismissed by a customer."
            },
        },
        {
            "name": "is_viewed",
            "metadata": {
                "comment": "A flag indicating whether the safety event was viewed at least once by a customer."
            },
        },
        {
            "name": "is_useful",
            "metadata": {
                "comment": "A flag indicating whether the safety event was rated as useful or not (i.e., thumbs-up/thumbs-down) by a customer."
            },
        },
        {
            "name": "is_actioned",
            "metadata": {
                "comment": "A flag indicating whether the safety event was actioned on - automatically or by a customer."
            },
        },
        {
            "name": "has_safety_event_review",
            "metadata": {
                "comment": "A flag indicating whether the safety event has an associated review job."
            },
        },
        {
            "name": "is_coached",
            "metadata": {
                "comment": "A flag indicating whether the safety event was coached.",
            },
        },
        {
            "name": "is_critical_event",
            "metadata": {
                "comment": "A flag indicating whether the safety event is considered a critical event - is either haCrash or haDrowsinessDetection."
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
    owners=[DATAENGINEERING],
    glossary_terms=[GlossaryTerm.SAFETY, GlossaryTerm.AI],
    upstreams=[
        AssetKey([Database.DATAENGINEERING, "safety_inbox_events_status_global"]),
        AssetKey([Database.PRODUCT_ANALYTICS_STAGING, "stg_organization_categories_global"]),
        AssetKey([Database.PRODUCT_ANALYTICS, "map_org_sam_number_latest_global"]),
    ],
)
def safety_inbox_events_resolution(context: AssetExecutionContext) -> str:
    return get_metric_sql(context)
