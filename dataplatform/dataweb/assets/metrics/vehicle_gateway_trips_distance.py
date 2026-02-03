from dagster import AssetKey, AssetExecutionContext
from dataweb import GlossaryTerm, metric
from dataweb.userpkgs.constants import AWSRegion, Database, DATAENGINEERING
from dataweb.userpkgs.utils import get_metric_sql

@metric(
    description="(Global) The aggregated distance (in miles) of trips.",
    expressions=[
        {
            "expression_type": "sum",
            "expression": "SUM(distance_miles)",
            "metadata": {
                "comment": "A sum of trip distance recorded by VGs for trips in miles."
            },
        },
        {
            "expression_type": "avg",
            "expression": "AVG(distance_miles)",
            "metadata": {
                "comment": "The average distance of trips recorded by a VG in miles."
            },
        },
    ],
    dimensions=[
        {
            "name": "date",
            "metadata": {
                "comment": "The field which partitions the table, in `YYYY-mm-dd` format."
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
            "name": "is_driver_assigned",
            "metadata": {
                "comment": "A flag indicating that a driver was assigned to the trip."
            },
        },
        {
            "name": "driver_assignment_source",
            "metadata": {"comment": "The body that assigned a driver to the trip."},
        },
        {
            "name": "device_type",
            "metadata": {"comment": "The kind of device."},
        },
        {
            "name": "attached_device_type",
            "metadata": {"comment": "The kind of camera device that is attached."},
        },
        {
            "name": "has_safety_event",
            "metadata": {
                "comment": "A flag indicating that a safety event ocurred on the trip."
            },
        },
        {
            "name": "has_camera",
            "metadata": {
                "comment": "A flag indicating that a camera was attached on the trip."
            },
        },
        {
            "name": "trip_type",
            "metadata": {
                "comment": "Indicates the type of trip: location_based, engine_based"
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
    glossary_terms=[GlossaryTerm.TELEMATICS, GlossaryTerm.MPR],
    upstreams=[
        AssetKey([Database.DATAENGINEERING, "trip_details_global"]),
        AssetKey([Database.PRODUCT_ANALYTICS_STAGING, "stg_organization_categories_global"]),
        AssetKey([Database.PRODUCT_ANALYTICS, "map_org_sam_number_latest_global"]),
    ],
)
def vehicle_gateway_trips_distance(context: AssetExecutionContext) -> str:
    return get_metric_sql(context)
