from dagster import AssetKey, AssetExecutionContext
from dataweb import GlossaryTerm, metric
from dataweb.userpkgs.constants import AWSRegion, REGIONAL_STG_ORGANIZATION_CATEGORY_DEPENDENCIES, Database, DATAENGINEERING
from dataweb.userpkgs.utils import get_metric_sql, get_all_regions


@metric(
    description="(Regional) Following distance event statistics for vehicles (VGs with attached dashcam only)",
    expressions=[
        {
            "expression_type": "sum",
            "expression": "COALESCE(SUM(following_distance_event_count), 0)",
            "metadata": {
                "comment": "The total number of following events"
            },
        },
        {
            "expression_type": "avg",
            "expression": "SUM(COALESCE(following_distance_event_count, 0)) * 1.0 / (SUM(COALESCE(driving_time_ms, 0)) / 3600000)",
            "metadata": {
                "comment": "The average number of following events per drive time hours"
            },
        }
    ],
    dimensions=[
        {
            "name": "date",
            "metadata": {"comment": "The field which partitions the table, in YYYY-mm-dd format."},
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
            "name": "device_id",
            "metadata": {"comment": "ID of vehicle gateway device."},
        },
        {
            "name": "driver_id",
            "metadata": {"comment": "The ID of the driver assigned to the trip, 0 if no driver is assigned."},
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
            "name": "trip_start_ms",
            "metadata": {
                "comment": "The timestamp of the start of the trip, in milliseconds since the Unix epoch."
            },
        },
        {
            "name": "trip_end_ms",
            "metadata": {
                "comment": "The timestamp of the end of the trip, in milliseconds since the Unix epoch."
            },
        },
    ],
    regions=get_all_regions(),
    owners=[DATAENGINEERING],
    glossary_terms=[GlossaryTerm.TELEMATICS, GlossaryTerm.BUSINESS_VALUE],
    upstreams=[
        AssetKey([Database.DATAMODEL_CORE, "dim_organizations"]),
        AssetKey([Database.DATAMODEL_CORE, "dim_devices"]),
        AssetKey([Database.SCORINGDB_SHARDS, "trip_scores"]),
        AssetKey([Database.DEFINITIONS, "behavior_label_type_enums"]),
        AssetKey([Database.PRODUCT_ANALYTICS, "dim_devices_safety_settings"]),
        AssetKey([Database.PRODUCT_ANALYTICS, "map_org_sam_number_latest"]),
        *REGIONAL_STG_ORGANIZATION_CATEGORY_DEPENDENCIES,
    ],
)
def vehicle_following_distance(context: AssetExecutionContext) -> str:
    return get_metric_sql(context)
