from dagster import AssetKey, AssetExecutionContext
from dataweb import GlossaryTerm, metric
from dataweb.userpkgs.constants import AWSRegion, REGIONAL_STG_ORGANIZATION_CATEGORY_DEPENDENCIES, Database, DATAENGINEERING
from dataweb.userpkgs.utils import get_metric_sql, get_all_regions


@metric(
    description="(Regional) The percentage of time that the vehicle is on that the engine is in an idle state for VG devices.",
    expressions=[
        {
            "expression_type": "percent",
            "expression": "SUM(idle_duration_ms * 1.0) / SUM(on_duration_ms)",
            "metadata": {
                "comment": "The ratio of time spent in the engine idle state to time spent in the engine on state."
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
            "name": "driver_id",
            "metadata": {"comment": "ID of driver or 0 if no driver_id is available."},
        },
        {
            "name": "device_id",
            "metadata": {"comment": "ID of vehicle gateway device."},
        },
        {
            "name": "interval_start",
            "metadata": {"comment": "Start of hourly interval for idling duration calculation."},
        },
        {
            "name": "interval_end",
            "metadata": {"comment": "End of hourly interval for idling duration calculation."},
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
    ],
    regions=get_all_regions(),
    owners=[DATAENGINEERING],
    glossary_terms=[GlossaryTerm.TELEMATICS, GlossaryTerm.BUSINESS_VALUE],
    upstreams=[
        AssetKey([Database.DATAMODEL_CORE, "dim_organizations"]),
        AssetKey(["ecodriving_report", "report"]),
        AssetKey([Database.PRODUCT_ANALYTICS, "map_org_sam_number_latest"]),
        *REGIONAL_STG_ORGANIZATION_CATEGORY_DEPENDENCIES,
    ],
)
def vehicle_idle_duration(context: AssetExecutionContext) -> str:
    return get_metric_sql(context)
