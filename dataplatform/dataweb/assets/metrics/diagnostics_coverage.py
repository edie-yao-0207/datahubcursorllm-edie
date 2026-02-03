from dagster import AssetKey, AssetExecutionContext
from dataweb import GlossaryTerm, metric
from dataweb.userpkgs.constants import AWSRegion, REGIONAL_STG_ORGANIZATION_CATEGORY_DEPENDENCIES, Database, DATAENGINEERING
from dataweb.userpkgs.utils import get_metric_sql, get_all_regions


@metric(
    description="(Regional) The number of devices covered by a given diagnostics stat",
    expressions=[
        {
            "expression_type": "count",
            "expression": "COUNT(DISTINCT CASE WHEN is_covered THEN device_id END)",
            "metadata": {
                "comment": "The count of devices covered by the given diagnostics stat"
            },
        },
        {
            "expression_type": "percent",
            "expression": "COUNT(DISTINCT CASE WHEN is_covered THEN device_id END) * 1.0 / COUNT(DISTINCT device_id)",
            "metadata": {
                "comment": "The percent of devices covered by the given diagnostics stat"
            },
        },
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
            "name": "product_id",
            "metadata": {
                "comment": "Product ID of device"
            },
        },
        {
            "name": "vehicle_make",
            "metadata": {
                "comment": "Make of vehicle"
            },
        },
        {
            "name": "vehicle_model",
            "metadata": {
                "comment": "Model of vehicle"
            },
        },
        {
            "name": "vehicle_year",
            "metadata": {
                "comment": "Year of vehicle"
            },
        },
        {
            "name": "vehicle_engine_model",
            "metadata": {
                "comment": "Engine model of vehicle"
            },
        },
        {
            "name": "vehicle_primary_fuel_type",
            "metadata": {
                "comment": "Primary fuel type of vehicle"
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
            "name": "type",
            "metadata": {
                "comment": "Type of diagnostics stat (odometer, for example)"
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
    ],
    regions=get_all_regions(),
    owners=[DATAENGINEERING],
    glossary_terms=[GlossaryTerm.TELEMATICS, GlossaryTerm.MPR],
    upstreams=[
        AssetKey([Database.PRODUCT_ANALYTICS, "dim_device_dimensions"]),
        AssetKey([Database.DATAMODEL_CORE, "dim_organizations"]),
        AssetKey([Database.PRODUCT_ANALYTICS, "agg_device_stats_secondary_coverage"]),
        AssetKey([Database.PRODUCT_ANALYTICS, "map_org_sam_number_latest"]),
        *REGIONAL_STG_ORGANIZATION_CATEGORY_DEPENDENCIES,
    ],
)
def diagnostics_coverage(context: AssetExecutionContext) -> str:
    return get_metric_sql(context)
