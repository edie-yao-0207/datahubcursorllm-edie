from dagster import AssetExecutionContext, DailyPartitionsDefinition
from dataweb import (
    NonEmptyDQCheck,
    NonNullDQCheck,
    PrimaryKeyDQCheck,
    ValueRangeDQCheck,
    table,
)
from dataweb.userpkgs.constants import (
    DATAENGINEERING,
    FRESHNESS_SLO_12PM_PST,
    AWSRegion,
    ColumnDescription,
    Database,
    TableType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.utils import (
    build_table_description,
    get_run_env,
    partition_key_ranges_from_context,
)

PRIMARY_KEYS = ["date", "org_id"]

SCHEMA = [
    {
        "name": "date",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "The field which partitions the table, in `YYYY-mm-dd` format."
        },
    },
    {
        "name": "org_id",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": ColumnDescription.ORG_ID,
        },
    },
    {
        "name": "org_name",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Name of organization sourced from datamodel_core.dim_organizations."
        },
    },
    {
        "name": "sam_number",
        "type": "string",
        "nullable": False,
        "metadata": {"comment": ColumnDescription.SAM_NUMBER},
    },
    {
        "name": "salesforce_sam_number",
        "type": "string",
        "nullable": False,
        "metadata": {"comment": "SAM number as reported in Salesforce, which can be different from sam_number above for parent/child accounts."},
    },
    {
        "name": "internal_type",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Field representing internal status of organizations. 0 represents customer organizations."
        },
    },
    {
        "name": "tenure_months",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Tenure of the org (in months) sourced from datamodel_core.dim_organizations_tenure."
        },
    },
    {
        "name": "is_paid_customer",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Paid customer status from datamodel_core.dim_organizations."
        },
    },
    {
        "name": "locale",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "Organization locale"},
    },
    {
        "name": "is_paid_telematics_customer",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Paid telematics customer status from datamodel_core.dim_organizations."
        },
    },
    {
        "name": "is_paid_safety_customer",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Paid telematics customer status from datamodel_core.dim_organizations."
        },
    },
    {
        "name": "is_paid_stce_customer",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Paid STCE customer status from datamodel_core.dim_organizations."
        },
    },
    {
        "name": "raw_average_daily_mileage_per_device",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment": "Raw value of average daily mileage per VG device over the past 28 days"
        },
    },
    {
        "name": "avg_mileage",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Category assignment for 'avg_mileage', one of ('less_than_250', 'greater_than_250', 'all')."
        },
    },
    {
        "name": "raw_max_subregion",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Subregion name associated with maximal amount of trips in previous 28 days."
        },
    },
    {
        "name": "raw_max_proportion_duration_subregion",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment": "Proportion of trips in previous 28 days starting in subregion for largest fraction of trips per organization."
        },
    },
    {
        "name": "region",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Category assignment of organization into 'region' category, one of ('northeast', 'midwest', 'southeast', 'southwest', 'west', 'usa_all', 'canada', 'mexico', 'all', 'eu_all')."
        },
    },
    {
        "name": "raw_fleet_size",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Count of active VG devices in previous 28 days for organization."
        },
    },
    {
        "name": "fleet_size",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Category assignment for organization into 'fleet_size' category, one of ('less_than_30', 'btw_31_and_100', 'btw_101_and_500', 'greater_than_500', 'all')."
        },
    },
    {
        "name": "industry_vertical_raw",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Raw value of org`s industry vertical sourced from datamodel_core.dim_organizations."
        },
    },
    {
        "name": "ai_model_industry_archetype",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "AI model-predicted industry archetype classification for the account"
        },
    },
    {
        "name": "ai_model_primary_industry",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "AI model-predicted primary industry classification for the account"
        },
    },
    {
        "name": "ai_model_secondary_industry",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "AI model-predicted secondary industry classification for the account"
        },
    },
    {
        "name": "industry_vertical",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Category assignment of organization to 'industry_vertical' category, one of ('field_services', 'transportation', 'public_sector', 'all')."
        },
    },
    {
        "name": "raw_fuel_category_diesel",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment": "Proportion of VG devices associated with diesel vehicles sourced from product_analytics_staging.stg_daily_fuel_type."
        },
    },
    {
        "name": "raw_fuel_category_gasoline",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment": "Proportion of VG devices associated with gasoline powered vehicles sourced from product_analytics_staging.stg_daily_fuel_type"
        },
    },
    {
        "name": "raw_fuel_category_electric",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment": "Proportion of VG devices associated with electric vehicles sourced from product_analytics_staging.stg_daily_fuel_type."
        },
    },
    {
        "name": "raw_fuel_category_other",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment": "Proportion of VG devices associated with vehicles not in (diesel, gas, electric) sourced from product_analytics_staging.stg_daily_fuel_type"
        },
    },
    {
        "name": "fuel_category",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Category assignment for organization to 'fuel_category' category, one of ('gas', 'diesel', 'electric', 'mixed', 'all')"
        },
    },
    {
        "name": "raw_primary_driving_environment_proportion_urban",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment": "Proportion of moving locations (sourced through kinesisstats.location) associated with organization VG devices associated with high population density places."
        },
    },
    {
        "name": "raw_primary_driving_environment_proportion_rural",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment": "Proportion of moving locations (sourced through kinesisstats.location) associated with organization VG devices associated with low population density places."
        },
    },
    {
        "name": "raw_primary_driving_environemnt_proportion_mixed",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment": "Proportion of moving locations (sourced through kinesisstats.location) associated with organization VG devices associated with medium population density places."
        },
    },
    {
        "name": "primary_driving_environment",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Category assignment for organization to 'primary_driving_environment' category, one of ('urban', 'rural', 'mixed', 'all')."
        },
    },
    {
        "name": "raw_fleet_composition_proportion_heavy",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment": "Proportion of vehicles associated with VG devices for organization where the gross vehicle weight rating is above 26,000 lbs."
        },
    },
    {
        "name": "raw_fleet_composition_proportion_medium",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment": "Proportion of vehicles associated with VG devices for organization where the gross vehicle weight rating is above 10,000 lbs and less than 26,000 lbs."
        },
    },
    {
        "name": "raw_fleet_composition_proportion_light",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment": "Proportion of vehicles associated with VG devices for organization where the gross vehicle weight rating is less than or equal to 10000 lbs."
        },
    },
    {
        "name": "fleet_composition",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Category assignment of organization to 'fleet_composition' category, one of ('heavy', 'medium', 'light', 'all')."
        },
    },
    {
        "name": "raw_non_hazmat_eligible_trip_proportion",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment": "Proportion of fleet`s trips associated with drivers having DOT numbers not eligible for hazmat transport."
        },
    },
    {
        "name": "raw_hazmat_eligible_trip_proportion",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment": "Proportion of fleet`s trips associated with drivers having DOT numbers eligible for hazmat transport."
        },
    },
    {
        "name": "transport_licenses_endorsements",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Category assignment of organization to 'transport_licenses_endorsements' category, one of ('hazmat_goods_carrier', 'all')."
        },
    },
    {
        "name": "account_size_segment",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Queried from edw.silver.dim_customer. Classifies accounts by size - Small Business, Mid Market, Enterprise"
        },
    },
    {
        "name": "account_arr_segment",
        "type": "string",
        "nullable": False,
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
]

QUERY = """--sql
    SELECT
    c.date,
    c.org_id,
    c.org_name,
    c.sam_number,
    o.salesforce_sam_number,
    o.internal_type,
    c.tenure_months,
    o.is_paid_customer,
    o.locale,
    c.is_paid_telematics_customer,
    c.is_paid_safety_customer,
    o.is_paid_stce_customer,
    c.raw_average_daily_mileage_per_device,
    c.avg_mileage,
    c.raw_max_usa_subregion as raw_max_subregion,
    c.raw_max_proportion_duration_usa_subregion as raw_max_proportion_duration_subregion,
    c.region,
    c.raw_fleet_size,
    c.fleet_size,
    c.industry_vertical_raw,
    c.ai_model_industry_archetype,
    c.ai_model_primary_industry,
    c.ai_model_secondary_industry,
    c.industry_vertical,
    c.raw_fuel_category_diesel,
    c.raw_fuel_category_gasoline,
    c.raw_fuel_category_electric,
    c.raw_fuel_category_other,
    c.fuel_category,
    c.raw_primary_driving_environment_proportion_urban,
    c.raw_primary_driving_environment_proportion_rural,
    c.raw_primary_driving_environemnt_proportion_mixed,
    c.primary_driving_environment,
    c.raw_fleet_composition_proportion_heavy,
    c.raw_fleet_composition_proportion_medium,
    c.raw_fleet_composition_proportion_light,
    c.fleet_composition,
    c.raw_non_hazmat_eligible_trip_proportion,
    c.raw_hazmat_eligible_trip_proportion,
    c.transport_licenses_endorsements,
    o.account_size_segment,
    o.account_arr_segment
    FROM product_analytics_staging.stg_organization_categories c
    JOIN datamodel_core.dim_organizations o
    ON c.org_id = o.org_id
    WHERE c.date BETWEEN '{PARTITION_START}' AND '{PARTITION_END}'
    AND o.date = (SELECT max(date) FROM datamodel_core.dim_organizations)

    UNION ALL

    SELECT
    c.date,
    c.org_id,
    c.org_name,
    c.sam_number,
    o.salesforce_sam_number,
    o.internal_type,
    c.tenure_months,
    o.is_paid_customer,
    o.locale,
    c.is_paid_telematics_customer,
    c.is_paid_safety_customer,
    o.is_paid_stce_customer,
    c.raw_average_daily_mileage_per_device,
    c.avg_mileage,
    c.raw_max_subregion,
    c.raw_max_proportion_duration_subregion,
    c.region,
    c.raw_fleet_size,
    c.fleet_size,
    c.industry_vertical_raw,
    c.ai_model_industry_archetype,
    c.ai_model_primary_industry,
    c.ai_model_secondary_industry,
    c.industry_vertical,
    c.raw_fuel_category_diesel,
    c.raw_fuel_category_gasoline,
    c.raw_fuel_category_electric,
    c.raw_fuel_category_other,
    c.fuel_category,
    c.raw_primary_driving_environment_proportion_urban,
    c.raw_primary_driving_environment_proportion_rural,
    c.raw_primary_driving_environemnt_proportion_mixed,
    c.primary_driving_environment,
    c.raw_fleet_composition_proportion_heavy,
    c.raw_fleet_composition_proportion_medium,
    c.raw_fleet_composition_proportion_light,
    c.fleet_composition,
    c.raw_non_hazmat_eligible_trip_proportion,
    c.raw_hazmat_eligible_trip_proportion,
    c.transport_licenses_endorsements,
    o.account_size_segment,
    o.account_arr_segment
    FROM data_tools_delta_share.product_analytics_staging.stg_organization_categories_eu c
    JOIN delta.`s3://samsara-eu-datamodel-warehouse/datamodel_core.db/dim_organizations` o
    ON c.org_id = o.org_id
    WHERE c.date BETWEEN '{PARTITION_START}' AND '{PARTITION_END}'
    AND o.date = (SELECT MAX(date) FROM delta.`s3://samsara-eu-datamodel-warehouse/datamodel_core.db/dim_organizations`)

    UNION ALL

    SELECT
    c.date,
    c.org_id,
    c.org_name,
    c.sam_number,
    o.salesforce_sam_number,
    o.internal_type,
    c.tenure_months,
    o.is_paid_customer,
    o.locale,
    c.is_paid_telematics_customer,
    c.is_paid_safety_customer,
    o.is_paid_stce_customer,
    c.raw_average_daily_mileage_per_device,
    c.avg_mileage,
    c.raw_max_usa_subregion,
    c.raw_max_proportion_duration_usa_subregion,
    c.region,
    c.raw_fleet_size,
    c.fleet_size,
    c.industry_vertical_raw,
    c.ai_model_industry_archetype,
    c.ai_model_primary_industry,
    c.ai_model_secondary_industry,
    c.industry_vertical,
    c.raw_fuel_category_diesel,
    c.raw_fuel_category_gasoline,
    c.raw_fuel_category_electric,
    c.raw_fuel_category_other,
    c.fuel_category,
    c.raw_primary_driving_environment_proportion_urban,
    c.raw_primary_driving_environment_proportion_rural,
    c.raw_primary_driving_environemnt_proportion_mixed,
    c.primary_driving_environment,
    c.raw_fleet_composition_proportion_heavy,
    c.raw_fleet_composition_proportion_medium,
    c.raw_fleet_composition_proportion_light,
    c.fleet_composition,
    c.raw_non_hazmat_eligible_trip_proportion,
    c.raw_hazmat_eligible_trip_proportion,
    c.transport_licenses_endorsements,
    o.account_size_segment,
    o.account_arr_segment
    FROM data_tools_delta_share_ca.product_analytics_staging.stg_organization_categories_ca c
    JOIN data_tools_delta_share_ca.datamodel_core.dim_organizations o
    ON c.org_id = o.org_id
    WHERE c.date BETWEEN '{PARTITION_START}' AND '{PARTITION_END}'
    AND o.date = (SELECT MAX(date) FROM data_tools_delta_share_ca.datamodel_core.dim_organizations)
--endsql"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="""Global table containing organization categories for each organization in Fleet Benchmarks""",
        row_meaning="""Each row represents the calculation of category values per organization_id for one date""",
        related_table_info={},
        table_type=TableType.DAILY_DIMENSION,
        freshness_slo_updated_by=FRESHNESS_SLO_12PM_PST,
    ),
    regions=[AWSRegion.US_WEST_2],
    owners=[DATAENGINEERING],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DailyPartitionsDefinition(start_date="2024-06-01"),
    write_mode=WarehouseWriteMode.OVERWRITE,
    dq_checks=[
        NonEmptyDQCheck(name="dq_non_empty_stg_organization_categories_global"),
        PrimaryKeyDQCheck(
            name="dq_pk_stg_organization_categories_global",
            primary_keys=PRIMARY_KEYS,
            block_before_write=True,
        ),
        NonNullDQCheck(
            name="dq_non_null_stg_organization_categories_global",
            non_null_columns=[
                "date",
                "fleet_composition",
                "primary_driving_environment",
                "fuel_category",
                "industry_vertical",
                "fleet_size",
                "region",
                "avg_mileage",
                "transport_licenses_endorsements",
                "is_paid_telematics_customer",
                "is_paid_safety_customer",
                "org_id",
                "org_name",
            ],
            block_before_write=False,
        ),
        ValueRangeDQCheck(
            name="dq_value_range_stg_organization_categories_global",
            column_range_map={"raw_fuel_category_diesel": (0, 1)},
            block_before_write=False,
        ),
    ],
    upstreams=[
        "us-west-2:product_analytics_staging.stg_organization_categories",
        "eu-west-1:product_analytics_staging.stg_organization_categories_eu",
        "ca-central-1:product_analytics_staging.stg_organization_categories_ca",
    ],
)
def stg_organization_categories_global(context: AssetExecutionContext) -> str:
    context.log.info("Updating stg_organization_categories_global")
    partition_keys = partition_key_ranges_from_context(context)[0]
    PARTITION_START = partition_keys[0]
    PARTITION_END = partition_keys[-1]
    query = QUERY.format(
        PARTITION_START=PARTITION_START,
        PARTITION_END=PARTITION_END,
    )
    context.log.info(f"{query}")
    return query
