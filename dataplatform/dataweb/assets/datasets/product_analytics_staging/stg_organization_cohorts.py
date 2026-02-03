from dagster import AssetExecutionContext, DailyPartitionsDefinition
from dataweb import NonEmptyDQCheck, NonNullDQCheck, PrimaryKeyDQCheck, table
from dataweb.userpkgs.constants import (
    DATAENGINEERING,
    FRESHNESS_SLO_12PM_PST,
    AWSRegion,
    ColumnDescription,
    Database,
    InstanceType,
    TableType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.query import create_run_config_overrides
from dataweb.userpkgs.utils import (
    build_table_description,
    partition_key_ranges_from_context,
)

PRIMARY_KEYS = ["date", "cohort_id", "org_id"]

SCHEMA = [
    {
        "name": "date",
        "type": "string",
        "nullable": False,
        "metadata": {"comment": ColumnDescription.DATE},
    },
    {
        "name": "cohort_id",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Cohort_id, always containing a v2- preface. An organization can have one or more cohort_id but only one default cohort_id."
        },
    },
    {
        "name": "avg_mileage",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Category value for 'avg_mileage' category, one of ('less_than_250', 'greater_than_250', 'all')."
        },
    },
    {
        "name": "region",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Category value for 'region' category, one of ('northeast','midwest','southeast','southwest', 'west', 'usa_all', 'canada', 'mexico','all')."
        },
    },
    {
        "name": "fleet_size",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Category value for 'fleet_size' category, one of ('less_than_30', 'btw_31_and_100', 'btw_101_and_500', 'greater_than_500', 'all')."
        },
    },
    {
        "name": "industry_vertical",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Category value for 'industry_vertical' category, one of ('field_services', 'transportation', 'public_sector', 'all'). For organizations with less than 1 month tenure, only the industry_vertical is used to assign cohorts. If there is no industry vertical in salesforce for the organization, the organization's default cohort will have 'all' for every category including industry vertical."
        },
    },
    {
        "name": "fuel_category",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Category value for 'fuel_category' category, one of ('gas', 'diesel', 'electric', 'mixed', 'all')"
        },
    },
    {
        "name": "primary_driving_environment",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Category value for 'primary_driving_environment' category, one of ('urban', 'rural', 'mixed', 'all')."
        },
    },
    {
        "name": "fleet_composition",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Category value for 'fleet_composition' category, one of ('heavy', 'medium', 'light', 'all')."
        },
    },
    {
        "name": "transport_licenses_endorsements",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Category value for 'transport_licenses_endorsements' category, one of ('all', 'hazmat_goods_carrier')."
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
        "name": "is_paid_telematics_customer",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Paid telematics customer status, sourced from datamodel_core.dim_organizations."
        },
    },
    {
        "name": "is_paid_safety_customer",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Paid safety customer status, sourced from datamodel_core.dim_organizations."
        },
    },
    {
        "name": "tenure_months",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Organization tenure in months sourced from datamodel_core.dim_organizations_tenure."
        },
    },
    {
        "name": "cohort_paid_safety_customers_tenure_6months",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Count of organizations with both paid safety customer status and at least 6 months of tenure corresponding to cohort_id."
        },
    },
    {
        "name": "cohort_paid_telematics_customers_tenure_6months",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Count of organizations with both paid telematics customer status and at least 6 months of tenure corresponding to cohort_id."
        },
    },
    {
        "name": "cohort_size",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Count valid benchmark organizations corresponding to cohort_id. A valid benchmark organization has either paid telematics or safety customer status and a tenure of at least 6 months."
        },
    },
    {
        "name": "is_valid",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "True if cohort contains at least 9 paid safety or telematics customers with a tenure of at least 6 months and at least 5 customers for which the mobile usage metric is available (due to having a dual facing dashcam). "
        },
    },
    {
        "name": "is_default_cohort",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "True only one time per org_id, for the smallest size valid cohort for which the organization corresponds to."
        },
    },
    {
        "name": "is_valid_benchmark_org",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "True if org is either a paid telematics or safety customer with a tenure >= 6 months. Used to compute the cohort_size and determine if organization metrics will be used to calculate a future metric benchmark."
        },
    },
]

QUERY = """
WITH
mobile_usage_orgs AS (
(SELECT DISTINCT org_id,
       is_metric_populated
FROM product_analytics_staging.stg_organization_benchmark_metrics
WHERE metric_type = 'MOBILE_USAGE_EVENTS_PER_1000MI'
AND date = '{PARTITION_START}')
UNION ALL
(SELECT DISTINCT org_id,
       is_metric_populated
FROM data_tools_delta_share.product_analytics_staging.stg_organization_benchmark_metrics
WHERE metric_type = 'MOBILE_USAGE_EVENTS_PER_1000MI'
AND date = '{PARTITION_START}')),
stg_organization_categories AS (
(SELECT org_id,
        is_paid_telematics_customer,
        is_paid_safety_customer,
        avg_mileage,
        region,
        fleet_size,
        industry_vertical,
        fuel_category,
        primary_driving_environment,
        fleet_composition,
        transport_licenses_endorsements,
        tenure_months
FROM product_analytics_staging.stg_organization_categories
WHERE date = '{PARTITION_START}')
UNION  ALL
(SELECT org_id,
        is_paid_telematics_customer,
        is_paid_safety_customer,
        avg_mileage,
        region,
        fleet_size,
        industry_vertical,
        fuel_category,
        primary_driving_environment,
        fleet_composition,
        transport_licenses_endorsements,
        tenure_months
FROM data_tools_delta_share.product_analytics_staging.stg_organization_categories_eu
WHERE date = '{PARTITION_START}')),
org_mapping_pre AS (
SELECT
       stg_organization_categories.*,
       COALESCE(mobile_usage_orgs.is_metric_populated, FALSE) AS has_mobile_usage_metric
FROM stg_organization_categories
LEFT OUTER JOIN mobile_usage_orgs ON mobile_usage_orgs.org_id = stg_organization_categories.org_id),
cat_1 AS (
SELECT org_id,
        is_paid_telematics_customer,
        is_paid_safety_customer,
        avg_mileage,
        region,
        fleet_size,
        industry_vertical,
        fuel_category,
        primary_driving_environment,
        fleet_composition,
        transport_licenses_endorsements,
        tenure_months,
        has_mobile_usage_metric
FROM org_mapping_pre WHERE tenure_months >= 1 UNION ALL
SELECT org_id,
        is_paid_telematics_customer,
        is_paid_safety_customer,
        avg_mileage,
        region,
        fleet_size,
        industry_vertical,
        fuel_category,
        primary_driving_environment,
        fleet_composition,
        'all' as transport_licenses_endorsements,
        tenure_months,
        has_mobile_usage_metric
FROM org_mapping_pre
WHERE transport_licenses_endorsements != 'all'
 and tenure_months >= 1 ),
cat_2 AS (
SELECT * FROM cat_1 UNION ALL
SELECT org_id,
        is_paid_telematics_customer,
        is_paid_safety_customer,
        avg_mileage,
        region,
        fleet_size,
        industry_vertical,
        fuel_category,
        primary_driving_environment,
        'all' as fleet_composition,
        transport_licenses_endorsements,
        tenure_months,
        has_mobile_usage_metric
FROM cat_1
WHERE fleet_composition != 'all'),
cat_3 AS (
SELECT * FROM cat_2 UNION ALL
SELECT org_id,
        is_paid_telematics_customer,
        is_paid_safety_customer,
        avg_mileage,
        region,
        fleet_size,
        industry_vertical,
        'all' as fuel_category,
        primary_driving_environment,
        fleet_composition,
        transport_licenses_endorsements,
        tenure_months,
        has_mobile_usage_metric
FROM cat_2
WHERE fuel_category != 'all'),
cat_4 AS (
SELECT * FROM cat_3 UNION ALL
SELECT org_id,
        is_paid_telematics_customer,
        is_paid_safety_customer,
        avg_mileage,
        region,
        'all' as fleet_size,
        industry_vertical,
        fuel_category,
        primary_driving_environment,
        fleet_composition,
        transport_licenses_endorsements,
        tenure_months,
        has_mobile_usage_metric
FROM cat_3
WHERE fleet_size != 'all'),
cat_5 AS (
SELECT * FROM cat_4 UNION ALL
SELECT org_id,
        is_paid_telematics_customer,
        is_paid_safety_customer,
        'all' as avg_mileage,
        region,
        fleet_size,
        industry_vertical,
        fuel_category,
        primary_driving_environment,
        fleet_composition,
        transport_licenses_endorsements,
        tenure_months,
        has_mobile_usage_metric
FROM cat_4
WHERE avg_mileage != 'all'),
cat_6 AS (
SELECT * FROM cat_5 UNION ALL
SELECT org_id,
        is_paid_telematics_customer,
        is_paid_safety_customer,
        avg_mileage,
        region,
        fleet_size,
        industry_vertical,
        fuel_category,
        'all' as primary_driving_environment,
        fleet_composition,
        transport_licenses_endorsements,
        tenure_months,
        has_mobile_usage_metric
FROM cat_5
WHERE primary_driving_environment != 'all'),
cat_7 AS (
SELECT * FROM cat_6 UNION ALL
SELECT org_id,
        is_paid_telematics_customer,
        is_paid_safety_customer,
        avg_mileage,
        region,
        fleet_size,
        'all' as industry_vertical,
        fuel_category,
        primary_driving_environment,
        fleet_composition,
        transport_licenses_endorsements,
        tenure_months,
        has_mobile_usage_metric
FROM cat_6
WHERE industry_vertical != 'all'),
cat_8 AS (
SELECT * FROM cat_7
UNION ALL
SELECT org_id,
        is_paid_telematics_customer,
        is_paid_safety_customer,
        avg_mileage,
        'all' as region,
        fleet_size,
        industry_vertical,
        fuel_category,
        primary_driving_environment,
        fleet_composition,
        transport_licenses_endorsements,
        tenure_months,
        has_mobile_usage_metric
FROM cat_7
WHERE region != 'all'
UNION ALL
SELECT org_id,
        is_paid_telematics_customer,
        is_paid_safety_customer,
        avg_mileage,
        'usa_all' as region,
        fleet_size,
        industry_vertical,
        fuel_category,
        primary_driving_environment,
        fleet_composition,
        transport_licenses_endorsements,
        tenure_months,
        has_mobile_usage_metric
FROM cat_7
WHERE region in ('northeast', 'midwest','southeast', 'southwest', 'west')),
org_mapping as (
SELECT * FROM cat_8 UNION ALL
SELECT org_id,
        is_paid_telematics_customer,
        is_paid_safety_customer,
        avg_mileage,
        region,
        fleet_size,
        industry_vertical,
        fuel_category,
        primary_driving_environment,
        fleet_composition,
        transport_licenses_endorsements,
        tenure_months,
        has_mobile_usage_metric
FROM org_mapping_pre
WHERE tenure_months < 1 UNION ALL
SELECT org_id,
        is_paid_telematics_customer,
        is_paid_safety_customer,
        avg_mileage,
        region,
        fleet_size,
        'all' as industry_vertical,
        fuel_category,
        primary_driving_environment,
        fleet_composition,
        transport_licenses_endorsements,
        tenure_months,
        has_mobile_usage_metric
FROM org_mapping_pre
WHERE tenure_months < 1
  AND industry_vertical != 'all'),
cohort_org_join AS (
SELECT cohorts.cohort_id,
       cohorts.avg_mileage,
       cohorts.region,
       cohorts.fleet_size,
       cohorts.industry_vertical,
       cohorts.fuel_category,
       cohorts.primary_driving_environment,
       cohorts.fleet_composition,
       cohorts.transport_licenses_endorsements,
       org_mapping.org_id,
       org_mapping.is_paid_telematics_customer, --paid safety or telematics will be used to select benchmarking inclusion
       org_mapping.is_paid_safety_customer,
       org_mapping.tenure_months, -- the tenure months will be used to select benchmarking inclusion
       org_mapping.has_mobile_usage_metric
FROM product_analytics_staging.map_benchmark_cohorts cohorts
LEFT OUTER JOIN org_mapping
  ON  org_mapping.avg_mileage = cohorts.avg_mileage
       AND org_mapping.fleet_size = cohorts.fleet_size
       AND org_mapping.fuel_category = cohorts.fuel_category
       AND org_mapping.primary_driving_environment = cohorts.primary_driving_environment
       AND org_mapping.fleet_composition = cohorts.fleet_composition
       AND org_mapping.transport_licenses_endorsements = cohorts.transport_licenses_endorsements
       AND org_mapping.industry_vertical = cohorts.industry_vertical
       AND org_mapping.region = cohorts.region
WHERE cohorts.date = '{PARTITION_START}'),
valid_cohort_calc AS (
SElECT cohort_id,
       COUNT(DISTINCT CASE WHEN is_paid_telematics_customer = TRUE THEN org_id END) AS cohort_paid_telematics_customers,
       COUNT(DISTINCT CASE WHEN is_paid_telematics_customer = TRUE
                            AND tenure_months >= 6 THEN org_id END) AS cohort_paid_telematics_customers_tenure_6months,
       COUNT(DISTINCT CASE WHEN is_paid_safety_customer = TRUE THEN org_id END) AS cohort_paid_safety_customers,
       COUNT(DISTINCT CASE WHEN is_paid_safety_customer = TRUE AND tenure_months >= 6 THEN org_id END) AS cohort_paid_safety_customers_tenure_6months,
       COUNT(DISTINCT CASE WHEN is_paid_safety_customer = TRUE OR is_paid_telematics_customer = TRUE THEN org_id END) AS cohort_paid_customers,
       COUNT(DISTINCT CASE WHEN (is_paid_safety_customer = TRUE OR is_paid_telematics_customer = TRUE)
                              AND tenure_months >= 6
                              AND has_mobile_usage_metric = TRUE THEN org_id END) AS cohort_mobile_usage_customers,
       COUNT(DISTINCT CASE WHEN (is_paid_safety_customer = TRUE OR is_paid_telematics_customer = TRUE) and tenure_months >= 6 THEN org_id END) AS  cohort_size
FROM cohort_org_join
GROUP BY  1),
valid_cohort_criteria AS (
SELECT cohort_id,
       cohort_paid_safety_customers_tenure_6months,
       cohort_paid_telematics_customers_tenure_6months,
       cohort_size,
       cohort_mobile_usage_customers,
       CASE WHEN cohort_size >= 9 AND cohort_mobile_usage_customers >= 5 THEN TRUE ELSE FALSE END is_valid
FROM valid_cohort_calc),
valid_cohort_join AS (
SELECT cohort_orgs.*  EXCEPT (cohort_orgs.has_mobile_usage_metric),
       valid_cohort_criteria.* EXCEPT (valid_cohort_criteria.cohort_id, valid_cohort_criteria.cohort_mobile_usage_customers)
FROM cohort_org_join cohort_orgs
LEFT OUTER JOIN valid_cohort_criteria on valid_cohort_criteria.cohort_id = cohort_orgs.cohort_id
--exclude cohorts with no corresponding orgs to avoid null primary key
WHERE cohort_orgs.org_id IS NOT NULL),
default_cohort_calc AS (
SELECT
org_id,
cohort_size,
cohort_id,
ROW_NUMBER() OVER (PARTITION BY org_id ORDER BY cohort_size ASC, cohort_id ASC) AS row_num
FROM valid_cohort_join
WHERE is_valid = TRUE
--default cohorts only contain industry_vertical, primary_driving_environment, avg_mileage, and fleet_composition
--other categories are optional and are set to "all" for the
AND region IN ('usa_all', 'eu_all', 'all')
AND fleet_size = 'all'
AND fuel_category = 'all'
AND transport_licenses_endorsements = 'all'),
default_cohort_selection AS (
SELECT
org_id,
cohort_id,
TRUE AS is_default_cohort
FROM default_cohort_calc
WHERE row_num = 1)

SELECT '{PARTITION_START}' AS date,
       valid_cohort_join.*,
       COALESCE(default_cohort_selection.is_default_cohort, FALSE) AS is_default_cohort,
       CASE WHEN (is_paid_safety_customer = TRUE
                 OR is_paid_telematics_customer = TRUE)
                 AND tenure_months >= 6 THEN TRUE ELSE FALSE END AS is_valid_benchmark_org
FROM valid_cohort_join
LEFT OUTER JOIN default_cohort_selection
       ON default_cohort_selection.cohort_id = valid_cohort_join.cohort_id
       AND default_cohort_selection.org_id = valid_cohort_join.org_id
"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="""Assignment of organizations to relevant cohorts and default cohorts. Calculation of whether cohorts are valid by meeting composition requirements.""",
        row_meaning="""Each row represents the assignment of an organization_id to 1 or more cohort_id per date.""",
        related_table_info={},
        table_type=TableType.STAGING,
        freshness_slo_updated_by=FRESHNESS_SLO_12PM_PST,
    ),
    regions=[AWSRegion.US_WEST_2],
    owners=[DATAENGINEERING],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DailyPartitionsDefinition(start_date="2024-06-01"),
    write_mode=WarehouseWriteMode.OVERWRITE,
    run_config_overrides=create_run_config_overrides(
        driver_instance_type=InstanceType.RD_FLEET_4XLARGE,
        worker_instance_type=InstanceType.RD_FLEET_4XLARGE,
        max_workers=48,
        spark_conf_overrides={
            "spark.delta.sharing.driver.accessThresholdToExpireMs": "9000000",
        }
    ),
    dq_checks=[
        NonEmptyDQCheck(name="dq_non_empty_stg_organization_cohorts"),
        PrimaryKeyDQCheck(
            name="dq_pk_stg_organization_cohorts",
            primary_keys=PRIMARY_KEYS,
            block_before_write=True,
        ),
        NonNullDQCheck(
            name="dq_non_null_stg_organization_cohorts",
            non_null_columns=[
                "date",
                "cohort_id",
                "avg_mileage",
                "region",
                "fleet_size",
                "industry_vertical",
                "fuel_category",
                "primary_driving_environment",
                "fleet_composition",
                "org_id",
                "is_paid_telematics_customer",
                "is_paid_safety_customer",
                "tenure_months",
                "cohort_paid_safety_customers_tenure_6months",
                "cohort_paid_telematics_customers_tenure_6months",
                "cohort_size",
                "is_valid",
                "is_default_cohort",
                "is_valid_benchmark_org",
            ],
            block_before_write=False,
        ),
    ],
    upstreams=[
        "us-west-2:product_analytics_staging.stg_organization_categories",
        "us-west-2:product_analytics_staging.map_benchmark_cohorts",
        "us-west-2:product_analytics_staging.stg_organization_benchmark_metrics",
        "eu-west-1:product_analytics_staging.stg_organization_categories_eu",
        "eu-west-1:product_analytics_staging.stg_organization_benchmark_metrics",
    ],
)
def stg_organization_cohorts(context: AssetExecutionContext) -> str:
    context.log.info("Updating stg_organization_cohorts")
    partition_keys = partition_key_ranges_from_context(context)[0]
    PARTITION_START = partition_keys[0]
    query = QUERY.format(
        PARTITION_START=PARTITION_START,
    )
    context.log.info(f"{query}")
    return query
