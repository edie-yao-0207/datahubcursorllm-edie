from dagster import AssetExecutionContext, DailyPartitionsDefinition
from dataweb import NonEmptyDQCheck, NonNullDQCheck, PrimaryKeyDQCheck, table
from dataweb.userpkgs.constants import (
    DATAENGINEERING,
    FRESHNESS_SLO_12PM_PST,
    MEMORY_OPTIMIZED_INSTANCE_POOL_KEY,
    AWSRegion,
    ColumnDescription,
    Database,
    TableType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.query import create_run_config_overrides
from dataweb.userpkgs.utils import (
    build_table_description,
    get_run_env,
    partition_key_ranges_from_context,
)
from pyspark.sql import DataFrame, SparkSession

PRIMARY_KEYS = ["date", "org_id", "cohort_id", "metric_type", "lookback_window", "metric_config_type"]


SCHEMA = [
    {
        "name": "date",
        "type": "string",
        "nullable": False,
        "metadata": {"comment": ColumnDescription.DATE},
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
    {
        "name": "metric_type",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Name of metric for inclusion in benchmarking. One of IDLING_PERCENT, CRASH_PER_MILLION_MI, SEVERE_SPEEDING, MOBILE_EVENTS_PER_1000MI, MPGE, DROWSINESS_PER_1000MI.",
        },
    },
    {
        "name": "lookback_window",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Lookback window for metric",
        },
    },
    {
        "name": "is_metric_populated",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "TRUE for all cases. This value is incorporated downstream to support sizing the number of organizations for which there is a valid benchmark value.",
        },
    },
    {
        "name": "metric_value",
        "type": "double",
        "nullable": False,
        "metadata": {
            "comment": "Calculation of monthly value of metric for organization_id for previous 28 days as of partition date.",
        },
    },
    {
        "name": "metric_config_type",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "For a given metric, outputs the corresponding configurable metric bucket (if exists) for that org.",
        },
    },
    {
        "name": "is_default_metric_config_type",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether metric config type is the default or not for a given cohort",
        },
    },
]

QUERY = """
WITH organization_config_buckets AS (
  -- bring together the org settings
  SELECT
    org_id,
    severe_speeding_bucket,
    mobile_usage_bucket,
    inattentive_driving_bucket
  FROM product_analytics_staging.stg_organization_config_buckets
  WHERE date = '{PARTITION_START}'

  UNION ALL

  SELECT
    org_id,
    severe_speeding_bucket,
    mobile_usage_bucket,
    inattentive_driving_bucket
  FROM data_tools_delta_share.product_analytics_staging.stg_organization_config_buckets
  WHERE date = '{PARTITION_START}'
),
benchmark_metrics AS (
  -- bring together the metrics
  SELECT
    org_id,
    metric_type,
    lookback_window,
    is_metric_populated,
    metric_value
  FROM {SOURCE}.stg_organization_benchmark_metrics
  WHERE date = '{PARTITION_START}'

  UNION ALL

  SELECT
    org_id,
    metric_type,
    lookback_window,
    is_metric_populated,
    metric_value
  FROM data_tools_delta_share.product_analytics_staging.stg_organization_benchmark_metrics
  WHERE date = '{PARTITION_START}'
),
non_configurable_metrics AS (
  -- metric_config_type always ALL here
  SELECT
    cohort.date,
    cohort.org_id,
    cohort_id,
    avg_mileage,
    region,
    fleet_size,
    industry_vertical,
    fuel_category,
    primary_driving_environment,
    fleet_composition,
    transport_licenses_endorsements,
    is_paid_telematics_customer,
    is_paid_safety_customer,
    tenure_months,
    cohort_paid_safety_customers_tenure_6months,
    cohort_paid_telematics_customers_tenure_6months,
    cohort_size,
    is_valid,
    is_default_cohort,
    is_valid_benchmark_org,
    metric.metric_type,
    metric.lookback_window,
    metric.is_metric_populated,
    metric.metric_value,
    'ALL' as metric_config_type
  FROM product_analytics_staging.stg_organization_cohorts cohort
  LEFT OUTER JOIN benchmark_metrics metric
    ON cohort.org_id = metric.org_id
  WHERE cohort.date = '{PARTITION_START}'
    AND cohort.is_valid
    AND cohort.is_valid_benchmark_org
    AND metric.metric_type not in {CONFIGURABLE_METRICS}
),
configurable_metrics AS (
  -- use bucket setting (unless null, in which case, use ALL)
  SELECT
    cohort.date,
    cohort.org_id,
    cohort_id,
    avg_mileage,
    region,
    fleet_size,
    industry_vertical,
    fuel_category,
    primary_driving_environment,
    fleet_composition,
    transport_licenses_endorsements,
    is_paid_telematics_customer,
    is_paid_safety_customer,
    tenure_months,
    cohort_paid_safety_customers_tenure_6months,
    cohort_paid_telematics_customers_tenure_6months,
    is_valid,
    is_default_cohort,
    is_valid_benchmark_org,
    metric.metric_type,
    metric.lookback_window,
    metric.is_metric_populated,
    metric.metric_value,
    CASE
        WHEN metric.metric_type = 'SEVERE_SPEEDING' THEN COALESCE(config.severe_speeding_bucket, 'ALL')
        WHEN metric.metric_type = 'MOBILE_USAGE_EVENTS_PER_1000MI' THEN COALESCE(config.mobile_usage_bucket, 'ALL')
        WHEN metric.metric_type = 'IA_DRIVING_COMBINED_EVENTS_PER_1000MI' THEN COALESCE(config.inattentive_driving_bucket, 'ALL')
    END AS metric_config_type
  FROM product_analytics_staging.stg_organization_cohorts cohort
  LEFT OUTER JOIN benchmark_metrics metric
    ON cohort.org_id = metric.org_id
  LEFT OUTER JOIN organization_config_buckets config
    ON metric.org_id = config.org_id
  WHERE cohort.date = '{PARTITION_START}'
    AND cohort.is_valid
    AND cohort.is_valid_benchmark_org
    AND metric.metric_type in {CONFIGURABLE_METRICS}
),
configurable_metrics_all AS (
-- get ALL grouping for configurable metrics
SELECT
    cohort.date,
    cohort.org_id,
    cohort_id,
    avg_mileage,
    region,
    fleet_size,
    industry_vertical,
    fuel_category,
    primary_driving_environment,
    fleet_composition,
    transport_licenses_endorsements,
    is_paid_telematics_customer,
    is_paid_safety_customer,
    tenure_months,
    cohort_paid_safety_customers_tenure_6months,
    cohort_paid_telematics_customers_tenure_6months,
    cohort_size,
    is_valid,
    is_default_cohort,
    is_valid_benchmark_org,
    metric.metric_type,
    metric.lookback_window,
    metric.is_metric_populated,
    metric.metric_value,
    'ALL' AS metric_config_type
  FROM product_analytics_staging.stg_organization_cohorts cohort
  LEFT OUTER JOIN benchmark_metrics metric
    ON cohort.org_id = metric.org_id
  WHERE cohort.date = '{PARTITION_START}'
    AND cohort.is_valid
    AND cohort.is_valid_benchmark_org
    AND metric.metric_type in {CONFIGURABLE_METRICS}
),
configurable_metric_cohort_size AS (
  -- get new cohort size for configurable metrics
  SELECT
    cohort_id,
    metric_type,
    metric_config_type,
    COUNT(DISTINCT org_id) as cohort_size
  FROM configurable_metrics
  GROUP BY 1, 2, 3
)
--union together
SELECT
    date,
    org_id,
    cohort_id,
    avg_mileage,
    region,
    fleet_size,
    industry_vertical,
    fuel_category,
    primary_driving_environment,
    fleet_composition,
    transport_licenses_endorsements,
    is_paid_telematics_customer,
    is_paid_safety_customer,
    tenure_months,
    cohort_paid_safety_customers_tenure_6months,
    cohort_paid_telematics_customers_tenure_6months,
    cohort_size,
    is_valid,
    is_default_cohort,
    is_valid_benchmark_org,
    metric_type,
    lookback_window,
    is_metric_populated,
    metric_value,
    metric_config_type,
    CASE
        WHEN is_default_cohort THEN true
        ELSE false
    END AS is_default_metric_config_type
FROM non_configurable_metrics

UNION ALL

SELECT
    date,
    org_id,
    cohort_id,
    avg_mileage,
    region,
    fleet_size,
    industry_vertical,
    fuel_category,
    primary_driving_environment,
    fleet_composition,
    transport_licenses_endorsements,
    is_paid_telematics_customer,
    is_paid_safety_customer,
    tenure_months,
    cohort_paid_safety_customers_tenure_6months,
    cohort_paid_telematics_customers_tenure_6months,
    cohort_size,
    is_valid,
    is_default_cohort,
    is_valid_benchmark_org,
    metric_type,
    lookback_window,
    is_metric_populated,
    metric_value,
    metric_config_type,
    CASE
        WHEN is_default_cohort THEN true
        ELSE false
    END AS is_default_metric_config_type
FROM configurable_metrics_all

UNION ALL

SELECT
    metrics.date,
    metrics.org_id,
    metrics.cohort_id,
    metrics.avg_mileage,
    metrics.region,
    metrics.fleet_size,
    metrics.industry_vertical,
    metrics.fuel_category,
    metrics.primary_driving_environment,
    metrics.fleet_composition,
    metrics.transport_licenses_endorsements,
    metrics.is_paid_telematics_customer,
    metrics.is_paid_safety_customer,
    metrics.tenure_months,
    metrics.cohort_paid_safety_customers_tenure_6months,
    metrics.cohort_paid_telematics_customers_tenure_6months,
    cohort_size.cohort_size,
    metrics.is_valid,
    metrics.is_default_cohort,
    metrics.is_valid_benchmark_org,
    metrics.metric_type,
    metrics.lookback_window,
    metrics.is_metric_populated,
    metrics.metric_value,
    metrics.metric_config_type,
    CASE
        WHEN metrics.is_default_cohort THEN true
        ELSE false
    END AS is_default_metric_config_type
FROM configurable_metrics metrics
JOIN configurable_metric_cohort_size cohort_size
    ON metrics.cohort_id = cohort_size.cohort_id
    AND metrics.metric_type = cohort_size.metric_type
    AND metrics.metric_config_type = cohort_size.metric_config_type
WHERE cohort_size.cohort_size >= 9
AND metrics.metric_config_type != 'ALL' -- remove from here as they'll be in above group
"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="""This table stores all the different metric_type_configs for a given org_id/cohort_id combination""",
        row_meaning="""Each row represents an org's metrics and their given metric_config_type (if configurable)""",
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
    dq_checks=[
        NonEmptyDQCheck(name="dq_non_empty_stg_organization_metric_configs"),
        PrimaryKeyDQCheck(
            name="dq_pk_stg_organization_metric_configs",
            primary_keys=PRIMARY_KEYS,
            block_before_write=True,
        ),
        NonNullDQCheck(
            name="dq_non_null_stg_organization_metric_configs",
            non_null_columns=PRIMARY_KEYS,
            block_before_write=True,
        ),
    ],
    upstreams=[
        "us-west-2|eu-west-1:product_analytics_staging.stg_organization_config_buckets",
        "us-west-2|eu-west-1:product_analytics_staging.stg_organization_benchmark_metrics",
        "us-west-2:product_analytics_staging.stg_organization_cohorts",
    ],
    run_config_overrides=create_run_config_overrides(
        instance_pool_type=MEMORY_OPTIMIZED_INSTANCE_POOL_KEY,
        max_workers=8,
    ),
)
def stg_organization_metric_configs(context: AssetExecutionContext) -> DataFrame:
    context.log.info("Updating stg_organization_metric_configs")
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    partition_keys = partition_key_ranges_from_context(context)[0]
    PARTITION_START = partition_keys[0]
    CONFIGURABLE_METRICS = [
        "SEVERE_SPEEDING",
        "MOBILE_USAGE_EVENTS_PER_1000MI",
        'IA_DRIVING_COMBINED_EVENTS_PER_1000MI',
    ]
    CONFIGURABLE_METRICS = (
        "(" + ", ".join("'" + metric + "'" for metric in CONFIGURABLE_METRICS) + ")"
    )
    if get_run_env() == "dev":
        source = "datamodel_dev"
    else:
        source = "product_analytics_staging"
    query = QUERY.format(
        PARTITION_START=PARTITION_START,
        SOURCE=source,
        CONFIGURABLE_METRICS=CONFIGURABLE_METRICS,
    )
    df = spark.sql(query)
    context.log.info(f"{query}")
    return df
