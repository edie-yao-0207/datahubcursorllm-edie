from dagster import AssetExecutionContext, DailyPartitionsDefinition
from dataweb import (
    NonEmptyDQCheck,
    NonNullDQCheck,
    PrimaryKeyDQCheck,
    SQLDQCheck,
    table,
)
from dataweb._core.dq_utils import Operator
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
from pyspark.sql import DataFrame, SparkSession

PRIMARY_KEYS = ["date", "cohort_id", "category_id", "category_value"]

SCHEMA = [
    {
        "name": "date",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": ColumnDescription.DATE,
        },
    },
    {
        "name": "cohort_id",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Id of cohort for corresponding category_id and category_value.",
        },
    },
    {
        "name": "category_id",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Name of category used for fleet benchmarking - primary_driving_environment, region, fleet_composition, avg_daily_mileage, fuel_category, industry_vertical, fleet_size",
        },
    },
    {
        "name": "category_value",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Corresponding value of category_id for each cohort_id. For example: the values for industry_vertical are field_services, transportation, public_sector, or all.",
        },
    },
]

QUERY = """
WITH cohorts AS (
SELECT DISTINCT
cohort_id,
avg_mileage,
industry_vertical,
region,
fleet_size,
fuel_category,
fleet_composition,
primary_driving_environment,
transport_licenses_endorsements
FROM product_analytics_staging.stg_organization_cohorts
WHERE is_valid = TRUE
 AND date = '{PARTITION_START}'),
 unioned AS (
(select cohort_id,
       'industry_vertical' as category_name,
       industry_vertical as category_value
from cohorts)
union all
(select cohort_id,
       'region' as category_name,
       region as category_value
from cohorts)
union all
(select cohort_id,
       'fuel_category' as category_name,
       fuel_category as category_value
from cohorts)
union all
(select cohort_id,
       'fleet_composition' as category_name,
       fleet_composition as category_value
from cohorts)
union all
(select cohort_id,
       'avg_daily_mileage' as category_name,
       avg_mileage as category_value
from cohorts)
union all
(select cohort_id,
       'primary_driving_environment' as category_name,
       primary_driving_environment as category_value
from cohorts)
union all
(select cohort_id,
       'fleet_size' as category_name,
       fleet_size as category_value
from cohorts)
union all
(select cohort_id,
       'transport_licenses_endorsements' as category_name,
       transport_licenses_endorsements as category_value
from cohorts))


select '{PARTITION_START}' AS date,
       cohort_id,
       category_name AS category_id,
       category_value
from unioned
order by cohort_id asc, category_id asc
"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="""Normalized representation of category_id's and category_values for valid cohort_id's.""",
        row_meaning="""Each row represents the category_value for each category_id of a cohort_id per date.""",
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
        NonEmptyDQCheck(name="dq_non_empty_stg_cohort_categories"),
        PrimaryKeyDQCheck(
            name="dq_pk_stg_cohort_categories",
            primary_keys=PRIMARY_KEYS,
            block_before_write=True,
        ),
        NonNullDQCheck(
            name="dq_non_null_stg_cohort_categories",
            non_null_columns=["date", "cohort_id", "category_id", "category_value"],
            block_before_write=False,
        ),
        SQLDQCheck(
            name="dq_sql_count_cohorts",
            sql_query="""
            select count(distinct cohort_id) as observed_value from df""",
            expected_value=50000,
            operator=Operator.lte,
            block_before_write=True,
        ),
    ],
    upstreams=[
        "us-west-2:product_analytics_staging.stg_organization_default_cohorts",
    ],
)
def stg_cohort_categories(context: AssetExecutionContext) -> DataFrame:
    context.log.info("Updating stg_cohort_categories")
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    partition_keys = partition_key_ranges_from_context(context)[0]
    PARTITION_START = partition_keys[0]
    query = QUERY.format(
        PARTITION_START=PARTITION_START,
    )
    if get_run_env() == "dev":
        path = "/Volumes/s3/benchmarking-metrics/root/staging/cohort_categories"
    else:
        # TODO change to prod when backend is ready to read from prod path
        # "/Volumes/s3/benchmarking-metrics/root/prod/cohort_categories"
        path = "/Volumes/s3/benchmarking-metrics/root/staging/cohort_categories"
    df = spark.sql(query)
    df_csv = df.select("cohort_id", "category_id", "category_value")
    df_csv.coalesce(1).write.mode("overwrite").option("header", "true").option(
        "mapreduce.fileoutputcommitter.marksuccessfuljobs", "false"
    ).csv(path)
    context.log.info(f"{query}")
    return df
