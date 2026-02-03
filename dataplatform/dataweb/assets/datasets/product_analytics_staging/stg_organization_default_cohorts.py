from dagster import AssetExecutionContext, DailyPartitionsDefinition
from dataweb import NonEmptyDQCheck, NonNullDQCheck, PrimaryKeyDQCheck, table
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

PRIMARY_KEYS = ["date", "org_id"]

# The freeze date is used to refer to the last date that cohorts should be generated for all organizations
# On a daily basis, default cohorts will be modified only for organizations with tenure <= 6 months
# Default cohorts for organizations with tenure > 6 months will only be modified if the COHORT_FREEZE_DATE changes
COHORT_FREEZE_DATE = "2025-12-31"

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
            "comment": "Id of single default cohort assigned to organization in product_analytics_staging.stg_organization_cohorts as of the date partition.",
        },
    },
]

QUERY = """
WITH
frozen_cohorts AS (
SELECT
org_id,
cohort_id,
tenure_months
FROM product_analytics_staging.stg_organization_cohorts
WHERE is_valid = TRUE
      AND is_default_cohort = TRUE
      AND date = '{COHORT_FREEZE_DATE}'
),
updated_cohorts AS (
SELECT
stg_organization_cohorts.org_id,
stg_organization_cohorts.cohort_id,
stg_organization_cohorts.tenure_months
FROM product_analytics_staging.stg_organization_cohorts stg_organization_cohorts
INNER JOIN (SELECT DISTINCT cohort_id FROM frozen_cohorts) frozen_cohorts
 ON frozen_cohorts.cohort_id = stg_organization_cohorts.cohort_id
WHERE stg_organization_cohorts.is_valid = TRUE
      AND stg_organization_cohorts.is_default_cohort = TRUE
      AND stg_organization_cohorts.date = '{PARTITION_START}')


SELECT
'{PARTITION_START}' AS date,
COALESCE(frozen_cohorts.org_id, updated_cohorts.org_id) AS org_id,
CASE WHEN frozen_cohorts.tenure_months >= 6 THEN frozen_cohorts.cohort_id
     WHEN updated_cohorts.tenure_months < 6 THEN updated_cohorts.cohort_id
     ELSE COALESCE(frozen_cohorts.cohort_id, updated_cohorts.cohort_id) END AS cohort_id
FROM frozen_cohorts
FULL OUTER JOIN updated_cohorts
  ON updated_cohorts.org_id = frozen_cohorts.org_id
"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="""Representation of organization_id to default cohort_id mapping per date as written to the s3 bucket to power the fleet benchmarks report.""",
        row_meaning="""Each row represents the daily pairing of an organization_id to a default cohort_id.""",
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
        NonEmptyDQCheck(name="dq_non_empty_stg_organization_default_cohorts"),
        PrimaryKeyDQCheck(
            name="dq_pk_stg_organization_default_cohorts",
            primary_keys=PRIMARY_KEYS,
            block_before_write=True,
        ),
        NonNullDQCheck(
            name="dq_non_null_stg_organization_default_cohorts",
            non_null_columns=["date", "org_id", "cohort_id"],
            block_before_write=False,
        ),
    ],
    upstreams=[
        "us-west-2:product_analytics_staging.stg_organization_cohorts",
    ],
)
def stg_organization_default_cohorts(context: AssetExecutionContext) -> DataFrame:
    context.log.info("Updating stg_organization_default_cohorts")
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    partition_keys = partition_key_ranges_from_context(context)[0]
    PARTITION_START = partition_keys[0]
    query = QUERY.format(
        PARTITION_START=PARTITION_START,
        COHORT_FREEZE_DATE=COHORT_FREEZE_DATE,
    )
    if get_run_env() == "dev":
        path = "/Volumes/s3/benchmarking-metrics/root/staging/org_cohort_mapping"
    else:
        path = "/Volumes/s3/benchmarking-metrics/root/prod/org_cohort_mapping"
    df = spark.sql(query)
    df_csv = df.select("org_id", "cohort_id")
    df_csv.coalesce(1).write.mode("append").option(
        "mapreduce.fileoutputcommitter.marksuccessfuljobs", "false"
    ).option("overwrite", "true").csv(path)
    context.log.info(f"{query}")
    return df
