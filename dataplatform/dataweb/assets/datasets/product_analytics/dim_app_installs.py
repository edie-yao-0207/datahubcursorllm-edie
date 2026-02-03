from dagster import AssetExecutionContext, DailyPartitionsDefinition
from dataweb import NonEmptyDQCheck, NonNullDQCheck, PrimaryKeyDQCheck, table
from dataweb.userpkgs.constants import (
    ALL_COMPUTE_REGIONS,
    DATAENGINEERING,
    FRESHNESS_SLO_9AM_PST,
    ColumnDescription,
    Database,
    TableType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.utils import build_table_description, get_run_env
from pyspark.sql import SparkSession

PRIMARY_KEYS = ["date", "org_id", "app_uuid", "api_token_id", "install_method"]

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
        "name": "app_name",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "The name of the application per the latest release stage sourced from clouddb.developer_apps"
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
        "name": "installed_at",
        "type": "timestamp",
        "nullable": False,
        "metadata": {
            "comment": "The timestamp of the first created api token id or oauth refresh token id corresponding to the organization and app uuid."
        },
    },
    {
        "name": "app_uuid",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "The unique identifier for the application sourced from the token name for api_token or fuzzy_match installation or from the oauth refresh token for oauth based installation."
        },
    },
    {
        "name": "api_token_id",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "If installed through api token or fuzzy matching, the api_token_id is sourced from the clouddb.api_tokens table and represents the token used to install the application."
        },
    },
    {
        "name": "install_method",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "One of oauth, fuzzy_match, or api_token pertaining to the method the customer used to install the application."
        },
    },
]

QUERY = """
SELECT *
FROM {source}.stg_app_installs
WHERE date = '{partition_date}'
"""


@table(
    database=Database.PRODUCT_ANALYTICS,
    description=build_table_description(
        table_desc="""Daily snapshot of application installs using oauth, fuzzy matching, and api token methods.""",
        row_meaning="""An install of an application""",
        related_table_info={},
        table_type=TableType.DAILY_DIMENSION,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[DATAENGINEERING],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DailyPartitionsDefinition(start_date="2024-09-12"),
    upstreams=["product_analytics_staging.stg_app_installs"],
    write_mode=WarehouseWriteMode.OVERWRITE,
    dq_checks=[
        NonEmptyDQCheck(name="dq_non_empty_dim_app_installs"),
        NonNullDQCheck(name="dq_non_null_dim_app_installs", non_null_columns=["org_id", "install_method"], block_before_write=True),
        PrimaryKeyDQCheck(name="dq_pk_dim_app_installs", primary_keys=PRIMARY_KEYS, block_before_write=True)
    ],
)
def dim_app_installs(context: AssetExecutionContext) -> str:
    context.log.info("Updating dim_app_installs")
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    partition_date = context.partition_key
    source = "product_analytics_staging"
    if get_run_env() == "dev":
        source = "datamodel_dev"
    query = QUERY.format(
        partition_date=partition_date,
        source=source,
    )
    context.log.info(f"{query}")
    return query
