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
from dataweb.userpkgs.utils import (
    build_table_description,
    get_run_env,
    partition_key_ranges_from_context,
)
from pyspark.sql import SparkSession

PRIMARY_KEYS = [
    "date",
    "org_id",
    "api_request_string",
    "ip_address",
    "status_code",
    "service_account_user_id",
    "api_token_id",
    "oauth_token_id",
    "oauth_token_app_uuid",
    "app_uuid",
    "api_token_name",
    "owning_team",
]

SCHEMA = [
    {
        "name": "date",
        "type": "date",
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
        "name": "route",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "The path of the api request, one component of the api_request_string including the method."
        },
    },
    {
        "name": "method",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "The method (ex: GET) of the qpi request, one component of the api_request_string including the route."
        },
    },
    {
        "name": "api_request_string",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "The concatenation of the method and route of the API request."
        },
    },
    {
        "name": "ip_address",
        "type": "string",
        "nullable": False,
        "metadata": {"comment": "The ip address of the API request."},
    },
    {
        "name": "status_code",
        "type": "long",
        "nullable": False,
        "metadata": {"comment": "The status code of the API request, ex: 200."},
    },
    {
        "name": "owning_team",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "The owning team of the method + route of the API request as recorded in definitions.api_owners."
        },
    },
    {
        "name": "service_account_user_id",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "If available, the service account associated with the API request."
        },
    },
    {
        "name": "api_token_id",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "If used as an authentication mechanism, the api token id associated with the API request. The api_token_name corresponds to this ID."
        },
    },
    {
        "name": "oauth_token_id",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "If used as an authentication mechanism, the oauth token id associated with the API request. The oauth_token_app_uuid is a decorated version of the app_uuid corresponding to this token."
        },
    },
    {
        "name": "oauth_token_app_uuid",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "If oauth is used as an authentication mechanism, the oauth token app uuid is a decoarated version of the app_uuid."
        },
    },
    {
        "name": "app_uuid",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "The application uuid, available when oauth authentication is used as an authentication mechanism."
        },
    },
    {
        "name": "api_token_name",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "If the api token (instead of oauth) is used as an authentication mechanism, the api token name corresponds to the name of the api token id."
        },
    },
    {
        "name": "num_requests",
        "type": "long",
        "nullable": False,
        "metadata": {"comment": "Number of requests per grouping aggregated by date."},
    },
    {
        "name": "total_request_duration_ms",
        "type": "long",
        "nullable": False,
        "metadata": {"comment": "Sum of duration of the requests per grouping."},
    },
    {
        "name": "num_requests_under_2s",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of requests with a duration under 2 seconds per grouping, representing low latency requests."
        },
    },
    {
        "name": "num_requests_under_7s",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of requests with a duration under 7 seconds per grouping, representing medium latency requests."
        },
    },
    {
        "name": "num_requests_under_15s",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Number of requests with a duration under 15 seconds per grouping, representing high latency requests."
        },
    },
    {
        "name": "app_name",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Name of application associated with most recent release state of the application by uuid."
        },
    },
]

QUERY = """
    SELECT *
    FROM {source}.stg_api_usage
    WHERE date between '{PARTITION_START}' and '{PARTITION_END}'
"""


@table(
    database=Database.PRODUCT_ANALYTICS,
    description=build_table_description(
        table_desc="""API usage data aggregated by request metadata per day. Attributes of an API request including the access method, ip_address, status code, and application name are used to count the number of requests among 2,7,and 15 second thresholds and sum the duration of requests to support service level analyses.""",
        row_meaning="""API usage metadata for a device""",
        related_table_info={},
        table_type=TableType.TRANSACTIONAL_FACT,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[DATAENGINEERING],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DailyPartitionsDefinition(start_date="2021-01-01"),
    write_mode=WarehouseWriteMode.OVERWRITE,
    dq_checks=[
        NonEmptyDQCheck(name="dq_non_empty_fct_api_usage"),
        NonNullDQCheck(name="dq_non_null_fct_api_usage", non_null_columns=["org_id", "api_request_string", "ip_address", "status_code", "service_account_user_id", "oauth_token_id", "oauth_token_app_uuid", "app_uuid"], block_before_write=True),
        PrimaryKeyDQCheck(name="dq_pk_fct_api_usage", primary_keys=PRIMARY_KEYS, block_before_write=True),
    ],
    backfill_batch_size=3,
    upstreams=["product_analytics_staging.stg_api_usage"],
)
def fct_api_usage(context: AssetExecutionContext) -> str:
    context.log.info("Updating fct_api_usage")
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    partition_keys = partition_key_ranges_from_context(context)[0]
    PARTITION_START = partition_keys[0]
    PARTITION_END = partition_keys[-1]
    source = "product_analytics_staging"
    if get_run_env() == "dev":
        source = "datamodel_dev"
    query = QUERY.format(
        source=source,
        PARTITION_START=PARTITION_START,
        PARTITION_END=PARTITION_END,
    )
    context.log.info(f"{query}")
    return query
