from dagster import AssetExecutionContext, DailyPartitionsDefinition
from dataweb import NonEmptyDQCheck, NonNullDQCheck, PrimaryKeyDQCheck, table
from dataweb.userpkgs.constants import (
    ALL_COMPUTE_REGIONS,
    DATAENGINEERING,
    FRESHNESS_SLO_9AM_PST,
    MEMORY_OPTIMIZED_INSTANCE_POOL_KEY,
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
from pyspark.sql import SparkSession

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
        "metadata": {"comment": "The Internal ID for the customer`s Samsara org."},
    },
    {
        "name": "route",
        "type": "string",
        "nullable": False,
        "metadata": {"comment": "Route being taken"},
    },
    {
        "name": "method",
        "type": "string",
        "nullable": False,
        "metadata": {"comment": "HTTP method being used"},
    },
    {
        "name": "api_request_string",
        "type": "string",
        "nullable": False,
        "metadata": {"comment": "Contains the API request being made"},
    },
    {
        "name": "ip_address",
        "type": "string",
        "nullable": False,
        "metadata": {"comment": "IP address where API call originated from"},
    },
    {
        "name": "status_code",
        "type": "long",
        "nullable": False,
        "metadata": {"comment": "Status code of API call"},
    },
    {
        "name": "owning_team",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "Team who owns API call"},
    },
    {
        "name": "service_account_user_id",
        "type": "long",
        "nullable": False,
        "metadata": {"comment": "User ID of serving account"},
    },
    {
        "name": "api_token_id",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "ID of API token being used"},
    },
    {
        "name": "oauth_token_id",
        "type": "string",
        "nullable": False,
        "metadata": {"comment": "OAuth token used in API call"},
    },
    {
        "name": "oauth_token_app_uuid",
        "type": "string",
        "nullable": False,
        "metadata": {"comment": "Unique ID of OAuth token/app"},
    },
    {
        "name": "app_uuid",
        "type": "string",
        "nullable": False,
        "metadata": {"comment": "Unique ID of app"},
    },
    {
        "name": "api_token_name",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "Name of API token"},
    },
    {
        "name": "num_requests",
        "type": "long",
        "nullable": False,
        "metadata": {"comment": "Number of requests made"},
    },
    {
        "name": "total_request_duration_ms",
        "type": "long",
        "nullable": False,
        "metadata": {"comment": "Total length of API request(s)"},
    },
    {
        "name": "num_requests_under_2s",
        "type": "long",
        "nullable": False,
        "metadata": {"comment": "Total requests under 2 seconds"},
    },
    {
        "name": "num_requests_under_7s",
        "type": "long",
        "nullable": False,
        "metadata": {"comment": "Total requests from 7 seconds"},
    },
    {
        "name": "num_requests_under_15s",
        "type": "long",
        "nullable": False,
        "metadata": {"comment": "Total requests under 15 seconds"},
    },
    {
        "name": "app_name",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "Name of the application"},
    },
]

QUERY = """
    WITH app_service_account_user_id AS (
    SELECT DISTINCT service_account_user_id,
        app_uuid
    FROM oauth2db_shards.oauth2_access_tokens),
    developer_apps AS (
    SELECT *
    FROM {source}.stg_developer_apps
    WHERE identifier = 'uuid'
    AND date = (SELECT MAX(date) FROM {source}.stg_developer_apps)),
    api_owners AS (
    SELECT DISTINCT route, method, team
    FROM definitions.api_owners),
    agg AS (
    SELECT logs.date,
        logs.org_id,
        logs.path_template AS route,
        logs.http_method AS method,
        CONCAT(logs.http_method,' ', logs.path_template) AS api_request_string,
        logs.ip_address,
        logs.status_code,
        COALESCE(CASE WHEN len(logs.team) > 0 THEN logs.team ELSE NULL END, api_owners.team) AS owning_team,
        logs.service_account_user_id,
        CASE WHEN len(logs.access_token_id) > 0 THEN logs.access_token_id ELSE NULL END AS api_token_id,
        logs.oauth_token_id,
        logs.oauth_token_app_uuid,
        COALESCE(app_service_account_user_id.app_uuid, oauth_access_tokens.app_uuid, UPPER(REPLACE(logs.oauth_token_app_uuid, '-', ''))) AS app_uuid,
        COALESCE(CASE WHEN len(logs.access_token_name) > 0 THEN logs.access_token_name ELSE NULL END, api_tokens.name) as api_token_name,
        COUNT(*) AS num_requests,
        SUM(duration_ms) AS total_request_duration_ms,
        SUM(CASE WHEN logs.duration_ms <= 2000 THEN 1 ELSE 0 END) AS num_requests_under_2s,
        SUM(CASE WHEN logs.duration_ms <= 7000 THEN 1 ELSE 0 END) AS num_requests_under_7s,
        SUM(CASE WHEN logs.duration_ms <= 15000 THEN 1 ELSE 0 END) AS num_requests_under_15s
    FROM datastreams_history.api_logs logs
    LEFT OUTER JOIN app_service_account_user_id
        ON app_service_account_user_id.service_account_user_id = logs.service_account_user_id
    LEFT OUTER JOIN oauth2db_shards.oauth2_access_tokens oauth_access_tokens
        ON oauth_access_tokens.id = UPPER(REPLACE(logs.oauth_token_id, '-', ''))
    LEFT OUTER JOIN clouddb.api_tokens api_tokens
        ON len(logs.access_token_id) > 0
        AND logs.access_token_id = api_tokens.id
    LEFT OUTER JOIN api_owners
        ON api_owners.route = logs.path_template
        AND api_owners.method = logs.http_method
    WHERE date between '{PARTITION_START}' and '{PARTITION_END}'
    GROUP BY ALL)

    SELECT agg.*, developer_apps.name AS app_name
    FROM agg
    LEFT OUTER JOIN developer_apps
    ON developer_apps.uuid = agg.app_uuid
"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="""API usage data aggregated by request metadata per day.""",
        row_meaning="""API usage data""",
        related_table_info={},
        table_type=TableType.STAGING,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[DATAENGINEERING],
    schema=SCHEMA,
    primary_keys=[
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
    ],
    partitioning=DailyPartitionsDefinition(start_date="2021-01-01"),
    write_mode=WarehouseWriteMode.OVERWRITE,
    backfill_batch_size=3,
    run_config_overrides=create_run_config_overrides(
        instance_pool_type=MEMORY_OPTIMIZED_INSTANCE_POOL_KEY,
        max_workers=12,
    ),
    dq_checks=[
        NonEmptyDQCheck(name="dq_non_empty_stg_api_usage"),
        NonNullDQCheck(name="dq_non_null_stg_api_usage", non_null_columns=["date", "route", "method", "api_request_string", "ip_address", "status_code", "service_account_user_id", "oauth_token_id", "oauth_token_app_uuid", "app_uuid", "num_requests", "total_request_duration_ms", "num_requests_under_2s", "num_requests_under_7s", "num_requests_under_15s"], block_before_write=True),
        PrimaryKeyDQCheck(name="dq_pk_stg_api_usage", primary_keys=["date", "org_id", "api_request_string", "ip_address", "status_code", "service_account_user_id", "api_token_id", "oauth_token_id", "oauth_token_app_uuid", "app_uuid", "api_token_name", "owning_team"], block_before_write=True)
    ]
)
def stg_api_usage(context: AssetExecutionContext) -> str:
    context.log.info("Updating stg_api_usage")
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
