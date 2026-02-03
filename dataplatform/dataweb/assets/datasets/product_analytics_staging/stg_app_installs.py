from dagster import AssetExecutionContext, DailyPartitionsDefinition
from dataweb import NonEmptyDQCheck, NonNullDQCheck, PrimaryKeyDQCheck, table
from dataweb.userpkgs.constants import (
    ALL_COMPUTE_REGIONS,
    DATAENGINEERING,
    FRESHNESS_SLO_9AM_PST,
    AWSRegion,
    ColumnDescription,
    Database,
    TableType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.utils import (
    build_table_description,
    get_region_from_context,
    get_run_env,
)
from pyspark.sql import SparkSession

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
        "metadata": {"comment": "Name of the application"},
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
        "metadata": {"comment": "Time at which the app was installed"},
    },
    {
        "name": "app_uuid",
        "type": "string",
        "nullable": True,
        "metadata": {"comment": "Unique ID of the application"},
    },
    {
        "name": "api_token_id",
        "type": "long",
        "nullable": True,
        "metadata": {"comment": "ID of the API token"},
    },
    {
        "name": "install_method",
        "type": "string",
        "nullable": False,
        "metadata": {"comment": "Install method of the application"},
    },
]

QUERY = """
WITH
developer_apps AS (
SELECT *
FROM {source}.stg_developer_apps
WHERE date = '{partition_date}'),
fuzzy_match AS
(SELECT DISTINCT
       lower(token_name) AS token_name,
       normalized_name
FROM {token_name_app_matches_table}),
fuzzy_match_installs AS (
SELECT
    lower(tokens.name) as token_name,
    tokens.org_id,
    tokens.id AS api_token_id,
    MIN(created_at) AS installed_at
FROM clouddb.api_tokens TIMESTAMP AS OF '{partition_date} 23:59:59.999' tokens
WHERE tokens.name not ilike 'app%'
GROUP BY 1,2,3),
token_app_installs AS (
SELECT
    replace(lower(tokens.name), 'app ', '') as token_name,
    tokens.org_id,
    tokens.id AS api_token_id,
    MIN(created_at) AS installed_at
FROM clouddb.api_tokens TIMESTAMP AS OF '{partition_date} 23:59:59.999' tokens
WHERE tokens.name ilike 'app%'
GROUP BY 1,2,3),
oauth_app_installs AS (
SELECT org_id,
       app_uuid,
       MIN(created_at) AS installed_at
FROM oauth2db_shards.oauth2_refresh_tokens
--cannot timetravel views
WHERE _timestamp <= TIMESTAMP('{partition_date} 23:59:59.999')
GROUP BY 1,2),
unified AS  (
--token installs
    (SELECT
        app.name AS app_name,
        token.org_id,
        token.installed_at,
        app.uuid AS app_uuid,
        token.api_token_id,
        'token' AS install_method
    FROM token_app_installs token
    INNER JOIN developer_apps app
        ON LOWER(app.name) = token.token_name
        AND app.identifier = 'name')
    UNION ALL
--fuzzy match
   (SELECT
        fm.normalized_name AS app_name,
        token.org_id,
        token.installed_at,
        CAST(NULL AS STRING) AS app_uuid,
        token.api_token_id,
        'fuzzy_match' AS install_method
    FROM fuzzy_match_installs token
    INNER JOIN fuzzy_match fm
     ON fm.token_name = token.token_name)
    UNION ALL
--oauth installs
    (SELECT
        app.name AS app_name,
        oauth.org_id,
        oauth.installed_at,
        app.uuid AS app_uuid,
        CAST(NULL AS BIGINT) AS api_token_id,
        'oauth' AS install_method
    FROM oauth_app_installs oauth
    INNER JOIN developer_apps app
        ON app.uuid = oauth.app_uuid
        AND app.identifier = 'uuid'))

SELECT '{partition_date}' as date,
        *
FROM unified
"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="""Stage application install data through different installation methods.""",
        row_meaning="""Application install data""",
        related_table_info={},
        table_type=TableType.STAGING,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[DATAENGINEERING],
    schema=SCHEMA,
    primary_keys=["date", "org_id", "app_uuid", "api_token_id", "install_method"],
    partitioning=DailyPartitionsDefinition(start_date="2024-09-12"),
    upstreams=["product_analytics_staging.stg_developer_apps"],
    dq_checks=[
        NonEmptyDQCheck(name="dq_non_empty_stg_app_installs"),
        NonNullDQCheck(name="dq_non_null_stg_app_installs", non_null_columns=["date", "app_name", "org_id", "installed_at", "install_method"], block_before_write=True),
        PrimaryKeyDQCheck(name="dq_pk_stg_app_installs", primary_keys=["date", "org_id", "app_uuid", "api_token_id", "install_method"], block_before_write=True)
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
)
def stg_app_installs(context: AssetExecutionContext) -> str:
    context.log.info("Updating stg_app_installs")
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    partition_date = context.partition_key
    source = "product_analytics_staging"
    if get_run_env() == "dev":
        source = "datamodel_dev"
    region = get_region_from_context(context)
    if region == AWSRegion.CA_CENTRAL_1:
        token_name_app_matches_table = "data_tools_delta_share.devecosystem_dev.token_name_app_matches"
    else:
        token_name_app_matches_table = "devecosystem_dev.token_name_app_matches"
    query = QUERY.format(
        partition_date=partition_date,
        source=source,
        token_name_app_matches_table=token_name_app_matches_table,
    )
    context.log.info(f"{query}")
    return query
