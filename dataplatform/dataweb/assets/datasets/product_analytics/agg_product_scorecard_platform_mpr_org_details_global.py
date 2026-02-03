from dagster import (
    AssetExecutionContext,
    DailyPartitionsDefinition,
)
from dataweb import (
    NonEmptyDQCheck,
    NonNullDQCheck,
    PrimaryKeyDQCheck,
    TrendDQCheck,
    table,
)
from dataweb.userpkgs.constants import (
    DATAENGINEERING,
    FRESHNESS_SLO_9AM_PST,
    AWSRegion,
    ColumnType,
    Database,
    DimensionConfig,
    DimensionDataType,
    TableType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.platform_constants import (
    DIMENSION_CONFIGS_ORG,
    PLATFORM_DIMENSION_TEMPLATES,
    PLATFORM_JOIN_MAPPINGS,
    PLATFORM_METRIC_CONFIGS_TEMPLATE_ORG,
    PLATFORM_METRICS_ORG,
    PLATFORM_REGION_MAPPINGS,
    PLATFORM_SCHEMA_ORG,
    PLATFORM_STANDARD_DIMENSIONS,
    QUERY_TEMPLATE_ORG,
)
from dataweb.userpkgs.utils import (
    build_table_description,
    generate_coalesce_statement,
    generate_full_outer_join_statements,
    generate_metric_cte,
    generate_metric_expression,
    generate_timeframe_metrics,
    get_formatted_metrics,
)
from pyspark.sql import DataFrame, SparkSession

PRIMARY_KEYS = ["date", "org_id"]

@table(
    database=Database.PRODUCT_ANALYTICS,
    description=build_table_description(
        table_desc="""A dataset containing Platform MPR metrics for faster retrieval in Tableau""",
        row_meaning="""Each row contains Platform MPR metrics for a given org""",
        related_table_info={},
        table_type=TableType.AGG,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=[AWSRegion.US_WEST_2],
    owners=[DATAENGINEERING],
    schema=PLATFORM_SCHEMA_ORG,
    upstreams=[
        "us-west-2|eu-west-1|ca-central-1:datamodel_core.dim_organizations",
        "us-west-2:product_analytics_staging.stg_organization_categories",
        "us-west-2|eu-west-1|ca-central-1:datamodel_platform.dim_users_organizations",
        "us-west-2|eu-west-1|ca-central-1:datamodel_platform.dim_users",
        "us-west-2|eu-west-1|ca-central-1:metrics_repo.device_tags_attributes_count",
        "us-west-2|eu-west-1|ca-central-1:metrics_repo.driver_tags_attributes_count",
        "us-west-2|eu-west-1|ca-central-1:csvuploadsdb_shards.csv_uploads",
    ],
    primary_keys=PRIMARY_KEYS,
    partitioning=DailyPartitionsDefinition(start_date="2025-05-01"),
    dq_checks=[
        NonEmptyDQCheck(name="dq_agg_product_scorecard_platform_mpr_org_details_global"),
        NonNullDQCheck(name="dq_non_null_agg_product_scorecard_platform_mpr_org_details_global", non_null_columns=PRIMARY_KEYS, block_before_write=True),
        PrimaryKeyDQCheck(name="dq_pk_agg_product_scorecard_platform_mpr_org_details_global", primary_keys=PRIMARY_KEYS, block_before_write=True),
        TrendDQCheck(name="dq_trend_agg_product_scorecard_platform_mpr_org_details_global", lookback_days=1, tolerance=0.1),
    ],
    backfill_batch_size=1,
    write_mode=WarehouseWriteMode.OVERWRITE,
    max_retries=3,
)
def agg_product_scorecard_platform_mpr_org_details_global(context: AssetExecutionContext) -> DataFrame:

    context.log.info("Updating agg_product_scorecard_platform_mpr_org_details_global")
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()

    DIMENSION_CONFIGS_ORG["root"] = DimensionConfig(name="root", data_type=DimensionDataType.STRING, default_value="'Unknown'", output_name="date")

    date_partition = context.partition_key
    anchor_partition, dates, metric_configs = generate_timeframe_metrics(
        date_partition=date_partition,
        period="day",
        metrics=PLATFORM_METRICS_ORG,
        dimension_template=PLATFORM_DIMENSION_TEMPLATES,
        region_mapping=PLATFORM_REGION_MAPPINGS,
        metric_template=PLATFORM_METRIC_CONFIGS_TEMPLATE_ORG,
        dimension_dict=PLATFORM_STANDARD_DIMENSIONS,
        offset=0,
    )

    metric_expressions = {
        metric_name: generate_metric_expression(PLATFORM_METRICS_ORG, metric_name)
        for metric_name in PLATFORM_METRICS_ORG.keys()
    }

    query = QUERY_TEMPLATE_ORG.format(
        DATEID=date_partition,
        START_OF_PERIOD=anchor_partition,
        PERIOD="day",
        END_OF_PERIOD=dates[0],
        COALESCE_STATEMENTS = generate_coalesce_statement(
            columns=[
                "root",
                "org_id",
                "org_name",
                "account_csm_email",
                "cloud_region",
                "region",
                "account_size_segment",
                "account_arr_segment",
                "account_industry",
                "industry_vertical",
                "is_paid_safety_customer",
                "is_paid_telematics_customer",
                "is_paid_stce_customer"
            ],
            aliases=[
                "user_roles",
                "device_details",
                "driver_details",
                "csv"
            ],
            column_type=ColumnType.DIMENSION,
            dimension_configs=DIMENSION_CONFIGS_ORG,
        ),
        METRIC_CTES = ",\n".join(
            generate_metric_cte(
                metric_name=metric_name,
                dimensions_by_region={
                    "us-west-2": config["dimensions_us"],
                    "eu-west-1": config["dimensions_eu"],
                    "ca-central-1": config["dimensions_ca"],
                },
                metrics=config["metrics"],
                sources={
                    "us-west-2": config["us_source"],
                    "eu-west-1": config["eu_source"],
                    "ca-central-1": config["ca_source"],
                },
                table_alias=config["table_alias"],
                join_alias=config["join_alias"],
                group_by="1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16",
                join_config=PLATFORM_JOIN_MAPPINGS,
                join_override=config.get("join_override"),
            )
            for metric_name, config in metric_configs.items()
        ).format(
            END_OF_PERIOD=dates[0],
        ),
        **get_formatted_metrics(metric_expressions),
        COALESCE_METRIC_STATEMENTS=generate_coalesce_statement(
            columns=[
                "user_roles.admin_users",
                "user_roles.custom_role_users",
                "user_roles.total_users",
                "device_details.device_attributes",
                "device_details.device_tags",
                "device_details.device_tags_attributes",
                "device_details.total_devices",
                "driver_details.driver_tags",
                "driver_details.driver_attributes",
                "driver_details.driver_tags_attributes",
                "driver_details.total_drivers",
                "csv.csv_upload_failures",
                "csv.csv_uploads",
                "csv.average_row_error_rate",
                "active_users.total_possible_cloud_dashboard_users",
                "active_users.daily_active_cloud_dashboard_users",
                "active_users.weekly_active_cloud_dashboard_users",
                "active_users.monthly_active_cloud_dashboard_users",
            ],
            column_type=ColumnType.METRIC,
            default_value=0
        ),
        FULL_OUTER_JOIN_STATEMENTS=generate_full_outer_join_statements(
            {
                "user_roles_aggregated": "user_roles",
                "device_details_aggregated": "device_details",
                "driver_details_aggregated": "driver_details",
                "csv_aggregated": "csv",
                "active_users_aggregated": "active_users",
            },
            {
                "org_id": "0",
            }
        )
    )
    context.log.info(f"Query: {query}")
    df = spark.sql(query)

    return df
