from dagster import (
    AssetExecutionContext,
    MonthlyPartitionsDefinition,
)
from dataweb import (
    NonEmptyDQCheck,
    NonNullDQCheck,
    PrimaryKeyDQCheck,
    SQLDQCheck,
    TrendDQCheck,
    table,
)
from dataweb._core.dq_utils import Operator
from dataweb.userpkgs.constants import (
    DATAENGINEERING,
    FRESHNESS_SLO_9AM_PST,
    MEMORY_OPTIMIZED_INSTANCE_POOL_KEY,
    AWSRegion,
    ColumnType,
    Database,
    DimensionConfig,
    DimensionDataType,
    TableType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.platform_revised_constants import (
    DIMENSION_CONFIGS,
    PLATFORM_DIMENSION_TEMPLATES,
    PLATFORM_JOIN_MAPPINGS,
    PLATFORM_METRIC_CONFIGS_TEMPLATE,
    PLATFORM_METRICS,
    PLATFORM_REGION_MAPPINGS,
    PLATFORM_SCHEMA,
    PLATFORM_STANDARD_DIMENSIONS,
    QUERY_TEMPLATE,
)
from dataweb.userpkgs.utils import (
    build_table_description,
    generate_coalesce_statement,
    generate_full_outer_join_statements,
    generate_metric_expression,
    generate_metric_cte,
    generate_metric_schema,
    generate_timeframe_metrics,
    get_formatted_metrics,
    partition_keys_from_context,
)
from dataweb.userpkgs.query import create_run_config_overrides
from pyspark.sql import DataFrame, SparkSession

PRIMARY_KEYS = [
    "date_month",
    "period_start",
    "period_end",
    "sam_number",
    "account_id",
]

@table(
    database=Database.PRODUCT_ANALYTICS,
    description=build_table_description(
        table_desc="""A dataset containing aggregated monthly Platform metrics. This is the new logic (the old one will be deprecated shortly)""",
        row_meaning="""Each row contains Platform metrics for a given set of dimensions each month""",
        related_table_info={},
        table_type=TableType.AGG,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=[AWSRegion.US_WEST_2],
    owners=[DATAENGINEERING],
    schema=generate_metric_schema(schema=PLATFORM_SCHEMA, date_column_name="date_month"),
    upstreams=[
        "us-west-2|eu-west-1|ca-central-1:datamodel_core.dim_organizations",
        "us-west-2|eu-west-1|ca-central-1:datamodel_platform.dim_users_organizations",
        "us-west-2|eu-west-1|ca-central-1:metrics_repo.device_tags_attributes_count",
        "us-west-2|eu-west-1|ca-central-1:metrics_repo.driver_tags_attributes_count",
        "us-west-2|eu-west-1|ca-central-1:finopsdb.customer_info",
        "us-west-2|eu-west-1|ca-central-1:datamodel_core.lifetime_device_online",
        "us-west-2|eu-west-1|ca-central-1:datastreams_history.mobile_logs",
        "us-west-2|eu-west-1|ca-central-1:dynamodb.device_installation_media_records_dynamo",
        "us-west-2|eu-west-1|ca-central-1:datamodel_core.dim_devices",
        "us-west-2|eu-west-1|ca-central-1:datamodel_core.lifetime_device_activity",
        "us-west-2|eu-west-1|ca-central-1:statsdb.dvirs",
        "us-west-2|eu-west-1|ca-central-1:datamodel_platform.dim_users",
        "us-west-2|eu-west-1|ca-central-1:csvuploadsdb_shards.csv_uploads",
        "us-west-2:product_analytics_staging.stg_customer_enriched",
    ],
    primary_keys=PRIMARY_KEYS,
    partitioning=MonthlyPartitionsDefinition(start_date="2023-01-01"),
    dq_checks=[
        NonEmptyDQCheck(name="dq_agg_product_scorecard_platform_mpr_global_revised_monthly"),
        NonNullDQCheck(name="dq_non_null_agg_product_scorecard_platform_mpr_global_revised_monthly", non_null_columns=PRIMARY_KEYS, block_before_write=True),
        PrimaryKeyDQCheck(name="dq_pk_agg_product_scorecard_platform_mpr_global_revised_monthly", primary_keys=PRIMARY_KEYS, block_before_write=True),
        TrendDQCheck(name="dq_trend_agg_product_scorecard_platform_mpr_global_revised_monthly", lookback_days=1, tolerance=0.1, period="months"),
        SQLDQCheck(
            name="dq_sql_agg_product_scorecard_platform_mpr_global_revised_monthly_count_check",
            sql_query="""
                SELECT COUNT(*) AS observed_value
                FROM df
            """,
            expected_value=40000,
            operator=Operator.gte,
            block_before_write=True,
        ),
        SQLDQCheck(
            name="dq_sql_agg_product_scorecard_platform_mpr_global_revised_monthly_full_admin_users_count_check",
            sql_query="""
                SELECT SUM(full_admin_users) AS observed_value
                FROM df
            """,
            expected_value=150000,
            operator=Operator.gte,
            block_before_write=True,
        ),
        SQLDQCheck(
            name="dq_sql_agg_product_scorecard_platform_mpr_global_revised_monthly_total_devices_count_check",
            sql_query="""
                SELECT SUM(total_devices) AS observed_value
                FROM df
            """,
            expected_value=5000000,
            operator=Operator.gte,
            block_before_write=True,
        ),
        SQLDQCheck(
            name="dq_sql_agg_product_scorecard_platform_mpr_global_revised_monthly_total_drivers_count_check",
            sql_query="""
                SELECT SUM(total_drivers) AS observed_value
                FROM df
            """,
            expected_value=750000,
            operator=Operator.gte,
            block_before_write=True,
        ),
    ],
    backfill_batch_size=1,
    write_mode=WarehouseWriteMode.OVERWRITE,
    max_retries=3,
    run_config_overrides=create_run_config_overrides(
        instance_pool_type=MEMORY_OPTIMIZED_INSTANCE_POOL_KEY,
        max_workers=8,
    ),
)
def agg_product_scorecard_platform_mpr_global_revised_monthly(context: AssetExecutionContext) -> DataFrame:

    context.log.info("Updating agg_product_scorecard_platform_mpr_global_revised_monthly")
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()

    DIMENSION_CONFIGS["root"] = DimensionConfig(name="root", data_type=DimensionDataType.STRING, default_value="'Unknown'", output_name="date_month")

    metric_expressions = {
        metric_name: generate_metric_expression(PLATFORM_METRICS, metric_name)
        for metric_name in PLATFORM_METRICS.keys()
    }

    date_partition = partition_keys_from_context(context)[0][0].replace("'", "")

    anchor_partition, dates, metric_configs = generate_timeframe_metrics(
        date_partition=date_partition,
        period="month",
        metrics=PLATFORM_METRICS,
        dimension_template=PLATFORM_DIMENSION_TEMPLATES,
        region_mapping=PLATFORM_REGION_MAPPINGS,
        metric_template=PLATFORM_METRIC_CONFIGS_TEMPLATE,
        dimension_dict=PLATFORM_STANDARD_DIMENSIONS,
        dimension_groups=["time_dimensions", "account_dimensions", "product_dimensions", "sam_dimensions"],
        offset=0,
    )

    query = QUERY_TEMPLATE.format(
        DATEID=date_partition,
        START_OF_PERIOD=anchor_partition,
        PERIOD="month",
        END_OF_PERIOD=dates[0],
        COALESCE_STATEMENTS = generate_coalesce_statement(
            columns=[
                "root",
                "period_start",
                "period_end",
                "sam_number",
                "account_size_segment",
                "account_arr_segment",
                "account_billing_country",
                "account_industry",
                "industry_vertical",
                "is_paid_safety_customer",
                "is_paid_telematics_customer",
                "is_paid_stce_customer",
                "account_csm_email",
                "account_name",
                "account_id",
            ],
            aliases=[
                "user_roles_aggregated",
                "device_details_aggregated",
                "driver_details_aggregated",
                "device_installs_aggregated",
                "at_photo_aggregated",
                "ag_photo_aggregated",
                "vg_photo_aggregated",
                "complete_flow_aggregated",
                "install_time_aggregated",
                "install_time_cm_aggregated",
                "vg_install_correctness_aggregated",
                "complete_flow_cm_aggregated",
                "csv_aggregated",
                "user_counts_aggregated"
            ],
            column_type=ColumnType.DIMENSION,
            dimension_configs=DIMENSION_CONFIGS
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
            START_OF_PERIOD=anchor_partition,
            END_OF_PERIOD=dates[0],
        ),
        **get_formatted_metrics(metric_expressions),
        COALESCE_METRIC_STATEMENTS=generate_coalesce_statement(
            columns=[
                "user_roles_aggregated.full_admin_account",
                "user_roles_aggregated.custom_role_account",
                "user_roles_aggregated.account_with_two_plus_users",
                "user_counts_aggregated.full_admin_users",
                "user_counts_aggregated.custom_role_users",
                "user_counts_aggregated.total_users",
                "device_details_aggregated.device_attributes_account",
                "device_details_aggregated.device_tags_account",
                "device_details_aggregated.device_account",
                "device_details_aggregated.device_attributes",
                "device_details_aggregated.device_tags",
                "device_details_aggregated.total_devices",
                "driver_details_aggregated.driver_tags_account",
                "driver_details_aggregated.driver_account",
                "driver_details_aggregated.driver_attributes_account",
                "driver_details_aggregated.driver_attributes",
                "driver_details_aggregated.driver_tags",
                "driver_details_aggregated.total_drivers",
                "device_installs_aggregated.installed_vg",
                "device_installs_aggregated.installed_at",
                "device_installs_aggregated.installed_ag",
                "device_installs_aggregated.installed_cm",
                "at_photo_aggregated.at_with_photo",
                "ag_photo_aggregated.ag_with_photo",
                "vg_photo_aggregated.vg_with_photo",
                "complete_flow_cm_aggregated.complete_flow_cm",
                "complete_flow_aggregated.complete_flow_at",
                "complete_flow_aggregated.complete_flow_ag",
                "complete_flow_aggregated.complete_flow_vg",
                "install_time_aggregated.total_time_to_install_vg",
                "install_time_aggregated.total_time_to_install_ag",
                "install_time_aggregated.total_time_to_install_at",
                "install_time_cm_aggregated.total_time_to_install_cm",
                "vg_install_correctness_aggregated.correctly_installed_vg",
                "cm_install_correctness_aggregated.correctly_installed_cm",
                "csv_aggregated.csv_upload_failures",
                "csv_aggregated.csv_uploads",
            ],
            column_type=ColumnType.METRIC,
            default_value=0
        ),
        FULL_OUTER_JOIN_STATEMENTS=generate_full_outer_join_statements(
            {
                "user_roles_aggregated": "user_roles_aggregated",
                "user_counts_aggregated": "user_counts_aggregated",
                "device_details_aggregated": "device_details_aggregated",
                "driver_details_aggregated": "driver_details_aggregated",
                "device_installs_aggregated": "device_installs_aggregated",
                "at_photo_aggregated": "at_photo_aggregated",
                "ag_photo_aggregated": "ag_photo_aggregated",
                "vg_photo_aggregated": "vg_photo_aggregated",
                "complete_flow_aggregated": "complete_flow_aggregated",
                "install_time_aggregated": "install_time_aggregated",
                "install_time_cm_aggregated": "install_time_cm_aggregated",
                "vg_install_correctness_aggregated": "vg_install_correctness_aggregated",
                "cm_install_correctness_aggregated": "cm_install_correctness_aggregated",
                "complete_flow_cm_aggregated": "complete_flow_cm_aggregated",
                "csv_aggregated": "csv_aggregated",
            },
            {
                "root": "'Unknown'",
                "period_start": "'Unknown'",
                "period_end": "'Unknown'",
                "sam_number": "'Unknown'",
                "account_id": "'Unknown'",
            }
        ),
    )
    context.log.info("Query to execute: " + query)
    monthly_df = spark.sql(query)

    return monthly_df
