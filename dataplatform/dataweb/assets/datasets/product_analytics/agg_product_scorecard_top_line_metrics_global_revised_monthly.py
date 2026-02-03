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
from dataweb.userpkgs.top_line_revised_constants import (
    DIMENSION_CONFIGS,
    QUERY_TEMPLATE,
    TOP_LINE_DIMENSION_TEMPLATES,
    TOP_LINE_JOIN_MAPPINGS,
    TOP_LINE_METRIC_CONFIGS_TEMPLATE,
    TOP_LINE_METRICS,
    TOP_LINE_REGION_MAPPINGS,
    TOP_LINE_SCHEMA,
    TOP_LINE_STANDARD_DIMENSIONS
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
        table_desc="""A dataset containing aggregated monthly top line metrics. This is the new logic (the old one will be deprecated shortly)""",
        row_meaning="""Each row contains top line metrics for a given set of dimensions each month""",
        related_table_info={},
        table_type=TableType.AGG,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=[AWSRegion.US_WEST_2],
    owners=[DATAENGINEERING],
    schema=generate_metric_schema(schema=TOP_LINE_SCHEMA, date_column_name="date_month"),
    upstreams=[
        "us-west-2|eu-west-1|ca-central-1:datamodel_core.dim_devices",
        "us-west-2|eu-west-1|ca-central-1:datamodel_telematics.fct_trips",
        "us-west-2|eu-west-1|ca-central-1:datamodel_core.dim_organizations",
        "us-west-2:datamodel_platform_silver.stg_cloud_routes",
        "us-west-2:product_analytics_staging.stg_customer_enriched",
        "us-west-2|eu-west-1|ca-central-1:datamodel_platform.dim_users_organizations",
        "us-west-2|eu-west-1|ca-central-1:datamodel_platform_bronze.raw_clouddb_users_organizations",
        "us-west-2|eu-west-1|ca-central-1:datamodel_platform.dim_license_assignment",
        "us-west-2|eu-west-1|ca-central-1:datamodel_platform.dim_drivers",
        "us-west-2|eu-west-1|ca-central-1:datamodel_core_silver.stg_device_activity_daily",
        "us-west-2|eu-west-1|ca-central-1:finopsdb.customer_info",
        "us-west-2|eu-west-1|ca-central-1:product_analytics.fct_api_usage",
    ],
    primary_keys=PRIMARY_KEYS,
    partitioning=MonthlyPartitionsDefinition(start_date="2023-01-01"),
    dq_checks=[
        NonEmptyDQCheck(name="dq_agg_product_scorecard_top_line_metrics_global_revised_monthly"),
        NonNullDQCheck(name="dq_non_null_agg_product_scorecard_top_line_metrics_global_revised_monthly", non_null_columns=PRIMARY_KEYS, block_before_write=True),
        PrimaryKeyDQCheck(name="dq_pk_agg_product_scorecard_top_line_metrics_global_revised_monthly", primary_keys=PRIMARY_KEYS, block_before_write=True),
        TrendDQCheck(name="dq_trend_agg_product_scorecard_top_line_metrics_global_revised_monthly", lookback_days=1, tolerance=0.1, period="months"),
        SQLDQCheck(
            name="dq_sql_agg_product_scorecard_top_line_metrics_global_revised_monthly_count_check",
            sql_query="""
                SELECT COUNT(*) AS observed_value
                FROM df
            """,
            expected_value=40000,
            operator=Operator.gte,
            block_before_write=True,
        ),
        SQLDQCheck(
            name="dq_sql_agg_product_scorecard_top_line_metrics_global_revised_monthly_driver_app_user_count_check",
            sql_query="""
                SELECT SUM(active_driver_app_users) AS observed_value
                FROM df
            """,
            expected_value=750000,
            operator=Operator.gte,
            block_before_write=True,
        ),
        SQLDQCheck(
            name="dq_sql_agg_product_scorecard_top_line_metrics_global_revised_monthly_licenses_count_check",
            sql_query="""
                SELECT SUM(total_licenses) AS observed_value
                FROM df
            """,
            expected_value=6000000,
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
def agg_product_scorecard_top_line_metrics_global_revised_monthly(context: AssetExecutionContext) -> DataFrame:

    context.log.info("Updating agg_product_scorecard_top_line_metrics_global_revised_monthly")
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()

    DIMENSION_CONFIGS["root"] = DimensionConfig(name="root", data_type=DimensionDataType.STRING, default_value="'Unknown'", output_name="date_month")

    metric_expressions = {
        metric_name: generate_metric_expression(TOP_LINE_METRICS, metric_name)
        for metric_name in TOP_LINE_METRICS.keys()
    }

    date_partition = partition_keys_from_context(context)[0][0].replace("'", "")

    anchor_partition, dates, metric_configs = generate_timeframe_metrics(
        date_partition=date_partition,
        period="month",
        metrics=TOP_LINE_METRICS,
        dimension_template=TOP_LINE_DIMENSION_TEMPLATES,
        region_mapping=TOP_LINE_REGION_MAPPINGS,
        metric_template=TOP_LINE_METRIC_CONFIGS_TEMPLATE,
        dimension_dict=TOP_LINE_STANDARD_DIMENSIONS,
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
                "trips",
                "miles",
                "active_drivers",
                "active_users_aggregated",
                "active_driver_app_users",
                "marketplace_integrations",
                "total_device_licenses",
                "active_devices",
                "api_requests",
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
                join_config=TOP_LINE_JOIN_MAPPINGS,
                join_override=config.get("join_override"),
            )
            for metric_name, config in metric_configs.items()
        ),
        **get_formatted_metrics(metric_expressions),
        COALESCE_METRIC_STATEMENTS=generate_coalesce_statement(
            columns=[
                "trips.trips",
                "miles.miles",
                "active_drivers.active_drivers",
                "active_users_aggregated.active_cloud_dashboard_users",
                "active_driver_app_users.active_driver_app_users",
                "marketplace_integrations.marketplace_integrations",
                "api_requests.api_requests",
                "total_device_licenses.device_licenses_ag",
                "total_device_licenses.device_licenses_vg",
                "total_device_licenses.device_licenses_cm",
                "total_device_licenses.device_licenses_at",
                "total_device_licenses.device_licenses_sg",
                "active_devices.active_devices_ag",
                "active_devices.active_devices_vg",
                "active_devices.active_devices_cm",
                "active_devices.active_devices_at",
                "active_devices.active_devices_sg",
                "assigned_licenses.total_licenses",
                "assigned_licenses.total_licenses_assigned",
                "assigned_licenses_software.total_licenses_software",
                "assigned_licenses_software.total_licenses_assigned_software",
                "assigned_licenses_hardware.total_licenses_hardware",
                "assigned_licenses_hardware.total_licenses_assigned_hardware",
            ],
            column_type=ColumnType.METRIC,
            default_value=0
        ),
        FULL_OUTER_JOIN_STATEMENTS=generate_full_outer_join_statements(
            {
                "trips": "trips",
                "miles": "miles",
                "active_drivers": "active_drivers",
                "active_users_aggregated": "active_users_aggregated",
                "active_driver_app_users": "active_driver_app_users",
                "marketplace_integrations": "marketplace_integrations",
                "total_device_licenses": "total_device_licenses",
                "assigned_licenses": "assigned_licenses",
                "assigned_licenses_software": "assigned_licenses_software",
                "assigned_licenses_hardware": "assigned_licenses_hardware",
                "active_devices": "active_devices",
                "api_requests": "api_requests",
            },
            {
                "root": "'Unknown'",
                "period_start": "'Unknown'",
                "period_end": "'Unknown'",
                "sam_number": "'Unknown'",
                "account_id": "'Unknown'",
            }
        )
    )
    context.log.info("Query to execute: " + query)
    monthly_df = spark.sql(query)

    return monthly_df
