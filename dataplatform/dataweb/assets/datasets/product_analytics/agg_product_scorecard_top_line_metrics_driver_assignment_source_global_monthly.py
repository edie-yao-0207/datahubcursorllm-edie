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
    DRIVER_ASSIGNMENT_DIMENSION_CONFIGS,
    DRIVER_ASSIGNMENT_METRIC_CONFIGS_TEMPLATE,
    DRIVER_ASSIGNMENT_METRICS,
    DRIVER_ASSIGNMENT_QUERY_TEMPLATE,
    DRIVER_ASSIGNMENT_SCHEMA,
    TOP_LINE_DIMENSION_TEMPLATES,
    TOP_LINE_JOIN_MAPPINGS,
    TOP_LINE_REGION_MAPPINGS,
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
    "driver_assignment_source",
]

@table(
    database=Database.PRODUCT_ANALYTICS,
    description=build_table_description(
        table_desc="""A dataset containing aggregated monthly driver assignment metrics.""",
        row_meaning="""Each row contains driver assignment metrics for a given set of dimensions each month""",
        related_table_info={},
        table_type=TableType.AGG,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=[AWSRegion.US_WEST_2],
    owners=[DATAENGINEERING],
    schema=generate_metric_schema(schema=DRIVER_ASSIGNMENT_SCHEMA, date_column_name="date_month"),
    upstreams=[
        "us-west-2|eu-west-1:datamodel_core.dim_devices",
        "us-west-2|eu-west-1:datamodel_telematics.fct_trips",
        "us-west-2|eu-west-1:datamodel_core.dim_organizations",
        "us-west-2|eu-west-1:finopsdb.customer_info",
        "us-west-2|eu-west-1:dynamodb.driver_assignment_settings",
    ],
    primary_keys=PRIMARY_KEYS,
    partitioning=MonthlyPartitionsDefinition(start_date="2023-01-01"),
    dq_checks=[
        NonEmptyDQCheck(name="dq_agg_product_scorecard_top_line_metrics_driver_assignment_source_global_monthly"),
        NonNullDQCheck(name="dq_non_null_agg_product_scorecard_top_line_metrics_driver_assignment_source_global_monthly", non_null_columns=PRIMARY_KEYS, block_before_write=True),
        PrimaryKeyDQCheck(name="dq_pk_agg_product_scorecard_top_line_metrics_driver_assignment_source_global_monthly", primary_keys=PRIMARY_KEYS, block_before_write=True),
        TrendDQCheck(name="dq_trend_agg_product_scorecard_top_line_metrics_driver_assignment_source_global_monthly", lookback_days=1, tolerance=0.1, period="months"),
        SQLDQCheck(
            name="dq_sql_agg_product_scorecard_top_line_metrics_driver_assignment_source_global_inward_assigned_duration_check",
            sql_query="""
                SELECT COUNT(*) AS observed_value
                FROM df
                WHERE inward_assigned_trip_duration > inward_total_trip_duration
            """,
            expected_value=0,
            operator=Operator.eq,
            block_before_write=True,
        ),
        SQLDQCheck(
            name="dq_sql_agg_product_scorecard_top_line_metrics_driver_assignment_source_global_outward_assigned_duration_check",
            sql_query="""
                SELECT COUNT(*) AS observed_value
                FROM df
                WHERE outward_assigned_trip_duration > outward_total_trip_duration
            """,
            expected_value=0,
            operator=Operator.eq,
            block_before_write=True,
        ),
        SQLDQCheck(
            name="dq_sql_agg_product_scorecard_top_line_metrics_driver_assignment_source_global_no_cm_assigned_duration_check",
            sql_query="""
                SELECT COUNT(*) AS observed_value
                FROM df
                WHERE no_cm_assigned_trip_duration > no_cm_total_trip_duration
            """,
            expected_value=0,
            operator=Operator.eq,
            block_before_write=True,
        ),
    ],
    backfill_batch_size=1,
    write_mode=WarehouseWriteMode.OVERWRITE,
    max_retries=3,
    run_config_overrides=create_run_config_overrides(
        instance_pool_type=MEMORY_OPTIMIZED_INSTANCE_POOL_KEY,
        max_workers=4,
    ),
)
def agg_product_scorecard_top_line_metrics_driver_assignment_source_global_monthly(context: AssetExecutionContext) -> DataFrame:

    context.log.info("Updating agg_product_scorecard_top_line_metrics_driver_assignment_source_global_monthly")
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()

    DRIVER_ASSIGNMENT_DIMENSION_CONFIGS["root"] = DimensionConfig(name="root", data_type=DimensionDataType.STRING, default_value="'Unknown'", output_name="date_month")

    metric_expressions = {
        metric_name: generate_metric_expression(DRIVER_ASSIGNMENT_METRICS, metric_name)
        for metric_name in DRIVER_ASSIGNMENT_METRICS.keys()
    }

    date_partition = partition_keys_from_context(context)[0][0].replace("'", "")

    anchor_partition, dates, metric_configs = generate_timeframe_metrics(
        date_partition=date_partition,
        period="month",
        metrics=DRIVER_ASSIGNMENT_METRICS,
        dimension_template=TOP_LINE_DIMENSION_TEMPLATES,
        region_mapping=TOP_LINE_REGION_MAPPINGS,
        metric_template=DRIVER_ASSIGNMENT_METRIC_CONFIGS_TEMPLATE,
        dimension_dict=TOP_LINE_STANDARD_DIMENSIONS,
        dimension_groups=["time_dimensions", "account_dimensions", "product_dimensions", "sam_dimensions", "driver_assignment_source_dimensions"],
        offset=0,
    )

    query = DRIVER_ASSIGNMENT_QUERY_TEMPLATE.format(
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
                "driver_assignment_source",
                "sign_in_reminder_enabled"
            ],
            aliases=[
                "driver_assignment_aggregated",
            ],
            column_type=ColumnType.DIMENSION,
            dimension_configs=DRIVER_ASSIGNMENT_DIMENSION_CONFIGS
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
                group_by="1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18",
                join_config=TOP_LINE_JOIN_MAPPINGS,
                join_override=config.get("join_override"),
            )
            for metric_name, config in metric_configs.items()
        ),
        **get_formatted_metrics(metric_expressions),
        COALESCE_METRIC_STATEMENTS=generate_coalesce_statement(
            columns=[
                "driver_assignment_aggregated.inward_assigned_trip_duration",
                "driver_assignment_aggregated.inward_total_trip_duration",
                "driver_assignment_aggregated.outward_assigned_trip_duration",
                "driver_assignment_aggregated.outward_total_trip_duration",
                "driver_assignment_aggregated.no_cm_assigned_trip_duration",
                "driver_assignment_aggregated.no_cm_total_trip_duration",
            ],
            column_type=ColumnType.METRIC,
            default_value=0
        ),
        FULL_OUTER_JOIN_STATEMENTS=generate_full_outer_join_statements(
            {
                "driver_assignment_aggregated": "driver_assignment_aggregated",
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
