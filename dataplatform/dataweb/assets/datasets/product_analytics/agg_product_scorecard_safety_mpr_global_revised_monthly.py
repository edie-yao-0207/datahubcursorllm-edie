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
    FRESHNESS_SLO_9PM_PST,
    MEMORY_OPTIMIZED_INSTANCE_POOL_KEY,
    AWSRegion,
    ColumnType,
    Database,
    DimensionConfig,
    DimensionDataType,
    TableType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.safety_constants import (
    DIMENSION_CONFIGS,
    SAFETY_DIMENSION_TEMPLATES,
    SAFETY_DIMENSIONS,
    SAFETY_JOIN_MAPPINGS,
    SAFETY_METRIC_CONFIGS_TEMPLATE,
    SAFETY_METRICS,
    SAFETY_QUERY,
    SAFETY_REGION_MAPPINGS,
    SAFETY_SCHEMA,
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
        table_desc="""A dataset containing aggregated monthly Safety MPR metrics""",
        row_meaning="""Each row contains top line metrics for a given set of dimensions each month""",
        related_table_info={},
        table_type=TableType.AGG,
        freshness_slo_updated_by=FRESHNESS_SLO_9PM_PST,
    ),
    regions=[AWSRegion.US_WEST_2],
    owners=[DATAENGINEERING],
    schema=generate_metric_schema(schema=SAFETY_SCHEMA, date_column_name="date_month"),
    upstreams=[
        "us-west-2:product_analytics_staging.stg_customer_enriched",
        "us-west-2|eu-west-1:datamodel_core.dim_organizations",
        "us-west-2|eu-west-1|ca-central-1:finopsdb.customer_info",
        "us-west-2|eu-west-1:datamodel_core.dim_devices",
        "us-west-2|eu-west-1:datamodel_platform.dim_drivers",
        "us-west-2|eu-west-1:datamodel_telematics.fct_trips",
        "us-west-2|eu-west-1:product_analytics.dim_devices_safety_settings",
        "us-west-2|eu-west-1|ca-central-1:formsdb_shards.form_submissions",
        "us-west-2|eu-west-1|ca-central-1:trainingdb_shards.courses",
        "us-west-2|eu-west-1|ca-central-1:trainingdb_shards.categories",
        "us-west-2|eu-west-1|ca-central-1:coachingdb_shards.coaching_sessions",
        "us-west-2|eu-west-1|ca-central-1:coachingdb_shards.coachable_item",
        "us-west-2|eu-west-1|ca-central-1:coachingdb_shards.coachable_item_share",
    ],
    primary_keys=PRIMARY_KEYS,
    partitioning=MonthlyPartitionsDefinition(start_date="2023-01-01"),
    dq_checks=[
        NonEmptyDQCheck(name="dq_agg_product_scorecard_safety_mpr_global_revised_monthly"),
        NonNullDQCheck(name="dq_non_null_agg_product_scorecard_safety_mpr_global_revised_monthly", non_null_columns=PRIMARY_KEYS, block_before_write=True),
        PrimaryKeyDQCheck(name="dq_pk_agg_product_scorecard_safety_mpr_global_revised_monthly", primary_keys=PRIMARY_KEYS, block_before_write=True),
        TrendDQCheck(name="dq_trend_agg_product_scorecard_safety_mpr_global_revised_monthly", lookback_days=1, tolerance=0.1, period="months"),
        SQLDQCheck(
            name="dq_sql_agg_product_scorecard_safety_mpr_global_revised_monthly_count_check",
            sql_query="""
                SELECT COUNT(DISTINCT CONCAT(sam_number, account_id)) AS observed_value
                FROM df
            """,
            expected_value=30000,
            operator=Operator.gte,
            block_before_write=True,
        ),
        SQLDQCheck(
            name="dq_sql_agg_product_scorecard_safety_mpr_global_revised_monthly_total_drivers_count_check",
            sql_query="""
                SELECT SUM(total_drivers) AS observed_value
                FROM df
            """,
            expected_value=2000000,
            operator=Operator.gte,
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
def agg_product_scorecard_safety_mpr_global_revised_monthly(context: AssetExecutionContext) -> DataFrame:

    context.log.info("Updating agg_product_scorecard_safety_mpr_global_revised_monthly")
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()

    DIMENSION_CONFIGS["root"] = DimensionConfig(name="root", data_type=DimensionDataType.STRING, default_value="'Unknown'", output_name="date_month")

    metric_expressions = {
        metric_name: generate_metric_expression(SAFETY_METRICS, metric_name)
        for metric_name in SAFETY_METRICS.keys()
    }

    date_partition = partition_keys_from_context(context)[0][0].replace("'", "")

    anchor_partition, dates, metric_configs = generate_timeframe_metrics(
        date_partition=date_partition,
        period="month",
        metrics=SAFETY_METRICS,
        dimension_template=SAFETY_DIMENSION_TEMPLATES,
        region_mapping=SAFETY_REGION_MAPPINGS,
        metric_template=SAFETY_METRIC_CONFIGS_TEMPLATE,
        dimension_dict=SAFETY_DIMENSIONS,
        dimension_groups=["time_dimensions", "account_dimensions", "product_dimensions", "sam_dimensions", "device_dimensions"],
        offset=0,
    )

    query = SAFETY_QUERY.format(
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
                "account_created_date",
                "account_renewal_12mo",
                "dual_facing_cam",
            ],
            aliases=[
                "drivers_aggregated",
                "trainings_aggregated",
                "manager_led_coaching_aggregated",
                "self_coaching_aggregated",
                "coaching_aggregated",
                "trips_aggregated",
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
                group_by="1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19",
                join_config=SAFETY_JOIN_MAPPINGS,
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
                "drivers_aggregated.total_drivers",
                "drivers_aggregated.total_drivers_using_app_month",
                "drivers_aggregated.total_drivers_using_app",
                "trainings_aggregated.assigned_trainings",
                "trainings_aggregated.total_drivers_assigned_training",
                "trainings_aggregated.total_drivers_completing_training_on_time",
                "manager_led_coaching_aggregated.drivers_completing_manager_led_coaching",
                "self_coaching_aggregated.drivers_completing_self_coaching",
                "coaching_aggregated.drivers_completing_coaching",
                "trips_aggregated.miles",
            ],
            column_type=ColumnType.METRIC,
            default_value=0
        ),
        FULL_OUTER_JOIN_STATEMENTS=generate_full_outer_join_statements(
            {
                "drivers_aggregated": "drivers_aggregated",
                "trainings_aggregated": "trainings_aggregated",
                "manager_led_coaching_aggregated": "manager_led_coaching_aggregated",
                "self_coaching_aggregated": "self_coaching_aggregated",
                "coaching_aggregated": "coaching_aggregated",
                "trips_aggregated": "trips_aggregated",
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
