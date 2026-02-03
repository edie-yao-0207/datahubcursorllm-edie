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
from dataweb.userpkgs.telematics_constants import (
    DIMENSION_CONFIGS,
    TELEMATICS_DIMENSION_TEMPLATES,
    TELEMATICS_JOIN_MAPPINGS,
    TELEMATICS_METRIC_CONFIGS_TEMPLATE,
    TELEMATICS_METRICS,
    TELEMATICS_REGION_MAPPINGS,
    TELEMATICS_SCHEMA,
    TELEMATICS_STANDARD_DIMENSIONS,
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
        table_desc="""A dataset containing aggregated monthly Telematics metrics""",
        row_meaning="""Each row contains Telematics metrics for a given set of dimensions each month""",
        related_table_info={},
        table_type=TableType.AGG,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=[AWSRegion.US_WEST_2],
    owners=[DATAENGINEERING],
    schema=generate_metric_schema(schema=TELEMATICS_SCHEMA, date_column_name="date_month"),
    upstreams=[
        "us-west-2:product_analytics_staging.stg_customer_enriched",
        "us-west-2|eu-west-1|ca-central-1:datamodel_core.dim_organizations",
        "us-west-2|eu-west-1|ca-central-1:finopsdb.customer_info",
        "us-west-2|eu-west-1|ca-central-1:datamodel_telematics.fct_dispatch_routes",
        "us-west-2|eu-west-1|ca-central-1:datamodel_core.dim_devices",
        "us-west-2|eu-west-1|ca-central-1:datamodel_core.lifetime_device_activity",
        "us-west-2|eu-west-1|ca-central-1:datamodel_telematics.fct_trips",
        "us-west-2|eu-west-1|ca-central-1:engineactivitydb_shards.engine_idle_events",
        "us-west-2|eu-west-1|ca-central-1:engineactivitydb_shards.engine_engage_events",
        "us-west-2|eu-west-1|ca-central-1:engineactivitydb_shards.unproductive_idling_config",
        "us-west-2|eu-west-1|ca-central-1:productsdb.devices",
        "us-west-2|eu-west-1|ca-central-1:eldhosdb_shards.driver_hos_violations",
        "us-west-2|eu-west-1|ca-central-1:eldhosdb_shards.driver_hos_ignored_violations",
        "us-west-2|eu-west-1|ca-central-1:compliancedb_shards.driver_hos_logs",
        "us-west-2|eu-west-1|ca-central-1:dataengineering.vehicle_mpg_lookback",
        "us-west-2|eu-west-1|ca-central-1:product_analytics_staging.dim_device_vehicle_properties",
    ],
    primary_keys=PRIMARY_KEYS,
    partitioning=MonthlyPartitionsDefinition(start_date="2023-01-01"),
    dq_checks=[
        NonEmptyDQCheck(name="dq_agg_product_scorecard_telematics_mpr_global_monthly"),
        NonNullDQCheck(name="dq_non_null_agg_product_scorecard_telematics_mpr_global_monthly", non_null_columns=PRIMARY_KEYS, block_before_write=True),
        PrimaryKeyDQCheck(name="dq_pk_agg_product_scorecard_telematics_mpr_global_monthly", primary_keys=PRIMARY_KEYS, block_before_write=True),
        TrendDQCheck(name="dq_trend_agg_product_scorecard_telematics_mpr_global_monthly", lookback_days=1, tolerance=0.1, period="months"),
        SQLDQCheck(
            name="dq_sql_agg_product_scorecard_telematics_mpr_global_monthly_row_count_check",
            sql_query="""
                SELECT COUNT(*) AS observed_value
                FROM df
            """,
            expected_value=30000,
            operator=Operator.gte,
            block_before_write=True,
        ),
        SQLDQCheck(
            name="dq_sql_agg_product_scorecard_telematics_mpr_global_monthly_yard_move_miles_check",
            sql_query="""
                SELECT COUNT(*) AS observed_value
                FROM df
                WHERE yard_move_miles > total_miles
            """,
            expected_value=0,
            operator=Operator.eq,
            block_before_write=False,
        ),
        SQLDQCheck(
            name="dq_sql_agg_product_scorecard_telematics_mpr_global_monthly_hos_violation_check",
            sql_query="""
                SELECT COUNT(*) AS observed_value
                FROM df
                WHERE violation_duration_ms > possible_driver_ms
            """,
            expected_value=0,
            operator=Operator.eq,
            block_before_write=False,
        ),
        SQLDQCheck(
            name="dq_sql_agg_product_scorecard_telematics_mpr_global_monthly_completed_routes_check",
            sql_query="""
                SELECT COUNT(*) AS observed_value
                FROM df
                WHERE completed_routes > routes
            """,
            expected_value=0,
            operator=Operator.eq,
            block_before_write=False,
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
def agg_product_scorecard_telematics_mpr_global_monthly(context: AssetExecutionContext) -> DataFrame:

    context.log.info("Updating agg_product_scorecard_telematics_mpr_global_monthly")
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()

    DIMENSION_CONFIGS["root"] = DimensionConfig(name="root", data_type=DimensionDataType.STRING, default_value="'Unknown'", output_name="date_month")

    metric_expressions = {
        metric_name: generate_metric_expression(TELEMATICS_METRICS, metric_name)
        for metric_name in TELEMATICS_METRICS.keys()
    }

    date_partition = partition_keys_from_context(context)[0][0].replace("'", "")

    anchor_partition, dates, metric_configs = generate_timeframe_metrics(
        date_partition=date_partition,
        period="month",
        metrics=TELEMATICS_METRICS,
        dimension_template=TELEMATICS_DIMENSION_TEMPLATES,
        region_mapping=TELEMATICS_REGION_MAPPINGS,
        metric_template=TELEMATICS_METRIC_CONFIGS_TEMPLATE,
        dimension_dict=TELEMATICS_STANDARD_DIMENSIONS,
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
                "account_renewal_12mo"
            ],
            aliases=[
                "routes_aggregated",
                "trips_aggregated",
                "vg_aggregated",
                "idling_aggregated",
                "drivers_aggregated",
                "hos_violation_aggregated",
                "unassigned_hos_aggregated",
                "assigned_hos_aggregated",
                "mpg_aggregated",
                "vehicle_mix_aggregated",
                "split_berth_aggregated",
                "yard_move_aggregated",
                "total_movement_aggregated",
                "engage_aggregated",
                "unproductive_idling_aggregated"
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
                group_by="1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17",
                join_config=TELEMATICS_JOIN_MAPPINGS,
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
                "routes_aggregated.routes",
                "routes_aggregated.completed_routes",
                "trips_aggregated.vg_trips",
                "vg_aggregated.active_vgs",
                "idling_aggregated.idle_duration_ms",
                "engage_aggregated.on_duration_ms",
                "unproductive_idling_aggregated.unproductive_idle_duration_ms",
                "drivers_aggregated.total_drivers",
                "drivers_aggregated.total_drivers_using_app_month",
                "drivers_aggregated.total_drivers_using_app",
                "hos_violation_aggregated.violation_duration_ms",
                "hos_violation_aggregated.possible_driver_ms",
                "unassigned_hos_aggregated.unassigned_driver_time_hours",
                "assigned_hos_aggregated.assigned_driver_time_hours",
                "mpg_aggregated.p50_mpg",
                "vehicle_mix_aggregated.vehicles_ev",
                "vehicle_mix_aggregated.vehicles_ice",
                "vehicle_mix_aggregated.vehicles_hybrid",
                "split_berth_aggregated.sleeper_berth_split_violations",
                "yard_move_aggregated.yard_move_miles",
                "total_movement_aggregated.total_miles"
            ],
            column_type=ColumnType.METRIC,
            default_value=0
        ),
        FULL_OUTER_JOIN_STATEMENTS=generate_full_outer_join_statements(
            {
                "routes_aggregated": "routes_aggregated",
                "trips_aggregated": "trips_aggregated",
                "vg_aggregated": "vg_aggregated",
                "idling_aggregated": "idling_aggregated",
                "drivers_aggregated": "drivers_aggregated",
                "hos_violation_aggregated": "hos_violation_aggregated",
                "unassigned_hos_aggregated": "unassigned_hos_aggregated",
                "assigned_hos_aggregated": "assigned_hos_aggregated",
                "mpg_aggregated": "mpg_aggregated",
                "vehicle_mix_aggregated": "vehicle_mix_aggregated",
                "split_berth_aggregated": "split_berth_aggregated",
                "yard_move_aggregated": "yard_move_aggregated",
                "total_movement_aggregated": "total_movement_aggregated",
                "engage_aggregated": "engage_aggregated",
                "unproductive_idling_aggregated": "unproductive_idling_aggregated"
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
