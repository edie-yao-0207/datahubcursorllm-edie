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
    EVENT_DIMENSION_CONFIGS,
    EVENT_SAFETY_JOIN_MAPPINGS,
    EVENT_SAFETY_METRIC_CONFIGS_TEMPLATE,
    EVENT_SAFETY_METRICS,
    EVENT_SAFETY_SCHEMA,
    EVENT_SAFETY_QUERY,
    SAFETY_DIMENSIONS,
    SAFETY_DIMENSION_TEMPLATES,
    SAFETY_REGION_MAPPINGS,
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
    "event_type",
]

@table(
    database=Database.PRODUCT_ANALYTICS,
    description=build_table_description(
        table_desc="""A dataset containing aggregated monthly Safety MPR metrics, grouped by event type""",
        row_meaning="""Each row contains top line metrics for a given event type and set of dimensions each month""",
        related_table_info={},
        table_type=TableType.AGG,
        freshness_slo_updated_by=FRESHNESS_SLO_9PM_PST,
    ),
    regions=[AWSRegion.US_WEST_2],
    owners=[DATAENGINEERING],
    schema=generate_metric_schema(schema=EVENT_SAFETY_SCHEMA, date_column_name="date_month"),
    upstreams=[
        "us-west-2|eu-west-1:datamodel_core.dim_organizations",
        "us-west-2|eu-west-1|ca-central-1:finopsdb.customer_info",
        "us-west-2|eu-west-1:datamodel_core.dim_devices",
        "us-west-2|eu-west-1:product_analytics.dim_devices_safety_settings",
        "us-west-2|eu-west-1:datamodel_telematics.fct_trips",
        "us-west-2|eu-west-1:datamodel_core.lifetime_device_activity",
        "us-west-2|eu-west-1|ca-central-1:safetyeventtriagedb_shards.detections",
        "us-west-2|eu-west-1|ca-central-1:safetyeventtriagedb_shards.triage_events_v2",
        "us-west-2|eu-west-1|ca-central-1:coachingdb_shards.coaching_sessions",
        "us-west-2|eu-west-1|ca-central-1:coachingdb_shards.coachable_item",
        "us-west-2|eu-west-1|ca-central-1:coachingdb_shards.coachable_item_share",
        "us-west-2|eu-west-1|ca-central-1:safetydb_shards.activity_events",
        "us-west-2|eu-west-1|ca-central-1:safetydb_shards.harsh_event_surveys",
        "us-west-2:product_analytics_staging.stg_customer_enriched",
    ],
    primary_keys=PRIMARY_KEYS,
    partitioning=MonthlyPartitionsDefinition(start_date="2023-01-01"),
    dq_checks=[
        NonEmptyDQCheck(name="dq_agg_product_scorecard_safety_mpr_event_global_monthly"),
        NonNullDQCheck(name="dq_non_null_agg_product_scorecard_safety_mpr_event_global_monthly", non_null_columns=PRIMARY_KEYS, block_before_write=True),
        PrimaryKeyDQCheck(name="dq_pk_agg_product_scorecard_safety_mpr_event_global_monthly", primary_keys=PRIMARY_KEYS, block_before_write=True),
        TrendDQCheck(name="dq_trend_agg_product_scorecard_safety_mpr_event_global_monthly", lookback_days=1, tolerance=0.1, period="months"),
        SQLDQCheck(
            name="dq_sql_agg_product_scorecard_safety_mpr_event_global_monthly_count_check",
            sql_query="""
                SELECT COUNT(DISTINCT CONCAT(sam_number, account_id)) AS observed_value
                FROM df
            """,
            expected_value=30000,
            operator=Operator.gte,
            block_before_write=True,
        ),
        SQLDQCheck(
            name="dq_sql_agg_product_scorecard_safety_mpr_event_global_monthly_detection_log_event_count_check",
            sql_query="""
                SELECT SUM(detection_log_events) AS observed_value
                FROM df
            """,
            expected_value=250000000,
            operator=Operator.gte,
            block_before_write=True,
        ),
        SQLDQCheck(
            name="dq_sql_agg_product_scorecard_safety_mpr_event_global_monthly_inbox_event_count_check",
            sql_query="""
                SELECT SUM(inbox_events) AS observed_value
                FROM df
            """,
            expected_value=20000000,
            operator=Operator.gte,
            block_before_write=True,
        ),
        SQLDQCheck(
            name="dq_sql_agg_product_scorecard_safety_mpr_event_global_monthly_inbox_event_viewed_check",
            sql_query="""
                SELECT COUNT(*) AS observed_value
                FROM df
                WHERE inbox_events_viewed > inbox_events
            """,
            expected_value=0,
            operator=Operator.eq,
            block_before_write=False,
        ),
        SQLDQCheck(
            name="dq_sql_agg_product_scorecard_safety_mpr_event_global_monthly_inbox_eligible_events_check",
            sql_query="""
                SELECT COUNT(*) AS observed_value
                FROM df
                WHERE eligible_events_with_driver > eligible_events
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
        max_workers=16,
    ),
)
def agg_product_scorecard_safety_mpr_event_global_monthly(context: AssetExecutionContext) -> DataFrame:

    context.log.info("Updating agg_product_scorecard_safety_mpr_event_global_monthly")
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()

    EVENT_DIMENSION_CONFIGS["root"] = DimensionConfig(name="root", data_type=DimensionDataType.STRING, default_value="'Unknown'", output_name="date_month")

    metric_expressions = {
        metric_name: generate_metric_expression(EVENT_SAFETY_METRICS, metric_name)
        for metric_name in EVENT_SAFETY_METRICS.keys()
    }

    date_partition = partition_keys_from_context(context)[0][0].replace("'", "")

    anchor_partition, dates, metric_configs = generate_timeframe_metrics(
        date_partition=date_partition,
        period="month",
        metrics=EVENT_SAFETY_METRICS,
        dimension_template=SAFETY_DIMENSION_TEMPLATES,
        region_mapping=SAFETY_REGION_MAPPINGS,
        metric_template=EVENT_SAFETY_METRIC_CONFIGS_TEMPLATE,
        dimension_dict=SAFETY_DIMENSIONS,
        dimension_groups=["time_dimensions", "account_dimensions", "product_dimensions", "sam_dimensions", "device_dimensions", "event_type_dimensions"],
        offset=0,
    )

    query = EVENT_SAFETY_QUERY.format(
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
                "event_type",
                "dual_facing_event_type",
            ],
            aliases=[
                "enabled_aggregated",
                "detection_aggregated",
                "triage_aggregated",
                "self_coaching_aggregated",
                "manager_led_coaching_aggregated",
                "trips_aggregated",
            ],
            column_type=ColumnType.DIMENSION,
            dimension_configs=EVENT_DIMENSION_CONFIGS
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
                group_by="1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21",
                join_config=EVENT_SAFETY_JOIN_MAPPINGS,
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
                "enabled_aggregated.event_type_enabled",
                "enabled_aggregated.in_cab_alerts_enabled",
                "enabled_aggregated.send_to_safety_inbox_enabled",
                "triage_aggregated.inbox_events",
                "triage_aggregated.inbox_events_viewed",
                "triage_aggregated.inbox_events_dismissed",
                "triage_aggregated.inbox_events_viewed_dismissed",
                "triage_aggregated.inbox_events_viewed_not_useful",
                "triage_aggregated.eligible_events",
                "triage_aggregated.eligible_events_with_driver",
                "detection_aggregated.detection_log_events",
                "detection_aggregated.in_cab_alerts",
                "detection_aggregated.events_without_alert",
                "detection_aggregated.detection_log_events_caps",
                "detection_aggregated.detection_log_events_threshold",
                "detection_aggregated.detection_log_events_nudges",
                "detection_aggregated.detection_log_events_speed",
                "detection_aggregated.detection_log_events_confidence",
                "detection_aggregated.detection_log_events_severity",
                "detection_aggregated.detection_log_events_ser",
                "detection_aggregated.detection_log_events_incab",
                "detection_aggregated.detection_log_events_other",
                "detection_aggregated.detection_log_events_no_filter",
                "self_coaching_aggregated.self_coaching_events",
                "self_coaching_aggregated.self_coaching_events_completed",
                "self_coaching_aggregated.total_drivers_assigned_self_coaching",
                "self_coaching_aggregated.total_drivers_completed_self_coaching",
                "manager_led_coaching_aggregated.manager_led_coaching_sessions",
                "manager_led_coaching_aggregated.manager_led_coaching_events",
                "manager_led_coaching_aggregated.manager_led_coaching_events_completed",
                "manager_led_coaching_aggregated.manager_led_coaches",
                "manager_led_coaching_aggregated.manager_led_coaches_coaching_on_time",
                "manager_led_coaching_aggregated.completed_manager_led_coaching_sessions",
                "manager_led_coaching_aggregated.completed_manager_led_coaching_sessions_14d",
                "manager_led_coaching_aggregated.manager_led_coaching_drivers",
                "manager_led_coaching_aggregated.manager_led_coaching_drivers_coached",
                "trips_aggregated.miles_event",
            ],
            column_type=ColumnType.METRIC,
            default_value=0
        ),
        FULL_OUTER_JOIN_STATEMENTS=generate_full_outer_join_statements(
            {
                "enabled_aggregated": "enabled_aggregated",
                "detection_aggregated": "detection_aggregated",
                "triage_aggregated": "triage_aggregated",
                "self_coaching_aggregated": "self_coaching_aggregated",
                "manager_led_coaching_aggregated": "manager_led_coaching_aggregated",
                "trips_aggregated": "trips_aggregated",
            },
            {
                "root": "'Unknown'",
                "period_start": "'Unknown'",
                "period_end": "'Unknown'",
                "sam_number": "'Unknown'",
                "account_id": "'Unknown'",
                "event_type": "'Unknown'",
            }
        ),
    )
    context.log.info("Query to execute: " + query)
    monthly_df = spark.sql(query)

    return monthly_df
