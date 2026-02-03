from dagster import (
    AssetExecutionContext,
    MonthlyPartitionsDefinition,
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
from pyspark.sql import DataFrame, SparkSession
from datetime import datetime
from dateutil.relativedelta import relativedelta

PRIMARY_KEYS = [
    "date_month",
    "period_start",
    "period_end",
    "cloud_region",
    "region",
    "account_arr_segment",
    "account_size_segment",
    "account_industry",
    "industry_vertical",
    "is_paid_safety_customer",
    "is_paid_telematics_customer",
    "is_paid_stce_customer"
]

@table(
    database=Database.PRODUCT_ANALYTICS,
    description=build_table_description(
        table_desc="""A dataset containing aggregated monthly Platform MPR metrics for faster retrieval in Tableau""",
        row_meaning="""Each row contains monthly Platform MPR metrics for a given set of dimensions""",
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
        "us-west-2|eu-west-1|ca-central-1:datamodel_platform.dim_users",
        "us-west-2:datamodel_platform_silver.stg_cloud_routes",
        "us-west-2|eu-west-1|ca-central-1:metrics_repo.device_tags_attributes_count",
        "us-west-2|eu-west-1|ca-central-1:metrics_repo.driver_tags_attributes_count",
        "us-west-2|eu-west-1|ca-central-1:csvuploadsdb_shards.csv_uploads",
    ],
    primary_keys=PRIMARY_KEYS,
    partitioning=MonthlyPartitionsDefinition(start_date="2023-01-01"),
    dq_checks=[
        NonEmptyDQCheck(name="dq_agg_product_scorecard_platform_mpr_global_monthly"),
        NonNullDQCheck(name="dq_non_null_agg_product_scorecard_platform_mpr_global_monthly", non_null_columns=["date_month", "period_start", "period_end"], block_before_write=True),
        PrimaryKeyDQCheck(name="dq_pk_agg_product_scorecard_platform_mpr_global_monthly", primary_keys=PRIMARY_KEYS, block_before_write=True),
        TrendDQCheck(name="dq_trend_agg_product_scorecard_platform_mpr_global_monthly", lookback_days=1, tolerance=0.1, period="months"),
    ],
    backfill_batch_size=1,
    write_mode=WarehouseWriteMode.OVERWRITE,
    max_retries=3,
)
def agg_product_scorecard_platform_mpr_global_monthly(context: AssetExecutionContext) -> DataFrame:

    context.log.info("Updating agg_product_scorecard_platform_mpr_global_monthly")
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
        dimension_groups=["time_dimensions", "region_dimensions", "account_dimensions", "product_dimensions"],
        offset=0,
    )

    base_date = datetime.strptime(date_partition, "%Y-%m-%d")

    anchor_day = base_date - relativedelta(months=0)
    end_date = datetime.strptime(dates[0], "%Y-%m-%d")
    day_difference = (end_date - anchor_day).days
    monthly_df = spark.sql(
            QUERY_TEMPLATE.format(
                DATEID=date_partition,
                START_OF_PERIOD=anchor_partition,
                DAY_DIFFERENCE=day_difference,
                PERIOD="month",
                END_OF_PERIOD=dates[0],
                COALESCE_STATEMENTS = generate_coalesce_statement(
                    columns=[
                        "root", "period_start", "period_end", "cloud_region", "region",
                        "account_size_segment", "account_arr_segment", "account_industry", "industry_vertical", "is_paid_safety_customer", "is_paid_telematics_customer",
                        "is_paid_stce_customer"
                    ],
                    aliases=[
                        "user_roles",
                        "device_details",
                        "driver_details",
                        "csv"
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
                        group_by="1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13",
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
                        "user_roles.full_admin_orgs",
                        "user_roles.custom_role_orgs",
                        "user_roles.total_orgs",
                        "device_details.device_attributes",
                        "device_details.device_attributes_orgs",
                        "device_details.device_tags",
                        "device_details.device_tags_orgs",
                        "device_details.device_tags_attributes",
                        "device_details.total_devices",
                        "device_details.total_device_orgs",
                        "driver_details.driver_tags",
                        "driver_details.driver_tags_orgs",
                        "driver_details.driver_attributes",
                        "driver_details.driver_attributes_orgs",
                        "driver_details.driver_tags_attributes",
                        "driver_details.total_drivers",
                        "driver_details.total_driver_orgs",
                        "csv.csv_upload_failures",
                        "csv.csv_uploads",
                        "csv.average_row_error_rate",
                        "active_users.active_cloud_dashboard_users",
                        "active_users.active_cloud_dashboard_orgs",
                        "total_users.total_possible_cloud_dashboard_users",
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
                        "total_users_aggregated": "total_users",
                    },
                    {
                        "root": "'Unknown'",
                        "period_start": "'Unknown'",
                        "period_end": "'Unknown'",
                        "region": "'Unknown'",
                        "cloud_region": "'Unknown'",
                        "account_size_segment": "'Unknown'",
                        "account_arr_segment": "'Unknown'",
                        "account_industry": "'Unknown'",
                        "industry_vertical": "'Unknown'",
                        "is_paid_safety_customer": "false",
                        "is_paid_telematics_customer": "false",
                        "is_paid_stce_customer": "false",
                    }
                )
            )
        )

    return monthly_df
