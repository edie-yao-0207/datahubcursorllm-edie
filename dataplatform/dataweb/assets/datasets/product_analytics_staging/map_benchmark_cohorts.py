import pandas as pd
from dagster import AssetExecutionContext, DailyPartitionsDefinition
from dataweb import (
    NonEmptyDQCheck,
    NonNullDQCheck,
    PrimaryKeyDQCheck,
    TrendDQCheck,
    table,
)
from dataweb.userpkgs.constants import (
    DATAENGINEERING,
    FRESHNESS_SLO_12PM_PST,
    AWSRegion,
    Database,
    TableType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.utils import (
    build_table_description,
    partition_key_ranges_from_context,
)
from pyspark.sql import DataFrame, SparkSession

PRIMARY_KEYS = ["date", "cohort_id"]

SCHEMA = [
    {
        "name": "date",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "The date which partitions the table, in `YYYY-mm-dd` format."
        },
    },
    {
        "name": "cohort_id",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Unique id for cohort, a numerical value that starts with v2-. The default cohort_id is cohort_id v2-1."
        },
    },
    {
        "name": "industry_vertical",
        "type": "string",
        "nullable": False,
        "metadata": {"comment": "Category representing type of industry of customer."},
    },
    {
        "name": "fleet_composition",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Category representing composition of fleet by gross vehicle weight rating class."
        },
    },
    {
        "name": "avg_mileage",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Category representing average daily driving distance per device of customer."
        },
    },
    {
        "name": "primary_driving_environment",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Category representing whether the customer primarily drives in urban or rural areas."
        },
    },
    {
        "name": "fleet_size",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Category representing size of customer's fleet in number of active devices."
        },
    },
    {
        "name": "region",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Category representing region of the United States or international region where customer trips primarily begin."
        },
    },
    {
        "name": "fuel_category",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Category representing primary engine type composition of customer fleet including gas, diesel, and electric."
        },
    },
    {
        "name": "transport_licenses_endorsements",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Category representing whether customer trips are conducted by drivers with endorsements to transport hazardous materials."
        },
    },
]

category_value_def = {
    "industry_vertical": ["field_services", "transportation", "public_sector", "all"],
    "region": [
        "northeast",
        "midwest",
        "southeast",
        "southwest",
        "west",
        "usa_all",
        "canada",
        "mexico",
        "eu_all",
        "all",
    ],
    "primary_driving_environment": ["urban", "rural", "mixed", "all"],
    "fleet_composition": ["heavy", "medium", "light", "mixed", "all"],
    "fleet_size": [
        "less_than_30",
        "btw_31_and_100",
        "btw_101_and_500",
        "greater_than_500",
        "all",
    ],
    "avg_mileage": ["less_than_250", "greater_than_250", "all"],
    "fuel_category": ["diesel", "gas", "electric", "mixed", "all"],
    "transport_licenses_endorsements": ["hazmat_goods_carrier", "all"],
}

category_priority = [
    ["industry_vertical", 0],
    ["fleet_composition", 1],
    ["avg_mileage", 2],
    ["primary_driving_environment", 3],
    ["fleet_size", 4],
    ["region", 5],
    ["fuel_category", 6],
    ["transport_licenses_endorsements", 7],
]


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="""Permutations of cohorts to support the Fleet Benchmarks report""",
        row_meaning="""Each row describes the categorical attributes of one cohort_id by date""",
        related_table_info={},
        table_type=TableType.LOOKUP,
        freshness_slo_updated_by=FRESHNESS_SLO_12PM_PST,
    ),
    regions=[AWSRegion.US_WEST_2],
    owners=[DATAENGINEERING],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DailyPartitionsDefinition(start_date="2024-06-01"),
    write_mode=WarehouseWriteMode.OVERWRITE,
    dq_checks=[
        NonEmptyDQCheck(name="dq_non_empty_map_benchmark_cohorts"),
        PrimaryKeyDQCheck(
            name="dq_pk_map_benchmark_cohorts",
            primary_keys=PRIMARY_KEYS,
            block_before_write=True,
        ),
        NonNullDQCheck(
            name="dq_non_null_map_benchmark_cohorts",
            non_null_columns=[
                "date",
                "cohort_id",
                "industry_vertical",
                "fleet_composition",
                "avg_mileage",
                "primary_driving_environment",
                "fleet_size",
                "region",
                "fuel_category",
                "transport_licenses_endorsements",
            ],
            block_before_write=False,
        ),
        TrendDQCheck(
            name="dq_trend_map_benchmark_cohorts",
            tolerance=0.1,
            lookback_days=1,
            block_before_write=False,
        ),
    ],
)
def map_benchmark_cohorts(context: AssetExecutionContext) -> DataFrame:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    context.log.info("Updating map_benchmark_cohorts")
    partition_keys = partition_key_ranges_from_context(context)[0]
    PARTITION_START = partition_keys[0]
    combinations = [[]]
    column_order = [category for category, priority in category_priority]

    for category, priority in category_priority:
        category_values = category_value_def[category]
        add_combinations = []
        for combination in combinations:
            for value in category_values:
                add_combinations.append(combination + [value])
        combinations = add_combinations

    df = pd.DataFrame(combinations, columns=column_order)
    df.insert(0, "cohort_id", ["v2-" + str(i + 1) for i in reversed(range(len(df)))])
    df.insert(0, "date", PARTITION_START)
    df = spark.createDataFrame(df)
    return df
