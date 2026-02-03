from dagster import AssetExecutionContext, AssetKey, DailyPartitionsDefinition
from dataweb import NonEmptyDQCheck, PrimaryKeyDQCheck, SQLDQCheck, NonNegativeDQCheck, ValueRangeDQCheck, NonNullDQCheck, JoinableDQCheck, TrendDQCheck,table
from dataweb.userpkgs.constants import AWSRegion, Database, DATAENGINEERING, WarehouseWriteMode
from dataweb.userpkgs.utils import partition_keys_from_context

daily_partition_def = DailyPartitionsDefinition(start_date="2024-06-01")

dq1 = NonEmptyDQCheck(name="dq_empty_example_sql_dataset")
dq2 = PrimaryKeyDQCheck(
    name="dq_primary_key_example_sql_dataset", primary_keys=["date"]
)
dq3 = SQLDQCheck(
    name="dq_sql_example_sql_dataset",
    sql_query="SELECT count(*) as observed_value FROM df",
    expected_value=1,
)

dq4 = NonNegativeDQCheck(name="dq_non_negative_example_sql_dataset", columns=["col1"])

dq5 = ValueRangeDQCheck(
    name="dq_value_range_example_sql_dataset",
    column_range_map={"col1": (0, 10)},
    block_before_write=True,
)

dq6 = NonNullDQCheck(
    name="non_null",
    non_null_columns=["col1", "date"],
)

dq7 = JoinableDQCheck(
    name="dq_joinable_example_sql_dataset",
    database_2=Database.DATAMODEL_DEV,
    input_asset_2="example_sql_dataset",
    join_keys=[("date", "date")],
    null_right_table_rows_ratio=0.05,
    check_before_write=True,
    block_before_write=True,
)

dq8 = TrendDQCheck(
    name="dq_trend_example_sql_dataset",
    tolerance=0.05,
    check_before_write=True,
    block_before_write=True,
)


@table(
    database=Database.DATAMODEL_DEV,
    upstreams=[AssetKey([AWSRegion.US_WEST_2, Database.DATAMODEL_DEV, "example_sql_dataset"])],
    description="A dataset that is produced by from a SQL query and checked via DQ.",
    regions=[AWSRegion.US_WEST_2],
    owners=[DATAENGINEERING],
    schema=[
        {"name": "col1", "type": "long", "nullable": False, "metadata": {}},
        {"name": "date", "type": "string", "nullable": False, "metadata": {}},
    ],
    primary_keys=["date"],
    partitioning=daily_partition_def,
    write_mode=WarehouseWriteMode.OVERWRITE,
    backfill_batch_size=5,
    dq_checks=[dq1, dq2, dq3, dq4, dq5, dq6, dq7, dq8],
)
def example_sql_dataset_with_dq(context: AssetExecutionContext) -> str:
    start_date = str(list(sorted(partition_keys_from_context(context)[0]))[0]).strip()

    context.log.warning(f"Partition keys inside asset: {partition_keys_from_context(context)}")

    query = (
        f"SELECT * from datamodel_dev.example_sql_dataset where date >= {start_date}"
    )
    return query
