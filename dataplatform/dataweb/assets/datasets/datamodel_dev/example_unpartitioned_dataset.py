from dagster import AssetExecutionContext, AssetKey, DailyPartitionsDefinition
from dataweb import NonEmptyDQCheck, PrimaryKeyDQCheck, SQLDQCheck, NonNegativeDQCheck, ValueRangeDQCheck, NonNullDQCheck, table
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


@table(
    database=Database.DATAMODEL_DEV,
    description="A dataset that is produced by from a SQL query.",
    regions=[AWSRegion.US_WEST_2],
    owners=[DATAENGINEERING],
    schema=[{'name': 'date', 'type': 'date', 'nullable': False, 'metadata': {}}],
    primary_keys=["date"],
    partitioning=None,
    write_mode=WarehouseWriteMode.OVERWRITE,
    dq_checks=[NonEmptyDQCheck(name="dq_empty_example_unpartitioned_dataset")],
)
def example_unpartitioned_dataset(context: AssetExecutionContext) -> str:

    query = "SELECT CURRENT_DATE() as date"

    return query
