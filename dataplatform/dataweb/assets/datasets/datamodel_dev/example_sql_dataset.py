from dagster import AssetExecutionContext, AssetKey, DailyPartitionsDefinition
from dataweb import NonEmptyDQCheck, PrimaryKeyDQCheck, SQLDQCheck, NonNegativeDQCheck, ValueRangeDQCheck, NonNullDQCheck, table
from dataweb.userpkgs.constants import AWSRegion, Database, DATAENGINEERING, SLACK_ALERTS_CHANNEL_DATA_ENGINEERING, WarehouseWriteMode
from dataweb.userpkgs.utils import partition_keys_from_context

daily_partition_def = DailyPartitionsDefinition(start_date="2024-06-01")


@table(
    database=Database.DATAMODEL_DEV,
    description="A dataset that is produced by from a SQL query.",
    regions=[AWSRegion.US_WEST_2],
    owners=[DATAENGINEERING],
    schema=[
        {"name": "col1", "type": "long", "nullable": False, "metadata": {}},
        {"name": "date", "type": "string", "nullable": False, "metadata": {}},
    ],
    upstreams=[AssetKey([AWSRegion.US_WEST_2, Database.DATAMODEL_DEV, "example_pyspark_dataset"])],
    primary_keys=["date"],
    partitioning=daily_partition_def,
    write_mode=WarehouseWriteMode.OVERWRITE,
    backfill_batch_size=5,
    notifications=[f"slack:{SLACK_ALERTS_CHANNEL_DATA_ENGINEERING}"],
    pypi_libraries=["mlflow==3.1.3"],
)
def example_sql_dataset(context: AssetExecutionContext) -> str:
    context.log.info("LOGGING FROM WITHIN ASSET FUNC")

    start_date = str(list(sorted(partition_keys_from_context(context)[0]))[0]).strip()

    query = f"(SELECT CAST(1 as BIGINT) as col1, {start_date} as date)"

    partition_keys = context.partition_keys

    if len(partition_keys) > 1:
        for partition_key in partition_keys[1:]:
            query += f" UNION ALL (SELECT CAST(1 as BIGINT) as col1, '{partition_key}' as date) "

    context.log.info(f"Query: {query}")
    return query
