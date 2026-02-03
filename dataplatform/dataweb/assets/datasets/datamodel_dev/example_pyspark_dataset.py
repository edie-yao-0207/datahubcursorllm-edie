from dagster import AssetExecutionContext, DailyPartitionsDefinition
from dataweb import table
from dataweb.userpkgs.constants import AWSRegion, Database, DATAENGINEERING, WarehouseWriteMode
from pyspark.sql import DataFrame, SparkSession


@table(
    database=Database.DATAMODEL_DEV,
    description="A dataset that is produced by from a 'pyspark' DataFrame.",
    regions=[AWSRegion.US_WEST_2],
    owners=[DATAENGINEERING],
    schema=[
        {"name": "org_id", "type": "integer", "nullable": True, "metadata": {}},
        {"name": "date", "type": "date", "nullable": True, "metadata": {}},
        {"name": "value", "type": "string", "nullable": True, "metadata": {}},
    ],
    primary_keys=["date"],
    partitioning=DailyPartitionsDefinition(start_date="2024-06-01"),
    write_mode=WarehouseWriteMode.OVERWRITE,
    backfill_batch_size=5,
)
def example_pyspark_dataset(context: AssetExecutionContext) -> DataFrame:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()

    context.log.info("dataweb reference working dbx")
    df = spark.table("datamodel_dev.rdsdeltalake_source_region_test_partitioned")
    return df
