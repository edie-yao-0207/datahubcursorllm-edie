from datetime import datetime

from dagster import AssetIn, AssetOut, DailyPartitionsDefinition, asset
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ...common.utils import AWSRegions, Database

DATABASE = Database.DATAMODEL_DEV


@asset(
    required_resource_keys={"databricks_pyspark_step_launcher"},
    key_prefix=[AWSRegions.US_WEST_2.value, DATABASE],
    metadata={
        "database": DATABASE,
        "schema": [
            {"name": "org_id", "type": "long", "nullable": True, "metadata": {}},
            {"name": "date", "type": "string", "nullable": True, "metadata": {}},
            {"name": "value", "type": "string", "nullable": True, "metadata": {}},
        ],
    },
    partitions_def=DailyPartitionsDefinition(
        start_date=datetime(2023, 1, 1), end_date=datetime(2023, 1, 4)
    ),
)
def datamodel_asset_1() -> DataFrame:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    df = spark.table("dataplatform_dev.rdsdeltalake_source_1")
    return df


@asset(
    required_resource_keys={"databricks_pyspark_step_launcher"},
    key_prefix=[AWSRegions.US_WEST_2.value, DATABASE],
    metadata={
        "database": DATABASE,
        "schema": [
            {"name": "org_id", "type": "long", "nullable": True, "metadata": {}},
            {"name": "date", "type": "string", "nullable": True, "metadata": {}},
            {"name": "value2", "type": "string", "nullable": True, "metadata": {}},
        ],
    },
    partitions_def=DailyPartitionsDefinition(
        start_date=datetime(2023, 1, 1), end_date=datetime(2023, 1, 4)
    ),
)
def datamodel_asset_2() -> DataFrame:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    df = spark.table("dataplatform_dev.rdsdeltalake_source_2")
    return df


@asset(
    required_resource_keys={"databricks_pyspark_step_launcher"},
    key_prefix=["us-west-2", DATABASE],
    metadata={
        "database": DATABASE,
        "description": "test table description",
        "schema": [
            {
                "name": "date",
                "type": "string",
                "nullable": True,
                "metadata": {"comment": "date column description"},
            },
            {
                "name": "org_id",
                "type": "long",
                "nullable": True,
                "metadata": {"comment": "org_id column description"},
            },
            {
                "name": "value",
                "type": "string",
                "nullable": True,
                "metadata": {"comment": "value column description"},
            },
            {
                "name": "value2",
                "type": "string",
                "nullable": True,
                "metadata": {"comment": "value2 column descripton"},
            },
            {
                "name": "value3",
                "type": {
                    "type": "struct",
                    "fields": [
                        {
                            "name": "nested_string",
                            "type": "string",
                            "nullable": False,
                            "metadata": {"comment": "nested struct column description"},
                        },
                        {
                            "name": "nested_array",
                            "type": {
                                "type": "array",
                                "elementType": "string",
                                "containsNull": True,
                            },
                            "nullable": True,
                            "metadata": {"comment": "nested array column description"},
                        },
                    ],
                },
                "nullable": True,
                "metadata": {"comment": "value3 column description"},
            },
            {
                "name": "value4",
                "type": {
                    "type": "struct",
                    "fields": [
                        {
                            "name": "nested_string",
                            "type": "string",
                            "nullable": True,
                            "metadata": {"comment": "nested struct column description"},
                        },
                        {
                            "name": "nested_struct",
                            "type": {
                                "type": "struct",
                                "fields": [
                                    {
                                        "name": "nested_long",
                                        "type": "long",
                                        "nullable": False,
                                        "metadata": {
                                            "comment": "long type within a nested struct"
                                        },
                                    }
                                ],
                            },
                            "nullable": True,
                            "metadata": {"comment": "nested array column description"},
                        },
                    ],
                },
                "nullable": True,
                "metadata": {"comment": "value3 column description"},
            },
            {
                "name": "value5",
                "type": {
                    "type": "array",
                    "elementType": "string",
                    "containsNull": False,
                },
                "nullable": True,
                "metadata": {"comment": "nested array column description"},
            },
        ],
        "write_mode": "merge",
        "primary_keys": ["date", "org_id"],
    },
    partitions_def=DailyPartitionsDefinition(
        start_date=datetime(2023, 1, 1), end_date=datetime(2023, 1, 4)
    ),
)
def datamodel_asset_3a(
    datamodel_asset_1: DataFrame, datamodel_asset_2: DataFrame
) -> DataFrame:
    df = datamodel_asset_1.join(datamodel_asset_2, how="inner", on=["date", "org_id"])
    df = df.withColumn(
        "value3",
        F.struct(
            F.lit("string_value").alias("nested_string"),
            F.array(F.lit("string_value"), F.lit(None)).alias("nested_array"),
        ),
    )
    df = df.withColumn(
        "value4",
        F.struct(
            F.struct(F.lit(1).astype("long").alias("nested_long")).alias(
                "nested_struct"
            ),
            F.lit("string_value").alias("nested_string"),
        ),
    )
    df = df.withColumn("value5", F.array(F.lit("string_value")))
    return df
