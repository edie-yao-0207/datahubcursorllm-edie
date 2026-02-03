from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StringType, StructField
from pyspark.sql.functions import udf
from samsaradev.infra.dataplatform.spark_helpers import df2dicts, assert_df_equal
import databricks_cost_with_serverless_datapipelines
import pytest
from unittest.mock import MagicMock, patch
import os
import json


def normalize_json_strings(df: DataFrame):
    """Normalize JSON strings by parsing and re-serializing them"""

    @udf(returnType=StringType())
    def normalize_json(col):
        if col:
            return json.dumps(json.loads(col))
        return col

    return df.withColumn(
        "clustercustomtags", normalize_json("clustercustomtags")
    ).withColumn("tags", normalize_json("tags"))


@patch(
    "databricks_cost_with_serverless_datapipelines.parse_tags",
    return_value={"node1": '{"newtag":"newtag1"}', "node2": '{"newtag":"newtag2"}'},
)
def test_databricks_cost_with_serverless_datapipelines(
    mock_parse_tags, spark: SparkSession
):

    spark.sql(f"CREATE DATABASE if not exists billing")

    # Create a test dataframe
    test_data = [
        # No tags with jobid and runname should have correct tags
        (
            "1",
            '{"Creator":"94266402-8dc8-44d7-8a4c-1439cb051aac"}',
            '{"Creator":"94266402-8dc8-44d7-8a4c-1439cb051aac","JobId":"123","RunName":"node1"}',
            "100",
        ),
        # No tags with jobid and no runname should get runname from entry #1 and have correct tags
        (
            "1",
            '{"Creator":"94266402-8dc8-44d7-8a4c-1439cb051aac"}',
            '{"Creator":"94266402-8dc8-44d7-8a4c-1439cb051aac","JobId":"123"}',
            "200",
        ),
        # Non datapipeline lambda user job is not in scope to update tags
        (
            "2",
            '{"Creator":"non-datapipeline-lambda"}',
            '{"Creator":"non-datapipeline-lambda","JobId":"456","RunName":"node2"}',
            "150",
        ),
        # No tags with jobid and runname should have correct tags for different node
        (
            "2",
            '{"Creator":"94266402-8dc8-44d7-8a4c-1439cb051aac"}',
            '{"Creator":"94266402-8dc8-44d7-8a4c-1439cb051aac","JobId":"789","RunName":"node2"}',
            "100",
        ),
        # Entry with samsara:dataplatform-job-type:data_pipelines_sql_transformation is not in scope to update tags
        (
            "2",
            '{"Creator":"94266402-8dc8-44d7-8a4c-1439cb051aac","samsara:dataplatform-job-type:data_pipelines_sql_transformation": "datapipeline-sql-transformation"}',
            '{"Creator":"94266402-8dc8-44d7-8a4c-1439cb051aac","JobId":"101112","RunName":"node2"}',
            "100",
        ),
        # Entry with samsara:pooled-job:dataplatform-job-type is not in scope to update tags
        (
            "2",
            '{"Creator":"94266402-8dc8-44d7-8a4c-1439cb051aac", "samsara:pooled-job:dataplatform-job-type": "datapipeline-sql-transformation"}',
            '{"Creator":"94266402-8dc8-44d7-8a4c-1439cb051aac","JobId":"121415","RunName":"node2"}',
            "100",
        ),
    ]
    test_schema = StructType(
        [
            StructField("workspaceid", StringType(), True),
            StructField("clustercustomtags", StringType(), True),
            StructField("tags", StringType(), True),
            StructField("dbus", StringType(), True),
        ]
    )
    test_df = spark.createDataFrame(test_data, test_schema)
    test_df.write.mode("overwrite").saveAsTable("billing.databricks")

    databricks_cost_with_serverless_datapipelines.main_cost_with_serverless_datapipelines(
        spark
    )

    actual_df = spark.sql(
        "SELECT * FROM billing.databricks_cost_with_serverless_datapipelines"
    )

    expected_df = spark.createDataFrame(
        [
            (
                "1",
                '{"Creator": "94266402-8dc8-44d7-8a4c-1439cb051aac", "newtag": "newtag1", "UpdatedTags": "true"}',
                '{"Creator":"94266402-8dc8-44d7-8a4c-1439cb051aac","JobId":"123","RunName":"node1"}',
                "100",
            ),
            (
                "1",
                '{"Creator": "94266402-8dc8-44d7-8a4c-1439cb051aac", "newtag": "newtag1", "UpdatedTags": "true"}',
                '{"Creator":"94266402-8dc8-44d7-8a4c-1439cb051aac","JobId":"123"}',
                "200",
            ),
            (
                "2",
                '{"Creator": "94266402-8dc8-44d7-8a4c-1439cb051aac", "newtag": "newtag2", "UpdatedTags": "true"}',
                '{"Creator":"94266402-8dc8-44d7-8a4c-1439cb051aac","JobId":"789","RunName":"node2"}',
                "100",
            ),
            (
                "2",
                '{"Creator": "94266402-8dc8-44d7-8a4c-1439cb051aac", "samsara:dataplatform-job-type:data_pipelines_sql_transformation": "datapipeline-sql-transformation", "newtag": "newtag2", "UpdatedTags": "true"}',
                '{"Creator":"94266402-8dc8-44d7-8a4c-1439cb051aac","JobId":"101112","RunName":"node2"}',
                "100",
            ),
            (
                "2",
                '{"Creator": "94266402-8dc8-44d7-8a4c-1439cb051aac", "samsara:pooled-job:dataplatform-job-type": "datapipeline-sql-transformation"}',
                '{"Creator":"94266402-8dc8-44d7-8a4c-1439cb051aac","JobId":"121415","RunName":"node2"}',
                "100",
            ),
            (
                "2",
                '{"Creator": "non-datapipeline-lambda"}',
                '{"Creator":"non-datapipeline-lambda","JobId":"456","RunName":"node2"}',
                "150",
            ),
        ],
        test_schema,
    )

    assert_df_equal(
        normalize_json_strings(actual_df), normalize_json_strings(expected_df)
    )
