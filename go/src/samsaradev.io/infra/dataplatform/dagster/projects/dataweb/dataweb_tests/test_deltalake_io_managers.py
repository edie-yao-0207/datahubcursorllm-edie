# from sre_constants import ANY
import json
import pathlib
from typing import Any, Dict, List, Union
from unittest.mock import PropertyMock

import mock
import pytest
from dagster import (
    DailyPartitionsDefinition,
    MonthlyPartitionsDefinition,
    MultiPartitionsDefinition,
    StaticPartitionsDefinition,
    build_output_context,
)
from dataweb._core.resources.io_managers.deltalake_io_managers import (
    DeltaTableIOManager,
)
from pyspark.sql import DataFrame, Row, SparkSession

TEST_DATA_DIR = pathlib.Path(__file__).parent / "testfiles"
TEST_DATA_FILE = TEST_DATA_DIR / "test_tables_data.json"

SCHEMA_1 = [
    {"name": "date", "type": "string"},
    {"name": "org_id", "type": "long"},
    {"name": "region", "type": "string"},
]

SCHEMA_2 = [
    {"name": "date", "type": "string"},
    {"name": "org_id", "type": "long"},
    {"name": "country", "type": "string"},
]

SCHEMA_3 = [
    {"name": "date_month", "type": "string"},
    {"name": "org_id", "type": "long"},
    {"name": "region", "type": "string"},
]

SCHEMA_4 = [
    {"name": "date_month", "type": "string"},
    {"name": "org_id", "type": "long"},
    {"name": "country", "type": "string"},
]


@pytest.fixture
def load_test_tables(spark_session: SparkSession):
    test_data = load_test_data()
    for db_table_name, data in test_data.items():
        create_catalog_table(spark_session, db_table_name, "delta", [], data)
    yield None
    for db_table_name, data in test_data.items():
        clean_catalog_table(spark_session, db_table_name)


def get_table_partition_cols(spark: SparkSession, db_table_name: str):
    tbl_details = spark.sql(f"DESCRIBE DETAIL {db_table_name}")
    partition_cols = tbl_details[["partitionColumns"]].first()[0]
    return partition_cols


def create_catalog_table(
    spark_session,
    db_table_name: str,
    format: str,
    partitions: List[str],
    test_data: List[Dict[str, Any]],
):
    db_name = db_table_name.split(".")[0]
    partitions = partitions or []
    spark_session.sql(f"CREATE DATABASE IF NOT EXISTS {db_name}")
    rows = map(lambda x: Row(**x), test_data)
    df = spark_session.createDataFrame(rows, samplingRatio=1.0)
    df.write.format(format).mode("overwrite").option(
        "overwriteSchema", "true"
    ).partitionBy(partitions).saveAsTable(db_table_name)


def clean_catalog_table(spark: SparkSession, db_table_name: str):
    spark.sql(f"DROP TABLE {db_table_name}")


def load_test_data():
    test_data_text = TEST_DATA_FILE.read_text()
    test_data = json.loads(test_data_text)
    return test_data


def assert_df_equal(df1: DataFrame, df2: DataFrame):
    df1 = df1.select(*sorted(df1.schema.fieldNames())).sort(
        *sorted(df1.schema.fieldNames())
    )
    df2 = df2.select(*sorted(df2.schema.fieldNames())).sort(
        *sorted(df2.schema.fieldNames())
    )
    collected_df1 = df1.collect()
    collected_df2 = df2.collect()
    assert (
        collected_df1 == collected_df2
    ), f"\ndf1: {collected_df1} \ndf2: {collected_df2}"


@pytest.mark.parametrize(
    "step_key,partition_keys,partitions_def,metadata,incoming_data,expected_data,expected_partitioning",
    [
        pytest.param(
            "us__datamodel_dev__t1",
            [],
            None,
            {"write_mode": "overwrite", "schema": SCHEMA_1},
            [
                {"org_id": 1, "date": "2024-01-01", "region": "us-west-2"},
            ],
            [
                {"org_id": 1, "date": "2024-01-01", "region": "us-west-2"},
            ],
            [],
            id="unpartitioned_overwrite",
        ),
        pytest.param(
            "us__datamodel_dev__t1",
            [],
            None,
            {
                "write_mode": "merge",
                "schema": SCHEMA_1,
                "primary_keys": ["date", "org_id"],
            },
            [
                {"org_id": 100, "date": "2024-01-03", "region": "us-west-2"},
            ],
            [
                {"org_id": 1, "date": "2024-01-01", "region": "us-west-2"},
                {"org_id": 2, "date": "2024-01-01", "region": "us-west-2"},
                {"org_id": 3, "date": "2024-01-01", "region": "eu-west-1"},
                {"org_id": 4, "date": "2024-01-01", "region": "eu-west-1"},
                {"org_id": 1, "date": "2024-01-02", "region": "us-west-2"},
                {"org_id": 2, "date": "2024-01-02", "region": "us-west-2"},
                {"org_id": 3, "date": "2024-01-02", "region": "eu-west-1"},
                {"org_id": 4, "date": "2024-01-02", "region": "eu-west-1"},
                {"org_id": 1, "date": "2024-01-03", "region": "us-west-2"},
                {"org_id": 2, "date": "2024-01-03", "region": "us-west-2"},
                {"org_id": 3, "date": "2024-01-03", "region": "eu-west-1"},
                {"org_id": 100, "date": "2024-01-03", "region": "us-west-2"},
            ],
            [],
            id="unpartitioned_merge",
        ),
        pytest.param(
            "us__datamodel_dev__t1",
            [
                "2024-01-01",
                "2024-01-02",
            ],
            DailyPartitionsDefinition(start_date="2024-01-01"),
            {"write_mode": "overwrite", "schema": SCHEMA_1},
            [
                {"org_id": 1, "date": "2024-01-01", "region": "eu-west-1"},
                {"org_id": 1, "date": "2024-01-02", "region": "eu-west-1"},
                {"org_id": 2, "date": "2024-01-05", "region": "eu-west-1"},
            ],
            [
                {"org_id": 1, "date": "2024-01-01", "region": "eu-west-1"},
                {"org_id": 1, "date": "2024-01-02", "region": "eu-west-1"},
                {"org_id": 1, "date": "2024-01-03", "region": "us-west-2"},
                {"org_id": 2, "date": "2024-01-03", "region": "us-west-2"},
                {"org_id": 3, "date": "2024-01-03", "region": "eu-west-1"},
            ],
            ["date"],
            id="single_daily_partitioned_overwrite",
        ),
        pytest.param(
            "us__datamodel_dev__t1",
            [
                "2024-01-01",
                "2024-01-02",
            ],
            DailyPartitionsDefinition(start_date="2024-01-01"),
            {
                "write_mode": "merge",
                "schema": SCHEMA_1,
                "primary_keys": ["date", "org_id"],
            },
            [
                {"org_id": 100, "date": "2024-01-01", "region": "eu-west-1"},
                {"org_id": 100, "date": "2024-01-02", "region": "eu-west-1"},
                {"org_id": 100, "date": "2024-01-05", "region": "eu-west-1"},
            ],
            [
                {"org_id": 100, "date": "2024-01-01", "region": "eu-west-1"},
                {"org_id": 100, "date": "2024-01-02", "region": "eu-west-1"},
                {"org_id": 1, "date": "2024-01-01", "region": "us-west-2"},
                {"org_id": 2, "date": "2024-01-01", "region": "us-west-2"},
                {"org_id": 3, "date": "2024-01-01", "region": "eu-west-1"},
                {"org_id": 4, "date": "2024-01-01", "region": "eu-west-1"},
                {"org_id": 1, "date": "2024-01-02", "region": "us-west-2"},
                {"org_id": 2, "date": "2024-01-02", "region": "us-west-2"},
                {"org_id": 3, "date": "2024-01-02", "region": "eu-west-1"},
                {"org_id": 4, "date": "2024-01-02", "region": "eu-west-1"},
                {"org_id": 1, "date": "2024-01-03", "region": "us-west-2"},
                {"org_id": 2, "date": "2024-01-03", "region": "us-west-2"},
                {"org_id": 3, "date": "2024-01-03", "region": "eu-west-1"},
            ],
            ["date"],
            id="single_daily_partitioned_merge",
        ),
        # This test will fail as dagster orders partition key names lexographically
        pytest.param(
            "us__datamodel_dev__t2",
            ["usa|2024-01-01", "usa|2024-01-02"],
            MultiPartitionsDefinition(
                {
                    "country": StaticPartitionsDefinition(["canada", "usa"]),
                    "date": DailyPartitionsDefinition(start_date="2024-01-01"),
                }
            ),
            {"write_mode": "overwrite", "schema": SCHEMA_1},
            [
                {"org_id": 100, "date": "2024-01-01", "country": "usa"},
                {"org_id": 100, "date": "2024-01-01", "country": "canada"},
                {"org_id": 3, "date": "2024-01-02", "country": "usa"},
                {"org_id": 4, "date": "2024-01-02", "country": "canada"},
            ],
            [
                {"org_id": 100, "date": "2024-01-01", "country": "usa"},
                {"org_id": 100, "date": "2024-01-01", "country": "canada"},
                {"org_id": 3, "date": "2024-01-02", "country": "usa"},
                {"org_id": 4, "date": "2024-01-02", "country": "canada"},
                {"org_id": 1, "date": "2024-01-03", "region": "us-west-2"},
                {"org_id": 2, "date": "2024-01-03", "region": "us-west-2"},
                {"org_id": 3, "date": "2024-01-03", "region": "eu-west-1"},
            ],
            ["country", "date"],
            id="multi_static_daily_partitioned_overwrite",
            marks=pytest.mark.xfail(strict=True),
        ),
        # This test will fail as dagster orders partition key names lexographically
        pytest.param(
            "us__datamodel_dev__t2",
            [
                "canada|2024-01-01",
                "usa|2024-01-01",
                "canada|2024-01-02",
                "usa|2024-01-02",
            ],
            MultiPartitionsDefinition(
                {
                    "country": StaticPartitionsDefinition(["canada", "usa"]),
                    "date": DailyPartitionsDefinition(start_date="2024-01-01"),
                }
            ),
            {
                "write_mode": "merge",
                "schema": SCHEMA_1,
                "primary_keys": ["date", "org_id"],
            },
            [
                {"org_id": 100, "date": "2024-01-01", "country": "usa"},
                {"org_id": 100, "date": "2024-01-01", "country": "canada"},
                {"org_id": 2, "date": "2024-01-02", "country": "canada"},
                {"org_id": 4, "date": "2024-01-02", "country": "canada"},
            ],
            [
                {"org_id": 1, "date": "2024-01-01", "country": "usa"},
                {"org_id": 100, "date": "2024-01-01", "country": "usa"},
                {"org_id": 100, "date": "2024-01-01", "country": "canada"},
                {"org_id": 2, "date": "2024-01-01", "country": "usa"},
                {"org_id": 3, "date": "2024-01-01", "country": "canada"},
                {"org_id": 4, "date": "2024-01-01", "country": "canada"},
                {"org_id": 1, "date": "2024-01-02", "country": "usa"},
                {"org_id": 2, "date": "2024-01-02", "country": "usa"},
                {"org_id": 2, "date": "2024-01-02", "country": "canada"},
                {"org_id": 3, "date": "2024-01-02", "country": "canada"},
                {"org_id": 3, "date": "2024-01-02", "country": "usa"},
                {"org_id": 4, "date": "2024-01-02", "country": "canada"},
            ],
            ["country", "date"],
            id="multi_static_daily_partitioned_merge",
            marks=pytest.mark.xfail(strict=True),
        ),
        pytest.param(
            "us__datamodel_dev__t1",
            [
                "2024-01-01|eu-west-1",
                "2024-01-01|us-west-2",
                "2024-01-02|eu-west-1",
                "2024-01-02|us-west-2",
            ],
            MultiPartitionsDefinition(
                {
                    "date": DailyPartitionsDefinition(start_date="2024-01-01"),
                    "region": StaticPartitionsDefinition(["eu-west-1", "us-west-2"]),
                }
            ),
            {"write_mode": "overwrite", "schema": SCHEMA_1},
            [
                {"org_id": 1, "date": "2024-01-01", "region": "eu-west-1"},
                {"org_id": 2, "date": "2024-01-02", "region": "eu-west-1"},
                {"org_id": 1, "date": "2024-01-02", "region": "eu-west-1"},
                {"org_id": 1, "date": "2024-01-02", "region": "us-west-2"},
                {"org_id": 2, "date": "2024-01-02", "region": "us-west-2"},
            ],
            [
                {"org_id": 1, "date": "2024-01-01", "region": "eu-west-1"},
                {"org_id": 2, "date": "2024-01-02", "region": "eu-west-1"},
                {"org_id": 1, "date": "2024-01-02", "region": "eu-west-1"},
                {"org_id": 1, "date": "2024-01-02", "region": "us-west-2"},
                {"org_id": 2, "date": "2024-01-02", "region": "us-west-2"},
                {"org_id": 1, "date": "2024-01-03", "region": "us-west-2"},
                {"org_id": 2, "date": "2024-01-03", "region": "us-west-2"},
                {"org_id": 3, "date": "2024-01-03", "region": "eu-west-1"},
            ],
            ["date", "region"],
            id="multi_daily_static_partitioned_overwrite",
        ),
        pytest.param(
            "us__datamodel_dev__t1",
            [
                "2024-01-01|eu-west-1",
                "2024-01-01|us-west-2",
                "2024-01-02|eu-west-1",
                "2024-01-02|us-west-2",
            ],
            MultiPartitionsDefinition(
                {
                    "date": DailyPartitionsDefinition(start_date="2024-01-01"),
                    "region": StaticPartitionsDefinition(["eu-west-1", "us-west-2"]),
                }
            ),
            {
                "write_mode": "merge",
                "schema": SCHEMA_1,
                "primary_keys": ["date", "org_id"],
            },
            [
                {"org_id": 1, "date": "2024-01-01", "region": "eu-west-1"},
                {"org_id": 2, "date": "2024-01-02", "region": "eu-west-1"},
                {"org_id": 1, "date": "2024-01-02", "region": "eu-west-1"},
                {"org_id": 100, "date": "2024-01-02", "region": "us-west-2"},
            ],
            [
                {"org_id": 1, "date": "2024-01-01", "region": "eu-west-1"},
                {"org_id": 2, "date": "2024-01-01", "region": "us-west-2"},
                {"org_id": 3, "date": "2024-01-01", "region": "eu-west-1"},
                {"org_id": 4, "date": "2024-01-01", "region": "eu-west-1"},
                {"org_id": 1, "date": "2024-01-02", "region": "eu-west-1"},
                {"org_id": 2, "date": "2024-01-02", "region": "eu-west-1"},
                {"org_id": 3, "date": "2024-01-02", "region": "eu-west-1"},
                {"org_id": 4, "date": "2024-01-02", "region": "eu-west-1"},
                {"org_id": 100, "date": "2024-01-02", "region": "us-west-2"},
                {"org_id": 1, "date": "2024-01-03", "region": "us-west-2"},
                {"org_id": 2, "date": "2024-01-03", "region": "us-west-2"},
                {"org_id": 3, "date": "2024-01-03", "region": "eu-west-1"},
            ],
            ["date", "region"],
            id="multi_daily_static_partitioned_merge",
        ),
        # This test will fail as a date partition is required
        pytest.param(
            "us__datamodel_dev__t2",
            ["canada", "usa"],
            StaticPartitionsDefinition(["canada", "usa"]),
            {"write_mode": "overwrite", "schema": SCHEMA_2},
            [
                {"org_id": 4, "date": "2024-01-02", "country": "canada"},
                {"org_id": 4, "date": "2024-01-02", "country": "usa"},
            ],
            [
                {"org_id": 4, "date": "2024-01-02", "country": "canada"},
                {"org_id": 4, "date": "2024-01-02", "country": "usa"},
            ],
            ["country"],
            id="single_static_partitioned_overwrite",
            marks=pytest.mark.xfail(strict=True),
        ),
        # This test will fail as a date partition is required
        pytest.param(
            "us__datamodel_dev__t2",
            ["canada", "usa"],
            StaticPartitionsDefinition(["canada", "usa"]),
            {
                "write_mode": "merge",
                "schema": SCHEMA_2,
                "primary_keys": ["date", "org_id"],
            },
            [
                {"org_id": 4, "date": "2024-01-02", "country": "canada"},
                {"org_id": 4, "date": "2024-01-02", "country": "usa"},
            ],
            [
                {"org_id": 1, "date": "2024-01-01", "country": "usa"},
                {"org_id": 2, "date": "2024-01-01", "country": "usa"},
                {"org_id": 3, "date": "2024-01-01", "country": "canada"},
                {"org_id": 4, "date": "2024-01-01", "country": "canada"},
                {"org_id": 1, "date": "2024-01-02", "country": "usa"},
                {"org_id": 2, "date": "2024-01-02", "country": "usa"},
                {"org_id": 3, "date": "2024-01-02", "country": "canada"},
                {"org_id": 4, "date": "2024-01-02", "country": "canada"},
                {"org_id": 4, "date": "2024-01-02", "country": "canada"},
                {"org_id": 4, "date": "2024-01-02", "country": "usa"},
            ],
            ["country"],
            id="single_static_partitioned_merge",
            marks=pytest.mark.xfail(strict=True),
        ),
        pytest.param(
            "us__datamodel_dev__t5",
            ["2024-01", "2024-02"],
            MonthlyPartitionsDefinition(
                start_date="2024-01", timezone="UTC", fmt="%Y-%m"
            ),
            {
                "write_mode": "merge",
                "schema": SCHEMA_3,
                "primary_keys": ["date_month", "org_id"],
            },
            [
                {"org_id": 1, "date_month": "2024-01", "region": "eu-west-1"},
                {"org_id": 2, "date_month": "2024-01", "region": "us-west-2"},
                {"org_id": 100, "date_month": "2024-01", "region": "eu-west-1"},
            ],
            [
                {"org_id": 1, "date_month": "2024-01", "region": "eu-west-1"},
                {"org_id": 2, "date_month": "2024-01", "region": "us-west-2"},
                {"org_id": 3, "date_month": "2024-01", "region": "eu-west-1"},
                {"org_id": 4, "date_month": "2024-01", "region": "eu-west-1"},
                {"org_id": 100, "date_month": "2024-01", "region": "eu-west-1"},
                {"org_id": 1, "date_month": "2024-02", "region": "us-west-2"},
                {"org_id": 2, "date_month": "2024-02", "region": "us-west-2"},
                {"org_id": 3, "date_month": "2024-02", "region": "eu-west-1"},
                {"org_id": 4, "date_month": "2024-02", "region": "eu-west-1"},
            ],
            ["date_month"],
            id="single_monthly_partitioned_merge",
        ),
        pytest.param(
            "us__datamodel_dev__t5",
            ["2024-01", "2024-02"],
            MonthlyPartitionsDefinition(
                start_date="2024-01", timezone="UTC", fmt="%Y-%m"
            ),
            {
                "write_mode": "overwrite",
                "schema": SCHEMA_3,
                "primary_keys": ["date_month", "org_id"],
            },
            [
                {"org_id": 1, "date_month": "2024-01", "region": "eu-west-1"},
                {"org_id": 2, "date_month": "2024-01", "region": "us-west-2"},
                {"org_id": 100, "date_month": "2024-01", "region": "eu-west-1"},
                {"org_id": 100, "date_month": "2024-02", "region": "eu-west-1"},
            ],
            [
                {"org_id": 1, "date_month": "2024-01", "region": "eu-west-1"},
                {"org_id": 2, "date_month": "2024-01", "region": "us-west-2"},
                {"org_id": 100, "date_month": "2024-01", "region": "eu-west-1"},
                {"org_id": 100, "date_month": "2024-02", "region": "eu-west-1"},
            ],
            ["date_month"],
            id="single_monthly_partitioned_overwrite",
        ),
        pytest.param(
            "us__datamodel_dev__t5",
            [
                "2024-01|eu-west-1",
                "2024-01|us-west-2",
                "2024-02|eu-west-1",
                "2024-02|us-west-2",
            ],
            MultiPartitionsDefinition(
                {
                    "date_month": MonthlyPartitionsDefinition(
                        start_date="2024-01", timezone="UTC", fmt="%Y-%m"
                    ),
                    "region": StaticPartitionsDefinition(["eu-west-1", "us-west-2"]),
                }
            ),
            {
                "write_mode": "merge",
                "schema": SCHEMA_3,
                "primary_keys": ["date_month", "org_id"],
            },
            [
                {"org_id": 1, "date_month": "2024-01", "region": "eu-west-1"},
                {"org_id": 2, "date_month": "2024-01", "region": "us-west-2"},
                {"org_id": 100, "date_month": "2024-01", "region": "eu-west-1"},
                {"org_id": 100, "date_month": "2024-02", "region": "eu-west-1"},
            ],
            [
                {"org_id": 1, "date_month": "2024-01", "region": "eu-west-1"},
                {"org_id": 2, "date_month": "2024-01", "region": "us-west-2"},
                {"org_id": 3, "date_month": "2024-01", "region": "eu-west-1"},
                {"org_id": 4, "date_month": "2024-01", "region": "eu-west-1"},
                {"org_id": 100, "date_month": "2024-01", "region": "eu-west-1"},
                {"org_id": 1, "date_month": "2024-02", "region": "us-west-2"},
                {"org_id": 2, "date_month": "2024-02", "region": "us-west-2"},
                {"org_id": 3, "date_month": "2024-02", "region": "eu-west-1"},
                {"org_id": 4, "date_month": "2024-02", "region": "eu-west-1"},
                {"org_id": 100, "date_month": "2024-02", "region": "eu-west-1"},
            ],
            ["date_month", "region"],
            id="multi_monthly_static_partitioned_merge",
        ),
        pytest.param(
            "us__datamodel_dev__t5",
            [
                "2024-01|eu-west-1",
                "2024-01|us-west-2",
            ],
            MultiPartitionsDefinition(
                {
                    "date_month": MonthlyPartitionsDefinition(
                        start_date="2024-01", timezone="UTC", fmt="%Y-%m"
                    ),
                    "region": StaticPartitionsDefinition(["eu-west-1", "us-west-2"]),
                }
            ),
            {
                "write_mode": "overwrite",
                "schema": SCHEMA_3,
                "primary_keys": ["date_month", "org_id"],
            },
            [
                {"org_id": 1, "date_month": "2024-01", "region": "eu-west-1"},
                {"org_id": 2, "date_month": "2024-01", "region": "us-west-2"},
                {"org_id": 100, "date_month": "2024-01", "region": "eu-west-1"},
            ],
            [
                {"org_id": 1, "date_month": "2024-01", "region": "eu-west-1"},
                {"org_id": 2, "date_month": "2024-01", "region": "us-west-2"},
                {"org_id": 100, "date_month": "2024-01", "region": "eu-west-1"},
                {"org_id": 1, "date_month": "2024-02", "region": "us-west-2"},
                {"org_id": 2, "date_month": "2024-02", "region": "us-west-2"},
                {"org_id": 3, "date_month": "2024-02", "region": "eu-west-1"},
                {"org_id": 4, "date_month": "2024-02", "region": "eu-west-1"},
            ],
            ["date_month", "region"],
            id="multi_monthly_static_partitioned_overwrite",
        ),
        # This test will fail as dagster orders partition key names lexographically
        pytest.param(
            "us__datamodel_dev__t5",
            [
                "2024-01|usa",
                "2024-01|canada",
            ],
            MultiPartitionsDefinition(
                {
                    "country": StaticPartitionsDefinition(["canada", "usa"]),
                    "date_month": MonthlyPartitionsDefinition(
                        start_date="2024-01", timezone="UTC", fmt="%Y-%m"
                    ),
                }
            ),
            {
                "write_mode": "overwrite",
                "schema": SCHEMA_4,
                "primary_keys": ["date_month", "org_id"],
            },
            [
                {"org_id": 100, "date_month": "2024-01", "country": "usa"},
                {"org_id": 100, "date_month": "2024-01", "country": "canada"},
                {"org_id": 3, "date_month": "2024-01", "country": "usa"},
                {"org_id": 4, "date_month": "2024-01", "country": "canada"},
            ],
            [
                {"org_id": 100, "date_month": "2024-01", "country": "usa"},
                {"org_id": 100, "date_month": "2024-01", "country": "canada"},
                {"org_id": 3, "date_month": "2024-01", "country": "usa"},
                {"org_id": 4, "date_month": "2024-01", "country": "canada"},
                {"org_id": 1, "date_month": "2024-02", "country": "usa"},
                {"org_id": 2, "date_month": "2024-02", "country": "usa"},
                {"org_id": 3, "date_month": "2024-02", "country": "canada"},
                {"org_id": 4, "date_month": "2024-02", "country": "canada"},
            ],
            ["date_month", "country"],
            id="multi_static_monthly_partitioned_overwrite",
            marks=pytest.mark.xfail(strict=True),
        ),
    ],
)
@mock.patch(
    "dagster._core.execution.context.output.OutputContext.asset_partition_keys",
    new_callable=PropertyMock,
)
@mock.patch(
    "dagster._core.execution.context.output.OutputContext.asset_partitions_def",
    new_callable=PropertyMock,
)
@mock.patch(
    "dagster._core.execution.context.output.OutputContext.has_asset_partitions",
    new_callable=PropertyMock,
)
def test_deltalake_io_manager_handle_output(
    mock_has_asset_partitions,
    mock_asset_partitions_def,
    mock_asset_partition_keys,
    spark_session,
    step_key: str,
    partition_keys: List[str],
    partitions_def: Union[DailyPartitionsDefinition, MultiPartitionsDefinition],
    metadata: Dict[str, Any],
    incoming_data: List[Dict[str, Any]],
    expected_data: List[Dict[str, Any]],
    expected_partitioning: List[str],
    load_test_tables: None,
):

    # Three key methods we need to mock to test DeltaTableIOManger.handle_output:
    # 1. context.has_asset_partitions
    # 2. context.asset_partitions_def
    # 3. context.asset_partition_keys
    mock_has_asset_partitions.return_value = True if partitions_def else False
    mock_asset_partitions_def.return_value = partitions_def
    mock_asset_partition_keys.return_value = partition_keys

    # Initialize artifacts
    step_key_parts = step_key.split("__")
    database = step_key_parts[-2]
    table = step_key_parts[-1]
    test_table = f"{database}.{table}"
    test_data = load_test_data()[test_table]
    create_catalog_table(
        spark_session=spark_session,
        db_table_name=test_table,
        format="delta",
        partitions=expected_partitioning,
        test_data=test_data,
    )

    output_context = build_output_context(
        step_key=step_key,
        asset_key=["us", database, table],
        name=table,
        metadata=metadata,
    )

    # Run test
    df = spark_session.createDataFrame(incoming_data)
    manager = DeltaTableIOManager()
    manager.handle_output(context=output_context, dataframe=df)

    df_expected = spark_session.createDataFrame(expected_data)
    df_actual = spark_session.table(test_table)

    assert get_table_partition_cols(spark_session, test_table) == expected_partitioning
    assert_df_equal(df_expected, df_actual)
