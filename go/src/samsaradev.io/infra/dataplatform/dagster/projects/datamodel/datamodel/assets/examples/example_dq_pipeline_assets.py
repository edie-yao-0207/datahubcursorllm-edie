from datetime import datetime

from dagster import DailyPartitionsDefinition, asset
from pyspark.sql import DataFrame, SparkSession

from ...common.constants import SLACK_ALERTS_CHANNEL_DATA_ENGINEERING
from ...common.utils import (
    AWSRegions,
    Database,
    DQGroup,
    JoinableDQCheck,
    NonEmptyDQCheck,
    NonNullDQCheck,
    PrimaryKeyDQCheck,
    SQLDQCheck,
    ValueRangeDQCheck,
    WarehouseWriteMode,
    build_asset_from_sql_in_region,
    build_merge_stg_asset,
    log_query_data,
)

# Pipeline Defaults
DATABASE = "dataplatform_dev"

databases = {
    "database_bronze": Database.DATAMODEL_CORE_BRONZE,
    "database_silver": Database.DATAMODEL_CORE_SILVER,
    "database_gold": Database.DATAMODEL_CORE,
}

DQ_TEST_PARTITIONS_DEF = DailyPartitionsDefinition(
    start_date=datetime(2023, 1, 1), end_date=datetime(2023, 1, 4)
)

SLACK_ALERTS_CHANNEL = SLACK_ALERTS_CHANNEL_DATA_ENGINEERING

dqs = DQGroup(
    group_name="dq_test",
    partition_def=DQ_TEST_PARTITIONS_DEF,
    slack_alerts_channel=SLACK_ALERTS_CHANNEL,
    regions=[AWSRegions.US_WEST_2.value],
)

asset_before_dq = build_asset_from_sql_in_region(
    name="asset_before_dq",
    sql_query="select * from dataplatform_dev.rdsdeltalake_source_1",
    primary_keys=["org_id", "date"],
    upstreams=set(),
    group_name="dq_test",
    database=DATABASE,
    databases=databases,
    region=AWSRegions.US_WEST_2.value,
    write_mode=WarehouseWriteMode.overwrite,
    schema=[
        {"name": "org_id", "type": "long", "nullable": True, "metadata": {}},
        {"name": "date", "type": "string", "nullable": True, "metadata": {}},
        {"name": "value", "type": "string", "nullable": True, "metadata": {}},
    ],
)

test_table_with_name = build_asset_from_sql_in_region(
    name="test_table_with_name",
    sql_query="select org_id, concat('name_',org_id) as org_name from dataplatform_dev.rdsdeltalake_source_1",
    primary_keys=[],
    upstreams=set(),
    group_name="dq_test",
    database=DATABASE,
    databases=databases,
    region=AWSRegions.US_WEST_2.value,
    write_mode=WarehouseWriteMode.overwrite,
    schema=[
        {
            "name": "org_id",
            "type": "long",
            "nullable": True,
            "metadata": {"comment": ""},
        },
        {
            "name": "org_name",
            "type": "string",
            "nullable": True,
            "metadata": {"comment": ""},
        },
    ],
)

test_table_with_name_and_missing_key = build_asset_from_sql_in_region(
    name="test_table_with_name_and_missing_key",
    sql_query="""select org_id, concat('name_',org_id) as org_name
        FROM dataplatform_dev.rdsdeltalake_source_1 order by org_id limit 3
        """,
    primary_keys=[],
    upstreams=set(),
    group_name="dq_test",
    database=DATABASE,
    databases=databases,
    region=AWSRegions.US_WEST_2.value,
    write_mode=WarehouseWriteMode.overwrite,
    schema=[
        {
            "name": "org_id",
            "type": "long",
            "nullable": True,
            "metadata": {"comment": ""},
        },
        {
            "name": "org_name",
            "type": "string",
            "nullable": True,
            "metadata": {"comment": ""},
        },
    ],
)

test_table_with_nulls = build_asset_from_sql_in_region(
    name="test_table_with_nulls",
    sql_query="""select date, org_id, null as org_name
        FROM dataplatform_dev.rdsdeltalake_source_1 where org_id = 1
        UNION ALL
        select date, org_id, 'no name' as org_name
        FROM dataplatform_dev.rdsdeltalake_source_1
        """,
    primary_keys=[],
    upstreams=set(),
    group_name="dq_test",
    database=DATABASE,
    databases=databases,
    region=AWSRegions.US_WEST_2.value,
    write_mode=WarehouseWriteMode.overwrite,
    schema=[
        {
            "name": "org_id",
            "type": "long",
            "nullable": True,
            "metadata": {"comment": ""},
        },
        {
            "name": "org_name",
            "type": "string",
            "nullable": True,
            "metadata": {"comment": ""},
        },
        {
            "name": "date",
            "type": "string",
            "nullable": True,
            "metadata": {"comment": ""},
        },
    ],
)


test_table_with_invalid_values = build_asset_from_sql_in_region(
    name="test_table_with_invalid_values",
    sql_query="""select date, org_id, org_id as org_id_plus_20
        FROM dataplatform_dev.rdsdeltalake_source_1
        UNION ALL
        select date, org_id, org_id + 20 as org_id_plus_20
        FROM dataplatform_dev.rdsdeltalake_source_1
        """,
    primary_keys=[],
    upstreams=set(),
    group_name="dq_test",
    database=DATABASE,
    databases=databases,
    region=AWSRegions.US_WEST_2.value,
    write_mode=WarehouseWriteMode.overwrite,
    schema=[
        {"name": "org_id", "type": "long", "nullable": True, "metadata": {}},
        {"name": "date", "type": "string", "nullable": True, "metadata": {}},
        {"name": "org_id_plus_20", "type": "long", "nullable": True, "metadata": {}},
    ],
)

test_table_with_valid_values = build_asset_from_sql_in_region(
    name="test_table_with_valid_values",
    sql_query="""select date, org_id
        FROM dataplatform_dev.rdsdeltalake_source_1
        """,
    primary_keys=[],
    upstreams=set(["test_table_with_invalid_values"]),
    group_name="dq_test",
    database=DATABASE,
    databases=databases,
    region=AWSRegions.US_WEST_2.value,
    write_mode=WarehouseWriteMode.overwrite,
    schema=[
        {"name": "org_id", "type": "long", "nullable": True, "metadata": {}},
        {"name": "date", "type": "string", "nullable": True, "metadata": {}},
        {"name": "value", "type": "string", "nullable": True, "metadata": {}},
    ],
)

# TODO - refactor to use a class instead of dict(list)
dqs["test_table_with_invalid_values"].append(
    ValueRangeDQCheck(
        name="test_table_with_invalid_values_blocking_value_check",
        database=DATABASE,
        table="test_table_with_invalid_values",
        column_range_map={"org_id": (0, 25), "org_id_plus_20": (5, 20)},
        blocking=True,
    )
)

dqs["test_table_with_valid_values"].append(
    ValueRangeDQCheck(
        name="test_table_with_valid_values_blocking_value_check",
        database=DATABASE,
        table="test_table_with_valid_values",
        column_range_map={"org_id": (0, 25)},
        blocking=True,
    )
)


test_table_with_primary_key_violations = build_asset_from_sql_in_region(
    name="test_table_with_primary_key_violations",
    sql_query="""
        with t as (select date, org_id
        FROM dataplatform_dev.rdsdeltalake_source_1)
        select * from t
        UNION ALL
        select * from t
        """,
    primary_keys=["date", "org_id"],
    upstreams=set(),
    group_name="dq_test",
    database=DATABASE,
    databases=databases,
    region=AWSRegions.US_WEST_2.value,
    write_mode=WarehouseWriteMode.overwrite,
    schema=[
        {"name": "date", "type": "string", "nullable": True, "metadata": {}},
        {"name": "org_id", "type": "long", "nullable": True, "metadata": {}},
    ],
)

dqs["test_table_with_primary_key_violations"].append(
    PrimaryKeyDQCheck(
        name="primary_key_failed_blocking_check",
        database=DATABASE,
        table="test_table_with_primary_key_violations",
        primary_keys=["date", "org_id"],
        blocking=True,
    )
)


@asset(
    required_resource_keys={"databricks_pyspark_step_launcher"},
    key_prefix=[AWSRegions.US_WEST_2.value, DATABASE],
    metadata={
        "database": DATABASE,
        "write_mode": "overwrite",
        "schema": [
            {"name": "date", "type": "string", "nullable": False, "metadata": {}},
            {"name": "org_id", "type": "long", "nullable": True, "metadata": {}},
            {"name": "event_date", "type": "string", "nullable": True, "metadata": {}},
            {"name": "org_plus_2", "type": "long", "nullable": True, "metadata": {}},
        ],
    },
    partitions_def=DQ_TEST_PARTITIONS_DEF,
    group_name="dq_test",
)
def partitioned_asset_before_dq(context) -> DataFrame:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    partition_date_str = context.asset_partition_key_for_output()
    sql_query = f"""
        select '{partition_date_str}' as date,
        org_id,
        date as event_date,
        org_id + 2 as org_plus_2
        from dataplatform_dev.rdsdeltalake_source_1
        where date = '{partition_date_str}'
        """
    df = spark.sql(sql_query)
    log_query_data(context, sql_query)
    return df


dqs[("asset_before_dq", "test_table_with_name")].append(
    JoinableDQCheck(
        name="joinability_pass",
        database=DATABASE,
        database_2=DATABASE,
        input_asset_1="asset_before_dq",
        input_asset_2="test_table_with_name",
        join_keys=[("org_id", "org_id")],
        blocking=True,
    )
)

dqs[("asset_before_dq", "test_table_with_name_and_missing_key")].append(
    JoinableDQCheck(
        name="joinability_fail",
        database=DATABASE,
        database_2=DATABASE,
        input_asset_1="asset_before_dq",
        input_asset_2="test_table_with_name_and_missing_key",
        join_keys=[("org_id", "org_id")],
        blocking=True,
    )
)


stg_asset_before_dq_query = """
select org_id, date, org_id + 1 as value from dataplatform_dev.rdsdeltalake_source_1 where org_id = 2
"""


@asset(
    key_prefix=[AWSRegions.US_WEST_2.value, DATABASE],
    compute_kind="sql",
    metadata={
        "database": DATABASE,
        "sql_query": stg_asset_before_dq_query,
        "schema": [
            {"name": "org_id", "type": "long", "nullable": True, "metadata": {}},
            {"name": "date", "type": "string", "nullable": True, "metadata": {}},
            {"name": "value", "type": "long", "nullable": True, "metadata": {}},
        ],
    },
    required_resource_keys={"databricks_pyspark_step_launcher"},
    group_name="dq_test",
)
def stg_asset_before_dq(context, asset_before_dq) -> DataFrame:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    df = spark.sql(stg_asset_before_dq_query)
    return df


stg_empty_asset_before_dq_query = (
    "select org_id, date from dataplatform_dev.rdsdeltalake_source_1 where 1 = 2"
)


@asset(
    key_prefix=[AWSRegions.US_WEST_2.value, DATABASE],
    compute_kind="sql",
    metadata={
        "database": DATABASE,
        "sql_query": stg_empty_asset_before_dq_query,
        "schema": [
            {"name": "org_id", "type": "long", "nullable": True, "metadata": {}},
            {"name": "date", "type": "string", "nullable": True, "metadata": {}},
        ],
    },
    required_resource_keys={"databricks_pyspark_step_launcher"},
    group_name="dq_test",
)
def stg_empty_asset_before_dq(context, asset_before_dq) -> DataFrame:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    df = spark.sql(stg_empty_asset_before_dq_query)
    return df


@asset(
    key_prefix=[AWSRegions.US_WEST_2.value, DATABASE],
    compute_kind="sql",
    metadata={
        "database": DATABASE,
        "schema": [
            {"name": "org_id", "type": "long", "nullable": True, "metadata": {}},
            {"name": "date", "type": "string", "nullable": True, "metadata": {}},
        ],
    },
    required_resource_keys={"databricks_pyspark_step_launcher"},
    group_name="dq_test",
)
def stg_non_empty_asset_before_dq(context, asset_before_dq) -> DataFrame:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    sql_query = """
    select org_id, date from dataplatform_dev.rdsdeltalake_source_1 LIMIT 3
    """
    log_query_data(context, sql_query)
    df = spark.sql(sql_query)
    return df


dqs["partitioned_asset_before_dq"].append(
    NonEmptyDQCheck(
        name="blocking_empty_partition_check",
        database=DATABASE,
        table="partitioned_asset_before_dq",
        blocking=True,
    )
)

dqs["partitioned_asset_before_dq"].append(
    NonNullDQCheck(
        name="blocking_non_null_partition_check",
        database=DATABASE,
        table="partitioned_asset_before_dq",
        non_null_columns=["org_id", "event_date"],
        blocking=True,
    )
)

dqs["test_table_with_nulls"].append(
    NonNullDQCheck(
        name="blocking_non_null_check",
        database=DATABASE,
        table="test_table_with_nulls",
        non_null_columns=["org_id", "org_name"],
        blocking=True,
    )
)


dqs["stg_empty_asset_before_dq"].append(
    NonEmptyDQCheck(
        name="stg_empty_asset_before_dq_blocking_empty_check",
        database=DATABASE,
        table="stg_empty_asset_before_dq",
        blocking=True,
    )
)

dqs["stg_non_empty_asset_before_dq"].append(
    NonEmptyDQCheck(
        name="stg_non_empty_asset_before_dq_blocking_empty_check",
        database=DATABASE,
        table="stg_non_empty_asset_before_dq",
        blocking=True,
    )
)

dqs["stg_asset_before_dq"].append(
    NonEmptyDQCheck(
        name="stg_asset_before_dq_blocking_empty_check",
        database=DATABASE,
        table="stg_asset_before_dq",
        blocking=True,
    )
)

dqs["stg_asset_before_dq"].append(
    SQLDQCheck(
        name="dq_nonblocking_fail_1",
        database=DATABASE,
        sql_query="""SELECT count(1) as observed_value FROM dataplatform_dev.rdsdeltalake_source_1 where value is null """,
        expected_value=1,
        blocking=False,
    )
)

dqs["stg_asset_before_dq"].append(
    SQLDQCheck(
        name="dq_nonblocking_pass_1",
        database=DATABASE,
        sql_query="""SELECT count(1) as observed_value FROM dataplatform_dev.rdsdeltalake_source_1 where value is null""",
        expected_value=0,
    )
)


dqs["stg_asset_before_dq"].append(
    SQLDQCheck(
        name="dq_blocking_pass_1",
        database=DATABASE,
        sql_query="""SELECT count(1) as observed_value FROM dataplatform_dev.rdsdeltalake_source_1""",
        expected_value=4,
        blocking=True,
    )
)

dqs["stg_asset_before_dq"].append(
    SQLDQCheck(
        name="dq_blocking_fail_1",
        database=DATABASE,
        sql_query="""SELECT count(1) as observed_value FROM dataplatform_dev.rdsdeltalake_source_1 where value is null""",
        expected_value=2,
        blocking=True,
    )
)


asset_after_dq = build_merge_stg_asset(
    merge_table="stg_asset_before_dq",
    merge_into_table="asset_after_dq",
    primary_keys=["org_id", "date"],
    upstreams=["dq_stg_asset_before_dq"],
    group_name="dq_test",
    database=DATABASE,
    region=AWSRegions.US_WEST_2.value,
)


dqs["asset_after_dq"].append(
    SQLDQCheck(
        name="dq_blocking_fail_2",
        database=DATABASE,
        sql_query="""SELECT count(1) as observed_value FROM dataplatform_dev.rdsdeltalake_source_1 where value is null""",
        expected_value=1,
        blocking=True,
    )
)


dq_assets = dqs.generate()
