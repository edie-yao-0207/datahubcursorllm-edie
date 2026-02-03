import time
from datetime import datetime, timedelta

from dagster import (
    AssetIn,
    DailyPartitionsDefinition,
    Nothing,
    TimeWindowPartitionMapping,
    asset,
)

from ...common.utils import (
    AWSRegions,
    Database,
    WarehouseWriteMode,
    apply_db_overrides,
    build_asset_from_sql_in_region,
)

partitions_def = DailyPartitionsDefinition(start_date="2023-09-16")

databases = {
    "database_bronze": "dataplatform_dev",
    "database_silver": "dataplatform_dev",
    "database_gold": "dataplatform_dev",
}

database_dev_overrides = {
    "database_bronze_dev": "dataplatform_dev",
    "database_silver_dev": Database.DATAMODEL_DEV,
    "database_gold_dev": Database.DATAMODEL_DEV,
}

databases = apply_db_overrides(databases, database_dev_overrides)

asset_name = "asset1"


@asset(
    io_manager_key="in_memory_io_manager",
    partitions_def=partitions_def,
    metadata={"schema": [{"name": "date", "type": "string"}]},
    key_prefix=[AWSRegions.US_WEST_2.value, "test_db"],
    group_name="example_depends_on_past",
    ins={
        asset_name: AssetIn(
            key_prefix=["test_db"],
            partition_mapping=TimeWindowPartitionMapping(
                start_offset=-1, end_offset=-1
            ),
            dagster_type=Nothing,
        )
    },
)
def asset1() -> None:
    return None


test_depends_on_past_asset = build_asset_from_sql_in_region(
    name="test_depends_on_past",
    sql_query="""
    SELECT '{DATEID}' AS date,
    org_id,
    concat('name_',org_id) AS org_name
    FROM {database_bronze}.rdsdeltalake_source_1
    """,
    primary_keys=[],
    upstreams=set(),
    group_name="example_depends_on_past",
    database=databases["database_silver_dev"],
    databases=databases,
    region="us-west-2",
    write_mode=WarehouseWriteMode.overwrite,
    partitions_def=partitions_def,
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
    depends_on_past=True,
)
