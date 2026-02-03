from collections import defaultdict
from datetime import datetime

import pandas as pd
from dagster import (
    AssetDep,
    AssetKey,
    BackfillPolicy,
    DailyPartitionsDefinition,
    IdentityPartitionMapping,
    MultiPartitionsDefinition,
    Nothing,
    StaticPartitionsDefinition,
    TimeWindowPartitionMapping,
    asset,
)
from pyspark.sql import DataFrame, SparkSession

from ...common.stats import similarity_score
from ...common.utils import (
    AWSRegions,
    Database,
    DQGroup,
    JoinableDQCheck,
    NonEmptyDQCheck,
    PrimaryKeyDQCheck,
    TableType,
    WarehouseWriteMode,
    apply_db_overrides,
    build_assets_from_sql,
    build_table_description,
    get_all_regions,
    get_code_location,
)

databases = {
    "database_bronze": Database.DATAMODEL_CORE_BRONZE,
    "database_gold": Database.DATAMODEL_CORE,
}

database_dev_overrides = {
    "database_bronze_dev": Database.DATAMODEL_DEV,
    "database_gold_dev": Database.DATAMODEL_DEV,
}

databases = apply_db_overrides(databases, database_dev_overrides)

SLACK_ALERTS_CHANNEL = "alerts-data-engineering"
GROUP_NAME = "gateway_device_intervals"
REQUIRED_RESOURCE_KEYS = {"databricks_pyspark_step_launcher"}
REQUIRED_RESOURCE_KEYS_EU = {"databricks_pyspark_step_launcher_eu"}
REQUIRED_RESOURCE_KEYS_CA = {"databricks_pyspark_step_launcher_ca"}

dqs = DQGroup(
    group_name=GROUP_NAME,
    partition_def=None,
    slack_alerts_channel=SLACK_ALERTS_CHANNEL,
    regions=get_all_regions(),
)

RAW_PRODUCTSDB_GATEWAY_DEVICE_HISTORY_QUERY = """
SELECT DATE(current_timestamp()) AS date,
      *
FROM productsdb.gateway_device_history
"""

FCT_GATEWAY_DEVICE_INTERVALS_QUERY = """
with base AS (
    (SELECT gateway_id,
            device_id,
            --prevent race conditions with same time pairings in the gateways table
            least(timestamp,
                  case when _timestamp > '2000-01-01' then _timestamp else null end) as timestamp,
            0 as prefer_gateway_table
    FROM datamodel_core_bronze.raw_productsdb_gateway_device_history)
    UNION ALL
    (SELECT id AS gateway_id,
            device_id,
            LEAST(updated_at, _timestamp) AS timestamp,
            --pairings in the gateway table always represent the true pairing of the gateway
            1 as prefer_gateway_table
    FROM datamodel_core_bronze.raw_productsdb_gateways
    WHERE date = (SELECT MAX(date) FROM datamodel_core_bronze.raw_productsdb_gateways) )),
    staged AS (
    select
        gateway_id,
        device_id,
        timestamp,
        prefer_gateway_table,
        --find the next gateway and device partitioned by both to allow elimination of gateway, device duplicates
        LEAD(gateway_id) OVER (PARTITION BY gateway_id, device_id ORDER BY timestamp desc, prefer_gateway_table desc) AS next_gateway_id,
        LEAD(device_id) OVER (PARTITION BY gateway_id, device_id ORDER BY timestamp desc, prefer_gateway_table desc) AS next_device_id,
        --find the next device_id for gateways to ensure new device id's are not eliminated
        LEAD(device_id) OVER (PARTITION BY gateway_id ORDER BY timestamp desc, prefer_gateway_table desc) AS next_gateway_device_id
        from base),
    ordered as (
    select
        gateway_id,
        device_id,
        timestamp,
        prefer_gateway_table,
        lead(device_id) over (partition by gateway_id order by timestamp asc, prefer_gateway_table asc) as next_device_id,
        lead(timestamp) over (partition by gateway_id order by timestamp asc, prefer_gateway_table asc) as next_device_timestamp,
        lag(device_id) over (partition by gateway_id order by timestamp) as prev_device_id,
        lead(gateway_id) over (partition by device_id order by timestamp asc, prefer_gateway_table asc) as next_gateway_id,
        lead(timestamp) over (partition by device_id order by timestamp asc, prefer_gateway_table asc) as next_gateway_timestamp,
        lag(gateway_id) over (partition by device_id order by timestamp) as prev_gateway_id,
        rank(prefer_gateway_table) over (partition by device_id, gateway_id order by prefer_gateway_table) as rank
    from staged
    -- eliminate cases when there are two rows of the same gateway + device without separation
    --if there is separation (there is another next device id), leave those rows as they are not duplicates
    where (gateway_id != coalesce(next_gateway_id,0)
        and device_id != coalesce(next_device_id,0))
    OR coalesce(next_gateway_device_id,0) != device_id)

    select
        gateway_id,
        device_id,
        timestamp as start_interval,
        case when next_device_id != device_id
                and next_device_timestamp is not null
                and (next_device_timestamp <= next_gateway_timestamp or next_gateway_timestamp is null)
            then next_device_timestamp
            when next_gateway_id != gateway_id
                and next_gateway_timestamp is not null
                and (next_gateway_timestamp <= next_device_timestamp or next_device_timestamp is null)
            then next_gateway_timestamp
            end as end_interval,
        next_device_id,
        prev_device_id,
        next_gateway_id,
        prev_gateway_id,
        CASE WHEN next_device_id IS NULL AND next_gateway_id IS NULL THEN TRUE ELSE FALSE END AS is_current
    from ordered
    where rank = 1
"""

FCT_GATEWAY_DEVICE_INTERVALS_SCHEMA = [
    {
        "name": "gateway_id",
        "type": "long",
        "nullable": False,
        "metadata": {"comment": "Static id related gateway hardware."},
    },
    {
        "name": "device_id",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Virtual id of accompanying vehicle for gateway. Can include the gateway's id as a placeholder."
        },
    },
    {
        "name": "start_interval",
        "type": "timestamp",
        "nullable": False,
        "metadata": {"comment": "Start of gateway to device pairing."},
    },
    {
        "name": "end_interval",
        "type": "timestamp",
        "nullable": False,
        "metadata": {
            "comment": "End of gateway to device pairing. The interval ends when the gateway is paired with a new device or the device is paired with a new gateway."
        },
    },
    {
        "name": "next_device_id",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "The next chronological device_id for the given gateway_id. Null if no next device_id."
        },
    },
    {
        "name": "prev_device_id",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "The previous chronological device_id for the given gateway_id. Null if no previous device_id."
        },
    },
    {
        "name": "next_gateway_id",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "The next chronological gateway_id for the given device_id. Null if no next gateway_id."
        },
    },
    {
        "name": "prev_gateway_id",
        "type": "long",
        "nullable": True,
        "metadata": {
            "comment": "The previous chronological gateway_id for the given device_id. Null if no previous gateway_id."
        },
    },
    {
        "name": "is_current",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "True if the interval represents the current state as of the last table materialization for the gateway to device pairing."
        },
    },
]

assets = build_assets_from_sql(
    name="raw_productsdb_gateway_device_history",
    sql_query=RAW_PRODUCTSDB_GATEWAY_DEVICE_HISTORY_QUERY,
    primary_keys=[],
    upstreams=[],
    schema=[],
    regions=get_all_regions(),
    group_name=GROUP_NAME,
    database=databases["database_bronze"],
    databases=databases,
    write_mode=WarehouseWriteMode.overwrite,
    partitions_def=None,
)

raw_productsdb_gateways_1 = assets[AWSRegions.US_WEST_2.value]
raw_productsdb_gateways_2 = assets[AWSRegions.EU_WEST_1.value]
raw_productsdb_gateways_3 = assets[AWSRegions.CA_CENTRAL_1.value]


fct_gateway_device_intervals_description = build_table_description(
    table_desc="""This is a transactional fact table for gateway and device pairings. In this context, gateway refers to the gateway_id and device refers to the device_id, a virtual representation of a distinct vehicle. The start and end times of each interval denotes the assignment period. If the end time is null, this would indicate that the gateway remains assigned to the device. Intervals starting prior to July 1, 2022 may have an inaccurate start_interval timestamp due to a data migration. The true timestamp of the start_interval is likely prior to the value recorded in the fct_gateway_device_interval table. This migration does not impact the accuracy of the device_id to gateway_id pairing or the end_interval timestamp of the interval.""",
    row_meaning="""One row in this table represents one interval of gateway and device pairing denoted by a unique start_time, gateway_id, and device_id. This table is unpartitioned, meaning the contents are overwritten with each run.""",
    table_type=TableType.TRANSACTIONAL_FACT,
    freshness_slo_updated_by="9am PST",
)


@asset(
    name="fct_gateway_device_intervals",
    owners=["team:DataEngineering"],
    description=fct_gateway_device_intervals_description,
    compute_kind="sql",
    metadata={
        "database": databases["database_gold"],
        "owners": ["team:DataEngineering"],
        "write_mode": WarehouseWriteMode.overwrite,
        "schema": FCT_GATEWAY_DEVICE_INTERVALS_SCHEMA,
        "code_location": get_code_location(),
        "primary_keys": ["gateway_id", "device_id", "start_interval"],
        "description": fct_gateway_device_intervals_description,
    },
    required_resource_keys=REQUIRED_RESOURCE_KEYS,
    group_name=GROUP_NAME,
    partitions_def=None,
    key_prefix=[AWSRegions.US_WEST_2.value, databases["database_gold"]],
    deps=[
        AssetDep(
            AssetKey(
                [
                    AWSRegions.US_WEST_2.value,
                    databases["database_bronze"],
                    "raw_productsdb_gateway_device_history",
                ]
            ),
        ),
        AssetDep(
            AssetKey(
                [
                    AWSRegions.US_WEST_2.value,
                    databases["database_bronze"],
                    "raw_productsdb_gateways",
                ]
            ),
        ),
    ],
)
def fct_gateway_device_intervals(context) -> DataFrame:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    query = FCT_GATEWAY_DEVICE_INTERVALS_QUERY
    context.log.info(query)
    df = spark.sql(query)
    context.log.info(df)
    return df


@asset(
    name="fct_gateway_device_intervals",
    owners=["team:DataEngineering"],
    description=fct_gateway_device_intervals_description,
    compute_kind="sql",
    metadata={
        "database": databases["database_gold"],
        "owners": ["team:DataEngineering"],
        "write_mode": WarehouseWriteMode.overwrite,
        "schema": FCT_GATEWAY_DEVICE_INTERVALS_SCHEMA,
        "code_location": get_code_location(),
        "primary_keys": ["gateway_id", "device_id", "start_interval"],
        "description": fct_gateway_device_intervals_description,
    },
    required_resource_keys=REQUIRED_RESOURCE_KEYS_EU,
    group_name=GROUP_NAME + "_eu",
    partitions_def=None,
    key_prefix=[AWSRegions.EU_WEST_1.value, databases["database_gold"]],
    deps=[
        AssetDep(
            AssetKey(
                [
                    AWSRegions.EU_WEST_1.value,
                    databases["database_bronze"],
                    "raw_productsdb_gateway_device_history",
                ]
            ),
        ),
        AssetDep(
            AssetKey(
                [
                    AWSRegions.EU_WEST_1.value,
                    databases["database_bronze"],
                    "raw_productsdb_gateways",
                ]
            ),
        ),
    ],
)
def fct_gateway_device_intervals_eu(context) -> DataFrame:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    query = FCT_GATEWAY_DEVICE_INTERVALS_QUERY
    context.log.info(query)
    df = spark.sql(query)
    context.log.info(df)
    return df


@asset(
    name="fct_gateway_device_intervals",
    owners=["team:DataEngineering"],
    description=fct_gateway_device_intervals_description,
    compute_kind="sql",
    metadata={
        "database": databases["database_gold"],
        "owners": ["team:DataEngineering"],
        "write_mode": WarehouseWriteMode.overwrite,
        "schema": FCT_GATEWAY_DEVICE_INTERVALS_SCHEMA,
        "code_location": get_code_location(),
        "primary_keys": ["gateway_id", "device_id", "start_interval"],
        "description": fct_gateway_device_intervals_description,
    },
    required_resource_keys=REQUIRED_RESOURCE_KEYS_CA,
    group_name=GROUP_NAME + "_ca",
    partitions_def=None,
    key_prefix=[AWSRegions.CA_CENTRAL_1.value, databases["database_gold"]],
    deps=[
        AssetDep(
            AssetKey(
                [
                    AWSRegions.CA_CENTRAL_1.value,
                    databases["database_bronze"],
                    "raw_productsdb_gateway_device_history",
                ]
            ),
        ),
        AssetDep(
            AssetKey(
                [
                    AWSRegions.CA_CENTRAL_1.value,
                    databases["database_bronze"],
                    "raw_productsdb_gateways",
                ]
            ),
        ),
    ],
)
def fct_gateway_device_intervals_ca(context) -> DataFrame:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    query = FCT_GATEWAY_DEVICE_INTERVALS_QUERY
    context.log.info(query)
    df = spark.sql(query)
    context.log.info(df)
    return df


dqs["fct_gateway_device_intervals"].append(
    PrimaryKeyDQCheck(
        name="dq_pk_fct_gateway_device_intervals",
        table="fct_gateway_device_intervals",
        primary_keys=["gateway_id", "device_id", "start_interval"],
        blocking=True,
        database=databases["database_gold_dev"],
    )
)

dqs["fct_gateway_device_intervals"].append(
    NonEmptyDQCheck(
        name="dq_empty_fct_gateway_device_intervals",
        table="fct_gateway_device_intervals",
        blocking=True,
        database=databases["database_gold_dev"],
    )
)

dqs["fct_gateway_device_intervals"].append(
    JoinableDQCheck(
        name="dq_joinable_fct_gateway_device_intervals_to_dim_devices",
        database=databases["database_gold_dev"],
        database_2=databases["database_gold_dev"],
        input_asset_1="fct_gateway_device_intervals",
        input_asset_2="dim_devices",
        join_keys=[("device_id", "device_id")],
        blocking=False,
        null_right_table_rows_ratio=0.01,
    )
)

dq_assets = dqs.generate()
