from collections import defaultdict

from dagster import (
    DailyPartitionsDefinition,
    MultiPartitionsDefinition,
    StaticPartitionsDefinition,
    asset,
)
from pyspark.sql import DataFrame, SparkSession

from ...common.constants import SLACK_ALERTS_CHANNEL_DATA_ENGINEERING
from ...common.utils import (
    AWSRegions,
    Database,
    NonEmptyDQCheck,
    WarehouseWriteMode,
    build_asset_from_sql_in_region,
    build_dq_asset,
)

SLACK_ALERTS_CHANNEL = SLACK_ALERTS_CHANNEL_DATA_ENGINEERING
key_prefix = "dataengineering_dev"

databases = {
    "database_bronze": Database.DATAMODEL_DEV,
    "database_silver": Database.DATAMODEL_DEV,
    "database_gold": Database.DATAMODEL_DEV,
}

pipeline_group_name = "telematics_poc"

pipeline_partitions_def = MultiPartitionsDefinition(
    {
        "date": DailyPartitionsDefinition(start_date="2023-10-01"),
        "object_stat": StaticPartitionsDefinition(
            ["ObjectStat1", "ObjectStat2", "ObjectStat3"]
        ),
    }
)

custom_macros = {"OBJSTAT": "object_stat"}

fct_partitioned_object_stats_query = """
SELECT 1 AS id, 'a' AS attribute, '{OBJSTAT}' AS object_stat, '{DATEID}' AS date UNION ALL
SELECT 1 AS id, 'a' AS attribute, '{OBJSTAT}' AS object_stat, '{DATEID}' AS date UNION ALL
SELECT 1 AS id, 'a' AS attribute, '{OBJSTAT}' AS object_stat, '{DATEID}' AS date
"""

fct_partitioned_object_stats_query_2 = """
SELECT 2 AS id, 'a' AS attribute, '{OBJSTAT}' AS object_stat, '{DATEID}' AS date UNION ALL
SELECT 2 AS id, 'a' AS attribute, '{OBJSTAT}' AS object_stat, '{DATEID}' AS date UNION ALL
SELECT 2 AS id, 'a' AS attribute, '{OBJSTAT}' AS object_stat, '{DATEID}' AS date
"""

fct_partitioned_object_stats_query_3 = """
SELECT 3 AS id, 'a' AS attribute, '{OBJSTAT}' AS object_stat, '{DATEID}' AS date UNION ALL
SELECT 3 AS id, 'a' AS attribute, '{OBJSTAT}' AS object_stat, '{DATEID}' AS date UNION ALL
SELECT 3 AS id, 'a' AS attribute, '{OBJSTAT}' AS object_stat, '{DATEID}' AS date
"""

fct_schema = [
    {"name": "id", "type": "integer", "nullable": False, "metadata": {}},
    {"name": "attribute", "type": "string", "nullable": False, "metadata": {}},
    {
        "name": "object_stat",
        "type": "string",
        "nullable": False,
        "metadata": {},
    },
    {"name": "date", "type": "string", "nullable": False, "metadata": {}},
]


fct_partitioned_object_stats = build_asset_from_sql_in_region(
    name="fct_partitioned_object_stats",
    sql_query=fct_partitioned_object_stats_query,
    primary_keys=[],
    upstreams=set(),
    group_name=pipeline_group_name,
    database=Database.DATAMODEL_DEV,
    databases=databases,
    schema=fct_schema,
    write_mode=WarehouseWriteMode.overwrite,
    region="us-west-2",
    partitions_def=pipeline_partitions_def,
    custom_macros=custom_macros,
)

fct_partitioned_object_stats_2 = build_asset_from_sql_in_region(
    name="fct_partitioned_object_stats_2",
    sql_query=fct_partitioned_object_stats_query_2,
    primary_keys=[],
    upstreams=set(),
    group_name=pipeline_group_name,
    database=Database.DATAMODEL_DEV,
    databases=databases,
    schema=fct_schema,
    write_mode=WarehouseWriteMode.overwrite,
    region=AWSRegions.US_WEST_2.value,
    partitions_def=pipeline_partitions_def,
    custom_macros=custom_macros,
)

fct_partitioned_object_stats_3 = build_asset_from_sql_in_region(
    name="fct_partitioned_object_stats_3",
    sql_query=fct_partitioned_object_stats_query_3,
    primary_keys=[],
    upstreams=set(),
    group_name=pipeline_group_name,
    database=Database.DATAMODEL_DEV,
    databases=databases,
    schema=fct_schema,
    write_mode=WarehouseWriteMode.overwrite,
    region=AWSRegions.US_WEST_2.value,
    partitions_def=pipeline_partitions_def,
    custom_macros=custom_macros,
)


agg_schema = [
    {"name": "object_stat", "type": "string", "nullable": False, "metadata": {}},
    {"name": "date", "type": "string", "nullable": False, "metadata": {}},
    {"name": "max_record", "type": "integer", "nullable": False, "metadata": {}},
]


def get_fct_partitioned_object_stats_agg_query(partition_value_map={}):
    if partition_value_map["object_stat"] == "ObjectStat1":
        fct_partitioned_object_stats_agg_query = """
        SELECT '{OBJSTAT}' AS object_stat, '{DATEID}' AS date, max(id) as max_record
        FROM {database_bronze}.fct_partitioned_object_stats
        where date = '{DATEID}' and object_stat = '{OBJSTAT}'
        GROUP BY 1,2
        """
    elif partition_value_map["object_stat"] == "ObjectStat2":
        fct_partitioned_object_stats_agg_query = """
        SELECT '{OBJSTAT}' AS object_stat, '{DATEID}' AS date, max(id) as max_record
        FROM {database_bronze}.fct_partitioned_object_stats_2
        where date = '{DATEID}' and object_stat = '{OBJSTAT}'
        GROUP BY 1,2
        """
    else:
        fct_partitioned_object_stats_agg_query = """
        SELECT '{OBJSTAT}' AS object_stat, '{DATEID}' AS date, 100 as max_record
        FROM {database_bronze}.fct_partitioned_object_stats_2
        where date = '{DATEID}' and object_stat = '{OBJSTAT}'
        GROUP BY 1,2
        """

    return fct_partitioned_object_stats_agg_query


fct_partitioned_object_stats_agg = build_asset_from_sql_in_region(
    name="fct_partitioned_object_stats_agg",
    sql_query=get_fct_partitioned_object_stats_agg_query,
    primary_keys=[],
    upstreams=set(
        [
            "fct_partitioned_object_stats",
            "fct_partitioned_object_stats_2",
            "fct_partitioned_object_stats_3",
        ]
    ),
    group_name=pipeline_group_name,
    database=Database.DATAMODEL_DEV,
    databases=databases,
    schema=agg_schema,
    write_mode=WarehouseWriteMode.overwrite,
    region=AWSRegions.US_WEST_2.value,
    partitions_def=pipeline_partitions_def,
    custom_macros=custom_macros,
)
