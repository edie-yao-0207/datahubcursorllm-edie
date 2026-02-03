from datetime import datetime

from dagster import DailyPartitionsDefinition

from ...common.constants import ReplicationGroups
from ...common.utils import (
    build_observation_function,
    build_source_assets,
    get_all_regions,
)

dispatchdb_shards_shardable_dispatch_routes = build_source_assets(
    name="shardable_dispatch_routes",
    database="dispatchdb_shards",
    regions=get_all_regions(),
    group_name=ReplicationGroups.RDS,
    partition_keys=[],
)

dispatchdb_shards_shardable_dispatch_jobs = build_source_assets(
    name="shardable_dispatch_jobs",
    database="dispatchdb_shards",
    regions=get_all_regions(),
    group_name=ReplicationGroups.RDS,
    partition_keys=["date"],
    partitions_def=DailyPartitionsDefinition(
        start_date=datetime(2023, 11, 1), end_offset=1
    ),
)
