from datetime import datetime

from dagster import DailyPartitionsDefinition

from ...common.constants import ReplicationGroups
from ...common.utils import build_source_assets, get_all_regions

dataprep_device_builds = build_source_assets(
    name="device_builds",
    database="dataprep",
    regions=get_all_regions(),
    group_name=ReplicationGroups.DATAPIPELINES,
    description="This table contains a daily summary of device build information for Samsara organizations.",
    partition_keys=["date"],
    partitions_def=DailyPartitionsDefinition(
        start_date=datetime(2022, 1, 1), end_offset=1
    ),
)


dataprep_active_devices = build_source_assets(
    name="active_devices",
    database="dataprep",
    regions=get_all_regions(),
    group_name=ReplicationGroups.DATAPIPELINES,
    description="This table contains a daily snapshot of the active/inactive product devices and their associated trip summary for each Samsara organization.",
    partition_keys=["date"],
    partitions_def=DailyPartitionsDefinition(
        start_date=datetime(2022, 1, 1), end_offset=1
    ),
)
