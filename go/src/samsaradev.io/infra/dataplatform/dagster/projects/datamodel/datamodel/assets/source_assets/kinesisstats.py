from datetime import datetime

from dagster import DailyPartitionsDefinition

from ...common.constants import ReplicationGroups
from ...common.utils import (
    build_observation_function,
    build_source_assets,
    get_all_regions,
)

kinesisstats_osdaccelerometer = build_source_assets(
    name="osdaccelerometer",
    database="kinesisstats",
    group_name=ReplicationGroups.KINESISSTATS,
    regions=get_all_regions(),
    description="Sent up by a VG or CM when it detects an event or or driver behavior that we want to surface to the user as important. Sent up both for accelerometer-detected events (e.g. crash) and AI-detected events (e.g. seatbelt).",
    partition_keys=["date"],
    partitions_def=DailyPartitionsDefinition(
        start_date=datetime(2023, 1, 1), end_offset=1
    ),
    observe_fn=build_observation_function(observation_window_days=28),
    auto_observe_interval_minutes=210,
)
