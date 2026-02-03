from datetime import datetime

from dagster import DailyPartitionsDefinition

from ...common.constants import ReplicationGroups
from ...common.utils import (
    build_observation_function,
    build_source_assets,
    get_all_regions,
)

safetydb_shards_harsh_event_surveys = build_source_assets(
    name="harsh_event_surveys",
    database="safetydb_shards",
    regions=get_all_regions(),
    group_name=ReplicationGroups.RDS,
    partition_keys=["date"],
    partitions_def=DailyPartitionsDefinition(
        start_date=datetime(2022, 1, 1), end_offset=1
    ),
    observe_fn=build_observation_function(observation_window_days=28),
    auto_observe_interval_minutes=210,
)

safetydb_shards_safety_events = build_source_assets(
    name="safety_events",
    database="safetydb_shards",
    regions=get_all_regions(),
    group_name=ReplicationGroups.RDS,
    description="Represents an event that we detect in a vehicle through the VG or CM that we surface to customers in the safety inbox and the safety dashboard. A row in this table represents a behavior committed by a driver that affects their safety score. Any events that occured in the last 28 days in safetydb.safety_events are kept up-to-date in this table.",
    partition_keys=["date"],
    partitions_def=DailyPartitionsDefinition(
        start_date=datetime(2020, 1, 1), end_offset=1
    ),
    observe_fn=build_observation_function(observation_window_days=28),
    auto_observe_interval_minutes=210,
)

safetydb_shards_activity_events = build_source_assets(
    name="activity_events",
    database="safetydb_shards",
    regions=get_all_regions(),
    group_name=ReplicationGroups.RDS,
    description="Represents an activity on a safety event, an event that we detect in a vehicle through the VG or CM that we surface to customers in the safety inbox and the safety dashboard. A row in this table represents an activity that occured on a given safety event. For any events that occured in the last 91 days in safetydb.safety_events, the corresponding activites from safetydb.activity_events are kept up-to-date in this table.",
    partition_keys=["date"],
    partitions_def=DailyPartitionsDefinition(
        start_date=datetime(2020, 1, 1), end_offset=1
    ),
    observe_fn=build_observation_function(observation_window_days=91),
    auto_observe_interval_minutes=210,
)
