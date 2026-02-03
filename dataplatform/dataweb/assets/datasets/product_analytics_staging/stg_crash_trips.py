from dagster import AssetExecutionContext, DailyPartitionsDefinition
from dataweb import (
    NonEmptyDQCheck,
    NonNullDQCheck,
    PrimaryKeyDQCheck,
    table,
)
from dataweb.userpkgs.constants import (
    DATAENGINEERING,
    FRESHNESS_SLO_9AM_PST,
    AWSRegion,
    ColumnDescription,
    Database,
    TableType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.utils import (
    build_table_description,
    partition_key_ranges_from_context,
)

PRIMARY_KEYS = ["date", "org_id", "device_id", "start_time_ms"]

SCHEMA = [
     {
        "name": "date",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": ColumnDescription.DATE,
        },
    },
    {
        "name": "org_id",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": ColumnDescription.ORG_ID,
        },
    },
    {
        "name": "device_id",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": ColumnDescription.DEVICE_ID,
        },
    },
    {
        "name": "timestamp",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "event_environment",
        "type": {'type': 'array', 'elementType': 'string', 'containsNull': True},
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "road_surface_condition",
        "type": {'type': 'array', 'elementType': 'string', 'containsNull': True},
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "manner_of_collision",
        "type": {'type': 'array', 'elementType': 'string', 'containsNull': True},
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "crash_types",
        "type": {'type': 'array', 'elementType': 'string', 'containsNull': True},
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "is_tp",
        "type": "integer",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "start_time_ms",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "end_time_ms",
        "type": "long",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "driver_id",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": ColumnDescription.DRIVER_ID,
        },
    },
    {
        "name": "duration_mins",
        "type": "double",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "trip_type",
        "type": "string",
        "nullable": False,
        "metadata": {},
    },
    {
        "name": "crash_source",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Source of crash event -labelbox or safety_event_review",
        },
    }
]

QUERY = """
with
--99 pct of safety event review decisions can occur up to 5 days after the trip end
safety_event_review_crashes as (
select string(date(from_unixtime(split(source_id, ',')[2]/1000))) as date,
       org_id,
       cast(split(source_id, ',')[1] as long) as device_id,
       cast(split(source_id, ',')[2] as long) as timestamp,
       cast(null as array<string>) as event_environment,
       cast(null as array<string>) as road_surface_condition,
       cast(null as array<string>) as manner_of_collision,
       cast(null as array<string>) as crash_types,
       1 as is_tp,
       'safety_event_review' as crash_source
from dataengineering.safety_event_review_results_details_region
where array_contains(unique_decisions, 'Submit') = TRUE
and detection_type = 'haCrash'
and date between date_sub('{PARTITION_START}', 4) and '{PARTITION_END}'),
labelbox_crashes as (
select cast(date as string) as date,
       org_id,
       device_id,
       timestamp,
       event_environment,
       road_surface_condition,
       manner_of_collision,
       crash_types,
       is_tp,
       'labelbox' as crash_source
from dojo.metrics_std_crash
where date between '{PARTITION_START}' and '{PARTITION_END}'
and is_tp = 1),
safety_events AS (
select distinct date,
      org_id,
      device_id,
      event_ms
from datamodel_safety.fct_safety_events
where date between '{PARTITION_START}' and '{PARTITION_END}'),
crash_safety_events AS (
select coalesce(labelbox_crashes.date, safety_event_review_crashes.date) as date,
       coalesce(labelbox_crashes.org_id, safety_event_review_crashes.org_id) as org_id,
       coalesce(labelbox_crashes.device_id, safety_event_review_crashes.device_id) as device_id,
       coalesce(labelbox_crashes.timestamp, safety_event_review_crashes.timestamp) as timestamp,
       coalesce(labelbox_crashes.event_environment, safety_event_review_crashes.event_environment) as event_environment,
       coalesce(labelbox_crashes.road_surface_condition, safety_event_review_crashes.road_surface_condition) as road_surface_condition,
       coalesce(labelbox_crashes.manner_of_collision, safety_event_review_crashes.manner_of_collision) as manner_of_collision,
       coalesce(labelbox_crashes.crash_types, safety_event_review_crashes.crash_types) as crash_types,
       coalesce(labelbox_crashes.is_tp, safety_event_review_crashes.is_tp) as is_tp,
       coalesce(labelbox_crashes.crash_source, safety_event_review_crashes.crash_source) as crash_source
from  safety_events
left join labelbox_crashes
  on labelbox_crashes.date = safety_events.date
  and labelbox_crashes.org_id = safety_events.org_id
  and labelbox_crashes.device_id = safety_events.device_id
  and labelbox_crashes.timestamp = safety_events.event_ms
left join safety_event_review_crashes
  on safety_event_review_crashes.date = safety_events.date
  and safety_event_review_crashes.org_id = safety_events.org_id
  and safety_event_review_crashes.device_id = safety_events.device_id
  and safety_event_review_crashes.timestamp = safety_events.event_ms
where coalesce(labelbox_crashes.crash_source, safety_event_review_crashes.crash_source) is not null),
trips as (
select *
from datamodel_telematics.fct_trips
where date between '{PARTITION_START}' and '{PARTITION_END}'),
crash_trips AS (
select crash_safety_events.*,
       trips.start_time_ms,
       trips.end_time_ms,
       trips.driver_id,
       trips.duration_mins,
       trips.trip_type,
       row_number() over (partition by crash_safety_events.date, trips.start_time_ms, crash_safety_events.org_id, crash_safety_events.device_id
                          order by crash_safety_events.crash_source asc) as row_num
from trips
inner join crash_safety_events
     on trips.date = crash_safety_events.date
     and trips.device_id = crash_safety_events.device_id
     and trips.org_id = crash_safety_events.org_id
     and crash_safety_events.timestamp between trips.start_time_ms and trips.end_time_ms)



select * except (row_num)
from crash_trips
where row_num = 1
"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="""Staging table for crash events joined with trip and safety event data.""",
        row_meaning="""Each row represents a crash event that occurred during a trip, with associated trip metadata.""",
        related_table_info={},
        table_type=TableType.STAGING,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=[AWSRegion.US_WEST_2],
    owners=[DATAENGINEERING],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DailyPartitionsDefinition(start_date="2023-09-01"),
    write_mode=WarehouseWriteMode.OVERWRITE,
    dq_checks=[
        NonEmptyDQCheck(name="dq_non_empty_stg_crash_trips", block_before_write=False),
        PrimaryKeyDQCheck(name="dq_pk_stg_crash_trips", primary_keys=PRIMARY_KEYS),
        NonNullDQCheck(
            name="dq_non_null_stg_crash_trips",
            non_null_columns=PRIMARY_KEYS,
        ),
    ],
    backfill_batch_size=5,
    upstreams=[
        "dojo.metrics_std_crash",
        "datamodel_safety.fct_safety_events",
        "datamodel_telematics.fct_trips",
        "dataengineering.safety_event_review_results_details_region"
    ],
)
def stg_crash_trips(context: AssetExecutionContext) -> str:
    context.log.info("Updating stg_crash_trips")
    partition_keys = partition_key_ranges_from_context(context)[0]
    PARTITION_START = partition_keys[0]
    PARTITION_END = partition_keys[-1]
    query = QUERY.format(
        PARTITION_START=PARTITION_START,
        PARTITION_END=PARTITION_END,
    )
    context.log.info(f"{query}")
    return query
