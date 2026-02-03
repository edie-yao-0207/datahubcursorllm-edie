from dagster import AssetExecutionContext
from dataweb.userpkgs.query import create_run_config_overrides
from dataweb import NonEmptyDQCheck, NonNullDQCheck, TrendDQCheck, PrimaryKeyDQCheck, table
from dataweb.userpkgs.constants import (
    DATAENGINEERING,
    FRESHNESS_SLO_9AM_PST,
    AWSRegion,
    Database,
    InstanceType,
    TableType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.schema import Column
from dataweb.userpkgs.utils import (
    build_table_description,
    partition_key_ranges_from_context,
    get_all_regions
)
from dataweb.userpkgs.firmware.schema import (
    Column,
    ColumnType,
    DataType,
    Metadata,
    columns_to_schema,
    get_primary_keys,
    get_non_null_columns,
)
from dataweb.userpkgs.query import format_date_partition_query


QUERY = """
with device_trips as (
  select
    org_id,
    device_id,
    date(date) as trip_date,
    from_utc_timestamp(start_time_utc, "UTC") as start_time_utc,
    from_utc_timestamp(end_time_utc, "UTC") as end_time_utc
  from
    datamodel_telematics.fct_trips t
  where
    date between '{date_start}' and '{date_end}'
    and device_id in (
      select
        device_id
      from
        datamodel_core.dim_devices d
      where
        d.date = t.date
        and d.device_type = 'VG - Vehicle Gateway'
    )
),
device_trip_intervals as (
  -- Generate an array with 24 hourly (start, end) timestamps for each hour of the trip_date.
  select
    *,
    cast(
      -- Merge two sequences into a list of hourly (start time, end time) pairs
      arrays_zip(
        -- Hours of the trip day from 00:00 to 23:00
        slice(
          sequence(timestamp(date(trip_date)), timestamp(date_add(trip_date, 1)), interval 1 HOUR),
          1,
          24
        ),
        -- Hours of the trip day from 01:00 to 00:00 (next day)
        slice(
          sequence(timestamp(date(trip_date)), timestamp(date_add(trip_date, 1)), interval 1 HOUR),
          2,
          24
        )
      ) as array<struct<start_time: timestamp, end_time: timestamp>>
    ) as trip_day_hourly_intervals
  from
    device_trips
),
device_trip_overlaps as (
  select
    org_id,
    device_id,
    trip_date,
    start_time_utc,
    end_time_utc,
    -- Find whether the trip overlaps with each hourly interval on the day.
    transform(
      trip_day_hourly_intervals,
      iv -> named_struct(
        "start_time",
        iv.start_time,
        "end_time",
        iv.end_time,
        "has_overlap",
        start_time_utc <= iv.end_time
        and end_time_utc >= iv.start_time
      )
    ) as all_time_intervals_with_overlap_flag
  from
    device_trip_intervals
)
select
  cast(trip_date as string) as date,
  org_id,
  device_id,
  case dayofweek(trip_date)
    when 1 then 'Sunday'
    when 2 then 'Monday'
    when 3 then 'Tuesday'
    when 4 then 'Wednesday'
    when 5 then 'Thursday'
    when 6 then 'Friday'
    when 7 then 'Saturday'
  end as day_of_week,
  date_format(tio.start_time, 'HH:mm:ss') as hour_start,
  date_format(tio.end_time, 'HH:mm:ss') as hour_end,
  any_value(tio.start_time) as hour_start_timestamp,
  any_value(tio.end_time) as hour_end_timestamp,
  max(tio.has_overlap) as on_trip
from
  device_trip_overlaps
  lateral view explode(all_time_intervals_with_overlap_flag) t as tio
group by
  trip_date,
  org_id,
  device_id,
  dayofweek(trip_date),
  date_format(tio.start_time, 'HH:mm:ss'),
  date_format(tio.end_time, 'HH:mm:ss')
"""

COLUMNS = [
    ColumnType.DATE,
    ColumnType.ORG_ID,
    ColumnType.DEVICE_ID,
    Column(
        name="day_of_week",
        type=DataType.STRING,
        nullable=False,
        metadata=Metadata(comment="Day of the week of the hourly bucket (e.g. Monday, Tuesday)."),
    ),
    Column(
        name="hour_start",
        primary_key=True,
        type=DataType.STRING,
        nullable=False,
        metadata=Metadata(comment="String-formatted hour the bucket starts on (e.g. 09:00)."),
    ),
    Column(
        name="hour_end",
        type=DataType.STRING,
        nullable=False,
        metadata=Metadata(comment="String-formatted hour the bucket ends on (e.g. 10:00)."),
    ),
    Column(
        name="hour_start_timestamp",
        type=DataType.TIMESTAMP,
        nullable=False,
        metadata=Metadata(comment="Timestamp (date + time) corresponding to the bucket's starting hour."),
    ),
    Column(
        name="hour_end_timestamp",
        type=DataType.TIMESTAMP,
        nullable=False,
        metadata=Metadata(comment="Timestamp correspondign to the bucket's ending hour."),
    ),
    Column(
        name="on_trip",
        type=DataType.BOOLEAN,
        nullable=False,
        metadata=Metadata(comment="Whether the device was on a trip during this hour."),
    ),
]


SCHEMA = columns_to_schema(*COLUMNS)
PRIMARY_KEYS = get_primary_keys(COLUMNS)
NON_NULL_COLUMNS = get_non_null_columns(COLUMNS)


@table(
    database=Database.DATAENGINEERING,
    description=build_table_description(
        table_desc="""
        This table tells which hours a VG-connected device was on a trip.
        It pivots datamodel_telematics.fct_trips to a longer format, where each
        device gets 24 hourly buckets and a flag indicating whether that device
        was on a trip sometime during that hour.
        """,
        row_meaning="""
        Each row corresponds to a (date, org_id, device_id, hour_start) combination.
        """,
        related_table_info={},
        table_type=TableType.TRANSACTIONAL_FACT,
        freshness_slo_updated_by="12pm PST",
    ),
    regions=get_all_regions(),
    owners=[DATAENGINEERING],
    schema=SCHEMA,
    upstreams=[
        "datamodel_telematics.fct_trips",
        "datamodel_core.dim_devices",
    ],
    primary_keys=PRIMARY_KEYS,
    partitioning=["date"],
    dq_checks=[
        NonEmptyDQCheck(
            name="dq_non_empty_fct_vehicle_hourly_on_trip_buckets",
            block_before_write=True,
        ),
        PrimaryKeyDQCheck(
            name="dq_pk_fct_vehicle_hourly_on_trip_buckets",
            primary_keys=PRIMARY_KEYS,
            block_before_write=True,
        ),
        NonNullDQCheck(
            name="dq_non_null_fct_vehicle_hourly_on_trip_buckets",
            non_null_columns=NON_NULL_COLUMNS,
            block_before_write=True,
        ),
    ],
    backfill_start_date="2024-01-01",
    backfill_batch_size=7,
    write_mode=WarehouseWriteMode.OVERWRITE,
)
def fct_vehicle_hourly_on_trip_buckets(context: AssetExecutionContext) -> str:
    return format_date_partition_query(QUERY, context)
