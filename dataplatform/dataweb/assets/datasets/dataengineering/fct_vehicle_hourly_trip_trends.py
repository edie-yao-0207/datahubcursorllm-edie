from dagster import AssetExecutionContext
from dataweb.userpkgs.query import create_run_config_overrides
from dataweb import (
    NonEmptyDQCheck,
    NonNullDQCheck,
    TrendDQCheck,
    PrimaryKeyDQCheck,
    table,
)
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
with trip_intervals as (
  select
    date,
    org_id,
    device_id,
    day_of_week,
    hour_start,
    hour_start_timestamp,
    cast(on_trip as int) as on_trip
  from
    {dataengineering}.fct_vehicle_hourly_on_trip_buckets
  where date between date_add('{date_start}', -181) and '{date_end}'
),
device_date_ranges as (
  select
    org_id,
    device_id,
    date(min(date)) as earliest_trip_date,
    date(max(date)) as latest_trip_date
  from
    trip_intervals
  group by
    org_id,
    device_id
),
device_hourly_calendar as (
  select
    org_id,
    device_id,
    explode(
      sequence(timestamp(earliest_trip_date), timestamp(latest_trip_date), interval 1 hour)
    ) as hour_start_timestamp,
    0 as on_trip
  from
    device_date_ranges
),
filled_trip_intervals_with_weeks as (
  select
    coalesce(t.date, cast(date(c.hour_start_timestamp) as string)) as date,
    coalesce(t.org_id, c.org_id) as org_id,
    coalesce(t.device_id, c.device_id) as device_id,
    coalesce(t.day_of_week, date_format(c.hour_start_timestamp, 'EEEE')) as day_of_week,
    coalesce(t.hour_start, date_format(c.hour_start_timestamp, 'HH:mm:ss')) as hour_start,
    coalesce(t.hour_start_timestamp, c.hour_start_timestamp) as hour_start_timestamp,
    date_trunc('week', coalesce(date(t.date), date(c.hour_start_timestamp))) as week_start,
    coalesce(t.on_trip, c.on_trip) as on_trip
  from trip_intervals t
  full outer join device_hourly_calendar c
    on t.org_id = c.org_id
    and t.device_id = c.device_id
    and t.hour_start_timestamp = c.hour_start_timestamp

),
rolling_metrics as (
  select
    date,
    org_id,
    device_id,
    day_of_week,
    hour_start,
    cast(week_start as date) as week_start,
    any_value(hour_start_timestamp) as hour_start_timestamp,
    avg(on_trip) as proportion_on_trip_this_week,
    avg(avg(on_trip)) over (rolling_4week) as rolling_4week_avg_proportion,
    sum(avg(on_trip)) over (rolling_4week) as rolling_4week_total_on_trip,
    count(1) over (rolling_4week) as rolling_4week_count,
    avg(avg(on_trip)) over (rolling_8week) as rolling_8week_avg_proportion,
    sum(avg(on_trip)) over (rolling_8week) as rolling_8week_total_on_trip,
    count(1) over (rolling_8week) as rolling_8week_count,
    avg(avg(on_trip)) over (rolling_12week) as rolling_12week_avg_proportion,
    sum(avg(on_trip)) over (rolling_12week) as rolling_12week_total_on_trip,
    count(1) over (rolling_12week) as rolling_12week_count,
    avg(avg(on_trip)) over (rolling_16week) as rolling_16week_avg_proportion,
    sum(avg(on_trip)) over (rolling_16week) as rolling_16week_total_on_trip,
    count(1) over (rolling_16week) as rolling_16week_count,
    avg(avg(on_trip)) over (rolling_26week) as rolling_26week_avg_proportion,
    sum(avg(on_trip)) over (rolling_26week) as rolling_26week_total_on_trip,
    count(1) over (rolling_26week) as rolling_26week_count
  from
    filled_trip_intervals_with_weeks
  group by
    date,
    org_id,
    device_id,
    day_of_week,
    hour_start,
    week_start
  window
    rolling_4week as (
      PARTITION BY device_id, day_of_week, hour_start
      ORDER BY week_start
      ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
    ),
    rolling_8week as (
      PARTITION BY device_id, day_of_week, hour_start
      ORDER BY week_start
      ROWS BETWEEN 7 PRECEDING AND CURRENT ROW
    ),
    rolling_12week as (
      PARTITION BY device_id, day_of_week, hour_start
      ORDER BY week_start
      ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
    ),
    rolling_16week as (
      PARTITION BY device_id, day_of_week, hour_start
      ORDER BY week_start
      ROWS BETWEEN 15 PRECEDING AND CURRENT ROW
    ),
    rolling_26week as (
      PARTITION BY device_id, day_of_week, hour_start
      ORDER BY week_start
      ROWS BETWEEN 25 PRECEDING AND CURRENT ROW
    )
)
select
  *
from
  rolling_metrics
where
  -- Filter out dates that were only needed
  -- to compute weekly rolling metrics
  date between '{date_start}' and '{date_end}'
"""

COLUMNS = [
    ColumnType.DATE,
    ColumnType.ORG_ID,
    ColumnType.DEVICE_ID,
    Column(
        name="day_of_week",
        type=DataType.STRING,
        nullable=False,
        metadata=Metadata(comment="Day of week (e.g. Monday, Tuesday)."),
    ),
    Column(
        name="hour_start",
        type=DataType.STRING,
        primary_key=True,
        nullable=False,
        metadata=Metadata(comment="String-formatted hour of day for the bucket."),
    ),
    Column(
        name="week_start",
        type=DataType.DATE,
        nullable=False,
        metadata=Metadata(comment="First date of the week of the bucket."),
    ),
    Column(
        name="hour_start_timestamp",
        type=DataType.TIMESTAMP,
        nullable=False,
        metadata=Metadata(comment="Timestamp (date and time) of the hourly bucket."),
    ),
    Column(
        name="proportion_on_trip_this_week",
        type=DataType.DOUBLE,
        nullable=False,
        metadata=Metadata(
            comment="Whether the vehicle was on a trip this week during this hour (will always be 1 or 0)."
        ),
    ),
    Column(
        name="rolling_4week_avg_proportion",
        type=DataType.DOUBLE,
        nullable=False,
        metadata=Metadata(
            comment="Proportion of weeks when the vehicle was on a trip during this hour over the past 4 weeks."
        ),
    ),
    Column(
        name="rolling_4week_total_on_trip",
        type=DataType.DOUBLE,
        nullable=False,
        metadata=Metadata(
            comment="Total number of weeks when the vehicle was on a trip during this hour over the past 4 weeks."
        ),
    ),
    Column(
        name="rolling_4week_count",
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(
            comment="Count of rows where this device appears during this hour over the past 4 weeks."
        ),
    ),
    Column(
        name="rolling_8week_avg_proportion",
        type=DataType.DOUBLE,
        nullable=False,
        metadata=Metadata(
            comment="Proportion of weeks when the vehicle was on a trip during this hour over the past 8 weeks."
        ),
    ),
    Column(
        name="rolling_8week_total_on_trip",
        type=DataType.DOUBLE,
        nullable=False,
        metadata=Metadata(
            comment="Total number of weeks when the vehicle was on a trip during this hour over the past 8 weeks."
        ),
    ),
    Column(
        name="rolling_8week_count",
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(
            comment="Count of rows where this device appears during this hour over the past 8 weeks."
        ),
    ),
    Column(
        name="rolling_12week_avg_proportion",
        type=DataType.DOUBLE,
        nullable=False,
        metadata=Metadata(
            comment="Proportion of weeks when the vehicle was on a trip during this hour over the past 12 weeks."
        ),
    ),
    Column(
        name="rolling_12week_total_on_trip",
        type=DataType.DOUBLE,
        nullable=False,
        metadata=Metadata(
            comment="Total number of weeks when the vehicle was on a trip during this hour over the past 12 weeks."
        ),
    ),
    Column(
        name="rolling_12week_count",
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(
            comment="Count of rows where this device appears during this hour over the past 12 weeks."
        ),
    ),
    Column(
        name="rolling_16week_avg_proportion",
        type=DataType.DOUBLE,
        nullable=False,
        metadata=Metadata(
            comment="Proportion of weeks when the vehicle was on a trip during this hour over the past 16 weeks."
        ),
    ),
    Column(
        name="rolling_16week_total_on_trip",
        type=DataType.DOUBLE,
        nullable=False,
        metadata=Metadata(
            comment="Total number of weeks when the vehicle was on a trip during this hour over the past 16 weeks."
        ),
    ),
    Column(
        name="rolling_16week_count",
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(
            comment="Count of rows where this device appears during this hour over the past 16 weeks."
        ),
    ),
    Column(
        name="rolling_26week_avg_proportion",
        type=DataType.DOUBLE,
        nullable=False,
        metadata=Metadata(
            comment="Proportion of weeks when the vehicle was on a trip during this hour over the past 26 weeks."
        ),
    ),
    Column(
        name="rolling_26week_total_on_trip",
        type=DataType.DOUBLE,
        nullable=False,
        metadata=Metadata(
            comment="Total number of weeks when the vehicle was on a trip during this hour over the past 26 weeks."
        ),
    ),
    Column(
        name="rolling_26week_count",
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(
            comment="Count of rows where this device appears during this hour over the past 26 weeks."
        ),
    ),
]

SCHEMA = columns_to_schema(*COLUMNS)
PRIMARY_KEYS = get_primary_keys(COLUMNS)
NON_NULL_COLUMNS = get_non_null_columns(COLUMNS)


@table(
    database=Database.DATAENGINEERING,
    description=build_table_description(
        table_desc="""
        This table provides historical counts and proportions of how many weeks a VG-connected device
        was on a trip during each hour of the day over 4, 8, 12, 16, and 26 week windows. Each window includes
        one hourly bucket per week, so the result provides a rolling baseline of how likely a vehicle
        is to be on a trip during a particular hour of the day given the day of the week.
        """,
        row_meaning="""
        Each row corresponds to a (date, org_id, device_id, hour_start) tuple.
        """,
        related_table_info={},
        table_type=TableType.TRANSACTIONAL_FACT,
        freshness_slo_updated_by="12pm PST",
    ),
    regions=get_all_regions(),
    owners=[DATAENGINEERING],
    schema=SCHEMA,
    upstreams=[
        "dataengineering.fct_vehicle_hourly_on_trip_buckets",
    ],
    primary_keys=PRIMARY_KEYS,
    partitioning=["date"],
    dq_checks=[
        NonEmptyDQCheck(
            name="dq_non_empty_fct_vehicle_hourly_trip_trends",
            block_before_write=True,
        ),
        PrimaryKeyDQCheck(
            name="dq_pk_fct_vehicle_hourly_trip_trends",
            primary_keys=PRIMARY_KEYS,
            block_before_write=True,
        ),
        NonNullDQCheck(
            name="dq_non_null_fct_vehicle_hourly_trip_trends",
            non_null_columns=NON_NULL_COLUMNS,
            block_before_write=True,
        ),
    ],
    backfill_start_date="2024-01-01",
    backfill_batch_size=7,
    write_mode=WarehouseWriteMode.OVERWRITE,
)
def fct_vehicle_hourly_trip_trends(context: AssetExecutionContext) -> str:
    return format_date_partition_query(QUERY, context)
