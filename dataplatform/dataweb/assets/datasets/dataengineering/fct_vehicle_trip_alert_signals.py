from dagster import AssetExecutionContext
from dataweb import (
    NonEmptyDQCheck,
    NonNullDQCheck,
    PrimaryKeyDQCheck,
    TrendDQCheck,
    table,
)
from dataweb.userpkgs.constants import (
    DATAENGINEERING,
    Database,
    TableType,
    WarehouseWriteMode,
    ALL_COMPUTE_REGIONS,
)
from dataweb.userpkgs.schema import Column
from dataweb.userpkgs.utils import (
    build_table_description,
)
from dataweb.userpkgs.firmware.schema import (
    Column,
    ColumnType,
    DataType,
    Metadata,
    columns_to_schema,
    get_primary_keys,
    get_non_null_columns,
    array_of,
    struct_of,
)
from dataweb.userpkgs.query import format_date_partition_query

QUERY = """
with org_trends as (
  -- aggregate org-level rolling 12-week average proportions for each hour and day
  select
    date,
    org_id,
    day_of_week,
    hour_start,
    avg(rolling_12week_avg_proportion) as rolling_12week_avg_proportion
  from
    dataengineering.fct_vehicle_hourly_trip_trends
  where
    date(date) between date('{date_end}') - interval 90 days and date('{date_end}')
  group by
    1,
    2,
    3,
    4
),
candidate_trips as (
  select
    date,
    date(date - interval 7 day) as match_date,
    org_id,
    device_id,
    driver_id,
    trip_type,
    duration_mins,
    distance_meters,
    start_time_ms,
    end_time_ms,
    trip_type,
    date_format(start_time_utc, 'HH:mm:ss') as start_time_only,
    date_format(
      from_unixtime(floor(unix_timestamp(start_time_utc) / (15 * 60)) * (15 * 60)), 'HH:mm:ss'
    ) as start_time_15min,
    date_format(
      from_unixtime(floor(unix_timestamp(start_time_utc) / (30 * 60)) * (30 * 60)), 'HH:mm:ss'
    ) as start_time_30min,
    date_format(
      from_unixtime(floor(unix_timestamp(start_time_utc) / (60 * 60)) * (60 * 60)), 'HH:mm:ss'
    ) as start_time_60min,
    date_format(
      from_unixtime(floor(unix_timestamp(end_time_utc) / (60 * 60)) * (60 * 60)), 'HH:mm:ss'
    ) as end_time_60min,
    cast(date_format(start_time_utc, 'EEEE') as string) as day_of_week,
    cast(date_format(end_time_utc, 'EEEE') as string) as day_of_week_end,
    lag(end_time_ms, 1) over (partition by org_id, device_id order by end_time_utc) as prev_end_time_ms,
    (start_time_ms - prev_end_time_ms) / (60 * 1000) AS dwell_from_prev_minutes
  from
    datamodel_telematics.fct_trips
  where
    date between date_sub('{date_start}', 2) and '{date_end}'
),
candidate_alert_flags as (
  select
    trips.org_id,
    trips.device_id,
    trips.driver_id,
    trips.date,
    trips.match_date,
    trips.day_of_week,
    trip_type,
    duration_mins,
    distance_meters,
    start_time_ms,
    end_time_ms,
    start_time_only,
    trips.start_time_60min,
    alert.rolling_4week_count,
    alert.rolling_8week_count,
    alert.rolling_12week_count,
    alert.rolling_16week_count,
    alert.rolling_26week_count,
    alert.rolling_4week_avg_proportion,
    alert.rolling_8week_avg_proportion,
    alert.rolling_12week_avg_proportion,
    alert.rolling_16week_avg_proportion,
    alert.rolling_26week_avg_proportion,
    dwell_from_prev_minutes,
    -- 0 cases
    case
      when alert.rolling_4week_avg_proportion = 0 then 1
      else 0
    end as alert_4week_interval_0,
    case
      when alert.rolling_8week_avg_proportion = 0 then 1
      else 0
    end as alert_8week_interval_0,
    case
      when alert.rolling_12week_avg_proportion = 0 then 1
      else 0
    end as alert_12week_interval_0,
    case
      when alert.rolling_16week_avg_proportion = 0 then 1
      else 0
    end as alert_16week_interval_0,
    case
      when alert.rolling_26week_avg_proportion = 0 then 1
      else 0
    end as alert_26week_interval_0,
    -- less than 1 in 10 weeks
    case
      when alert.rolling_4week_avg_proportion < 0.10 then 1
      else 0
    end as alert_4week_interval_10,
    case
      when alert.rolling_8week_avg_proportion < 0.10 then 1
      else 0
    end as alert_8week_interval_10,
    case
      when alert.rolling_12week_avg_proportion < 0.10 then 1
      else 0
    end as alert_12week_interval_10,
    case
      when alert.rolling_16week_avg_proportion < 0.10 then 1
      else 0
    end as alert_16week_interval_10,
    case
      when alert.rolling_26week_avg_proportion < 0.10 then 1
      else 0
    end as alert_26week_interval_10,
    -- less than 1 in 4 weeks
    case
      when alert.rolling_4week_avg_proportion < 0.25 then 1
      else 0
    end as alert_4week_interval_25,
    case
      when alert.rolling_8week_avg_proportion < 0.25 then 1
      else 0
    end as alert_8week_interval_25,
    case
      when alert.rolling_12week_avg_proportion < 0.25 then 1
      else 0
    end as alert_12week_interval_25,
    case
      when alert.rolling_16week_avg_proportion < 0.25 then 1
      else 0
    end as alert_16week_interval_25,
    case
      when alert.rolling_26week_avg_proportion < 0.25 then 1
      else 0
    end as alert_26week_interval_25,
    -- less than 1 in 2 weeks
    case
      when alert.rolling_4week_avg_proportion < 0.50 then 1
      else 0
    end as alert_4week_interval_50,
    case
      when alert.rolling_8week_avg_proportion < 0.50 then 1
      else 0
    end as alert_8week_interval_50,
    case
      when alert.rolling_12week_avg_proportion < 0.50 then 1
      else 0
    end as alert_12week_interval_50,
    case
      when alert.rolling_16week_avg_proportion < 0.50 then 1
      else 0
    end as alert_16week_interval_50,
    case
      when alert.rolling_26week_avg_proportion < 0.50 then 1
      else 0
    end as alert_26week_interval_50,
    case
      when org_trends.rolling_12week_avg_proportion < 0.10 then 1
      else 0
    end as alert_12week_interval_10_org,
    lag(
      case
        when alert.rolling_12week_avg_proportion = 0 then 1
        else 0
      end,
      1
    ) over (partition by trips.device_id order by start_time_ms) as lag1_alert_12week_interval_0
  from
    candidate_trips trips
      left join dataengineering.fct_vehicle_hourly_trip_trends alert
        on trips.org_id = alert.org_id
        and trips.device_id = alert.device_id
        and trips.match_date = alert.date
        and trips.day_of_week = alert.day_of_week
        and alert.hour_start in (trips.start_time_60min, trips.end_time_60min)
      left join org_trends
        on trips.org_id = org_trends.org_id
        and trips.match_date = org_trends.date
        and trips.day_of_week_end = org_trends.day_of_week
        and trips.start_time_60min = org_trends.hour_start
),
aggregated_trips as (
    select
    date,
    org_id,
    device_id,
    start_time_ms,
    end_time_ms,
    any_value(driver_id) as driver_id,
    any_value(trip_type) as trip_type,
    max(duration_mins) > 5 as is_trip_long_enough,
    max(distance_meters) > 1000 as is_trip_far_enough,
    min(alert_12week_interval_0) = 1 as is_trip_start_and_end_unusual,
    max(lag1_alert_12week_interval_0) = 0 as were_previous_trip_start_and_end_unusual,
    min(alert_12week_interval_10_org) = 1 as is_trip_unusual_for_org
    from
    candidate_alert_flags
    group by
    1,
    2,
    3,
    4,
    5
),
labeled_trips as (
select *,
    is_trip_long_enough and is_trip_far_enough and is_trip_start_and_end_unusual and is_trip_unusual_for_org as is_anomalous_trip
    from aggregated_trips
),
grouped_labeled_trips as (
select
    *,
    sum(
      if(
        (
          is_anomalous_trip <> coalesce(
            lag(is_anomalous_trip) over (
                partition by org_id, device_id, driver_id
                order by start_time_ms
              ),
            not is_anomalous_trip
          )
        ),
        1,
        0
      )
    ) over (partition by org_id, device_id, driver_id order by start_time_ms) as trip_group_index
  from
    labeled_trips

)
select
  date,
  org_id,
  device_id,
  driver_id,
  min(start_time_ms) as start_time_ms,
  max(end_time_ms) as end_time_ms,
  any_value(trip_type) as trip_type,
  min(start_time_ms)
  - lag(max(end_time_ms)) over (
      partition by org_id, device_id, driver_id
      order by max(end_time_ms)
    ) as ms_since_previous_alert,
  '1.1' as algorithm_version,
  count(1) as count_trips_within_anomaly,
  array_agg(
    named_struct("start_time_ms", start_time_ms, "end_time_ms", end_time_ms)
  ) as trip_timestamps_within_anomaly
from
  grouped_labeled_trips
where
  is_anomalous_trip
  and date between '{date_start}' and '{date_end}'
group by
  date,
  org_id,
  device_id,
  driver_id,
  trip_group_index

"""

COLUMNS = [
    ColumnType.DATE,
    ColumnType.ORG_ID,
    ColumnType.DEVICE_ID,
    Column(
        name="driver_id",
        type=DataType.LONG,
        metadata=Metadata(comment="Samsara ID of the trip's driver."),
        nullable=True,
    ),
    Column(
        name="start_time_ms",
        type=DataType.LONG,
        metadata=Metadata(
            comment="The start timestamp of the trip in epoch milliseconds"
        ),
        primary_key=True,
    ),
    Column(
        name="end_time_ms",
        type=DataType.LONG,
        metadata=Metadata(
            comment="The end timestamp of the trip in epoch milliseconds"
        ),
    ),
    Column(
        name="trip_type",
        type=DataType.STRING,
        metadata=Metadata(
            comment="Indicates the type of trip: locationbased, enginebased"
        ),
    ),
    Column(
        name="ms_since_previous_alert",
        type=DataType.LONG,
        metadata=Metadata(
            comment="Milliseconds since the end of the last trip that was signaled as anomalous"
        ),
        nullable=True,
    ),
    Column(
        name="algorithm_version",
        type=DataType.STRING,
        metadata=Metadata(
            comment="Version of the heuristic used to detect the anomaly",
        ),
    ),
    Column(
        name="count_trips_within_anomaly",
        type=DataType.LONG,
        metadata=Metadata(
            comment="Number of consecutive flagged trips between start and end time of thie anomaly",
        ),
    ),
    Column(
        name="trip_timestamps_within_anomaly",
        type=array_of(
            struct_of(("start_time_ms", DataType.LONG), ("end_time_ms", DataType.LONG))
        ),
        metadata=Metadata(
            comment="Start and end timestamps of consecutive flagged trips within the anomaly",
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
        Contains trips that are considered anomalous based on the hour of day
        they occurred.
        """,
        row_meaning="""
        Each row corresponds to a trip from fct_trips that matches anomaly conditions.
        """,
        related_table_info={},
        table_type=TableType.TRANSACTIONAL_FACT,
        freshness_slo_updated_by="12pm PST",
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[DATAENGINEERING],
    schema=SCHEMA,
    upstreams=[
        "datamodel_telematics.fct_trips",
        "dataengineering.fct_vehicle_hourly_trip_trends",
    ],
    primary_keys=PRIMARY_KEYS,
    partitioning=["date"],
    dq_checks=[
        NonEmptyDQCheck(
            name="dq_non_empty_fct_vehicle_trip_alert_signals",
            block_before_write=False,
        ),
        PrimaryKeyDQCheck(
            name="dq_pk_fct_vehicle_trip_alert_signals",
            primary_keys=PRIMARY_KEYS,
            block_before_write=True,
        ),
        NonNullDQCheck(
            name="dq_non_null_fct_vehicle_trip_alert_signals",
            non_null_columns=NON_NULL_COLUMNS,
            block_before_write=True,
        ),
        TrendDQCheck(
            name="dq_trend_fct_vehicle_trip_alert_signals",
            tolerance=0.5,
        ),
    ],
    backfill_start_date="2024-01-01",
    backfill_batch_size=3,
    write_mode=WarehouseWriteMode.OVERWRITE,
)
def fct_vehicle_trip_alert_signals(context: AssetExecutionContext) -> str:
    return format_date_partition_query(QUERY, context)
