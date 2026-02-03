from dagster import AssetExecutionContext
from dataweb.userpkgs.query import create_run_config_overrides
from dataweb import (
    NonEmptyDQCheck,
    NonNullDQCheck,
    PrimaryKeyDQCheck,
    table,
)
from dataweb.userpkgs.constants import (
    DATAENGINEERING,
    Database,
    TableType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.schema import Column
from dataweb.userpkgs.utils import (
    build_table_description,
    get_all_regions,
)
from dataweb.userpkgs.firmware.schema import (
    Column,
    ColumnType,
    DataType,
    Metadata,
    columns_to_schema,
    get_primary_keys,
    get_non_null_columns,
    map_of,
)
from dataweb.userpkgs.query import format_date_partition_query


QUERY = """
with org_averages as (
  select
    date,
    org_id,
    hour_start,
    week_start,
    avg(rolling_12week_avg_proportion) rolling_12week_avg_proportion,
    sum(rolling_12week_total_on_trip) rolling_12week_total_trips
  from
    dataengineering.fct_vehicle_hourly_trip_trends
  where
    date between date(date_trunc("week", "{date_start}")) and "{date_start}"
  group by
    1,
    2,
    3,
    4
),
daily_org_averages as (
  select
    date,
    org_id,
    week_start,
    map_from_arrays(
      collect_list(hour_start), collect_list(rolling_12week_avg_proportion)
    ) as rolling_12week_avg_proportion_per_hour,
    map_from_arrays(
      collect_list(hour_start), collect_list(rolling_12week_total_trips)
    ) as rolling_12week_total_trips_per_hour
  from
    org_averages
  group by
    1,
    2,
    3
),
weekly_org_averages as (
  select
    "{date_start}" as date,
    org_id,
    week_start,
    map_from_arrays(
      collect_list(date), collect_list(rolling_12week_avg_proportion_per_hour)
    ) as daily_rolling_12week_avg_proportion_per_hour,
    map_from_arrays(
      collect_list(date), collect_list(rolling_12week_total_trips_per_hour)
    ) as daily_rolling_12week_total_trips_per_hour
  from
    daily_org_averages
  group by
    1,
    2,
    3
),
weekly_averages_with_default_values as (
  select
    *,
    -- For orgs with dates missing in the range between week_start and date,
    -- we want to fill in default values, which are maps from hours of the day to 0.
    -- The below columns build up that default value map which can be zipped with
    -- any incomplete metric maps.
    transform(sequence(date(week_start), date(date), interval 1 day), d -> string(d)) date_range,
    transform(
      sequence(timestamp(date), timestamp(date) + interval 23 hour, interval 1 hour),
      ts -> date_format(ts, 'HH:mm:ss')
    ) day_hours,
    map_from_arrays(day_hours, array_repeat(0D, size(day_hours))) as double_values,
    map_from_arrays(date_range, array_repeat(double_values, size(date_range))) default_double_values
  from
    weekly_org_averages
)
select
  date,
  org_id,
  week_start,
  map_zip_with(
    daily_rolling_12week_avg_proportion_per_hour,
    default_double_values,
    (day_of_week, map_with_metric_values, map_with_default_values) -> coalesce(
      map_with_metric_values, map_with_default_values
    )
  ) as daily_rolling_12week_avg_proportion_per_hour,
  map_zip_with(
    daily_rolling_12week_total_trips_per_hour,
    default_double_values,
    (day_of_week, map_with_metric_values, map_with_default_values) -> coalesce(
      map_with_metric_values, map_with_default_values
    )
  ) as daily_rolling_12week_total_trips_per_hour
from
  weekly_averages_with_default_values
"""

COLUMNS = [
    ColumnType.DATE,
    ColumnType.ORG_ID,
    Column(
        name="week_start",
        type=DataType.DATE,
        nullable=False,
        metadata=Metadata(comment="First day of the week for the data in the map."),
        primary_key=True,
    ),
    Column(
        name="daily_rolling_12week_avg_proportion_per_hour",
        type=map_of(
            DataType.STRING,
            map_of(DataType.STRING, DataType.DOUBLE, value_contains_null=True),
            value_contains_null=True,
        ),
        metadata=Metadata(
            comment="""
            Maps each day of the week (date range from `week_start` to `date`) to a 24-item map.
            Each nested map's keys are the hours of the day (00:00 - 23:00), and the values are
            the org-level 12 week rolling average of on-trip proportion for the given hour (inner key) + date (outer key) combination.
            """
        ),
    ),
    Column(
        name="daily_rolling_12week_total_trips_per_hour",
        type=map_of(
            DataType.STRING,
            map_of(DataType.STRING, DataType.DOUBLE, value_contains_null=True),
            value_contains_null=True,
        ),
        metadata=Metadata(
            comment="""
            Maps each day of the week (date range from `week_start` to `date`) to a 24-item map.
            Each nested map's keys are the hours of the day (00:00 - 23:00), and the values are
            the org-level 12 week total number of trips for the given hour (inner key) + date (outer key) combination.
            """
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
        This table provides org-level metrics for how often an org's vehicles are on a trip
        on a given date and hour of day. The metrics are formatted as map columns, which store
        the metric value for each date between the start of the week (the previous Monday) and the
        partition date (meaning the metric maps have at most 7 values). Each Sunday's data (with all
        7 of the previous day's metric values) will be loaded into DynamoDB to power org-level trends
        in the vehicle misuse page of the cloud dashboard, which means this is a production asset.
        """,
        row_meaning="""
        Each row corresponds to a (date, org_id) tuple, and contains metric map columns with values
        from the dates between `week_start` and `date`..
        """,
        related_table_info={},
        table_type=TableType.TRANSACTIONAL_FACT,
        freshness_slo_updated_by="12pm PST",
    ),
    regions=get_all_regions(),
    owners=[DATAENGINEERING],
    schema=SCHEMA,
    upstreams=[
        "dataengineering.fct_vehicle_hourly_trip_trends",
    ],
    primary_keys=PRIMARY_KEYS,
    partitioning=["date"],
    dq_checks=[
        NonEmptyDQCheck(
            name="dq_non_empty_fct_org_weekly_trip_trends",
            block_before_write=True,
        ),
        PrimaryKeyDQCheck(
            name="dq_pk_fct_org_weekly_trip_trends",
            primary_keys=PRIMARY_KEYS,
            block_before_write=True,
        ),
        NonNullDQCheck(
            name="dq_non_null_fct_org_weekly_trip_trends",
            non_null_columns=NON_NULL_COLUMNS,
            block_before_write=True,
        ),
    ],
    backfill_start_date="2024-01-01",
    backfill_batch_size=1,
    write_mode=WarehouseWriteMode.OVERWRITE,
)
def fct_org_weekly_trip_trends(context: AssetExecutionContext) -> str:
    return format_date_partition_query(QUERY, context)
