from dagster import AssetExecutionContext
from dataweb import NonEmptyDQCheck, NonNullDQCheck, PrimaryKeyDQCheck, table
from dataweb.userpkgs.constants import (
    DATAENGINEERING,
    FRESHNESS_SLO_9AM_PST,
    AWSRegion,
    Database,
    TableType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.utils import build_table_description, partition_key_ranges_from_context, get_all_regions

@table(
    database=Database.DATAENGINEERING,
    description=build_table_description(
        table_desc="""Table which pre-computes rolling window averages of MPG at the org_id level. Uses the same logic as the cloud dashboard to compute MPG (meaning MPG in this table should match the value on the cloud dashboard for a given range).""",
        row_meaning="""Each row provides average MPG, total fuel and energy consumed, and total distance traveled at the org level over the previous window (window length given by the `lookback_window` column).""",
        related_table_info={},
        table_type=TableType.DAILY_DIMENSION,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=get_all_regions(),
    owners=[DATAENGINEERING],
    schema=[
        {"name": "date", "type": "string", "nullable": False, "metadata": {"comment": "The date of the report; i.e. the end date of the window for which mpg is calculated. YYYY-mm-dd."}},
        {"name": "lookback_window", "type": "string", "nullable": False, "metadata": {"comment": "Length of the report window. Currently `364d`, `182d`, `91d`, `28d` (28 day window), or `7d`."}},
        {"name": "org_id", "type": "long", "nullable": False, "metadata": {"comment": "The Samsara Cloud Dashboard ID of the organization."}},
        {"name": "weighted_mpg", "type": "double", "nullable": True, "metadata": {"comment": "The weighted average of per-vehicle MPG for the given org across the lookback window (364, 182, 91, 28, or 7 days)"}},
        {"name": "total_fuel_consumed_gallons", "type": "double", "nullable": True, "metadata": {"comment": "The total amount of fuel (in gallons) that the org consumed."}},
        {"name": "total_distance_traveled_miles", "type": "double", "nullable": True, "metadata": {"comment": "The total distance traveled (in miles) by the org's vehicles."}},
        {"name": "total_energy_consumed_kwh", "type": "double", "nullable": True, "metadata": {"comment": "The total energy consumed (in kilowatt-hours) by the org's electric vehicles."}},
    ],
    upstreams=["dataengineering.vehicle_mpg_lookback"],
    primary_keys=["date", "lookback_window", "org_id"],
    partitioning=["date"],
    dq_checks=[NonEmptyDQCheck(name="dq_non_empty_vehicle_mpg_lookback_by_org"),
               PrimaryKeyDQCheck(name="dq_pk_vehicle_mpg_lookback_by_org", primary_keys=["date", "lookback_window", "org_id"], block_before_write=True),
               NonNullDQCheck(name="dq_non_null_vehicle_mpg_lookback_by_org", non_null_columns=["org_id"], block_before_write=True),
    ],
    backfill_start_date="2023-01-01",
    backfill_batch_size=3,
    write_mode=WarehouseWriteMode.OVERWRITE,
)
def vehicle_mpg_lookback_by_org(context: AssetExecutionContext) -> str:
    partition_keys = partition_key_ranges_from_context(context)[0]
    PARTITION_START = partition_keys[0]
    PARTITION_END = partition_keys[-1]


    query = f"""
    select
    date,
    lookback_window,
    org_id,
    sum(
        IF(
        has_fuel_consumed
        and total_distance_traveled_miles > 0,
        weighted_mpg * total_distance_traveled_miles,
        NULL
        )
    ) / sum(
        IF(
        has_fuel_consumed
        and total_distance_traveled_miles > 0,
        total_distance_traveled_miles,
        NULL
        )
    ) weighted_mpg,
    cast(sum(total_fuel_consumed_gallons) as double) total_fuel_consumed_gallons,
    cast(sum(total_distance_traveled_miles) as double) total_distance_traveled_miles,
    cast(sum(total_energy_consumed_kwh) as double) total_energy_consumed_kwh
    from
    dataengineering.vehicle_mpg_lookback
    group by
    1,
    2,
    3
    """
    return query
