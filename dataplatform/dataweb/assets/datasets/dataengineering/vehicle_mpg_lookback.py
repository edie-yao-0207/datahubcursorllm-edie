from dagster import AssetExecutionContext
from dataweb import NonEmptyDQCheck, NonNullDQCheck, PrimaryKeyDQCheck, table
from dataweb.userpkgs.constants import (
    DATAENGINEERING,
    FRESHNESS_SLO_9AM_PST,
    AWSRegion,
    Database,
    InstanceType,
    TableType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.query import create_run_config_overrides
from dataweb.userpkgs.utils import build_table_description, partition_key_ranges_from_context, get_all_regions

@table(
    database=Database.DATAENGINEERING,
    description=build_table_description(
        table_desc="""Table which pre-computes rolling window averages of MPG at the device_id level. Uses the same logic as the cloud dashboard to compute MPG (meaning MPG in this table should match the value on the cloud dashboard for a given range).""",
        row_meaning="""Each row provides average MPG, total fuel and energy consumed, and total distance traveled at the device level over the previous window (window length given by the `lookback_window` column).""",
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
        {"name": "device_id", "type": "long", "nullable": False, "metadata": {"comment": "Unique identifier for devices. Joins to datamodel_core.dim_devices"}},
        {"name": "weighted_mpg", "type": "double", "nullable": True, "metadata": {"comment": "The weighted average of per-vehicle MPG for the given vehicle across the lookback window (364, 182, 91, 28, or 7 days)"}},
        {"name": "gaseous_mpg", "type": "double", "nullable": True, "metadata": {"comment": "The weighted average of per-vehicle gaseous MPG for the given vehicle across the lookback window (364, 182, 91, 28, or 7 days)"}},
        {"name": "total_fuel_consumed_gallons", "type": "double", "nullable": True, "metadata": {"comment": "The total amount of fuel (in gallons) that the vehicle consumed."}},
        {"name": "total_distance_traveled_miles", "type": "double", "nullable": True, "metadata": {"comment": "The total distance traveled (in miles) by the vehicle"}},
        {"name": "total_electric_distance_traveled_m", "type": "double", "nullable": True, "metadata": {"comment": "The total distnance traveled (in meters) by the electric vehicles."}},
        {"name": "total_energy_consumed_kwh", "type": "double", "nullable": True, "metadata": {"comment": "The total energy consumed (in kilowatt-hours) by the electric vehicles."}},
        {"name": "has_fuel_consumed", "type": "boolean", "nullable": True, "metadata": {"comment": "Whether any fuel was consumed or not"}},
    ],
    upstreams=["fuel_energy_efficiency_report.aggregated_vehicle_driver_rows_v4"],
    primary_keys=["date", "lookback_window", "org_id", "device_id"],
    partitioning=["date"],
    dq_checks=[NonEmptyDQCheck(name="dq_non_empty_vehicle_mpg_lookback"),
               PrimaryKeyDQCheck(name="dq_pk_vehicle_mpg_lookback", primary_keys=["date", "lookback_window", "org_id", "device_id"], block_before_write=True),
               NonNullDQCheck(name="dq_non_null_vehicle_mpg_lookback", non_null_columns=["org_id", "device_id"], block_before_write=True),
    ],
    backfill_start_date="2023-01-01",
    backfill_batch_size=3,
    write_mode=WarehouseWriteMode.OVERWRITE,
    run_config_overrides=create_run_config_overrides(
        driver_instance_type=InstanceType.RD_FLEET_2XLARGE,
        worker_instance_type=InstanceType.RD_FLEET_4XLARGE,
        max_workers=8,
    ),
)
def vehicle_mpg_lookback(context: AssetExecutionContext) -> str:
    partition_keys = partition_key_ranges_from_context(context)[0]
    PARTITION_START = partition_keys[0]
    PARTITION_END = partition_keys[-1]


    query = f"""
    WITH date_spine AS (
        SELECT explode(sequence(
            cast('{PARTITION_START}' as date),
            cast('{PARTITION_END}' as date),
            interval '1' day
        )) as report_date
    ),
    lookback_intervals AS (
        SELECT *
        FROM (VALUES
            ('364d', 363),  -- 364 days lookback
            ('334d', 333),  -- 334 days lookback (solely for YIR purposes)
            ('182d', 181),  -- 182 days lookback
            ('91d', 90),  -- 91 days lookback
            ('28d', 27),  -- 28 days lookback
            ('7d', 6)     -- 7 days lookback
        ) as T (interval_length, days_back)
    ),
    vg_devices as (
        select
            device_id
        from
            datamodel_core.dim_devices
        where
            date = (
            select
                max(date)
            from
                datamodel_core.dim_devices
            )
            and device_type = 'VG - Vehicle Gateway'
    ),
    per_vehicle as (
        select
            cast(d.report_date as string) as date,
            l.interval_length,
            a.org_id,
            a.object_id,
            sum(
                IF(
                    a.date between
                        cast(date_add(DAY, -l.days_back, d.report_date) as string)
                        and cast(d.report_date as string),
                    coalesce(fuel_consumed_ml, 0) + coalesce(fuel_consumed_ml_as_gaseous_type, 0),
                    NULL
                )
            ) as total_fuel_consumed_ml,
            sum(
                IF(
                    a.date between
                        cast(date_add(DAY, -l.days_back, d.report_date) as string)
                        and cast(d.report_date as string),
                    coalesce(energy_consumed_kwh, 0),
                    NULL
                )
            ) as total_energy_consumed_kwh,
            sum(
                IF(
                    a.date between
                        cast(date_add(DAY, -l.days_back, d.report_date) as string)
                        and cast(d.report_date as string),
                    coalesce(electric_distance_traveled_m, 0),
                    NULL
                )
            ) as total_electric_distance_traveled_m,
            sum(
                IF(
                    a.date between
                        cast(date_add(DAY, -l.days_back, d.report_date) as string)
                        and cast(d.report_date as string),
                    coalesce(distance_traveled_m, 0),
                    NULL
                )
            ) as total_distance_traveled_m,
            sum(
                IF(
                    a.date between
                        cast(date_add(DAY, -l.days_back, d.report_date) as string)
                        and cast(d.report_date as string),
                    coalesce(distance_traveled_m_original, 0),
                    NULL
                )
            ) as total_distance_traveled_m_original,
            sum(
                IF(
                    a.date between
                        cast(date_add(DAY, -l.days_back, d.report_date) as string)
                        and cast(d.report_date as string),
                    coalesce(gaseous_fuel_consumed_grams, 0),
                    NULL
                )
            ) as total_gaseous_fuel_consumed_grams,
            sum(
                IF(
                    a.date between
                        cast(date_add(DAY, -l.days_back, d.report_date) as string)
                        and cast(d.report_date as string),
                    coalesce(fuel_consumed_ml_original, 0) + coalesce(gaseous_fuel_consumed_grams_original, 0) + coalesce(energy_consumed_kwh_original, 0),
                    NULL
                )
            ) as total_original_consumption
        from date_spine d
        CROSS JOIN lookback_intervals l
        INNER JOIN fuel_energy_efficiency_report.aggregated_vehicle_driver_rows_v4 a
            ON a.date between
                cast(date_add(DAY, -l.days_back, d.report_date) as string)
                and cast(d.report_date as string)
        where a.object_id IN (select device_id from vg_devices)
        AND
            a.date between
            cast(cast(date '{PARTITION_START}' + interval '-363' day as string) as string)
            and
            '{PARTITION_END}'
        AND object_type = 1 -- Vehicle
        group by 1, 2, 3, 4
    ),
    per_vehicle_corrected_distance as (
        select
            date,
            interval_length,
            org_id,
            object_id,
            if(total_original_consumption = 0, total_distance_traveled_m_original, total_distance_traveled_m) as total_distance_traveled_m,
            total_fuel_consumed_ml,
            total_energy_consumed_kwh,
            total_electric_distance_traveled_m,
            total_gaseous_fuel_consumed_grams
        from per_vehicle
    ),
    mpg_per_vehicle as (
        select
            date,
            interval_length,
            org_id,
            object_id,
            CASE
            WHEN total_fuel_consumed_ml > 0
            and total_energy_consumed_kwh > 0 AND total_electric_distance_traveled_m > 0 THEN total_electric_distance_traveled_m / total_distance_traveled_m * 33705 / (
                total_energy_consumed_kwh * 1000 / (
                total_electric_distance_traveled_m * 0.0006213711922
                )
            ) + (
                1 - total_electric_distance_traveled_m / total_distance_traveled_m
            ) * (
                total_distance_traveled_m - total_electric_distance_traveled_m
            ) * 0.0006213711922 / (total_fuel_consumed_ml * 0.000264172)
            WHEN total_fuel_consumed_ml = 0
            AND total_energy_consumed_kwh > 0 AND total_electric_distance_traveled_m > 0 THEN 33705 / (
                total_energy_consumed_kwh * 1000 / (
                total_electric_distance_traveled_m * 0.0006213711922
                )
            )
            WHEN total_fuel_consumed_ml > 0
            THEN total_distance_traveled_m * 0.0006213711922 / (total_fuel_consumed_ml * 0.000264172)
            ELSE 0
            END weighted_mpge,
            CASE
            WHEN total_gaseous_fuel_consumed_grams > 0 THEN (total_distance_traveled_m * 0.0006213711922) / (
                total_gaseous_fuel_consumed_grams * 1.4 * 0.000264172
            )
            ELSE 0
            END gaseous_mpg,
            (total_fuel_consumed_ml) * 0.000264172 as total_fuel_consumed_gallons,
            total_distance_traveled_m * 0.0006213711922 as total_distance_traveled_miles,
            total_electric_distance_traveled_m as total_electric_distance_traveled_m,
            total_energy_consumed_kwh,
            abs(total_fuel_consumed_ml) + abs(total_energy_consumed_kwh) > 0 as has_fuel_consumed
        from
            per_vehicle_corrected_distance
    )
    select
        date,
        interval_length as lookback_window,
        org_id,
        object_id as device_id,
        -- The cloud dashboard limits reporting per-vehicle MPGe to the range [2, 200]. We apply the same filter
        -- to ensure org-level MPGe parity with the cloud dashboard.
        if(weighted_mpge between 2 and 200, weighted_mpge, NULL) as weighted_mpg,
        if(gaseous_mpg between 2 and 200, gaseous_mpg, NULL) as gaseous_mpg,
        cast(total_fuel_consumed_gallons as double) as total_fuel_consumed_gallons,
        total_distance_traveled_miles,
        cast(total_electric_distance_traveled_m as double) as total_electric_distance_traveled_m,
        cast(total_energy_consumed_kwh as double) as total_energy_consumed_kwh,
        has_fuel_consumed
    from
        mpg_per_vehicle
    """
    return query
