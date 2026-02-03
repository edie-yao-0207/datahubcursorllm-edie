from dagster import AssetExecutionContext, DailyPartitionsDefinition
from dataweb import NonEmptyDQCheck, NonNullDQCheck, PrimaryKeyDQCheck, table
from dataweb.userpkgs.constants import (
    AWSRegion,
    ColumnDescription,
    Database,
    DATAENGINEERING,
    TableType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.utils import (
    build_table_description,
    get_run_env,
    partition_key_ranges_from_context,
    get_all_regions
)
from dataweb.userpkgs.asset_helpers.vehicle_diagnostics_funnel_helpers import vehicle_class_mappings
from pyspark.sql import SparkSession, functions as F, types as T, Window, DataFrame
from functools import reduce
from pyspark.sql import Column
from dataclasses import dataclass

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
        "name": "name",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Signal ID",
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
        "name": "cable_name",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Name of cable connected to device.",
        },
    },
    {
        "name": "product_name",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Name of the VG model connected to the device.",
        },
    },
    {
        "name": "region",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Country code of region the device is located in.",
        },
    },
    {
        "name": "year",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Vehicle year",
        },
    },
    {
        "name": "make",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Vehicle make",
        },
    },
    {
        "name": "model",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Vehicle model",
        },
    },
    {
        "name": "engine_type_name",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Type of engine in the vehicle, e.g. ICE.",
        },
    },
    {
        "name": "is_signal_applicable",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether the signal applies to the vehicle, device, and cable.",
        },
    },
    {
        "name": "priority_level",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Priority of signal.",
        },
    },
    {
        "name": "signal_name",
        "type": "string",
        "nullable": False,
        "metadata": {
            "comment": "Human-readable signal name.",
        },
    },
    {
        "name": "total",
        "type": "long",
        "nullable": False,
        "metadata": {
            "comment": "Total value of the signal.",
        },
    },
    {
        "name": "p0_relevant_flag",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether the row is relevant to p0 signals.",
        },
    },
    {
        "name": "available_flag",
        "type": "boolean",
        "nullable": False,
        "metadata": {
            "comment": "Whether or not the signal is available.",
        },
    },
    {
        "name": "config_list",
        "type": {"type": "array", "elementType": {"type": "array", "elementType": "string", "containsNull": True}},
        "nullable": True,
        "metadata": {
            "comment": "List of signal configurations.",
        },
    },
    {
        "name": "classification",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Signal classification",
        },
    },
    {
        "name": "config_status",
        "type": "string",
        "nullable": True,
        "metadata": {
            "comment": "Status of the signal's configuration.",
        },
    },
]


@table(
    database=Database.DATAENGINEERING,
    description=build_table_description(
        table_desc="""
        Provides signal coverage at the device level for VG-connected devices.
        Used to power the [Diagnostics Attribution dashboard](https://10az.online.tableau.com/#/site/samsaradashboards/views/DIagnosticsAttribution-LimitedUse/AttributionOverview?:iid=1).
        """,
        row_meaning="""
        Each row corresponds to a device/signal pair.
        """,
        related_table_info={},
        table_type=TableType.TRANSACTIONAL_FACT,
        freshness_slo_updated_by="9am PST",
    ),
    regions=[AWSRegion.US_WEST_2, AWSRegion.EU_WEST_1],  # This uses a firmware table which isn't in CA yet
    owners=[DATAENGINEERING],
    schema=SCHEMA,
    upstreams=[
        "us-west-2:datamodel_core.dim_devices",
        "us-west-2:datamodel_core.dim_organizations",
        "us-west-2:datamodel_telematics.fct_trips_daily",
        "us-west-2:firmware_dev.agg_segment_availability_context_monthly",
        "us-west-2:firmware_dev.etl_device_first_order_statistics",
        "us-west-2:kinesisstats.osdvin",
        "us-west-2:product_analytics_staging.fct_telematics_products",
        "us-west-2:product_analytics_staging.stg_daily_fuel_type",
        "us-west-2:vindb_shards.device_vin_metadata",
    ],
    primary_keys=["date", "name", "device_id"],
    partitioning=["date"],
    dq_checks=[
        NonEmptyDQCheck(
            name="dq_non_empty_vehicle_diagnostics_funnel", block_before_write=True
        ),
        PrimaryKeyDQCheck(
            name="dq_pk_vehicle_diagnostics_funnel",
            primary_keys=["date", "name", "device_id"],
            block_before_write=True,
        ),
        NonNullDQCheck(
            name="dq_non_null_vehicle_diagnostics_funnel",
            non_null_columns=["device_id"],
            block_before_write=True,
        ),
    ],
    backfill_start_date="2025-01-01",
    backfill_batch_size=1,
    write_mode=WarehouseWriteMode.OVERWRITE,
)
def vehicle_diagnostics_funnel(context: AssetExecutionContext) -> DataFrame:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    partition_keys = partition_key_ranges_from_context(context)[0]
    PARTITION_START = partition_keys[0]

    devices_date = spark.table("datamodel_core.dim_devices").select(
        F.least(
            F.max(F.col("date")).cast("date"),
            F.last_day(
                F.date_add(
                    F.lit(PARTITION_START).cast("date"),
                    -14,
                )
            ),
        ).alias("filter_date")
    )
    filter_date = str(devices_date.collect()[0]["filter_date"])

    devices = (
        spark.table("datamodel_core.dim_devices")
        .alias("d")
        .join(
            spark.table("datamodel_core.dim_organizations").alias("o"),
            how="inner",
            on=["date", "org_id"],
        )
        .filter(
            (F.col("d.date") == F.lit(filter_date))
            & F.col("product_name").like("VG%")
            & (F.col("o.internal_type") == 0)
        )
        .select(
            "d.org_id",
            "d.device_id",
            "o.locale",
            "o.org_name",
            "d.product_id",
            "d.product_name",
            F.col("d.associated_vehicle_make").alias("make"),
            F.col("d.associated_vehicle_model").alias("model"),
            F.col("d.associated_vehicle_year").alias("year"),
        )
    )

    can_connected = (
        devices.alias("d")
        .join(
            spark.table("firmware_dev.etl_device_first_order_statistics").alias("b"),
            on=["org_id", "device_id"],
            how="left",
        )
        .filter(
            (
                F.col("b.date").between(
                    F.date_trunc("MONTH", F.date_add(F.lit(PARTITION_START), -14)),
                    F.last_day(F.date_add(F.lit(PARTITION_START), -14)),
                )
            )
            & (F.col("b.type") == "kinesisstats.osDCanConnected")
            & (F.col("b.field") == "value.int_value")
        )
        .groupBy("d.org_id", "b.device_id")
        .agg(
            F.min("b.min").alias("min"),
            F.max("b.max").alias("max"),
            F.sum("b.count").alias("count"),
            F.count_if(
                (F.col("b.stddev") > 0)
                | (F.col("b.min").isin(2, 3))
                | (F.col("b.max").isin(2, 3)),
            ).alias("count_days_supported"),
        )
    )

    cable_data = spark.sql(
        """
        select
        a.org_id
        , a.device_id
        , a.product_id
        , mode(b.mode) as mode
        , min(b.min) as min
        , max(b.max) as max
        , sum(b.count) as count
        , count(b.stddev > 0 or b.min in (2,3) or b.max in (2,3)) as count_days_supported

      from
        {devices} a

      left join
        firmware_dev.etl_device_first_order_statistics b
        on a.org_id = b.org_id
        and a.device_id = b.device_id

      where
        b.date between current_date() - interval 60 days and current_date()
        and b.type = "kinesisstats.osDObdCableId"
        and b.field = "value.int_value"

          group by all
            """,
        devices=devices,
    )

    vehicle_class_cases = reduce(
        lambda case_col, next_mapping: case_col.when(
            F.col("cable_name").isin(*next_mapping.cable_names), next_mapping.class_name
        ),
        vehicle_class_mappings[1:],
        F.when(
            F.col("cable_name").isin(*vehicle_class_mappings[0].cable_names),
            vehicle_class_mappings[0].class_name,
        ),
    )


    cable_connected = (
        cable_data.alias("a")
        .join(
            spark.table("product_analytics_staging.fct_telematics_products").alias("b"),
            how="left",
            on=[
                F.col("a.product_id") == F.col("b.product_id"),
                F.col("a.mode") == F.col("b.cable_id"),
            ],
        )
        .select("a.*", "b.cable_name", vehicle_class_cases.alias("vehicle_class"))
    )
    engine_state = spark.sql(
        """
        select
    a.org_id
    , a.device_id
    , min(b.min) as min
    , max(b.max) as max
    , sum(b.count) as count
    , count_if(b.stddev > 0 or b.min = 1 or b.max = 1) as count_days_supported

    from
    {devices} a

    join
    firmware_dev.etl_device_first_order_statistics b
    on a.org_id = b.org_id
    and a.device_id = b.device_id

    where
    b.date between date(date_trunc('MONTH', DATE_ADD({date}, -14))) and last_day(DATE_ADD({date}, -14))
    and b.type = "kinesisstats.osDEngineState"
    and b.field = "value.int_value"

    group by
    a.org_id
    , a.device_id
        """,
        date=PARTITION_START,
        devices=devices,
    )

    gps_distance_driven = spark.sql(
        """
        select
    a.org_id
    , a.device_id
    , min(b.total_distance_meters) as min
    , max(b.total_distance_meters) as max
    , count(b.total_distance_meters > 10000) as count_days_supported
    , sum(b.total_distance_meters) as sum
    , avg(b.total_distance_meters) as avg

    from
    {devices} a

    left join
    datamodel_telematics.fct_trips_daily b
    on a.org_id = b.org_id
    and a.device_id = b.device_id

    where
    b.date between date(date_trunc('MONTH', DATE_ADD({date}, -14))) and last_day(DATE_ADD({date}, -14))
    group by all
        """,
        devices=devices,
        date=PARTITION_START,
    )

    vin_decoding = spark.sql(
        """
        select
    a.org_id
    , a.device_id
    , b.make
    , b.model
    , b.year
    , b.engine_model
    , b.primary_fuel_type
    , b.secondary_fuel_type

    from
    {devices} a

    left join
    vindb_shards.device_vin_metadata b
    on a.org_id = b.org_id
    and a.device_id = b.device_id

    group by all
        """,
        devices=devices,
    )

    canonical_fuel_type = spark.sql(
        """
        with
    data as (
      select
        a.org_id
        , a.device_id
        , max((date, (engine_type, fuel_type) as values)).values as values

      from
        {devices} a

      join
        product_analytics_staging.stg_daily_fuel_type b
        on a.org_id = b.org_id
        and a.device_id = b.device_id

      where
        b.date between date(date_trunc('MONTH', DATE_ADD({date}, -14))) and last_day(DATE_ADD({date}, -14))

      group by all
    )

    select
    org_id
    , device_id
    , values.engine_type
    , values.fuel_type
    from
    data
        """,
        date=PARTITION_START,
        devices=devices,
    )

    vin = spark.sql(
        """
        select
    a.org_id
    , a.device_id
    , count(1) as count
    , count_if(value.proto_value.vin_event.vin is not null) as count_days_supported
    , count(distinct value.proto_value.vin_event.vin) as count_distinct
    from
    {devices} a
    inner join
    kinesisstats.osdvin b
    on
      a.org_id = b.org_id
      and a.device_id = b.object_id
    where
    b.date between date(date_trunc('MONTH', DATE_ADD({date}, -14))) and last_day(DATE_ADD({date}, -14))
    and not b.value.is_databreak
    and not b.value.is_end
    GROUP BY ALL
        """,
        date=PARTITION_START,
        devices=devices,
    )
    stats = [
        can_connected.alias("connected"),
        cable_connected.alias("cable"),
        engine_state.alias("engine_state"),
        gps_distance_driven.alias("gps_distance"),
        vin_decoding.alias("vin_decoding"),
        canonical_fuel_type.alias("cft_fuel_type"),
        vin.alias("vin"),
    ]

    joined = (
        reduce(
            lambda df1, df2: df1.join(
                df2, how="left_outer", on=["org_id", "device_id"]
            ),
            stats,
            devices.alias("device"),
        )
        .select(
            F.col("device.org_id"),
            F.col("device.device_id"),
            F.col("device.org_name"),
            F.col("device.product_name"),
            F.coalesce(F.col("cable.cable_name"), F.lit("unknown")).alias("cable_name"),
            F.coalesce(F.col("cable.vehicle_class"), F.lit("unknown")).alias(
                "vehicle_class"
            ),
            F.coalesce(F.col("device.make"), F.col("vin_decoding.make")).alias("make"),
            F.coalesce(F.col("device.model"), F.col("vin_decoding.model")).alias(
                "model"
            ),
            F.coalesce(F.col("device.year"), F.col("vin_decoding.year")).alias("year"),
            F.col("vin_decoding.engine_model").alias("vin_engine_model"),
            F.coalesce(F.col("cft_fuel_type.engine_type"), F.lit("unknown")).alias(
                "engine_type"
            ),
            F.coalesce(F.col("cft_fuel_type.fuel_type"), F.lit("unknown")).alias(
                "fuel_type"
            ),
            # A candidate device has driven at least 10km in
            # a day in the last 60 days
            F.coalesce(F.col("gps_distance.avg"), F.lit(0)).alias(
                "avg_gps_distance_meters"
            ),
            F.coalesce(
                F.col("gps_distance.count_days_supported") > 0, F.lit(False)
            ).alias("is_gps_distance_driven"),
            F.coalesce(F.col("vin.count_days_supported") > 0, F.lit(False)).alias(
                "is_vin_supported"
            ),
            F.coalesce(F.col("vin.count_distinct"), F.lit(0)).alias(
                "distinct_count_vin"
            ),
            F.coalesce(F.col("connected.count_days_supported") > 0, F.lit(False)).alias(
                "is_connected"
            ),
            (
                F.col("vin_decoding.make").isNotNull()
                & F.col("vin_decoding.model").isNotNull()
                & F.col("vin_decoding.year").isNotNull()
            ).alias("is_vin_decoding_supported"),
            (
                F.col("vin_decoding.make").eqNullSafe(F.col("device.make"))
                & F.col("vin_decoding.model").eqNullSafe(F.col("device.model"))
                & F.col("vin_decoding.year").eqNullSafe(F.col("device.year"))
            ).alias("is_decoded_vin_equal_to_product_vin"),
            F.col("cable.cable_name").isNotNull().alias("is_cable_supported"),
            F.coalesce(
                F.col("engine_state.count_days_supported") > 0, F.lit(False)
            ).alias("is_engine_state_supported"),
            F.col("cable.mode").alias("cable_id"),
            F.col("cable.product_id"),
        )
        .withColumn(
            "is_missing_any_mmyef",
            (
                F.col("make").isNull()
                | F.col("model").isNull()
                | F.col("year").isNull()
                | (F.col("engine_type") == "unknown")
                | (F.col("fuel_type") == "unknown")
            ),
        )
    )


    @dataclass
    class FailureReason:
        reason_name: str
        condition: Column

    failure_reasons = [
        FailureReason(
            reason_name="0001_NO_DIAGNOSTICS_POWER_ONLY",
            condition=F.col("vehicle_class") == "power_only",
        ),
        FailureReason(
            reason_name="0050_NO_MMYEF_NO_VIN",
            condition=(~F.col("is_vin_supported") & F.col("is_missing_any_mmyef")),
        ),
        FailureReason(
            reason_name="0051_NO_MMYEF_DECODING_ERROR",
            condition=(F.col("is_vin_supported") & F.col("is_missing_any_mmyef")),
        ),
        FailureReason(
            reason_name="0200_ERR_ON_DRIVE_NO_CABLE_DETECTED",
            condition=(
                ~F.col("is_cable_supported") | (F.col("vehicle_class") == "unknown")
            ),
        ),
        FailureReason(
            reason_name="400_ERR_ON_DRIVE_NO_CONNECTION",
            condition=(~F.col("is_connected")),
        ),
    ]

    default_failure_reason = FailureReason(
        reason_name="1000_ERR_AE_NEEDED", condition=F.lit(True)
    )
    failure_reason_cases = reduce(
        lambda case_col, next_reason: case_col.when(
            next_reason.condition, next_reason.reason_name
        ),
        failure_reasons[1:],
        F.when(failure_reasons[0].condition, failure_reasons[0].reason_name),
    ).otherwise(default_failure_reason.reason_name)

    fail_reason = joined.select(
        "org_id",
        "device_id",
        "cable_id",
        "product_id",
        failure_reason_cases.alias("classification"),
    )

    def get_priority_from_signal_name(name_column):
        return F.split_part(name_column, F.lit("_"), F.lit(1))

    def format_signal_name(name_column):
        """
        Formats the signal name string by removing priority and switching underscores to spaces.
        e.g. `P50_Average_Line_to_Line_ACRMS_Voltage` is formatted to `Average Line to Line ACRMS Voltage`.
        """

        split_name = F.split(name_column, F.lit("_"))
        return F.array_join(F.slice(split_name, 2, F.size(split_name)), delimiter=" ")

    def normalize_product_name(product_name_column):
        """
        Normalizes VG55 product names to VG54.
        """
        return F.replace(product_name_column, search=F.lit("55"), replace=F.lit("54"))

    agg_data = (
        spark.table("firmware_dev.agg_segment_availability_context_monthly")
        .filter(
            F.col("date").between(
                F.date_trunc("month", F.date_add(F.lit(PARTITION_START), -14)),
                F.date_add(F.lit(PARTITION_START), -14),
            )
            & F.col("product_name").startswith("VG")
            & F.col("is_availability_candidate")
            & (get_priority_from_signal_name(F.col("name")) == F.lit("P0"))
        )
        .withColumn("priority_level", get_priority_from_signal_name(F.col("name")))
        .withColumn("signal_name", format_signal_name(F.col("name")))
        .withColumn("product_name", normalize_product_name(F.col("product_name")))
        .groupBy(
            "date",
            "name",
            "org_id",
            "device_id",
            "cable_name",
            "product_name",
            "region",
            "make",
            "model",
            "year",
            "engine_type_name",
            "is_signal_applicable",
            "priority_level",
            "signal_name",
        )
        .agg(
            F.count_if(F.col("is_available")).alias("available"),
            F.count(F.expr("*")).alias("total"),
        )
    )


    p0_relevant_cases = (
        F.when(
            F.col("signal_name").isin(
                "VIN", "Engine State", "Odometer", "Vehicle Speed"
            ),
            F.lit(True),
        )
        .when(
            F.col("signal_name").isin("Fuel Consumption", "Fuel Level")
            & (F.col("region") == "MX")
            & (F.col("engine_type_name") == "ICE"),
            F.lit(True),
        )
        .when(F.col("priority_level") != "P0", F.lit(True))
        .otherwise(F.col("is_signal_applicable"))
    )

    agg_data = agg_data.withColumn("p0_relevant_flag", p0_relevant_cases)

    segment_availability = (
        spark.table("firmware_dev.agg_segment_availability_context_monthly")
        .filter(
            (F.col("date") >= "2024-01-01")
            & F.col("is_availability_candidate")
            & get_priority_from_signal_name(F.col("name")).isin(
                *[F.lit(priority) for priority in ["P0", "P1", "P2", "P3"]]
            )
            & F.col("product_name").startswith("VG")
        )
        .withColumn("product_name", normalize_product_name(F.col("product_name")))
    )

    output = (
        segment_availability.groupBy(
            "date",
            "region",
            "name",
            "cable_name",
            "product_name",
            "make",
            "model",
            "year",
            "engine_type_name",
        )
        .agg(
            F.max(F.col("is_signal_applicable")).alias("is_signal_applicable"),
            F.count_if(F.col("is_available")).alias("available_count"),
            F.count(F.expr("*")).alias("total_count"),
        )
        .withColumn(
            "total_for_cable_availability",
            F.when(F.col("total_count") >= 20, F.col("total_count")),
        )
        .withColumn(
            "available_count_for_cable_availability",
            F.when(F.col("total_count") >= 20, F.col("available_count")),
        )
        .groupBy(
            "date",
            "region",
            "name",
            "make",
            "model",
            "year",
            "engine_type_name",
        )
        .agg(
            F.max(F.col("is_signal_applicable")).alias("is_signal_applicable"),
            F.sum(F.col("available_count")).alias("available_count"),
            F.sum(F.col("total_count")).alias("total_count"),
            F.max(
                F.col("available_count_for_cable_availability")
                / F.col("total_for_cable_availability")
            ).alias("cable_available_pct"),
            F.array_distinct(
                F.array_agg(
                    F.array(
                        F.col("product_name"),
                        F.col("cable_name"),
                    ),
                )
            ).alias("config_list"),
        )
        .withColumn("priority_level", get_priority_from_signal_name(F.col("name")))
        .withColumn("signal_name", format_signal_name(F.col("name")))
        .withColumn(
            "available_flag",
            F.when(F.col("available_count") / F.col("total_count") >= 0.2, F.lit(True))
            .when(F.col("cable_available_pct") >= 0.2, F.lit(True))
            .otherwise(F.lit(False)),
        )
    )


    available_flag_window = (
        Window.orderBy("date")
        .partitionBy("name", "make", "model", "year", "engine_type_name")
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    )

    signal_availability = (
        output.withColumn(
            "available_flag_long",
            F.max(F.when(F.col("total_count") >= 20, F.col("available_flag"))).over(
                available_flag_window
            ),
        )
        .withColumn(
            "available_flag", F.coalesce("available_flag_long", "available_flag")
        )
        .withColumn(
            "P0_relevant_flag",
            F.when(
                (F.col("priority_level") == "P0") & (F.col("signal_name") != "VIN"),
                F.col("is_signal_applicable"),
            ).otherwise(F.lit(True)),
        )
        .withColumn("count_check", F.coalesce("total_count", F.lit(0)) >= 20)
        .filter(
            F.col("date").between(
                F.date_trunc("month", F.date_add(F.lit(PARTITION_START), -14)),
                F.date_add(F.lit(PARTITION_START), -14),
            )
            & (F.col("region") != "EMEA")
        )
        .withColumn("config", F.explode("config_list"))
        .groupBy(
            "date",
            "region",
            "priority_level",
            "signal_name",
            "make",
            "model",
            "year",
            "engine_type_name",
            "is_signal_applicable",
            "available_flag",
            "P0_relevant_flag",
            "count_check",
            "total_count",
        )
        .agg(
            F.array_distinct(
                F.array_agg(
                    F.when(F.col("cable_available_pct") >= 0.2, F.col("config"))
                )
            ).alias("config_list")
        )
    )

    result = (
        agg_data.alias("o")
        .join(fail_reason, how="left", on=["device_id"])
        .join(
            signal_availability.alias("a"),
            how="left",
            on=[
                "make",
                "model",
                "year",
                "engine_type_name",
                "region",
                "priority_level",
                "signal_name",
            ],
        )
        .select(
            "o.*",
            "a.available_flag",
            "a.config_list",
            F.when(~F.col("o.p0_relevant_flag"), "N/A")
            .when(F.col("available") > 0, "OK")
            .when(
                F.col("available_flag")
                & (F.col("classification") == "1000_ERR_AE_NEEDED")
                & ~F.array_contains(
                    F.col("config_list"),
                    F.array(F.col("product_name"), F.col("cable_name")),
                ),
                "0300_EQUIPMENT_SWAP_NEEDED",
            )
            .otherwise(F.col("classification"))
            .alias("classification"),
            F.when(F.col("available") == 1, "Yes")
            .when(
                (F.array_size(F.col("config_list")) == 0) & F.col("available_flag"),
                "No - Swap",
            )
            .when(
                F.array_contains(
                    F.col("config_list"),
                    F.array(F.col("product_name"), F.col("cable_name")),
                ),
                "No - Good Config",
            )
            .when(F.col("available_flag"), "No - Swap")
            .otherwise("No - Unknown")
            .alias("config_status"),
        )
        .select(
            F.lit(PARTITION_START).alias("date"),
            "name",
            "org_id",
            "device_id",
            "cable_name",
            "product_name",
            "region",
            "year",
            "make",
            "model",
            "engine_type_name",
            "is_signal_applicable",
            "priority_level",
            "signal_name",
            "total",
            "p0_relevant_flag",
            "available_flag",
            "config_list",
            "classification",
            "config_status",
        )
    )

    result.explain()
    return result
