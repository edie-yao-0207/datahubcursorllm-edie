from functools import reduce
from typing import List, Optional

from dagster import AssetExecutionContext
from dataweb.userpkgs.query import create_run_config_overrides
from dataweb import NonEmptyDQCheck, NonNullDQCheck, TrendDQCheck, table
from dataweb.userpkgs.asset_helpers.predictive_maintenance_utils import (
    ObjectStatSource,
    ObjectStatSourceWithDataframe,
    stats,
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
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import array_max, broadcast, col

from dataweb.userpkgs.interval_helpers import (
    day_ms,
    intersect_intervals,
    string_to_date,
    to_ms,
)

schema = [
    Column(name="date", type="string", nullable=False, comment="Date (YYYY-mm-dd) the partition was loaded."),
    Column(name="org_id", type="long", nullable=False, comment="Cloud dashboard organization ID."),
    Column(name="device_id", type="long", nullable=False, comment="ID of the device the stats correspond to."),
    Column(name="time", type="double", nullable=False, comment="Timestamp of the most-frequently-updating stat (location)"),
] + [col.output_column for stat in stats for col in stat.value_columns]
schema = [column.as_dict() for column in schema]

@table(
    database=Database.DATAENGINEERING,
    description=build_table_description(
        table_desc="""
        This table provides object stats at the (org_id, device_id, time) level for each device with a J1939 connection.
        It has one row per device per most frequent stat update (location), or about one row per device
        every 5 seconds while the device is online. Stats whose refresh intervals are longer than 5 seconds are duplicated
        across rows between each update.
        """,
        row_meaning="""Each row provides the latest object stat values for a single (`date`, `org_id`, `device_id`) as of `time`.""",
        related_table_info={},
        table_type=TableType.TRANSACTIONAL_FACT,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    schema=schema,
    primary_keys=["date", "org_id", "device_id", "time"],
    partitioning=["date"],
    upstreams=[
        f"{upstream}" for upstream in [
            "clouddb.organizations",
            "kinesisstats_history.osdobdcableid",
        ] + [stat.table_name for stat in stats]
    ],
    dq_checks=[
        NonEmptyDQCheck(
            name="dq_non_empty_fct_combined_diagnostics", block_before_write=True
        ),
        NonNullDQCheck(
            name="dq_non_null_fct_combined_diagnostics",
            non_null_columns=["date", "org_id", "device_id", "time"],
            block_before_write=True,
        ),
        TrendDQCheck(
            name="dq_trend_fct_combined_diagnostics",
            lookback_days=1,
            tolerance=0.25,
            period="weeks"
        )
    ],
    backfill_start_date="2024-01-01",
    backfill_batch_size=3,
    write_mode=WarehouseWriteMode.OVERWRITE,
    regions=get_all_regions(),
    owners=[DATAENGINEERING],
    run_config_overrides=create_run_config_overrides(
        driver_instance_type=InstanceType.RD_FLEET_2XLARGE,
        worker_instance_type=InstanceType.RD_FLEET_4XLARGE,
        max_workers=16,
    ),
)
def fct_combined_diagnostics(context: AssetExecutionContext) -> DataFrame:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()

    partition_dates = partition_key_ranges_from_context(context)[0]
    start_date = partition_dates[0]
    end_date = partition_dates[-1]

    device_ids = spark.sql(
        f"""
    SELECT
        /*+ BROADCAST(o) */
        DISTINCT o.id as org_id,
        object_id as device_id
    FROM
        clouddb.organizations o
        INNER JOIN kinesisstats_history.osdobdcableid c ON o.id = c.org_id
    WHERE
        0 = 0
        AND date between cast(date_add(DAY, -6, date('{start_date}')) as string)
        and '{end_date}'
        AND o.internal_type = 0
        AND value.int_value in (1, 2, 5, 6, 7, 10, 11, 12, 14, 15, 17, 26)
    """
    )

    def get_objectstat_query(
        stat_source: ObjectStatSource,
        device_ids: DataFrame,
        org_ids: Optional[List[int]] = None,
    ) -> ObjectStatSourceWithDataframe:
        if org_ids is not None and any(org_ids):
            device_ids = device_ids.where(col("org_id").isin(org_ids))

        df = (
            spark.table(stat_source.table_name)
            .alias("o")
            .join(
                broadcast(device_ids),
                (col("o.org_id") == device_ids["org_id"])
                & (col(f"o.{stat_source.object_id_column}") == device_ids["device_id"]),
                how="leftsemi",
            )
            .where(col("o.date").between(start_date, end_date))
            .select(
                col("date"),
                col("time"),
                col("o.org_id"),
                col(f"o.{stat_source.object_id_column}").alias("device_id"),
                col("value"),
            )
        )

        return ObjectStatSourceWithDataframe(
            source=stat_source,
            df=stat_source.calculate_intervals(
                df,
                "value",
                to_ms(string_to_date(start_date)),
                to_ms(string_to_date(end_date)) + day_ms() - 1,
                spark=spark,
            )
            .withColumnRenamed("state_value", "value")
            .select("*", *stat_source.get_aliased_value_columns("value"))
            .drop("value", "state_value"),
        )

    stat_dfs = [
        get_objectstat_query(stat, device_ids)
        for stat in stats
    ]

    def join_stats_dataframes(
        combined_dataframe: DataFrame, next_stat: ObjectStatSourceWithDataframe
    ) -> DataFrame:
        return intersect_intervals(
            combined_dataframe,
            next_stat.df,
            right_hand_columns_to_keep=next_stat.source.final_table_value_columns,
            join_type="left",
            range_join_hint_interval_length_ms=next_stat.source.time_interval_bucket_ms,
        )


    all_but_first_stats = stat_dfs[1:]
    first_df = stat_dfs[0].df
    combined_stats = reduce(
        join_stats_dataframes,
        all_but_first_stats,
        first_df,
    )

    columns_for_output = ["date", "org_id", "device_id", "time"] + sorted(
        [column for stat in stats for _, column in stat._value_column_mapping()]
    )

    combined_stats_renamed = combined_stats.withColumnRenamed("start_ms", "time").select(
        *(columns_for_output)
    ).withColumn(
        "vehicle_fault_event_j1939_faults_mil_status",
        array_max("vehicle_fault_event_j1939_faults_mil_status"),
    ).withColumn(
        "vehicle_fault_event_j1939_faults_red_lamp_status",
        array_max("vehicle_fault_event_j1939_faults_red_lamp_status"),
    )

    return combined_stats_renamed
