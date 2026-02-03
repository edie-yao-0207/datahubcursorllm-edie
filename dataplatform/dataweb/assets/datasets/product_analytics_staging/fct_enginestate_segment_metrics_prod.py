from dagster import AssetExecutionContext
from pyspark.sql import DataFrame, SparkSession
from dataweb.userpkgs.firmware.constants import DATAWEB_PARTITION_DATE
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb import table, build_general_dq_checks
from dataweb.userpkgs.firmware.engine_state import (
    get_engine_state_segment_stats,
    ENGINE_STATE_SEGMENT_METRICS_SCHEMA,
    ENGINE_STATE_SEGMENT_METRICS_PRIMARY_KEYS,
    ENGINE_STATE_SEGMENT_METRICS_NON_NULL_COLUMNS,
)
from dataweb.userpkgs.firmware.table import (
    KinesisStatsHistory,
)
from dataweb.userpkgs.constants import (
    ALL_COMPUTE_REGIONS,
    FIRMWAREVDP,
    FRESHNESS_SLO_9AM_PST,
    Database,
    InstanceType,
    TableType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.utils import (
    build_table_description,
)
from dataweb.userpkgs.query import create_run_config_overrides


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="""
        Production engine state segment metrics table that analyzes signals during segments of time
        in which production engine state is reported in order to determine the accuracy of our
        engine state algorithm.
        """.strip(),
        row_meaning="""
        Each row represents a time segment with:
        - Reported production engine state
        - Signals (e.g. engine RPM, speed) emitted during this time segment.
        """.strip(),
        related_table_info={},
        table_type=TableType.STAGING,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[FIRMWAREVDP],
    schema=ENGINE_STATE_SEGMENT_METRICS_SCHEMA,
    primary_keys=ENGINE_STATE_SEGMENT_METRICS_PRIMARY_KEYS,
    partitioning=DATAWEB_PARTITION_DATE,
    upstreams=[
        AnyUpstream(KinesisStatsHistory.OSD_ENGINE_STATE),
        AnyUpstream(KinesisStatsHistory.OSD_ENGINE_GAUGE),
        AnyUpstream(KinesisStatsHistory.OSD_ENGINE_SECONDS),
        AnyUpstream(KinesisStatsHistory.OSD_ENGINE_MILLI_KNOTS),
        AnyUpstream(KinesisStatsHistory.OSD_POWER_STATE),
        AnyUpstream(KinesisStatsHistory.LOCATION),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    backfill_batch_size=2,
    run_config_overrides=create_run_config_overrides(
        driver_instance_type=InstanceType.MD_FLEET_4XLARGE,
        worker_instance_type=InstanceType.MD_FLEET_4XLARGE,
    ),
    dq_checks=build_general_dq_checks(
        asset_name="fct_enginestate_segment_metrics_prod",
        primary_keys=ENGINE_STATE_SEGMENT_METRICS_PRIMARY_KEYS,
        non_null_keys=ENGINE_STATE_SEGMENT_METRICS_NON_NULL_COLUMNS,
    ),
)
def fct_enginestate_segment_metrics_prod(
    context: AssetExecutionContext
) -> DataFrame:
    """
    Production engine state segment metrics table.

    Uses production engine state (osdenginestate) and power state (osdpowerstate)
    to generate segment metrics with signal data.
    """
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()

    return get_engine_state_segment_stats(
        context=context,
        spark=spark,
        engine_state_table="osdenginestate",
        power_state_table="osdpowerstate",
    )
