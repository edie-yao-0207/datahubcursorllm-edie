"""
CAN Trace ASC Chunks Asset

This asset processes CAN frames from fct_sampled_can_frames and converts them into
ASC (ASCII Vector CANoe) format chunks. Each trace is divided into chunks of 100,000
frames to enable efficient processing of large traces.
"""

from dagster import AssetExecutionContext
from dataweb import table, build_general_dq_checks
from dataweb.userpkgs.constants import (
    FIRMWAREVDP,
    FRESHNESS_SLO_9AM_PST,
    AWSRegion,
    Database,
    DQCheckMode,
    InstanceType,
    TableType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.firmware.constants import DATAWEB_PARTITION_DATE
from dataweb.userpkgs.firmware.schema import (
    Column,
    ColumnType,
    DataType,
    Metadata,
    columns_to_schema,
    get_non_null_columns,
    get_primary_keys,
)
from dataweb.userpkgs.firmware.table import ProductAnalyticsStaging
from dataweb.userpkgs.firmware.upstream import AnyUpstream

# Note: pandas UDF functions are defined locally to avoid serialization issues
from dataweb.userpkgs.query import (
    create_run_config_overrides,
    format_date_partition_query,
)
from dataweb.userpkgs.utils import build_table_description

# Spark imports for dataweb framework
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, floor, col

COLUMNS = [
    ColumnType.DATE,
    ColumnType.ORG_ID,
    ColumnType.DEVICE_ID,
    ColumnType.TRACE_UUID,
    Column(
        name="part_idx",
        type=DataType.INTEGER,
        nullable=False,
        primary_key=True,
        metadata=Metadata(comment="Chunk index within the trace (0-based)."),
    ),
    Column(
        name="asc_text",
        type=DataType.STRING,
        nullable=False,
        metadata=Metadata(comment="ASC format text content for this chunk."),
    ),
    Column(
        name="num_frames",
        type=DataType.INTEGER,
        nullable=False,
        metadata=Metadata(comment="Number of CAN frames in this chunk."),
    ),
    Column(
        name="start_ts",
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(
            comment="Timestamp of first frame in chunk (microseconds since Unix epoch)."
        ),
    ),
    Column(
        name="end_ts",
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(
            comment="Timestamp of last frame in chunk (microseconds since Unix epoch)."
        ),
    ),
    Column(
        name="chunk_size_bytes",
        type=DataType.INTEGER,
        nullable=False,
        metadata=Metadata(comment="Size of ASC text in bytes."),
    ),
]

SCHEMA = columns_to_schema(*COLUMNS)
PRIMARY_KEYS = get_primary_keys(COLUMNS)
NON_NULL_COLUMNS = get_non_null_columns(COLUMNS)

# Configuration constants
FRAMES_PER_CHUNK = 100000

QUERY = """
SELECT
    date,
    org_id,
    device_id,
    trace_uuid,
    timestamp_unix_us,
    source_interface,
    id,
    direction,
    payload
FROM {product_analytics_staging}.fct_sampled_can_frames
WHERE date BETWEEN "{date_start}" AND "{date_end}"
"""


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="This table contains CAN traces converted to ASC (ASCII Vector CANoe) format chunks. "
        "Each trace is divided into chunks of 100,000 frames to enable efficient processing. "
        "The ASC format is compatible with Vector CANoe and other automotive analysis tools.",
        row_meaning="Each row represents a chunk of CAN frames converted to ASC format for a specific trace.",
        table_type=TableType.TRANSACTIONAL_FACT,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=[AWSRegion.US_WEST_2],  # CAN recording only enabled in the US currently
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DATAWEB_PARTITION_DATE,
    upstreams=[
        AnyUpstream(ProductAnalyticsStaging.FCT_SAMPLED_CAN_FRAMES),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    single_run_backfill=True,
    run_config_overrides=create_run_config_overrides(
        driver_instance_type=InstanceType.RD_FLEET_2XLARGE,
        worker_instance_type=InstanceType.RD_FLEET_4XLARGE,
        max_workers=16,
    ),
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.FCT_CAN_TRACE_ASC_CHUNKS.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
    dq_check_mode=DQCheckMode.WHOLE_RESULT,
)
def fct_can_trace_asc_chunks(context: AssetExecutionContext) -> DataFrame:
    """
    Convert CAN frames to ASC format chunks.

    This asset processes CAN frames from fct_sampled_can_frames and converts them into
    ASC (ASCII Vector CANoe) format chunks. Each trace is divided into manageable chunks
    to enable processing of large traces that may contain millions of frames.

    Processing strategy:
    1. Query new CAN frames (traces not already processed)
    2. Add row numbers and divide into chunks of FRAMES_PER_CHUNK
    3. Convert each chunk to ASC format using pandas UDF
    4. Store results partitioned by trace_uuid for efficient access

    Args:
        context: Dagster asset execution context with partition information

    Returns:
        Spark DataFrame containing ASC chunks for the current partition
    """
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()

    # Get CAN frames for processing
    formatted_query = format_date_partition_query(QUERY, context)
    context.log.info(f"Executing query to get CAN frames:\n{formatted_query}")

    df = spark.sql(formatted_query)

    # Add chunking logic using window functions
    context.log.info(f"Dividing frames into chunks of {FRAMES_PER_CHUNK:,} frames each")

    window = Window.partitionBy("trace_uuid").orderBy("timestamp_unix_us")
    df_chunked = (
        df.withColumn("row_num", row_number().over(window))
        .withColumn("part_idx", floor((col("row_num") - 1) / FRAMES_PER_CHUNK))
        .drop("row_num")
    )

    # Define pandas UDF locally to avoid serialization issues
    def can_trace_chunk_to_asc(pdf):
        """
        Convert a chunk of CAN frames to ASC format.

        This function is designed to run as a pandas UDF on Spark worker nodes.
        It takes a pandas DataFrame containing CAN frames for a single trace_uuid
        and part_idx, and converts them to ASC format text.

        Args:
            pdf: pandas DataFrame with columns:
                - trace_uuid: String UUID of the trace
                - part_idx: Integer chunk index
                - timestamp_unix_us: Long timestamp in microseconds
                - source_interface: String interface name (e.g., "can0", "can1")
                - id: Long CAN message ID
                - direction: Long direction (1=TX, 2=RX)
                - payload: Binary payload data

        Returns:
            pandas DataFrame with ASC chunk information
        """
        import datetime
        import io
        import pandas as pd

        if pdf.empty:
            return pd.DataFrame(
                [
                    {
                        "trace_uuid": None,
                        "part_idx": None,
                        "asc_text": None,
                        "num_frames": 0,
                        "start_ts": 0,
                        "end_ts": 0,
                        "chunk_size_bytes": 0,
                    }
                ]
            ).dropna(subset=["trace_uuid"])

        trace_uuid = pdf["trace_uuid"].iloc[0]
        part_idx = pdf["part_idx"].iloc[0]
        df_sorted = pdf.sort_values("timestamp_unix_us")

        base_ts = df_sorted["timestamp_unix_us"].iloc[0] / 1_000_000
        header_time = datetime.datetime.fromtimestamp(base_ts).strftime(
            "%a %b %d %H:%M:%S %Y"
        )

        out = io.StringIO()

        # Write ASC header only for the first chunk
        if part_idx == 0:
            out.write(f"date {header_time}\n")
            out.write("base hex timestamps absolute\n")
            out.write("no internal events logged\n")

        for _, row in df_sorted.iterrows():
            # Calculate relative timestamp
            t = (
                row["timestamp_unix_us"] - df_sorted["timestamp_unix_us"].iloc[0]
            ) / 1_000_000

            # Extract interface number (assumes format like "can0", "can1")
            iface = row.get("source_interface", "")
            iface_num = int(iface[-1]) + 1 if iface and iface[-1].isdigit() else 0

            # Format CAN ID as hex
            can_id = format(row["id"] & 0x1FFFFFFF, "X")

            # Determine direction
            direction = "Tx" if row["direction"] == 1 else "Rx"

            # Convert payload to hex string
            payload_bytes = bytes(row["payload"]) if row["payload"] is not None else b""
            payload_hex = " ".join(f"{b:02X}" for b in payload_bytes)

            # Write ASC line
            out.write(
                f"{t:.6f} {iface_num}\t{can_id}x\t{direction}\td {len(payload_bytes)} {payload_hex}\n"
            )

        asc_str = out.getvalue()
        return pd.DataFrame(
            [
                {
                    "trace_uuid": trace_uuid,
                    "part_idx": part_idx,
                    "asc_text": asc_str,
                    "num_frames": len(df_sorted),
                    "start_ts": df_sorted["timestamp_unix_us"].iloc[0],
                    "end_ts": df_sorted["timestamp_unix_us"].iloc[-1],
                    "chunk_size_bytes": len(asc_str.encode("utf-8")),
                }
            ]
        )

    # Convert chunks to ASC format using pandas UDF
    context.log.info("Converting chunks to ASC format")

    # Define schema for the UDF output
    from pyspark.sql.types import (
        StructType,
        StructField,
        StringType,
        IntegerType,
        LongType,
    )

    chunk_schema = StructType(
        [
            StructField("trace_uuid", StringType(), False),
            StructField("part_idx", IntegerType(), False),
            StructField("asc_text", StringType(), False),
            StructField("num_frames", IntegerType(), False),
            StructField("start_ts", LongType(), False),
            StructField("end_ts", LongType(), False),
            StructField("chunk_size_bytes", IntegerType(), False),
        ]
    )

    result = df_chunked.groupBy(
        "date", "org_id", "device_id", "trace_uuid", "part_idx"
    ).applyInPandas(can_trace_chunk_to_asc, schema=chunk_schema)

    # Add back the date, org_id, device_id columns for partitioning
    # We need to rejoin with the original data to get these values
    trace_metadata = df_chunked.select(
        "trace_uuid", "date", "org_id", "device_id"
    ).distinct()

    final_result = result.join(trace_metadata, on="trace_uuid", how="inner").select(
        "date",
        "org_id",
        "device_id",
        "trace_uuid",
        "part_idx",
        "asc_text",
        "num_frames",
        "start_ts",
        "end_ts",
        "chunk_size_bytes",
    )

    chunk_count = final_result.count()
    context.log.info(f"Generated {chunk_count:,} ASC chunks")

    return final_result
