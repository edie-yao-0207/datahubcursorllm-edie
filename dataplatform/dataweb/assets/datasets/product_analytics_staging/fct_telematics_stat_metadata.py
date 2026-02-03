from enum import Enum
from typing import List
from collections import Counter
import os

from dagster import AssetExecutionContext
from dataweb import build_general_dq_checks, table
from dataweb.userpkgs.constants import (
    ALL_COMPUTE_REGIONS,
    FIRMWAREVDP,
    FRESHNESS_SLO_9AM_PST,
    Database,
    TableType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.firmware.kinesisstats import KinesisStatsMetric
from dataweb.userpkgs.firmware.metric import MetricEnum, check_unique_metric_strings
from dataweb.userpkgs.firmware.schema import (
    Column,
    ColumnType,
    DataType,
    Metadata,
    columns_to_names,
    columns_to_schema,
    dataweb_to_spark_schema,
    convert_spark_schema_to_pandas_dtypes,
    create_pandas_dataframe_with_schema,
    get_primary_keys,
)
from dataweb.userpkgs.firmware.table import ProductAnalyticsStaging
from dataweb.userpkgs.utils import (
    build_table_description,
    schema_to_columns_with_property,
)
from pyspark.sql import DataFrame, Row, SparkSession
import pyspark.sql.functions as F

from . import (
    fct_osdenginefault,
    fct_hourlyfuelconsumption,
    stg_daily_faults,
    fct_osdvin,
)


class TableColumn(Enum):
    def __str__(self):
        return str(self.value)

    PRODUCT_AREA = Column(
        name="product_area",
        type=DataType.STRING,
        nullable=False,
        primary_key=True,
        metadata=Metadata(
            comment="Product area such as Diagnostics, Refridgeration Units, etc."
        ),
    )
    SUB_PRODUCT_AREA = Column(
        name="sub_product_area",
        type=DataType.STRING,
        nullable=False,
        primary_key=True,
        metadata=Metadata(
            comment="Sub product area, like Maintenance, Electric Vehicles, etc."
        ),
    )
    SIGNAL_NAME = Column(
        name="signal_name",
        type=DataType.STRING,
        nullable=False,
        primary_key=True,
        metadata=Metadata(comment="Signal data name."),
    )
    DEFAULT_PRIORITY = Column(
        name="default_priority",
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(
            comment="The default priority applied to a diagnostic. Lower is higher priority."
        ),
    )
    METRIC_NAME = Column(
        name="metric_name",
        type=DataType.STRING,
        nullable=False,
        primary_key=True,
        metadata=Metadata(
            comment="Name of the stat in the formatting of a column name",
        ),
    )
    TYPE = Column(
        name="type",
        type=DataType.STRING,
        nullable=False,
        primary_key=True,
        metadata=Metadata(
            comment="Type identifier for the metric (duplicate of metric_name for compatibility)",
        ),
    )
    DEFINITION_TABLE = Column(
        name="definition_table",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(
            comment="(Optional) Definition table name, populated if the value represents an enumeration. Allows for translation to readable words."
        ),
    )
    VALUE_MIN = Column(
        name="value_min",
        type=DataType.DOUBLE,
        nullable=True,
        metadata=Metadata(
            comment="Minimum value of the signal. If the min and max are the same, the boundry is not set."
        ),
    )
    VALUE_MAX = Column(
        name="value_max",
        type=DataType.DOUBLE,
        nullable=True,
        metadata=Metadata(
            comment="Maximum value of the signal. If the min and max are the same, the boundry is not set."
        ),
    )
    DELTA_MIN = Column(
        name="delta_min",
        type=DataType.DOUBLE,
        nullable=True,
        metadata=Metadata(
            comment="Minimum delta of a signal. A delta is a change in value between time ordered data points. If the min and max are the same, the boundry is not set."
        ),
    )
    DELTA_MAX = Column(
        name="delta_max",
        type=DataType.DOUBLE,
        nullable=True,
        metadata=Metadata(
            comment="Maximum delta of a signal. A delta is a change in value between time ordered data points. If the min and max are the same, the boundry is not set."
        ),
    )
    DELTA_VELOCITY_MIN = Column(
        name="delta_velocity_min",
        type=DataType.DOUBLE,
        nullable=True,
        metadata=Metadata(
            comment="Minimum delta velocity of a signal. A delta is a change in value between time ordered data points, over the amount of time. If the min and max are the same, the boundry is not set."
        ),
    )
    DELTA_VELOCITY_MAX = Column(
        name="delta_velocity_max",
        type=DataType.DOUBLE,
        nullable=True,
        metadata=Metadata(
            comment="Maximum delta velocity of a signal. A delta is a change in value between time ordered data points, over the amount of time. If the min and max are the same, the boundry is not set."
        ),
    )
    DOCUMENTATION_LINK = Column(
        name="documentation_link",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(
            comment="Associated documentation link related to the siganl. More historic system information may not be available."
        ),
    )
    METRIC_TABLE = Column(
        name="metric_table",
        type=DataType.STRING,
        nullable=True,
        primary_key=True,
        metadata=Metadata(comment="Name of the table where the metric is stored."),
    )
    METRIC_COLUMN = Column(
        name="metric_column",
        type=DataType.STRING,
        nullable=True,
        primary_key=True,
        metadata=Metadata(
            comment="Name of the column in the metric table where the metric is stored."
        ),
    )
    EXPORTED_TO_DATA_LAKE = Column(
        name="exported_to_data_lake",
        type=DataType.BOOLEAN,
        nullable=False,
        metadata=Metadata(comment="Whether the metric is exported to the data lake."),
    )
    IS_APPLICABLE_ICE = Column(
        name="is_applicable_ice",
        type=DataType.BOOLEAN,
        nullable=False,
        metadata=Metadata(
            comment="Whether this signal is applicable to Internal Combustion Engine (ICE) vehicles"
        ),
    )
    IS_APPLICABLE_HYDROGEN = Column(
        name="is_applicable_hydrogen",
        type=DataType.BOOLEAN,
        nullable=False,
        metadata=Metadata(
            comment="Whether this signal is applicable to hydrogen fuel cell vehicles"
        ),
    )
    IS_APPLICABLE_HYBRID = Column(
        name="is_applicable_hybrid",
        type=DataType.BOOLEAN,
        nullable=False,
        metadata=Metadata(
            comment="Whether this signal is applicable to hybrid vehicles"
        ),
    )
    IS_APPLICABLE_PHEV = Column(
        name="is_applicable_phev",
        type=DataType.BOOLEAN,
        nullable=False,
        metadata=Metadata(
            comment="Whether this signal is applicable to Plug-in Hybrid Electric Vehicles (PHEV)"
        ),
    )
    IS_APPLICABLE_BEV = Column(
        name="is_applicable_bev",
        type=DataType.BOOLEAN,
        nullable=False,
        metadata=Metadata(
            comment="Whether this signal is applicable to Battery Electric Vehicles (BEV)"
        ),
    )
    IS_APPLICABLE_UNKNOWN = Column(
        name="is_applicable_unknown",
        type=DataType.BOOLEAN,
        nullable=False,
        metadata=Metadata(
            comment="Whether this signal is applicable to vehicles with unknown powertrain type"
        ),
    )


# Create the column list once for reuse
# Include OBD_VALUE from ColumnType since it's a common column type
TABLE_COLUMNS = [
    TableColumn.PRODUCT_AREA.value,
    TableColumn.SUB_PRODUCT_AREA.value,
    TableColumn.SIGNAL_NAME.value,
    TableColumn.DEFAULT_PRIORITY.value,
    TableColumn.METRIC_NAME.value,
    TableColumn.TYPE.value,
    TableColumn.DEFINITION_TABLE.value,
    TableColumn.VALUE_MIN.value,
    TableColumn.VALUE_MAX.value,
    TableColumn.DELTA_MIN.value,
    TableColumn.DELTA_MAX.value,
    TableColumn.DELTA_VELOCITY_MIN.value,
    TableColumn.DELTA_VELOCITY_MAX.value,
    TableColumn.DOCUMENTATION_LINK.value,
    ColumnType.OBD_VALUE.value,
    TableColumn.METRIC_TABLE.value,
    TableColumn.METRIC_COLUMN.value,
    TableColumn.EXPORTED_TO_DATA_LAKE.value,
    TableColumn.IS_APPLICABLE_ICE.value,
    TableColumn.IS_APPLICABLE_HYDROGEN.value,
    TableColumn.IS_APPLICABLE_HYBRID.value,
    TableColumn.IS_APPLICABLE_PHEV.value,
    TableColumn.IS_APPLICABLE_BEV.value,
    TableColumn.IS_APPLICABLE_UNKNOWN.value,
]

SCHEMA = columns_to_schema(*TABLE_COLUMNS)

NON_NULL_COLUMNS = schema_to_columns_with_property(SCHEMA, "nullable")

METRIC_ENUMS: List[MetricEnum] = [
    KinesisStatsMetric,
    fct_osdenginefault.TableMetric,
    fct_hourlyfuelconsumption.TableMetric,
    stg_daily_faults.TableMetric,
    fct_osdvin.TableMetric,
]

PRIMARY_KEYS: List[str] = get_primary_keys(TABLE_COLUMNS)
OUTPUT_SCHEMA = dataweb_to_spark_schema(TABLE_COLUMNS)

METRICS: List[MetricEnum] = [
    enumeration for enumerations in METRIC_ENUMS for enumeration in enumerations
]

# Verify all metrics have distinct signal_names
_signal_name_counts = {}
for metric in METRICS:
    signal_name = metric.metadata.signal_name
    if signal_name in _signal_name_counts:
        _signal_name_counts[signal_name].append(str(metric))
    else:
        _signal_name_counts[signal_name] = [str(metric)]

_duplicate_signal_names = {
    signal_name: metrics
    for signal_name, metrics in _signal_name_counts.items()
    if len(metrics) > 1
}

assert len(_duplicate_signal_names) == 0, (
    f"Duplicate signal_names found in telematics metrics: "
    f"{', '.join(f'{signal_name}: {metrics}' for signal_name, metrics in _duplicate_signal_names.items())}"
)

# Verify all metrics have distinct labels
_label_counts = {}
for metric in METRICS:
    label = metric.label
    if label in _label_counts:
        _label_counts[label].append(str(metric))
    else:
        _label_counts[label] = [str(metric)]

_duplicate_labels = {
    label: metrics for label, metrics in _label_counts.items() if len(metrics) > 1
}

assert len(_duplicate_labels) == 0, (
    f"Duplicate labels found in telematics metrics: "
    f"{', '.join(f'{label}: {metrics}' for label, metrics in _duplicate_labels.items())}"
)

# Verify no duplicate entries across key fields
_duplicate_entries = [
    item
    for item, count in Counter(
        [
            (
                data.type,
                data.field,
                data.metadata.product_area,
                data.metadata.sub_product_area,
                data.metadata.signal_name,
            )
            for data in METRICS
        ]
    ).items()
    if count > 1
]

assert (
    len(_duplicate_entries) == 0
), f"Duplicate entries found in telematics metrics: {_duplicate_entries}"

# Verify all metrics have unique string representations (used for partitions)
for metric_enum in METRIC_ENUMS:
    check_unique_metric_strings(metric_enum)

# Ensure all required fields are present for DQ checks
missing_required_fields = [
    data
    for data in METRICS
    if not (
        data.type
        and data.field
        and data.metadata.product_area
        and data.metadata.sub_product_area
        and data.metadata.signal_name
    )
]

assert (
    len(missing_required_fields) == 0
), f"Metrics with missing required fields: {missing_required_fields}"

RAW_ROWS = [
    {
        TableColumn.PRODUCT_AREA: metric.metadata.product_area,
        TableColumn.SUB_PRODUCT_AREA: metric.metadata.sub_product_area,
        TableColumn.SIGNAL_NAME: metric.metadata.signal_name,
        TableColumn.DEFAULT_PRIORITY: metric.metadata.default_priority,
        TableColumn.METRIC_NAME: str(metric),
        TableColumn.TYPE: str(metric),
        TableColumn.DEFINITION_TABLE: metric.metadata.definition_table,
        TableColumn.VALUE_MIN: metric.metadata.value_min,
        TableColumn.VALUE_MAX: metric.metadata.value_max,
        TableColumn.DELTA_MIN: metric.metadata.delta_min,
        TableColumn.DELTA_MAX: metric.metadata.delta_max,
        TableColumn.DELTA_VELOCITY_MIN: metric.metadata.delta_velocity_min,
        TableColumn.DELTA_VELOCITY_MAX: metric.metadata.delta_velocity_max,
        TableColumn.DOCUMENTATION_LINK: metric.metadata.documentation_link,
        ColumnType.OBD_VALUE: metric.metadata.obd_value,
        TableColumn.METRIC_TABLE: str(metric.type),
        TableColumn.METRIC_COLUMN: str(metric.field),
        TableColumn.EXPORTED_TO_DATA_LAKE: not metric.value.excluded_from_data_lake,
        TableColumn.IS_APPLICABLE_ICE: metric.metadata.is_applicable_ice,
        TableColumn.IS_APPLICABLE_HYDROGEN: metric.metadata.is_applicable_hydrogen,
        TableColumn.IS_APPLICABLE_HYBRID: metric.metadata.is_applicable_hybrid,
        TableColumn.IS_APPLICABLE_PHEV: metric.metadata.is_applicable_phev,
        TableColumn.IS_APPLICABLE_BEV: metric.metadata.is_applicable_bev,
        TableColumn.IS_APPLICABLE_UNKNOWN: metric.metadata.is_applicable_unknown,
    }
    for metric in METRICS
]

CLEANED_ROWS = [{str(k): v for k, v in row.items()} for row in RAW_ROWS]
OUTPUT_DATA = [Row(**row) for row in CLEANED_ROWS]

# Generate CSV export using pandas
import pandas as pd


# Helper functions are now imported from dataweb.userpkgs.firmware.schema


def export_telematics_stat_metadata_csv():
    """Export telematics stat metadata to CSV using pandas with schema-based typing."""
    # Automatically convert OUTPUT_SCHEMA to pandas dtype mapping
    schema_mapping = convert_spark_schema_to_pandas_dtypes(OUTPUT_SCHEMA)

    # Create DataFrame with schema (similar to Spark's createDataFrame)
    df = create_pandas_dataframe_with_schema(CLEANED_ROWS, schema_mapping)

    # Define output path
    output_path = "/tmp/telematics_stat_metadata.csv"

    # Write to CSV with proper formatting
    df.to_csv(output_path, index=False, float_format="%.6f")

    print(f"Exported telematics stat metadata CSV to {output_path}")
    print(f"Exported {len(df)} metrics to CSV")

    # Print sample values to verify types
    print("Sample values with correct types:")
    for col in [
        str(TableColumn.VALUE_MIN),
        str(TableColumn.VALUE_MAX),
        str(TableColumn.DELTA_MIN),
        str(TableColumn.DELTA_MAX),
    ]:
        if col in df.columns and not df[col].isna().all():
            sample_val = (
                df[col].dropna().iloc[0] if not df[col].dropna().empty else "N/A"
            )
            print(f"  {col}: {sample_val} (dtype: {df[col].dtype})")

    return output_path

# NOTE: Set EXPORT_TELEMATICS_CSV=1 to export updates to the master Google Sheet
# https://docs.google.com/spreadsheets/d/1Yp47ISelelLvVkFp3yyYdZvy2hkxYEJk6lDSkVNrWT8/edit?gid=1777009324#gid=1777009324
# Export CSV
if os.environ.get("EXPORT_TELEMATICS_CSV"):
    csv_path = export_telematics_stat_metadata_csv()
    print(f"Exported telematics stat metadata CSV to: {csv_path}")


@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Prioritization of signals for telematics products. Prorities are based on feedback from Telematics product.",
        row_meaning="Each row represents signals and their associated metadata",
        related_table_info={},
        table_type=TableType.STAGING,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    write_mode=WarehouseWriteMode.OVERWRITE,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.FCT_TELEMATICS_STAT_METADATA.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
)
def fct_telematics_stat_metadata(context: AssetExecutionContext) -> DataFrame:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()

    return spark.createDataFrame(
        schema=OUTPUT_SCHEMA,
        data=OUTPUT_DATA,
    )
