from dataclasses import asdict, dataclass
from enum import Enum
from typing import List, Tuple, Union

from dataweb.userpkgs.constants import ColumnDescription
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    BinaryType,
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
)


@dataclass
class ConfluenceLink:
    NONE = ""
    FIRMWARE_HIERARCHY = (
        "https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/5876986/Firmware+Builds+Release+Tools+Overview#Firmware-Hierarchy",
    )
    PRODUCT_VERSIONING = "https://samsaradev.atlassian.net/l/c/VBb75518"


@dataclass
class DataType:
    LONG = "long"
    STRING = "string"
    BYTE = "byte"
    BINARY = "binary"
    INTEGER = "integer"
    DOUBLE = "double"
    ARRAY = "array"
    STRUCT = "struct"
    MAP = "map"
    BOOLEAN = "boolean"
    DATE = "date"
    TIMESTAMP = "timestamp"
    SHORT = "short"


def array_of(element_type: Union[DataType, dict], contains_null: bool = True) -> dict:
    """
    Create an array type definition.

    Args:
        element_type: DataType enum or complex type dict (e.g., struct definition)
        contains_null: Whether array elements can be null (defaults to True)

    Returns:
        dict: Array type definition with explicit containsNull property
    """
    return {
        "type": DataType.ARRAY,
        "elementType": (
            str(element_type) if isinstance(element_type, DataType) else element_type
        ),
        "containsNull": contains_null,
    }


def map_of(
    key_type: Union[DataType, str],
    value_type: Union[DataType, dict],
    value_contains_null: bool = False,
) -> dict:
    """Create a MAP type definition.

    Args:
        key_type: The key type (must be a primitive type)
        value_type: The value type (can be complex)
        value_contains_null: Whether values can be null (defaults to False for consistency)

    Returns:
        Dictionary defining a MAP type for DataWeb
    """
    return {
        "type": DataType.MAP,
        "keyType": str(key_type) if isinstance(key_type, DataType) else key_type,
        "valueType": (
            str(value_type) if isinstance(value_type, DataType) else value_type
        ),
        "valueContainsNull": value_contains_null,
    }


def struct_of(*fields: Tuple[str, DataType]) -> dict:
    return {
        "type": DataType.STRUCT,
        "fields": [
            {"name": field[0], "type": field[1], "nullable": True} for field in fields
        ],
    }


def struct_with_comments(*fields) -> dict:
    """Enhanced struct_of that supports field descriptions.

    Args:
        *fields: Tuples of (name, DataType, description) or (name, DataType)
    """
    result_fields = []
    for field in fields:
        if len(field) == 3:  # (name, type, description)
            field_def = {
                "name": field[0],
                "type": field[1],
                "nullable": True,
                "metadata": {"comment": field[2]},
            }
        else:  # (name, type) - backward compatibility
            field_def = {"name": field[0], "type": field[1], "nullable": True}
        result_fields.append(field_def)

    return {
        "type": DataType.STRUCT,
        "fields": result_fields,
    }


def struct_from_columns(*columns, force_nullable: bool = False) -> dict:
    """
    Create a struct type definition from Column objects or ColumnType enums.

    Unlike struct_of() which takes (name, type) tuples, this function works with
    existing Column definitions, preserving their metadata and type information.

    Args:
        *columns: Column objects or ColumnType enum values
        force_nullable: If True, forces all struct fields to be nullable=True
                       (useful for nested structures in maps/arrays)

    Returns:
        dict: Struct type definition with DataType.STRUCT and fields list

    Example:
        struct_from_columns(
            ColumnType.APPLICATION_ID,     # Uses existing ColumnType definition
            ColumnType.DATA_IDENTIFIER,    # Preserves original types and nullability
            my_custom_column              # Works with custom Column objects
        )
    """
    # Map DataType enum to Spark SQL type names
    type_mapping = {
        DataType.LONG: "long",
        DataType.INTEGER: "integer",
        DataType.DOUBLE: "double",
        DataType.STRING: "string",
        DataType.BOOLEAN: "boolean",
        DataType.DATE: "date",
        DataType.BINARY: "binary",
    }

    fields = []
    for col in columns:
        # Explicitly handle Column objects and ColumnType enum values
        if isinstance(col, Column):
            # Direct Column object
            column = col
        elif (
            isinstance(col, Enum)
            and hasattr(col, "value")
            and isinstance(col.value, Column)
        ):
            # ColumnType enum containing a Column
            column = col.value
        else:
            # Invalid type - fail explicitly
            raise TypeError(
                f"struct_from_columns() expects Column objects or ColumnType enums, "
                f"got {type(col).__name__}: {col}"
            )

        # Determine nullable setting
        if force_nullable:
            # Force all fields to be nullable (for nested map/array structures)
            field_nullable = True
        else:
            # Use the original column's nullable setting
            field_nullable = column.nullable

        field = {
            "name": column.name,
            "type": type_mapping.get(column.type, str(column.type).lower()),
            "nullable": field_nullable,
            "metadata": {
                "comment": column.metadata.comment.value
                if hasattr(column.metadata.comment, "value")
                else str(column.metadata.comment),
                "confluence": column.metadata.confluence
                if isinstance(column.metadata.confluence, str)
                else str(column.metadata.confluence),
            }
            if column.metadata
            else {},
        }
        fields.append(field)

    return {
        "type": DataType.STRUCT,
        "fields": fields,
        # Don't include containsNull - DataWeb's _transform_col adds it at array level
    }


@dataclass
class ComplexType:
    XY_ARRAY = {
        "type": DataType.ARRAY,
        "elementType": {
            "type": DataType.STRUCT,
            "fields": [
                {
                    "name": "x",
                    "type": DataType.DOUBLE,
                    "nullable": True,
                    "metadata": {"comment": "X-axis data points of function"},
                },
                {
                    "name": "y",
                    "type": DataType.DOUBLE,
                    "nullable": True,
                    "metadata": {"comment": "Y-axis data points of function"},
                },
            ],
            "containsNull": True,
        },
    }
    STRING_ARRAY = {
        "type": DataType.ARRAY,
        "elementType": "string",
        "containsNull": True,
    }


@dataclass
class Metadata:
    comment: ColumnDescription
    confluence: ConfluenceLink = ConfluenceLink.NONE


@dataclass
class Column:
    name: str
    type: DataType
    metadata: Metadata
    nullable: bool = False
    primary_key: bool = False

    def __str__(self) -> str:
        return self.name


class ColumnType(Enum):
    def __str__(self) -> str:
        return str(self.value)

    def __lt__(self, other):
        if isinstance(other, ColumnType):
            return str(self) < str(other)
        return NotImplemented

    def __eq__(self, other):
        if isinstance(other, ColumnType):
            return str(self) == str(other)
        return NotImplemented

    def __hash__(self):
        # Hash by string representation to match __eq__ behavior
        # This makes ColumnType enum members hashable and usable as dictionary keys
        return hash(str(self))

    DATE = Column(
        name="date",
        type=DataType.STRING,
        nullable=False,
        primary_key=True,
        metadata=Metadata(
            comment=ColumnDescription.DATE,
            confluence=ConfluenceLink.NONE,
        ),
    )
    DATE_TYPED_DATE = Column(
        name="date",
        type=DataType.DATE,
        nullable=True,
        primary_key=True,
        metadata=Metadata(
            comment=ColumnDescription.DATE,
        ),
    )
    ORG_ID = Column(
        name="org_id",
        type=DataType.LONG,
        nullable=True,
        primary_key=True,
        metadata=Metadata(
            comment=ColumnDescription.ORG_ID,
            confluence=ConfluenceLink.NONE,
        ),
    )
    DEVICE_ID = Column(
        name="device_id",
        type=DataType.LONG,
        nullable=True,
        primary_key=True,
        metadata=Metadata(
            comment=ColumnDescription.DEVICE_ID,
            confluence=ConfluenceLink.NONE,
        ),
    )
    TRACE_UUID = Column(
        name="trace_uuid",
        type=DataType.STRING,
        nullable=False,
        primary_key=True,
        metadata=Metadata(comment="UUID of the sensor replay trace."),
    )
    FRAME_ID = Column(
        name="frame_id",
        type=DataType.LONG,
        nullable=False,
        primary_key=True,
        metadata=Metadata(
            comment="Unique identifier for each frame within a trace, ensuring uniqueness even with duplicate timestamps."
        ),
    )
    GATEWAY_ID = Column(
        name="gateway_id",
        type=DataType.LONG,
        nullable=True,
        metadata=Metadata(
            comment=ColumnDescription.GATEWAY_ID,
            confluence=ConfluenceLink.NONE,
        ),
    )
    MARKET = Column(
        name="market",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(
            comment=ColumnDescription.MARKET,
            confluence=ConfluenceLink.NONE,
        ),
    )
    QUARANTINE_ENABLED = Column(
        name="quarantine_enabled",
        type=DataType.BYTE,
        nullable=True,
        metadata=Metadata(
            comment=ColumnDescription.QUARANTINE_ENABLED,
            confluence=ConfluenceLink.NONE,
        ),
    )
    INTERNAL_TYPE_NAME = Column(
        name="internal_type_name",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(
            comment=ColumnDescription.INTERNAL_TYPE_NAME,
            confluence=ConfluenceLink.NONE,
        ),
    )
    BUILD = Column(
        name="build",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(
            comment=ColumnDescription.BUILD,
            confluence=ConfluenceLink.NONE,
        ),
    )
    PRODUCT_GROUP_ID = Column(
        name="product_group_id",
        type=DataType.INTEGER,
        nullable=True,
        metadata=Metadata(
            comment=ColumnDescription.PRODUCT_GROUP_ID,
            confluence=ConfluenceLink.FIRMWARE_HIERARCHY,
        ),
    )
    PRODUCT_PROGRAM_ID = Column(
        name="product_program_id",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(
            comment=ColumnDescription.PRODUCT_PROGRAM_ID,
            confluence=ConfluenceLink.FIRMWARE_HIERARCHY,
        ),
    )
    FIRMWARE_PROGRAM_ENROLLMENT_TYPE = Column(
        name="firmware_program_enrollment_type",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(
            comment=ColumnDescription.FIRMWARE_PROGRAM_ENROLLMENT_TYPE,
            confluence=ConfluenceLink.FIRMWARE_HIERARCHY,
        ),
    )
    ROLL_OUT_STAGE_ID = Column(
        name="rollout_stage_id",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(
            comment=ColumnDescription.ROLL_OUT_STAGE_ID,
            confluence=ConfluenceLink.FIRMWARE_HIERARCHY,
        ),
    )
    PRODUCT_NAME = Column(
        name="product_name",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(
            comment=ColumnDescription.PRODUCT_NAME,
            confluence=ConfluenceLink.PRODUCT_VERSIONING,
        ),
    )
    VARIANT_NAME = Column(
        name="variant_name",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(
            comment=ColumnDescription.VARIANT_NAME,
            confluence=ConfluenceLink.PRODUCT_VERSIONING,
        ),
    )
    CABLE_NAME = Column(
        name="cable_name",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(
            comment=ColumnDescription.CABLE_NAME,
            confluence=ConfluenceLink.PRODUCT_VERSIONING,
        ),
    )
    MAKE = Column(
        name="make",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(
            comment=ColumnDescription.MAKE,
            confluence=ConfluenceLink.NONE,
        ),
    )
    MODEL = Column(
        name="model",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(
            comment=ColumnDescription.MODEL,
            confluence=ConfluenceLink.NONE,
        ),
    )
    YEAR = Column(
        name="year",
        type=DataType.LONG,
        nullable=True,
        metadata=Metadata(
            comment=ColumnDescription.YEAR,
            confluence=ConfluenceLink.NONE,
        ),
    )
    ENGINE_MODEL = Column(
        name="engine_model",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(
            comment=ColumnDescription.ENGINE_MODEL,
            confluence=ConfluenceLink.NONE,
        ),
    )
    FUEL_TYPE = Column(
        name="fuel_type",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(
            comment=ColumnDescription.FUEL_TYPE,
            confluence=ConfluenceLink.NONE,
        ),
    )
    ENGINE_TYPE = Column(
        name="engine_type",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(
            comment=ColumnDescription.ENGINE_TYPE,
            confluence=ConfluenceLink.NONE,
        ),
    )
    SIGNAL = Column(
        name="signal",
        type=DataType.STRING,
        nullable=False,
        metadata=Metadata(
            comment=ColumnDescription.SIGNAL,
            confluence=ConfluenceLink.NONE,
        ),
    )
    SIGNAL_NAME = Column(
        name="signal_name",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(comment="Name of the signal (OBD_VALUE)."),
    )
    BUS = Column(
        name="bus",
        type=DataType.STRING,
        nullable=False,
        metadata=Metadata(
            comment=ColumnDescription.BUS,
            confluence=ConfluenceLink.NONE,
        ),
    )
    SOURCE = Column(
        name="source",
        type=DataType.STRING,
        nullable=False,
        metadata=Metadata(
            comment=ColumnDescription.SOURCE,
            confluence=ConfluenceLink.NONE,
        ),
    )
    ELECTRIFICATION_LEVEL = Column(
        name="electrification_level",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(
            comment=ColumnDescription.ELECTRIFICATION_LEVEL,
            confluence=ConfluenceLink.NONE,
        ),
    )
    PRIMARY_FUEL_TYPE = Column(
        name="primary_fuel_type",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(
            comment=ColumnDescription.PRIMARY_FUEL_TYPE,
            confluence=ConfluenceLink.NONE,
        ),
    )
    SECONDARY_FUEL_TYPE = Column(
        name="secondary_fuel_type",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(
            comment=ColumnDescription.SECONDARY_FUEL_TYPE,
            confluence=ConfluenceLink.NONE,
        ),
    )
    POPULATION_COUNT = Column(
        name="population_count",
        type=DataType.LONG,
        nullable=True,
        metadata=Metadata(
            comment=ColumnDescription.POPULATION_COUNT,
            confluence=ConfluenceLink.NONE,
        ),
    )
    HISTOGRAM_DISCRETE = Column(
        name="histogram_discrete",
        type=ComplexType.XY_ARRAY,
        nullable=True,
        metadata=Metadata(comment="The histogram of discrete values."),
    )
    TYPE = Column(
        name="type",
        type=DataType.STRING,
        nullable=False,
        primary_key=True,
        metadata=Metadata(
            comment=ColumnDescription.TYPE,
            confluence=ConfluenceLink.NONE,
        ),
    )
    BUS_ID = Column(
        name="bus_id",
        type=DataType.LONG,
        nullable=True,
        metadata=Metadata(
            comment=ColumnDescription.BUS_ID,
            confluence=ConfluenceLink.NONE,
        ),
    )
    BUS_NAME = Column(
        name="bus_name",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(
            comment="The interface on the VG and connected device where the data was sourced from (ie. CAN0 on pins 6/14).",
            confluence=ConfluenceLink.NONE,
        ),
    )
    REQUEST_ID = Column(
        name="request_id",
        type=DataType.LONG,
        nullable=True,
        metadata=Metadata(
            comment=ColumnDescription.REQUEST_ID,
            confluence=ConfluenceLink.NONE,
        ),
    )
    RESPONSE_ID = Column(
        name="response_id",
        type=DataType.LONG,
        nullable=True,
        metadata=Metadata(
            comment=ColumnDescription.RESPONSE_ID,
            confluence=ConfluenceLink.NONE,
        ),
    )
    ECU_ID = Column(
        name="ecu_id",
        type=DataType.LONG,
        nullable=True,
        metadata=Metadata(
            comment="The ECU address of where a signal came from.",
            confluence=ConfluenceLink.NONE,
        ),
    )
    DATA_ID = Column(
        name="data_id",
        type=DataType.LONG,
        nullable=True,
        metadata=Metadata(
            comment="The data identifier (PGN, PID, DID) of a signal.",
            confluence=ConfluenceLink.NONE,
        ),
    )
    APPLICATION_ID = Column(
        name="application_id",
        type=DataType.INTEGER,
        nullable=False,
        metadata=Metadata(
            comment="Application layer protocol enum value. See ApplicationProtocolType for mapping."
        ),
    )
    OBD_VALUE = Column(
        name="obd_value",
        type=DataType.LONG,
        nullable=True,
        metadata=Metadata(
            comment=ColumnDescription.OBD_VALUE,
            confluence=ConfluenceLink.NONE,
        ),
    )
    SUM = Column(
        name="sum",
        type=DataType.DOUBLE,
        nullable=True,
        metadata=Metadata(
            comment=ColumnDescription.SUM,
            confluence=ConfluenceLink.NONE,
        ),
    )
    COUNT = Column(
        name="count",
        type=DataType.LONG,
        nullable=True,
        metadata=Metadata(
            comment=ColumnDescription.COUNT,
            confluence=ConfluenceLink.NONE,
        ),
    )
    AVG = Column(
        name="avg",
        type=DataType.DOUBLE,
        nullable=True,
        metadata=Metadata(
            comment=ColumnDescription.AVG,
            confluence=ConfluenceLink.NONE,
        ),
    )
    STDDEV = Column(
        name="stddev",
        type=DataType.DOUBLE,
        nullable=True,
        metadata=Metadata(
            comment=ColumnDescription.STDDEV,
            confluence=ConfluenceLink.NONE,
        ),
    )
    VARIANCE = Column(
        name="variance",
        type=DataType.DOUBLE,
        nullable=True,
        metadata=Metadata(
            comment=ColumnDescription.VARIANCE,
            confluence=ConfluenceLink.NONE,
        ),
    )
    MEAN = Column(
        name="mean",
        type=DataType.DOUBLE,
        nullable=True,
        metadata=Metadata(
            comment=ColumnDescription.MEAN,
            confluence=ConfluenceLink.NONE,
        ),
    )
    MEDIAN = Column(
        name="median",
        type=DataType.DOUBLE,
        nullable=True,
        metadata=Metadata(
            comment=ColumnDescription.MEDIAN,
            confluence=ConfluenceLink.NONE,
        ),
    )
    MODE = Column(
        name="mode",
        type=DataType.DOUBLE,
        nullable=True,
        metadata=Metadata(
            comment=ColumnDescription.MODE,
            confluence=ConfluenceLink.NONE,
        ),
    )
    FIRST = Column(
        name="first",
        type=DataType.DOUBLE,
        nullable=True,
        metadata=Metadata(
            comment=ColumnDescription.FIRST,
            confluence=ConfluenceLink.NONE,
        ),
    )
    LAST = Column(
        name="last",
        type=DataType.DOUBLE,
        nullable=True,
        metadata=Metadata(
            comment=ColumnDescription.LAST,
            confluence=ConfluenceLink.NONE,
        ),
    )
    KURTOSIS = Column(
        name="kurtosis",
        type=DataType.DOUBLE,
        nullable=True,
        metadata=Metadata(
            comment=ColumnDescription.KURTOSIS,
            confluence=ConfluenceLink.NONE,
        ),
    )
    PERCENTILE = Column(
        name="percentile",
        type=ComplexType.XY_ARRAY,
        nullable=True,
        metadata=Metadata(
            comment=ColumnDescription.PERCENTILE,
            confluence=ConfluenceLink.NONE,
        ),
    )
    HISTOGRAM = Column(
        name="histogram",
        type=ComplexType.XY_ARRAY,
        nullable=True,
        metadata=Metadata(
            comment=ColumnDescription.HISTOGRAM,
            confluence=ConfluenceLink.NONE,
        ),
    )
    MIN = Column(
        name="min",
        type=DataType.DOUBLE,
        nullable=True,
        metadata=Metadata(
            comment=ColumnDescription.MIN,
            confluence=ConfluenceLink.NONE,
        ),
    )
    MAX = Column(
        name="max",
        type=DataType.DOUBLE,
        nullable=True,
        metadata=Metadata(
            comment=ColumnDescription.MAX,
            confluence=ConfluenceLink.NONE,
        ),
    )
    COUNT_BOUND_ANOMALY = Column(
        name="count_bound_anomaly",
        type=DataType.LONG,
        nullable=True,
        metadata=Metadata(
            comment=ColumnDescription.COUNT_BOUND_ANOMALY,
            confluence=ConfluenceLink.NONE,
        ),
    )
    COUNT_DELTA_ANOMALY = Column(
        name="count_delta_anomaly",
        type=DataType.LONG,
        nullable=True,
        metadata=Metadata(
            comment=ColumnDescription.COUNT_DELTA_ANOMALY,
            confluence=ConfluenceLink.NONE,
        ),
    )
    COUNT_DELTA_OVER_TIME_ANOMALY = Column(
        name="count_delta_over_time_anomaly",
        type=DataType.LONG,
        nullable=True,
        metadata=Metadata(
            comment=ColumnDescription.COUNT_DELTA_OVER_TIME_ANOMALY,
            confluence=ConfluenceLink.NONE,
        ),
    )
    METRIC_NAME = Column(
        name="metric_name",
        type=DataType.STRING,
        nullable=False,
        metadata=Metadata(
            comment=ColumnDescription.METRIC_NAME,
            confluence=ConfluenceLink.NONE,
        ),
    )
    POPULATION_ID = Column(
        name="population_id",
        type=DataType.STRING,
        nullable=False,
        metadata=Metadata(
            comment=ColumnDescription.POPULATION_ID,
            confluence=ConfluenceLink.NONE,
        ),
    )
    TIME = Column(
        name="time",
        type=DataType.LONG,
        nullable=True,
        metadata=Metadata(
            comment=ColumnDescription.TIME,
            confluence=ConfluenceLink.NONE,
        ),
    )
    END_TIME = Column(
        name="end_time",
        type=DataType.LONG,
        nullable=True,
        metadata=Metadata(
            comment=ColumnDescription.END_TIME,
            confluence=ConfluenceLink.NONE,
        ),
    )
    VALUE = Column(
        name="value",
        type=DataType.DOUBLE,
        nullable=True,
        metadata=Metadata(
            comment=ColumnDescription.VALUE,
            confluence=ConfluenceLink.NONE,
        ),
    )
    PRODUCT_ID = Column(
        name="product_id",
        type=DataType.LONG,
        nullable=True,
        metadata=Metadata(
            comment="Value representing the product SKU",
            confluence=ConfluenceLink.PRODUCT_VERSIONING,
        ),
    )
    BOARD_REVISION = Column(
        name="board_revision",
        type=DataType.LONG,
        nullable=True,
        metadata=Metadata(
            comment="Board revision identifier",
            confluence=ConfluenceLink.PRODUCT_VERSIONING,
        ),
    )
    PRODUCT_VERSION = Column(
        name="product_version",
        type=DataType.LONG,
        nullable=True,
        metadata=Metadata(
            comment="[Deprecated: use product_version_str] A product change requiring multiple versions of the same SKU being manufactured in parallel. Example: Building with two different MCUs for a given SKU, you can track using “HW-SKU-NA A0-000” and “HW-SKU-NA A1-000”. NOTE: these changes must not require visibility at the sales/customer/logistics level. See https://samsaradev.atlassian.net/l/c/VBb75518 for more information.",
            confluence=ConfluenceLink.PRODUCT_VERSIONING,
        ),
    )
    PRODUCT_VERSION_STR = Column(
        name="product_version_str",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(
            comment="A product change requiring multiple versions of the same SKU being manufactured in parallel. Example: Building with two different MCUs for a given SKU, you can track using “HW-SKU-NA A0-000” and “HW-SKU-NA A1-000”. NOTE: these changes must not require visibility at the sales/customer/logistics level. See https://samsaradev.atlassian.net/l/c/VBb75518 for more information.",
            confluence=ConfluenceLink.PRODUCT_VERSIONING,
        ),
    )
    BOM_VERSION = Column(
        name="bom_version",
        type=DataType.LONG,
        nullable=True,
        metadata=Metadata(
            comment="Any change to the Bill of Materials (BOM) will result in rolling to the next BOM Version. This will capture all PCB changes, EE changes, ME changes. It can also be used to track the point at which a second source becomes qualified. See https://samsaradev.atlassian.net/l/c/VBb75518 for more information.",
            confluence=ConfluenceLink.PRODUCT_VERSIONING,
        ),
    )
    CABLE_ID = Column(
        name="cable_id",
        type=DataType.LONG,
        nullable=True,
        metadata=Metadata(
            comment="The most frequently reported diagnostic cable ID reported over the previous 7 days. See definitions.cable_type_mappings for the meaning of this column.",
            confluence=ConfluenceLink.NONE,
        ),
    )
    HAS_MODI = Column(
        name="has_modi",
        type=DataType.BOOLEAN,
        nullable=True,
        metadata=Metadata(
            comment="Set true if a Samsara ACC-BDH(E) diagnostics accessory is connceted to a gateway.",
            confluence=ConfluenceLink.NONE,
        ),
    )
    HAS_USB_SERIAL = Column(
        name="has_usb_serial",
        type=DataType.BOOLEAN,
        nullable=True,
        metadata=Metadata(
            comment="Set true if a Samsara ACC-RS232 serial cable is connceted to a device. This is used in for material spreader and Bendix safety integrations.",
            confluence=ConfluenceLink.NONE,
        ),
    )
    HAS_FALKO = Column(
        name="has_falko",
        type=DataType.BOOLEAN,
        nullable=True,
        metadata=Metadata(
            comment="Set true if a Samsara ACC-BTUH is connceted to a device. This accessory enables a secondary CAN interface to be used by the VG for tachograph integrations.",
            confluence=ConfluenceLink.NONE,
        ),
    )
    HAS_CM3X = Column(
        name="has_cm3x",
        type=DataType.BOOLEAN,
        nullable=True,
        metadata=Metadata(
            comment="Set true if a Samsara CM31 or CM32 is connceted to a device. Cameras draw power and network connectivity via a USB connection.",
            confluence=ConfluenceLink.NONE,
        ),
    )
    HAS_J1708_USB = Column(
        name="has_j1708_usb",
        type=DataType.BOOLEAN,
        nullable=True,
        metadata=Metadata(
            comment="Set true if a Samsara CBL-VG-BJ1708 is connceted to a device. USB is required to provide J1708/J1587 diagnostics on the VG3X.",
            confluence=ConfluenceLink.NONE,
        ),
    )
    HAS_OCTO = Column(
        name="has_octo",
        type=DataType.BOOLEAN,
        nullable=True,
        metadata=Metadata(
            comment="Set true if a Samsara AHD1 is connceted to a device.",
            confluence=ConfluenceLink.NONE,
        ),
    )
    HAS_BRIGID = Column(
        name="has_brigid",
        type=DataType.BOOLEAN,
        nullable=True,
        metadata=Metadata(
            comment="Set true if a Samsara CM33 or CM34 is connceted to a device.",
            confluence=ConfluenceLink.NONE,
        ),
    )
    HAS_CM2X = Column(
        name="has_cm2x",
        type=DataType.BOOLEAN,
        nullable=True,
        metadata=Metadata(
            comment="Set true if a Samsara CM21 or CM22 is connceted to a device. Cameras draw power and network connectivity via a USB connection.",
            confluence=ConfluenceLink.NONE,
        ),
    )
    HAS_DUCKBILL_CABLE = Column(
        name="has_duckbill_cable",
        type=DataType.BOOLEAN,
        nullable=True,
        metadata=Metadata(
            comment="Set true if a Samsara ACC-VG-ACCR (duckbill cable) is connceted to a device.",
            confluence=ConfluenceLink.NONE,
        ),
    )
    GROUPING_ID = Column(
        name="grouping_id",
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(
            comment="A unique ID for a particular grouping of devices",
            confluence=ConfluenceLink.NONE,
        ),
    )
    POPULATION_TYPE = Column(
        name="population_type",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(
            comment="Grouping name derived from grouping_id bitfield.",
            confluence=ConfluenceLink.NONE,
        ),
    )
    POPULATION_NAME = Column(
        name="population_name",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(
            comment="Grouping type derived from grouping_id bitfield.",
            confluence=ConfluenceLink.NONE,
        ),
    )
    INTERNAL_TYPE = Column(
        name="internal_type",
        type=DataType.LONG,
        nullable=False,
        metadata=Metadata(
            comment="The type of organization a deivce is in (ie customer, internal, test, etc).",
            confluence=ConfluenceLink.NONE,
        ),
    )
    VARIANT_ID = Column(
        name="variant_id",
        type=DataType.INTEGER,
        nullable=False,
        metadata=Metadata(
            comment="The internal ID of the Samsara product variant",
            confluence=ConfluenceLink.NONE,
        ),
    )
    TOTAL_DISTANCE_METERS = Column(
        name="total_distance_meters",
        type=DataType.DOUBLE,
        nullable=True,
        metadata=Metadata(
            comment="Total trip distance travelled in a day.",
            confluence=ConfluenceLink.NONE,
        ),
    )
    ROLLOUT_STAGE_ID = Column(
        name="rollout_stage_id",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(
            comment="A Rollout Stage is a group of organizations (for org-level enrollment) or a group of gateways (for gateway-level enrollment) that receive the same firmware at the same time in the firmware release process. They are the equivalent of “cells” for Samsara’s Cloud Engineering teams.",
            confluence=ConfluenceLink.FIRMWARE_HIERARCHY,
        ),
    )
    SERIAL = Column(
        name="serial",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(
            comment="Device serial number.",
            confluence=ConfluenceLink.NONE,
        ),
    )
    LOCALE = Column(
        name="locale",
        type=DataType.STRING,
        nullable=False,
        metadata=Metadata(
            comment="Locale of the organization. Represents where an organization is centered (US, IE, NL, UK, etc).",
            confluence=ConfluenceLink.NONE,
        ),
    )
    ENGINE_HP = Column(
        name="engine_hp",
        type=DataType.INTEGER,
        nullable=True,
        metadata=Metadata(
            comment="Horsepower of the vehicle engine. This may not be known due to limitation from either our VIN decoder providers or due to VIN unavailability.",
            confluence=ConfluenceLink.NONE,
        ),
    )
    ASSET_SUBTYPE = Column(
        name="asset_subtype",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(
            comment="The subtype of the asset.",
            confluence="https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/5142344/RFC+-+Implement+VehicleDutyType+and+VehicleType",
        ),
    )
    ASSET_TYPE_NAME = Column(
        name="asset_type_name",
        type=DataType.STRING,
        nullable=False,
        metadata=Metadata(
            comment="The diagnostic cable accessary identifier installed with a gateway.",
            confluence="https://samsara.atlassian-us-gov-mod.net/wiki/spaces/RD/pages/5292609/Asset+Type",
        ),
    )
    ASSET_TYPE_SEGMENT = Column(
        name="asset_type_segment",
        type=DataType.STRING,
        nullable=False,
        metadata=Metadata(
            comment="The diagnostic cable accessary identifier installed with a gateway.",
            confluence=ConfluenceLink.NONE,
        ),
    )
    TIMESTAMP_LAST_CABLE_ID = Column(
        name="timestamp_last_cable_id",
        type=DataType.TIMESTAMP,
        nullable=True,
        metadata=Metadata(
            comment="The timestamp of the last cable ID update.",
            confluence=ConfluenceLink.NONE,
        ),
    )
    INCLUDE_IN_AGG_METRICS = Column(
        name="include_in_agg_metrics",
        type=DataType.BOOLEAN,
        nullable=False,
        metadata=Metadata(
            comment="Whether or not this device will be included in VDP aggregated device metrics (see agg_population_metrics).",
            confluence=ConfluenceLink.NONE,
        ),
    )
    DIAGNOSTICS_CAPABLE = Column(
        name="diagnostics_capable",
        type=DataType.BOOLEAN,
        nullable=True,
        metadata=Metadata(
            comment="Indicates if the current Samsara hardware installed is capable of reading diagnostics.",
            confluence=ConfluenceLink.NONE,
        ),
    )
    HEX_ECU_ID = Column(
        name="hex_ecu_id",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(
            comment="The request identifier (in hex string) of a diagnostic command.",
        ),
    )
    HEX_REQUEST_ID = Column(
        name="hex_request_id",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(
            comment="The request identifier (in hex string) of a diagnostic command.",
        ),
    )
    HEX_RESPONSE_ID = Column(
        name="hex_response_id",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(
            comment="The response identifier (in hex string) of a diagnostic command.",
        ),
    )
    HEX_DATA_ID = Column(
        name="hex_data_id",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(
            comment="The data identifier (PGN, PID, DID) (in hex string) of a diagnostic.",
        ),
    )
    GROUPING_HASH = Column(
        name="grouping_hash",
        type=DataType.STRING,
        nullable=False,
        primary_key=True,
        metadata=Metadata(comment="The hash of the grouping."),
    )
    GROUPING_LABEL = Column(
        name="grouping_label",
        type=DataType.STRING,
        nullable=False,
        primary_key=True,
        metadata=Metadata(comment="The label of the grouping."),
    )
    EQUIPMENT_TYPE = Column(
        name="equipment_type",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(
            comment="The equipment type of the device.",
        ),
    )
    DEVICE_TYPE = Column(
        name="device_type",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(
            comment="The device type of the device",
        ),
    )
    GROUPING_LEVEL = Column(
        name="grouping_level",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(
            comment="The level of the grouping. Represents a population of devices that are similar to each other.",
        ),
    )
    ISSUE = Column(
        name="issue",
        type=DataType.STRING,
        nullable=True,
        primary_key=True,
        metadata=Metadata(
            comment="The issue of the device.",
        ),
    )
    POWERTRAIN = Column(
        name="powertrain",
        type=DataType.INTEGER,
        nullable=True,
        primary_key=True,
        metadata=Metadata(comment="Powertrain of the promotion."),
    )
    FUEL_GROUP = Column(
        name="fuel_group",
        type=DataType.INTEGER,
        nullable=True,
        primary_key=True,
        metadata=Metadata(comment="Fuel group of the promotion."),
    )
    WMI = Column(
        name="wmi",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(comment="The WMI of the device VIN."),
    )
    VDS = Column(
        name="vds",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(comment="The VDS of the device VIN."),
    )
    MODEL_YEAR_CODE = Column(
        name="model_year_code",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(comment="The model year code of the device VIN."),
    )
    COUNT_ICE = Column(
        name="count_ice",
        type=DataType.LONG,
        nullable=True,
        metadata=Metadata(comment="The count of ICE devices."),
    )
    COUNT_HYDROGEN = Column(
        name="count_hydrogen",
        type=DataType.LONG,
        nullable=True,
        metadata=Metadata(comment="The count of hydrogen devices."),
    )
    COUNT_HYBRID = Column(
        name="count_hybrid",
        type=DataType.LONG,
        nullable=True,
        metadata=Metadata(comment="The count of hybrid devices."),
    )
    COUNT_BEV = Column(
        name="count_bev",
        type=DataType.LONG,
        nullable=True,
        metadata=Metadata(comment="The count of BEV devices."),
    )
    COUNT_PHEV = Column(
        name="count_phev",
        type=DataType.LONG,
        nullable=True,
        metadata=Metadata(comment="The count of PHEV devices."),
    )
    COUNT_UNKNOWN = Column(
        name="count_unknown",
        type=DataType.LONG,
        nullable=True,
        metadata=Metadata(comment="The count of unknown devices."),
    )
    TRIM = Column(
        name="trim",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(comment="Trim level of the vehicle."),
    )
    # Signal-related columns
    BIT_START = Column(
        name="bit_start",
        type=DataType.INTEGER,
        metadata=Metadata(comment="Starting bit position of the signal."),
    )
    BIT_LENGTH = Column(
        name="bit_length",
        type=DataType.INTEGER,
        metadata=Metadata(comment="Length of the signal in bits."),
    )
    SIGN = Column(
        name="sign",
        type=DataType.LONG,
        metadata=Metadata(comment="Mapping to vdp.proto Sign."),
    )
    ENDIAN = Column(
        name="endian",
        type=DataType.LONG,
        metadata=Metadata(comment="Mapping to vdp.proto Endian."),
    )
    SCALE = Column(
        name="scale",
        type=DataType.DOUBLE,
        metadata=Metadata(
            comment="Scaling factor giving the bit resolution of the signal."
        ),
    )
    INTERNAL_SCALING = Column(
        name="internal_scaling",
        type=DataType.DOUBLE,
        nullable=True,
        metadata=Metadata(
            comment="Internal scaling factor to translate engineering units to OBD_VALUE units."
        ),
    )
    OFFSET = Column(
        name="offset",
        type=DataType.DOUBLE,
        metadata=Metadata(comment="Offset value to apply after scaling."),
    )
    MINIMUM = Column(
        name="minimum",
        type=DataType.DOUBLE,
        metadata=Metadata(comment="Minimum valid value for the signal."),
    )
    MAXIMUM = Column(
        name="maximum",
        type=DataType.DOUBLE,
        metadata=Metadata(comment="Maximum valid value for the signal."),
    )
    UNIT = Column(
        name="unit",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(comment="Engineering unit of measurement."),
    )
    DATA_IDENTIFIER = Column(
        name="data_identifier",
        type=DataType.LONG,
        nullable=True,
        metadata=Metadata(comment="Data identifier for the signal request."),
    )
    TRANSPORT_ID = Column(
        name="transport_id",
        type=DataType.INTEGER,
        nullable=False,
        metadata=Metadata(
            comment="Transport layer protocol enum value. See TransportProtocolType for mapping."
        ),
    )
    TIMESTAMP_UNIX_US = Column(
        name="timestamp_unix_us",
        type=DataType.LONG,
        nullable=False,
        primary_key=True,
        metadata=Metadata(comment="Timestamp in microseconds since Unix epoch."),
    )
    SOURCE_INTERFACE = Column(
        name="source_interface",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(
            comment="CAN interface where frame was captured (CAN0, CAN1, etc.)."
        ),
    )
    PAYLOAD = Column(
        name="payload",
        type=DataType.BINARY,
        nullable=True,
        metadata=Metadata(comment="Raw payload data from CAN frames."),
    )
    STREAM_ID = Column(
        name="stream_id",
        type=DataType.LONG,
        nullable=True,
        metadata=Metadata(
            comment="Stream identifier hash for grouping related frames (xxhash64)."
        ),
    )
    DIRECTION = Column(
        name="direction",
        type=DataType.LONG,
        nullable=True,
        metadata=Metadata(comment="Frame direction (TX/RX). 1 = TX, 2 = RX"),
    )
    DESCRIPTION = Column(
        name="description",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(comment="Signal description."),
    )
    MAPPING = Column(
        name="mapping",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(comment="Additional mapping information from J1939 source."),
    )
    PGN = Column(
        name="pgn",
        type=DataType.INTEGER,
        nullable=True,
        metadata=Metadata(comment="Parameter Group Number for J1939 signals."),
    )
    SPN = Column(
        name="spn",
        type=DataType.INTEGER,
        nullable=True,
        metadata=Metadata(comment="Suspect Parameter Number for J1939 signals."),
    )
    SOURCE_ID = Column(
        name="source_id",
        type=DataType.INTEGER,
        nullable=True,
        metadata=Metadata(
            comment="Source system enum ID. See global_obd_configuration.go for mapping."
        ),
    )
    PASSIVE_RESPONSE_MASK = Column(
        name="passive_response_mask",
        type=DataType.LONG,
        nullable=True,
        metadata=Metadata(comment="Passive response mask for broadcast signals."),
    )
    DATA_SOURCE_ID = Column(
        name="data_source_id",
        type=DataType.INTEGER,
        nullable=False,
        metadata=Metadata(
            comment="Data source indicator. See SignalSourceType enum for values."
        ),
    )
    PROTOCOL_ID = Column(
        name="protocol_id",
        type=DataType.LONG,
        nullable=True,
        metadata=Metadata(
            comment="Protocol ID. See vdp_obd_protocol.proto for mapping."
        ),
    )
    SIGNAL_UUID = Column(
        name="signal_uuid",
        type=DataType.STRING,
        nullable=True,
        metadata=Metadata(
            comment="Unique identifier for SPS signals (NULL for generic signals)."
        ),
    )
    REQUEST_PERIOD_MS = Column(
        name="request_period_ms",
        type=DataType.LONG,
        nullable=True,
        metadata=Metadata(
            comment="Request period in milliseconds. NULL or 0 indicates broadcast-based signal."
        ),
    )
    IS_BROADCAST = Column(
        name="is_broadcast",
        type=DataType.BOOLEAN,
        nullable=False,
        metadata=Metadata(
            comment="Whether this signal is broadcast-based (true) or request-based (false). "
            "Determined by request_period_ms and protocol-specific logic."
        ),
    )
    IS_STANDARD = Column(
        name="is_standard",
        type=DataType.BOOLEAN,
        nullable=False,
        metadata=Metadata(
            comment="Whether this signal uses standard protocol ranges. "
            "TRUE for SAE J1939 standard ranges, OBD-II J1979 modes, etc. "
            "FALSE for proprietary, unknown, or non-standard protocols."
        ),
    )


def get_primary_keys(columns: List[Union[Column, ColumnType]]) -> List[str]:
    return [
        (col.value if isinstance(col, ColumnType) else col).name
        for col in columns
        if (col.value if isinstance(col, ColumnType) else col).primary_key
    ]


def get_non_null_columns(columns: List[Union[Column, ColumnType]]) -> List[str]:
    return [
        (col.value if isinstance(col, ColumnType) else col).name
        for col in columns
        if not (col.value if isinstance(col, ColumnType) else col).nullable
    ]


def columns_to_schema(*columns: Column) -> List[dict]:
    def to_column(obj):
        """Convert ColumnType or Column to a Column instance."""
        if isinstance(obj, Enum) and isinstance(obj.value, Column):
            return obj.value  # Extract the Column from the Enum
        elif isinstance(obj, Column):
            return obj  # It's already a Column
        else:
            raise ValueError(f"Unsupported object type: {type(obj)} {type(obj.value)}")

    return [asdict(to_column(column)) for column in columns]


def columns_to_names(*columns: Column) -> List[str]:
    for column in columns:
        if isinstance(column, Column) or (
            isinstance(column, Enum) and isinstance(column.value, Column)
        ):
            continue
        raise ValueError(f"Unsupported object type: {type(column)}")
    return [str(column) for column in columns]


def get_column_names(columns: List[Union[Column, Enum]]) -> List[str]:
    """
    Extract column names from a list of Column objects or ColumnType enums.

    Args:
        columns: List of Column objects or ColumnType enums

    Returns:
        List of column name strings
    """
    names = []
    for col_def in columns:
        if isinstance(col_def, Column):
            names.append(col_def.name)
        elif isinstance(col_def, Enum) and isinstance(col_def.value, Column):
            names.append(col_def.value.name)
        else:
            raise ValueError(f"Unsupported column type: {type(col_def)}")
    return names


def dataweb_to_spark_schema(columns: List[Union[Column, Enum]]) -> StructType:
    """
    Convert DataWeb Column objects to Spark StructType.

    Args:
        columns: List of Column objects or ColumnType enums

    Returns:
        Spark StructType for use with createDataFrame

    Raises:
        ValueError: If column type cannot be mapped
    """
    fields = []
    for col_def in columns:
        if isinstance(col_def, Column):
            column = col_def
        elif isinstance(col_def, Enum) and isinstance(col_def.value, Column):
            column = col_def.value
        else:
            raise ValueError(f"Unsupported column type: {type(col_def)}")

        # Map DataWeb data types to Spark types
        if column.type == DataType.STRING:
            spark_type = StringType()
        elif column.type == DataType.INTEGER:
            spark_type = IntegerType()
        elif column.type == DataType.LONG:
            spark_type = LongType()
        elif column.type == DataType.DOUBLE:
            spark_type = DoubleType()
        elif column.type == DataType.BOOLEAN:
            spark_type = BooleanType()
        elif column.type == DataType.DATE:
            spark_type = DateType()
        elif column.type == DataType.BINARY:
            spark_type = BinaryType()
        else:
            raise Exception(f"Unsupported column type: {column.type}")

        fields.append(StructField(column.name, spark_type, column.nullable))

    return StructType(fields)


def convert_spark_schema_to_pandas_dtypes(spark_schema):
    """
    Convert Spark StructType schema to pandas dtype mapping.

    Args:
        spark_schema: PySpark StructType schema

    Returns:
        Dictionary mapping column names to pandas dtypes
    """
    dtype_mapping = {}

    for field in spark_schema.fields:
        column_name = field.name
        spark_type = field.dataType
        is_nullable = field.nullable

        # Map Spark types to pandas dtypes
        if isinstance(spark_type, StringType):
            dtype_mapping[column_name] = "string"
        elif isinstance(spark_type, LongType):
            # Use nullable Int64 for nullable LongType fields
            dtype_mapping[column_name] = "Int64" if is_nullable else "int64"
        elif isinstance(spark_type, IntegerType):
            # Use nullable Int32 for nullable IntegerType fields
            dtype_mapping[column_name] = "Int32" if is_nullable else "int32"
        elif isinstance(spark_type, DoubleType):
            dtype_mapping[column_name] = "float64"
        elif isinstance(spark_type, BooleanType):
            dtype_mapping[column_name] = "boolean"
        else:
            dtype_mapping[column_name] = "string"

    return dtype_mapping


def create_pandas_dataframe_with_schema(data, schema):
    """
    Create a pandas DataFrame with explicit schema typing, similar to Spark's createDataFrame.

    Args:
        data: List of dictionaries or list of rows
        schema: Dictionary mapping column names to pandas dtypes

    Returns:
        pandas DataFrame with proper typing
    """
    import pandas as pd

    # Create DataFrame first without dtype specification
    df = pd.DataFrame(data)

    # Apply type conversions for each column based on schema
    for column, dtype in schema.items():
        if column in df.columns:
            if dtype == "float64":
                df[column] = pd.to_numeric(df[column], errors="coerce").astype(
                    "float64"
                )
            elif dtype == "int64":
                df[column] = pd.to_numeric(df[column], errors="coerce").astype("int64")
            elif dtype == "int32":
                df[column] = pd.to_numeric(df[column], errors="coerce").astype("int32")
            elif dtype == "Int64":
                # Nullable integer type - can handle NaN/None
                df[column] = pd.to_numeric(df[column], errors="coerce").astype("Int64")
            elif dtype == "Int32":
                # Nullable integer type - can handle NaN/None
                df[column] = pd.to_numeric(df[column], errors="coerce").astype("Int32")
            elif dtype == "boolean":
                df[column] = df[column].astype("boolean")
            elif dtype == "string":
                df[column] = df[column].astype("string")

    return df


def get_empty_dataframe(
    spark: SparkSession, columns: List[Union[Column, ColumnType]]
) -> DataFrame:
    """
    Return an empty Spark DataFrame with the specified schema.

    Args:
        columns: List of Column objects or ColumnType enums

    Returns:
        Empty Spark DataFrame with the specified schema
    """
    spark_schema = dataweb_to_spark_schema(columns)
    return spark.createDataFrame([], spark_schema)


def complex_dataweb_to_spark_type(dataweb_type):
    """
    Convert complex DataWeb type definitions to Spark types.

    Args:
        dataweb_type: DataWeb type definition (dict or string)

    Returns:
        Corresponding Spark data type
    """
    from pyspark.sql.types import (
        ArrayType,
        BinaryType,
        BooleanType,
        DateType,
        DoubleType,
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
    )

    if isinstance(dataweb_type, str):
        # Handle basic string type references
        type_map = {
            DataType.STRING: StringType(),
            DataType.INTEGER: IntegerType(),
            DataType.LONG: LongType(),
            DataType.DOUBLE: DoubleType(),
            DataType.BOOLEAN: BooleanType(),
            DataType.BINARY: BinaryType(),
            DataType.DATE: DateType(),
        }
        return type_map.get(dataweb_type, StringType())

    elif isinstance(dataweb_type, dict):
        if dataweb_type.get("type") == DataType.ARRAY:
            # Convert array type
            element_type = complex_dataweb_to_spark_type(dataweb_type["elementType"])
            contains_null = dataweb_type.get("containsNull", True)
            return ArrayType(element_type, contains_null)

        elif dataweb_type.get("type") == DataType.MAP:
            # Convert map type
            key_type = complex_dataweb_to_spark_type(dataweb_type["keyType"])
            value_type = complex_dataweb_to_spark_type(dataweb_type["valueType"])
            value_contains_null = dataweb_type.get("valueContainsNull", False)
            return MapType(key_type, value_type, value_contains_null)

        elif dataweb_type.get("type") == DataType.STRUCT:
            # Convert struct type
            fields = []
            for field in dataweb_type.get("fields", []):
                field_name = field["name"]
                field_type = complex_dataweb_to_spark_type(field["type"])
                field_nullable = field.get("nullable", True)
                fields.append(StructField(field_name, field_type, field_nullable))
            return StructType(fields)

        else:
            # Handle other basic types specified in dict format
            return complex_dataweb_to_spark_type(
                dataweb_type.get("type", DataType.STRING)
            )

    else:
        raise Exception(f"Unsupported dataweb type: {dataweb_type}")
