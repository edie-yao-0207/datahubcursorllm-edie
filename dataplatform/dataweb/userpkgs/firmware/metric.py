import re
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List

from dataweb.userpkgs.firmware.table import DatabaseTable


def sql_safe_name(value):
    if value is None:
        return "NULL"
    return str(value)


@dataclass
class StatMetadata:
    product_area: str = field(default=None)
    sub_product_area: str = field(default=None)
    signal_name: str = field(default=None)
    default_priority: int = field(default=1)
    definition_table: str = field(default=None)
    value_min: float = field(default=None)
    value_max: float = field(default=None)
    delta_min: float = field(default=None)
    delta_max: float = field(default=None)
    delta_velocity_min: float = field(default=None)
    delta_velocity_max: float = field(default=None)
    documentation_link: str = field(default=None)
    obd_value: int = field(default=None)
    is_applicable_ice: bool = field(default=True)
    is_applicable_hydrogen: bool = field(default=True)
    is_applicable_hybrid: bool = field(default=True)
    is_applicable_phev: bool = field(default=True)
    is_applicable_bev: bool = field(default=True)
    is_applicable_unknown: bool = field(default=True)

    def __post_init__(self):
        # Convert integer values to floats for PySpark compatibility
        if self.value_min is not None and isinstance(self.value_min, int):
            self.value_min = float(self.value_min)
        if self.value_max is not None and isinstance(self.value_max, int):
            self.value_max = float(self.value_max)
        if self.delta_min is not None and isinstance(self.delta_min, int):
            self.delta_min = float(self.delta_min)
        if self.delta_max is not None and isinstance(self.delta_max, int):
            self.delta_max = float(self.delta_max)
        if self.delta_velocity_min is not None and isinstance(self.delta_velocity_min, int):
            self.delta_velocity_min = float(self.delta_velocity_min)
        if self.delta_velocity_max is not None and isinstance(self.delta_velocity_max, int):
            self.delta_velocity_max = float(self.delta_velocity_max)
            
        self.value_min_sql_str = sql_safe_name(self.value_min)
        self.value_max_sql_str = sql_safe_name(self.value_max)
        self.delta_min_sql_str = sql_safe_name(self.delta_min)
        self.delta_max_sql_str = sql_safe_name(self.delta_max)
        self.delta_velocity_min_sql_str = sql_safe_name(self.delta_velocity_min)
        self.delta_velocity_max_sql_str = sql_safe_name(self.delta_velocity_max)


@dataclass
class Metric:
    type: str
    field: str
    label: str
    excluded_from_data_lake: bool = False
    metadata: StatMetadata = field(default_factory=lambda: StatMetadata())
    filter_condition: str = ""

    def __post_init__(self):
        # Define a regex pattern to match any character that is NOT allowed in partition names.
        # Allowed characters: letters (a-z, A-Z), numbers (0-9), hyphens (-), and underscores (_)
        # We exclude other characters because:
        # - Many file systems and storage systems (e.g., HDFS, S3) do not support special characters like * ? : < > | \ / in paths.
        # - Spark and SQL contexts may misinterpret certain characters (e.g., period . or equals sign =) as operators or delimiters.
        # - Replacing restricted characters with a hyphen (-) ensures compatibility across different environments.
        pattern = r"[^a-zA-Z0-9_]"  # Match anything that's NOT a letter, number, hyphen, or underscore

        if re.match(pattern, self.label) is not None:
            raise Exception(f"invalid metric name {self.label}")

    def __str__(self):
        return self.label


def check_unique_metric_strings(enum_class: Enum):
    seen = set()
    for entry in enum_class:
        string_representation = str(entry)
        if string_representation in seen:
            raise ValueError(
                f"Duplicate string representation found: {string_representation}"
            )
        seen.add(string_representation)


class StrEnum(str, Enum):
    def __str__(self):
        return self.value


class MetricValue(StrEnum):
    VALUE = "value"
    INT_VALUE = "value.int_value"
    DOUBLE_VALUE = "value.double_value"


class MetricEnum(Enum):
    def __str__(self):
        return str(self.value)

    @property
    def label(self) -> str:
        return self.value.label

    @property
    def type(self) -> int:
        return self.value.type

    @property
    def field(self) -> int:
        return self.value.field

    @property
    def metadata(self) -> StatMetadata:
        return self.value.metadata


def metric_values_to_partition_map(
    *metrics: Metric,
) -> Dict[DatabaseTable, List[MetricValue]]:
    result = defaultdict(list)

    for metric in metrics:
        result[f"{metric}"].append(f"{metric.value.field}")

    return dict(result)


def metrics_with_defined_bounds(
    metadata_list: List[StatMetadata],
) -> List[Metric]:
    def has_defined_bounds(metadata: StatMetadata) -> bool:
        return (
            metadata.value_min != metadata.value_max
            or metadata.delta_min != metadata.delta_max
            or metadata.delta_velocity_min != metadata.delta_velocity_max
        )

    return [x.metric for x in metadata_list if has_defined_bounds(x)]
