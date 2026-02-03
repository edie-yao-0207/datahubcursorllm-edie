from typing import List, Optional, Dict, Union
from dagster import AssetExecutionContext
from dataweb import get_databases
from dataweb.userpkgs.constants import InstanceType
from dataweb.userpkgs.firmware.schema import Column, ColumnType, DataType, Metadata
from dataweb.userpkgs.utils import get_run_env, partition_key_ranges_from_context
from dataweb.userpkgs.firmware.populations import Population, population_id_value_expr


class SafeDict(dict):
    def __missing__(self, key):
        return f"{{{key}}}"


def format_query(query: str, **kwargs) -> str:
    return query.format_map(SafeDict(get_databases(), **kwargs))


def format_date_partition_query(
    query: str, context: AssetExecutionContext, partitions=None, **kwargs
) -> str:
    # Get date range from partitions if provided, otherwise fall back to context
    if partitions is not None:
        date_start = partitions.date_start
        date_end = partitions.date_end
    else:
        partition_keys = partition_key_ranges_from_context(context)[0]
        date_start = partition_keys[0]
        date_end = partition_keys[-1]

    return format_query(query, date_start=date_start, date_end=date_end, **kwargs)


def generate_grouping_label(columns: List[ColumnType]) -> str:
    parts = []
    for col in columns:
        col_name = col.name.lower()
        parts.append(
            f"CASE WHEN GROUPING({col_name}) = 0 "
            f"THEN '(' || COALESCE(CAST({col_name} AS STRING), '?') || ')' END"
        )
    return "CONCAT_WS(' ',\n  " + ",\n  ".join(parts) + "\n) AS grouping_label"


def generate_grouping_hash(columns: List[ColumnType]) -> str:
    parts = []
    for col in columns:
        parts.append(
            f"CASE WHEN GROUPING({col}) = 0 THEN COALESCE(CAST({col} AS STRING), 'null') ELSE '__not_grouped__' END"
        )
    return "MD5(CONCAT_WS('||',\n  " + ",\n  ".join(parts) + "\n)) AS grouping_hash"


def generate_grouping_sets(
    columns: List[ColumnType], groupings: List[List[ColumnType]]
) -> str:
    # Check that all groupings reference only defined columns
    for group in groupings:
        for col in group:
            if col not in columns:
                raise ValueError(f"Grouping contains undefined dimension: {col}")

    grouping_sets_body = ",\n      ".join(
        f"({', '.join(f'{col}' for col in group)})" if group else "()"
        for group in groupings
    )

    return f"GROUPING SETS (\n      {grouping_sets_body}\n    )"


def generate_grouping_level(columns: List[str]) -> str:
    case_lines = ",\n        ".join(
        f"CASE WHEN GROUPING({col}) = 0 THEN '{col}' ELSE NULL END" for col in columns
    )
    return f"""
    CONCAT_WS(" + ",
      FILTER(ARRAY(
        {case_lines}
      ), x -> x IS NOT NULL)
    ) AS grouping_level
    """


def generate_distinct_counts(columns: List[ColumnType]) -> str:
    return ",\n  ".join(
        f"COUNT(DISTINCT {col}) AS count_distinct_{col}" for col in columns
    )


def generate_distinct_count_columns(columns: List[str]) -> List[Column]:
    return [
        Column(
            name=f"count_distinct_{col}",
            type=DataType.LONG,
            nullable=True,
            metadata=Metadata(comment=f"Distinct count of {col} in the group."),
        )
        for col in columns
    ]


def set_columns_nullable(columns: List[Union[Column, ColumnType]]) -> List[Column]:
    result = []
    for col in columns:
        if isinstance(col, ColumnType):
            col = col.value

        if isinstance(col, Column):
            result.append(
                Column(
                    name=col.name,
                    type=col.type,
                    nullable=True,
                    metadata=col.metadata,
                )
            )
        else:
            raise TypeError(f"Unsupported column type: {type(col)}")
    return result


def build_population_schema_header(
    columns: Optional[List[ColumnType]] = None,
    partition_columns: Optional[List[ColumnType]] = None,
    aggregate_columns: Optional[List[ColumnType]] = None,
    include_grouping_label: bool = False,
) -> List[Column]:
    columns = columns or []
    aggregate_columns = aggregate_columns or []
    partition_columns = (
        partition_columns if partition_columns is not None else [ColumnType.DATE]
    )

    return list(
        filter(
            lambda col: col is not None,
            [
                *partition_columns,
                ColumnType.GROUPING_HASH,
                ColumnType.GROUPING_LABEL if include_grouping_label else None,
                *set_columns_nullable(columns),
                *aggregate_columns,
            ],
        )
    )


def format_agg_date_partition_query(
    context: AssetExecutionContext,
    query: str,
    dimensions: List[ColumnType],
    groupings: List[List[ColumnType]],
) -> str:
    return format_date_partition_query(
        query,
        context,
        dimensions=",\n  ".join(f"{col}" for col in dimensions),
        grouping_sets=generate_grouping_sets(dimensions, groupings),
        grouping_hash=generate_grouping_hash(dimensions),
        population_id="%s AS population_id"
        % population_id_value_expr("GROUPING_ID()", Population.MOST_GRANULAR),
        grouping_level=generate_grouping_level(dimensions),
        grouping_label=generate_grouping_label(dimensions),
        count_distinct_device_id=generate_distinct_counts([ColumnType.DEVICE_ID]),
        count_distinct_columns=generate_distinct_counts(dimensions),
        subsample="ABS(HASH(device_id)) % 100 < 5"
        if get_run_env() == "dev"
        else "TRUE",
    )


def create_run_config_overrides(
    min_workers: int = 1,
    max_workers: int = 4,
    spark_conf_overrides: Dict[str, str] = {},
    instance_pool_type: Optional[str] = None,
    driver_instance_type: Optional[InstanceType] = None,
    worker_instance_type: Optional[InstanceType] = None,
) -> dict:
    if driver_instance_type or worker_instance_type:
        # Driver instance type/worker instance type are provided, no instance pool
        return {
            "spark_conf_overrides": spark_conf_overrides,
            "min_workers": min_workers,
            "max_workers": max_workers,
            "driver_instance_type": driver_instance_type,
            "worker_instance_type": worker_instance_type,
        }
    else:
        # Use instance pool
        return {
            "spark_conf_overrides": spark_conf_overrides,
            "min_workers": min_workers,
            "max_workers": max_workers,
            "instance_pool_type": instance_pool_type,
        }


def verify_groupings_are_ordered(
    dimensions: List[ColumnType], groupings: List[List[ColumnType]]
):
    # Use .name for clarity; could also use .value if that's preferred
    allowed_field_names = set(field.name for field in dimensions)

    for idx, grouping in enumerate(groupings):
        grouping_names = [field.name for field in grouping]

        # Check that each field in grouping is allowed
        for field_name in grouping_names:
            if field_name not in allowed_field_names:
                raise ValueError(
                    f"Grouping {idx} contains an unknown field: {field_name}"
                )

        # Check order based on index in `dimensions`
        logical_indexes = [
            next(i for i, d in enumerate(dimensions) if d.name == name)
            for name in grouping_names
        ]
        if logical_indexes != sorted(logical_indexes):
            raise ValueError(
                f"Grouping {idx} has fields out of logical order: {grouping_names}"
            )


def sort_groupings_by_logical_order(
    groupings: List[List[ColumnType]], logical_order: List[ColumnType]
) -> List[List[ColumnType]]:
    # Build a map from field name to its position
    order_index = {col.name: i for i, col in enumerate(logical_order)}

    sorted_groupings = []
    for grouping in groupings:
        try:
            sorted_grouping = sorted(grouping, key=lambda col: order_index[col.name])
            sorted_groupings.append(sorted_grouping)
        except KeyError as e:
            raise ValueError(f"Unknown ColumnType in grouping: {e}")
    return sorted_groupings
