from dagster import AssetExecutionContext
from dataweb import build_general_dq_checks, get_databases, table
from dataweb.userpkgs.constants import (
    ALL_COMPUTE_REGIONS,
    FIRMWAREVDP,
    FRESHNESS_SLO_9AM_PST,
    Database,
    TableType,
    WarehouseWriteMode,
)
from dataweb.userpkgs.firmware.constants import DATAWEB_PARTITION_DATE
from dataweb.userpkgs.firmware.populations import (
    DEVICE_COLUMNS,
    GROUPING_SETS,
    ColumnType,
    Population,
    population_id_value_expr,
    NULLABLE_DIMENSION_COLUMNS,
)
from dataweb.userpkgs.firmware.schema import (
    Column,
    ColumnType,
    DataType,
    Metadata,
    columns_to_names,
    columns_to_schema,
)
from dataweb.userpkgs.firmware.table import ProductAnalytics
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.utils import (
    build_table_description,
    partition_key_ranges_from_context,
    schema_to_columns_with_property,
)

DISTINCT_COUNT_COLUMNS = [
    ColumnType.ORG_ID,
    ColumnType.DEVICE_ID,
]

COUNT_IF_COLUMNS = [
    ColumnType.INCLUDE_IN_AGG_METRICS,
]

COLUMNS = (
    [
        ColumnType.DATE,
        ColumnType.POPULATION_ID,
        ColumnType.GROUPING_ID,
        ColumnType.POPULATION_TYPE,
        ColumnType.POPULATION_NAME,
    ]
    + NULLABLE_DIMENSION_COLUMNS
    + [
        Column(
            name=f"count_distinct_{column}",
            type=DataType.LONG,
            nullable=False,
            metadata=Metadata(
                comment=f"Denotes if the {column} column is grouped in the query",
            ),
        )
        for column in DISTINCT_COUNT_COLUMNS
    ]
    + [
        Column(
            name=f"is_grouped_{column}",
            type=DataType.BOOLEAN,
            nullable=False,
            metadata=Metadata(
                comment=f"Denotes if the {column} column is grouped in the query",
            ),
        )
        for column in Population.MOST_GRANULAR.value
    ]
    + [
        Column(
            name=f"count_if_{column}",
            type=DataType.LONG,
            nullable=False,
            metadata=Metadata(
                comment=f"Denotes if the {column} column is non-zero",
            ),
        )
        for column in COUNT_IF_COLUMNS
    ]
)

SCHEMA = columns_to_schema(*COLUMNS)

PRIMARY_KEYS = columns_to_names(*COLUMNS)

NON_NULL_KEYS = schema_to_columns_with_property(SCHEMA, "nullable")

QUERY = """

WITH
    combined AS (
        SELECT
           *

        FROM
            {product_analytics}.dim_device_dimensions

        WHERE
            date BETWEEN "{date_start}" AND "{date_end}"
    )

SELECT
    date

    , {population_id_value_expr} AS population_id

    , GROUPING_ID() AS grouping_id

    , CONCAT_WS(" ", TRANSFORM(
        FILTER(
            ZIP_WITH(
                ZIP_WITH(
                    SPLIT(
                        LPAD(
                            BIN(GROUPING_ID())
                            , SIZE(ARRAY({dimension_columns}))
                            , "0"
                        )
                        , ""
                    )
                    , ARRAY({dimension_columns})
                    , (x, y) -> NAMED_STRUCT("include", x, "value", y)
                )
                , ARRAY({dimension_strings})
                , (x, y) -> NAMED_STRUCT("include", x.include, "value", x.value, "name", y)
            )
            , x -> x.include = 0
        )
        , x -> FORMAT_STRING("%s", x.name)
    )) AS population_type

    , CONCAT_WS(" ", TRANSFORM(
        FILTER(
            ZIP_WITH(
                ZIP_WITH(
                    SPLIT(
                        LPAD(
                            BIN(GROUPING_ID())
                            , SIZE(ARRAY({dimension_columns}))
                            , "0"
                        )
                        , ""
                    )
                    , ARRAY({dimension_columns})
                    , (x, y) -> NAMED_STRUCT("include", x, "value", y)
                )
                , ARRAY({dimension_strings})
                , (x, y) -> NAMED_STRUCT("include", x.include, "value", x.value, "name", y)
            )
            , x -> x.include = 0 AND x.value IS NOT NULL
        )
        , x -> FORMAT_STRING("%s", x.value)
    )) AS population_name

    , {dimension_columns}
    , {distinct_counts}
    , {is_grouped}
    , {count_ifs}

FROM
    combined

GROUP BY
    date
    , GROUPING SETS (
        {grouping_sets}
    )

ORDER BY
    date, {dimension_columns} ASC

"""

FMT_DIMENSION_COLUMNS_CASTED = ", ".join(
    [
        f"CAST(COALESCE({dimension}, 'null') AS STRING)"
        for dimension in Population.MOST_GRANULAR.value
    ]
)

FMT_DISTINCT_COUNTS = ", ".join(
    [
        f"COUNT(DISTINCT {column}) AS count_distinct_{column}"
        for column in DISTINCT_COUNT_COLUMNS
    ]
)

FMT_DIMENSION_STRINGS = ", ".join(
    [f"'{dimension}'" for dimension in Population.MOST_GRANULAR.value]
)

FMT_IS_GROUPED = ", ".join(
    [
        f"((1 - GROUPING({column})) = 1) AS is_grouped_{column}"
        for column in Population.MOST_GRANULAR.value
    ]
)

FMT_COUNT_IFS = ", ".join(
    [
        f"COUNT_IF({column}) AS count_if_{column}"
        for column in COUNT_IF_COLUMNS
    ]
)


@table(
    database=Database.PRODUCT_ANALYTICS,
    description=build_table_description(
        table_desc="A generated set of populations composed of different devices. This is used downstream to generate metrics over populations.",
        row_meaning="All devices in use on a given day",
        related_table_info={},
        table_type=TableType.DAILY_DIMENSION,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DATAWEB_PARTITION_DATE,
    upstreams=[
        AnyUpstream(ProductAnalytics.DIM_DEVICE_DIMENSIONS),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    backfill_batch_size=5,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalytics.DIM_POPULATIONS.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_KEYS,
        block_before_write=True,
    ),
    priority=5,
)
def dim_populations(context: AssetExecutionContext) -> str:
    partition_keys = partition_key_ranges_from_context(context)[0]

    return QUERY.format(
        date_start=partition_keys[0],
        date_end=partition_keys[-1],
        product_analytics=get_databases()[Database.PRODUCT_ANALYTICS],
        grouping_sets=GROUPING_SETS,
        dimension_columns=DEVICE_COLUMNS,
        dimension_columns_casted=FMT_DIMENSION_COLUMNS_CASTED,
        distinct_counts=FMT_DISTINCT_COUNTS,
        count_ifs=FMT_COUNT_IFS,
        dimension_strings=FMT_DIMENSION_STRINGS,
        is_grouped=FMT_IS_GROUPED,
        population_id_value_expr=population_id_value_expr("GROUPING_ID()", Population.MOST_GRANULAR)
    )
