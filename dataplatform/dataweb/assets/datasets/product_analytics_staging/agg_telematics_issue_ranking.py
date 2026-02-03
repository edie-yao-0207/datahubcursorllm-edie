from typing import List, Tuple, Union
from dagster import AssetExecutionContext
from dataweb import build_general_dq_checks, table
from dataweb.userpkgs.constants import (
    ALL_COMPUTE_REGIONS,
    FIRMWAREVDP,
    FRESHNESS_SLO_9AM_PST,
    Database,
    TableType,
    WarehouseWriteMode,
    DQCheckMode,
)
from dataweb.userpkgs.firmware.constants import (
    DATAWEB_PARTITION_DATE,
)
from dataweb.userpkgs.firmware.schema import (
    Column,
    ColumnType,
    DataType,
    Metadata,
    columns_to_schema,
    get_primary_keys,
    get_non_null_columns,
)
from dataweb.userpkgs.query import build_population_schema_header, format_date_partition_query
from dataweb.userpkgs.firmware.table import ProductAnalyticsStaging
from dataweb.userpkgs.firmware.upstream import AnyUpstream
from dataweb.userpkgs.utils import (
    build_table_description,
)

def generate_rankings(
    partition_by: List[Union[ColumnType, str]],
    order_by: List[Union[ColumnType, str]],
    prefix: str = "rank"
) -> Tuple[str, List[Column]]:
    lines = []
    columns = []

    partition_by_strs = [str(p) if not isinstance(p, str) else p for p in partition_by]
    order_by_strs = [str(o) if not isinstance(o, str) else o for o in order_by]

    for col in order_by_strs:
        partition_str = ", ".join(partition_by_strs)
        rank_name = f"{prefix}_{'_'.join(partition_by_strs)}_by_{col}"
        rank_expr = f"""
RANK() OVER (
    PARTITION BY {partition_str}
    ORDER BY {col} DESC
) AS {rank_name}
"""
        lines.append(rank_expr)
        columns.append(Column(
            name=rank_name,
            type=DataType.INTEGER,
            nullable=False,
            metadata=Metadata(
                comment=f"Rank of {col.replace('_', ' ')} grouped by {', '.join(partition_by_strs)}. Lower is higher impact."
            ),
        ))

    return ",\n\n".join(lines), columns

def generate_rankings(
    partition_by: List[Union[ColumnType, str]],
    order_by: List[Union[ColumnType, str]],
    prefix: str = "rank"
) -> Tuple[str, List[Column]]:
    lines = []
    columns = []

    partition_by_strs = [str(p) if not isinstance(p, str) else p for p in partition_by]
    order_by_strs = [str(o) if not isinstance(o, str) else o for o in order_by]

    for col in order_by_strs:
        partition_str = ", ".join(partition_by_strs)
        rank_name = f"{prefix}_{'_'.join(partition_by_strs)}_by_{col}"
        rank_expr = f"""RANK() OVER (
    PARTITION BY {partition_str}
    ORDER BY {col} DESC
) AS {rank_name}"""
        lines.append(rank_expr)
        columns.append(Column(
            name=rank_name,
            type=DataType.INTEGER,
            nullable=False,
            metadata=Metadata(
                comment=f"Rank of {col.replace('_', ' ')} grouped by {', '.join(partition_by_strs)}. Lower is higher impact."
            ),
        ))

    return ",\n\n".join(lines), columns


RANKINGS_MARKET_ISSUE, RANKINGS_MARKET_ISSUE_COLUMNS = generate_rankings(
    partition_by=[ColumnType.DATE, ColumnType.MARKET, ColumnType.GROUPING_LEVEL, ColumnType.ISSUE],
    order_by=["count_distinct_device_id_with_issue", "issue_rate", "weighted_issue_rate"],
    prefix="rank_market_issue"
)

RANKINGS_MARKET, RANKINGS_MARKET_COLUMNS = generate_rankings(
    partition_by=[ColumnType.DATE, ColumnType.MARKET, ColumnType.GROUPING_LEVEL],
    order_by=["count_distinct_device_id_with_issue", "issue_rate", "weighted_issue_rate"],
    prefix="rank_market"
)

QUERY = """
WITH data AS (
    SELECT
        population.date,
        problem.issue,
        population.market,
        population.grouping_level,
        population.grouping_hash,
        problem.count_distinct_device_id AS count_distinct_device_id_with_issue,
        problem.count_distinct_device_id / population.count_distinct_device_id AS issue_rate,
        problem.count_distinct_device_id / population.count_distinct_org_id AS issue_rate_per_org,
        problem.count_distinct_device_id / population.count_distinct_device_id * LOG10(problem.count_distinct_device_id + 1) AS weighted_issue_rate
    FROM {product_analytics_staging}.agg_telematics_populations AS population
    JOIN {product_analytics_staging}.agg_telematics_population_issues AS problem USING (date, grouping_hash)
    WHERE date BETWEEN "{date_start}" AND "{date_end}"
)
SELECT
    date,
    issue,
    grouping_hash,
    count_distinct_device_id_with_issue,
    issue_rate,
    issue_rate_per_org,
    weighted_issue_rate,
    {rankings_market_issue},
    {rankings_market}
FROM data
"""

COLUMNS =  build_population_schema_header(
    partition_columns=[
        ColumnType.DATE,
        ColumnType.ISSUE,
    ],
    aggregate_columns=[
        *[
            Column(
                name="count_distinct_device_id_with_issue",
                type=DataType.LONG,
                nullable=False,
                metadata=Metadata(
                    comment="Count of distinct devices with the issue."
                ),
            ),
            Column(
                name="issue_rate",
                type=DataType.DOUBLE,
                nullable=True,
                metadata=Metadata(
                    comment="Issue rate of the issue in the population."
                ),
            ),
            Column(
                name="issue_rate_per_org",
                type=DataType.DOUBLE,
                nullable=True,
                metadata=Metadata(
                    comment="Issue rate of the issue in the population per org."
                ),
            ),
            Column(
                name="weighted_issue_rate",
                type=DataType.DOUBLE,
                nullable=True,
                metadata=Metadata(
                    comment="Weighted issue rate of the issue in the population."
                ),
            ),
        ],
        *RANKINGS_MARKET_ISSUE_COLUMNS,
        *RANKINGS_MARKET_COLUMNS,
    ]
)

SCHEMA = columns_to_schema(*COLUMNS)
PRIMARY_KEYS = get_primary_keys(COLUMNS)
NON_NULL_COLUMNS = get_non_null_columns(COLUMNS)

@table(
    database=Database.PRODUCT_ANALYTICS_STAGING,
    description=build_table_description(
        table_desc="Telematics device issue report.",
        row_meaning="The device issue report for telematics devices.",
        table_type=TableType.STAGING,
        freshness_slo_updated_by=FRESHNESS_SLO_9AM_PST,
    ),
    regions=ALL_COMPUTE_REGIONS,
    owners=[FIRMWAREVDP],
    schema=SCHEMA,
    primary_keys=PRIMARY_KEYS,
    partitioning=DATAWEB_PARTITION_DATE,
    upstreams=[
        AnyUpstream(ProductAnalyticsStaging.AGG_TELEMATICS_POPULATIONS),
        AnyUpstream(ProductAnalyticsStaging.AGG_TELEMATICS_POPULATION_ISSUES),
    ],
    write_mode=WarehouseWriteMode.OVERWRITE,
    single_run_backfill=True,
    dq_checks=build_general_dq_checks(
        asset_name=ProductAnalyticsStaging.AGG_TELEMATICS_ISSUE_RANKING.value,
        primary_keys=PRIMARY_KEYS,
        non_null_keys=NON_NULL_COLUMNS,
        block_before_write=True,
    ),
    dq_check_mode=DQCheckMode.WHOLE_RESULT,
)
def agg_telematics_issue_ranking(context: AssetExecutionContext) -> str:
    return format_date_partition_query(
        QUERY,
        context,
        rankings_market_issue=RANKINGS_MARKET_ISSUE,
        rankings_market=RANKINGS_MARKET,
    )