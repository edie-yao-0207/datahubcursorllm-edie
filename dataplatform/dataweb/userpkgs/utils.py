import ast
import copy
import os
import textwrap
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from enum import Enum
from functools import reduce
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Literal,
    Mapping,
    NamedTuple,
    Optional,
    Set,
    Tuple,
    Union,
)
from pathlib import Path
from dagster import AssetExecutionContext
from dataweb.userpkgs.constants import (
    INDUSTRY_VERTICALS,
    AWSRegion,
    ColumnType,
    DimensionConfig,
    RunEnvironment,
    TableType,
)
from pyspark.sql import DataFrame, SparkSession


def schema_to_columns_with_property(
    schema: Dict[str, str],
    property: Optional[str] = None,
) -> List[str]:
    """
    Convert a given schema to a list of columns with a given property.

    Args:
        schema (Dict[str, str]): The schema to convert.
        property (Optional[str]): The property to filter by. Defaults to None meaning all columns

    Returns:
        List[str]: Matching column names.
    """
    return list(
        map(
            lambda x: x["name"],
            filter(
                lambda column: not column.get(property) if property else True,
                schema,
            ),
        )
    )


def partition_keys_from_context(context: AssetExecutionContext) -> List[List[str]]:
    partition_dim_keys = []
    if context.assets_def.partitions_def:
        _keys = list(zip(*[key.split("|") for key in context.partition_keys]))
        first_partition_keys = [f"'{k}'" for k in _keys[0]]
        partition_dim_keys.append(first_partition_keys)
        if len(_keys) == 2:
            second_partition_keys = [f"'{k}'" for k in _keys[1]]
            partition_dim_keys.append(second_partition_keys)
    return partition_dim_keys


def partition_key_ranges_from_context(
    context: AssetExecutionContext,
) -> List[Tuple[str, str]]:

    partition_key_ranges = []
    if context.assets_def.partitions_def:
        partitions_key_start = context.partition_key_range.start.split("|")
        partitions_key_end = context.partition_key_range.end.split("|")

        partition_key_ranges.append((partitions_key_start[0], partitions_key_end[0]))

        if len(partitions_key_start) == 2:
            partition_key_ranges.append(
                (partitions_key_start[1], partitions_key_end[1])
            )

    return partition_key_ranges


def partition_key_ranges_from_range_tags(
    context: AssetExecutionContext,
) -> List[Tuple[str, str]]:
    """
    Gets the requested partition key range for a materialization from Dagster's run
    tags: asset_partition_range_start/asset_partition_range_end. This can be useful
    for backfill policies which only support range backfills (e.g. single run).

    :returns List of [start, end] tuples for each dimension of a multipartition.
    """
    if "dagster/partition" in context.run.tags:
        start_tag = context.run.tags["dagster/partition"]
        end_tag = context.run.tags["dagster/partition"]
    else:
        start_tag = context.run.tags["dagster/asset_partition_range_start"]
        end_tag = context.run.tags["dagster/asset_partition_range_end"]

    start_dimensions = start_tag.split("|")
    end_dimensions = end_tag.split("|")

    return list(zip(start_dimensions, end_dimensions))


def replace_timetravel_string(sql_query: str) -> str:
    """
    Replaces the timetravel string in the given SQL query with an empty string.

    Args:
        sql_query (str): The SQL query to be processed.

    Returns:
        str: The SQL query with the timetravel string replaced.

    """
    tokens = sql_query.split()

    updated_tokens = []

    for token in tokens:
        if "@202" in token:
            token = token.split("@202")[0]
        updated_tokens.append(token)

    return " ".join(updated_tokens)


def get_timetravel_str(partition_date_str: str) -> str:
    return "@" + partition_date_str.replace("-", "") + "24595909999"


def date_range_from_current_date(
    days: int = 0, offset: int = 0, start_date: str = None
) -> List[str]:

    if start_date:
        _date = datetime.strptime(start_date, "%Y-%m-%d")
    else:
        _date = datetime.utcnow()

    idx = 0
    keys = []
    while idx < days:
        key = (_date - timedelta(days=(idx + offset))).date()
        keys.append(f"{key}")
        idx += 1
    return keys


def date_range_from_current_date_multi_partitions(
    days: int = 0, offset: int = 0, static_partitions: list = []
) -> List[str]:
    today = datetime.utcnow().date()
    idx = 0
    keys = []
    while idx < days:
        for static_partition in static_partitions:
            keys.append(f"{today - timedelta(days=(idx + offset))}|{static_partition}")
        idx += 1
    return keys


def get_all_regions() -> List[AWSRegion]:
    return [
        AWSRegion.US_WEST_2,
        AWSRegion.EU_WEST_1,
        AWSRegion.CA_CENTRAL_1,
    ]


def get_run_env() -> Literal["prod", "dev"]:
    env = os.getenv("ENV", "dev")
    assert env in list(
        asdict(RunEnvironment()).values()
    ), "environment variable 'ENV' must be one of ('prod', 'dev')"
    return asdict(RunEnvironment()).get(env.upper())


table_type_map = {
    TableType.DAILY_DIMENSION: """This is a daily snapshot dimension table - a daily snapshot dimension table is a table partitioned by the ‘date’ column where each date partition represents the entity’s attributes on a given date

For example - A daily dimension snapshot table of organizations will provide one row per day per organization id.

To avoid duplications, you should filter on a specific date, or join on the primary keys and date to a fact table. For example:

dim.date = [fact_date_value]""",
    TableType.SLOWLY_CHANGING_DIMENSION: """
This is a slowly changing dimension table - a slowly changing dimension table is a table that shows the information of an entity on a date range

For example - A slowly changing dimension table for an organization will show that the organization was linked to account ‘abc‘ between the dates [start_date] and [end_date] and linked to a different account between [start_date] and [end_date]

To query the state of an entity on a given date you will want to use the condition

[date_value] BETWEEN start_date and end_date""",
    TableType.TRANSACTIONAL_FACT: """This is a transactional fact table - a transactional fact table is a

table where each date partition stores the transactions that happened on a given date.

To query activity on a time period, filter on the date column

For example - A fact trips table will show all the trips taken on a date or a range of dates""",
    TableType.LOOKUP: """This is a lookup table - a lookup table has no history or partition columns.

It is meant to give you a way to look up the association between various entities.

To use this table join or filter on the primary key to get its most recent values""",
    TableType.ACCUMULATING_SNAPSHOT_FACT: """This is an accumulating snapshot fact table -
an accumulating snapshot fact table tracks the lifecycle of an entity and updates rows as new data is introduced.
The date tracks the original date that the event or activity occurred.

Example - A safety event will reside in the date partition when the event originally happened.
If actions were taken on that safety event on a later date (e.g. coaching actions),
those actions will still reside in the original date partition.
""",
    TableType.LIFETIME: """This is a lifetime table partitioned by date.

A row for a given date, stores the cumulative historical activity for the entity including when was their first action,
last action and all dates when an action occurred.""",
    TableType.AGG: """
    This is a daily aggregated dataset designed for summaries and is partitioned by date.

A row for a given date, stores aggregated metrics and often times shows additional historic data from previous time windows (previous month, same month last year, year to date, ...).
    """,
    TableType.STAGING: """
    This is a staging dataset. Users who do not have a technical understanding of the source data for this table should not use it, but rather the downstream table(s) that are built on top of this table.
    """,
    TableType.MONTHLY_REPORTING_AGG: """This is a daily aggregated dataset designed for monthly summaries and is partitioned by date.

A row for a given date, stores monthly aggregated metrics and often times shows additional historic data from previous time windows (previous month, same month last year, year to date, ...).""",
}


def build_table_description(
    table_desc: str,
    row_meaning: str,
    table_type: TableType,
    freshness_slo_hours: Optional[int] = None,
    freshness_slo_updated_by: str = "",
    related_table_info: Dict[str, str] = {},
    caveats: Optional[str] = None,
) -> str:
    """
    Builds a description for a table based on the provided parameters.

    Args:
        table_desc (str): The table description to give more context on what the table means. This should give general context to users not familiar with the table on what type of data is in this table. Add business context, link to more documentation, etc.
        row_meaning (str): The meaning of each row in the table.
        table_type (TableType): The type of the table.
        freshness_slo_hours (Optional[int]): The maximum age of the table in hours.
        freshness_slo_updated_by (Optional[str]): The time of day the table will be updated by.
        related_table_info (Dict[str, str], optional): The information about related tables. Defaults to {}.
        caveats (str, optional): Any additional caveats or notes about the table. Defaults to None.

    Returns:
        str: The formatted table description.
    """

    assert (
        not freshness_slo_hours or not freshness_slo_updated_by
    ), "Either freshness_slo_hours or freshness_slo_updated_by must be provided, not both"

    assert (
        freshness_slo_hours or freshness_slo_updated_by
    ), "Either freshness_slo_hours or freshness_slo_updated_by must be provided"

    if freshness_slo_updated_by:
        SLO_CLAUSE = f"This table will be updated with yesterday`s data by {freshness_slo_updated_by} each day."
    elif freshness_slo_hours:
        SLO_CLAUSE = f"This table will be updated at least once every {freshness_slo_hours} hours."

    table_desc = textwrap.dedent(table_desc)
    row_meaning = textwrap.dedent(row_meaning)

    related_table_clause = ""

    for tbl, desc in related_table_info.items():
        desc = textwrap.dedent(desc)
        related_table_clause += f"   - {tbl} => {desc.strip()}.\n"

    related_table_clause = textwrap.dedent(related_table_clause)

    table_type_clause = table_type_map.get(table_type, "no value found")

    table_type_clause = textwrap.dedent(table_type_clause)

    result_str = f"""**Table Description**:
{table_desc}

**Meaning of each row**:
{row_meaning}

**Table Type**:
{table_type_clause}

**SLO**:
{SLO_CLAUSE}
Breaches to this guarantee will be monitored and responded to during business days (9am-5pm PT).

{"**Related Tables**:" if related_table_info else ""}
{related_table_clause}
{caveats if caveats else ""}"""

    formatted_str = textwrap.dedent(result_str)

    return formatted_str


def merge_pyspark_dataframes(dfs: List[DataFrame]) -> DataFrame:
    """Merges a list of pyspark dataframes into one without eliminating duplicates.
    :returns Merged dataframe.
    """
    return reduce(lambda x, y: x.unionAll(y), dfs)


def verify_data_equality(
    spark: SparkSession,
    context: AssetExecutionContext,
    table1: DataFrame,
    table2: str,
    date: str,
) -> float:
    """
    Verifies data equality between two tables for a given date.

    Args:
        spark: The SparkSession instance
        context: Dagster asset execution context
        table1: PySpark DataFrame for the first table
        table2: Name of the second table as a string
        date: Date string to compare data for

    Returns:
        float: Match rate percentage between the two tables
    """
    context.log.info("verifying data equality")

    table1.createOrReplaceTempView("table1")

    query = f"""
    with t as (
    SELECT *, 'in table1 but not table2' AS case FROM table1
    EXCEPT
    SELECT *, 'in table1 but not table2' AS case FROM {table2} WHERE date = '{date}'
    UNION ALL
    SELECT *, 'in table2 but not table1' AS case FROM {table2} WHERE date = '{date}'
    EXCEPT
    SELECT *, 'in table2 but not table1' AS case FROM table1
    )
    select * from t order by 1,2,3,4,7
    """
    result = spark.sql(query)
    if result.count() == 0:
        context.log.info(
            "The data is exactly the same between the two tables for the given date."
        )
    else:
        context.log.info(result.show(truncate=False))
    match_query = """
    SELECT COUNT(*) AS total_records FROM table1
    """
    total_records = spark.sql(match_query).collect()[0]["total_records"]

    mismatch_query = f"""
    SELECT COUNT(*) AS mismatch_records FROM (
        SELECT * FROM table1
        EXCEPT
        SELECT * FROM {table2} WHERE date = '{date}'
        UNION ALL
        SELECT * FROM {table2} WHERE date = '{date}'
        EXCEPT
        SELECT * FROM table1
    ) AS mismatches
    """
    mismatch_records = spark.sql(mismatch_query).collect()[0]["mismatch_records"]

    match_rate = (total_records - mismatch_records) / total_records * 100
    context.log.info(f"Match Rate: {match_rate:.2f}%")
    return match_rate


def get_needed_dates(root: datetime, period: str) -> list[str]:
    """
    Generate the dates based on the given period and starting point
    Parameters:
        root (datetime): The starting point
        period (str): The time period in question (weekly, monthly, etc.)
    Returns:
        list[str]: A list of dates for that period given the starting point
    """

    # Product Scorecard only supports weekly/monthly now, but just in case

    if period == "month":
        end_of_period = root + (relativedelta(months=1) - relativedelta(days=1))
    elif period == "week":
        end_of_period = root + relativedelta(days=6)
    elif period == "quarter":
        end_of_period = root + (relativedelta(months=3) - relativedelta(days=1))
    elif period == "day":
        end_of_period = root
    elif period == "year":
        end_of_period = root + (relativedelta(years=1) - relativedelta(days=1))
    else:
        raise ValueError(f"Unknown period: {period}")
    return [
        end_of_period.strftime("%Y-%m-%d"),
    ]


def generate_coalesce_statement(
    columns: List[str],
    aliases: Optional[List[str]] = None,
    column_type: ColumnType = ColumnType.METRIC,
    dimension_configs: Optional[Dict[str, DimensionConfig]] = None,
    default_value: Union[int, str] = 0,
    value_overrides: List[str] = None,
) -> str:
    """Generate COALESCE statements for SQL queries.
    Args:
        columns: List of column names
        aliases: List of table aliases (required for dimensions)
        column_type: Whether this is a DIMENSION or METRIC column
        dimension_configs: Configuration for dimension columns including data types
        default_value: Default value for metrics (ignored for dimensions)
        value_overrides: List of columns to override the default value
    Returns:
        str: COALESCE statement for the given columns
    """
    if not columns:
        raise ValueError("The columns list cannot be empty.")

    if column_type == ColumnType.DIMENSION and not aliases:
        raise ValueError("Aliases are required for dimension columns.")

    if column_type == ColumnType.DIMENSION and not dimension_configs:
        raise ValueError("Dimension configurations are required for dimension columns.")

    coalesce_statements = []

    for column in columns:
        if column_type == ColumnType.DIMENSION:
            config = dimension_configs[column]
            coalesce_parts = [f"{alias}.{column}" for alias in aliases]
            output_name = config.output_name or column
            coalesce_expr = (
                f"COALESCE({', '.join(coalesce_parts)}, {config.default_value}) "
                f"AS {output_name}"
            )
        else:
            # For metrics, use the simple form with default value
            output_name = column.split(".")[1]
            if value_overrides and output_name in value_overrides:
                coalesce_expr = f"COALESCE({column}, null) AS {output_name}"
            else:
                coalesce_expr = f"COALESCE({column}, {default_value}) AS {output_name}"

        coalesce_statements.append(coalesce_expr)

    return ",\n".join(coalesce_statements)


def generate_full_outer_join_statements(
    tables_aliases: dict[str, str], dimensions_defaults: dict[str, str]
) -> str:
    """
    Generate a series of FULL OUTER JOIN statements for SQL.
    Parameters:
        tables_aliases (dict): A dictionary where the key is the table name and the value is the alias.
        dimensions_defaults (dict): A dictionary where the key is a dimension and the value is a default value.
    Returns:
        str: A string containing FULL OUTER JOIN statements for SQL.
    """
    join_statements = []
    table_aliases = list(tables_aliases.values())

    for i in range(1, len(table_aliases)):
        current_alias = table_aliases[i]
        previous_aliases = table_aliases[:i]
        on_conditions = []

        for dimension, default in dimensions_defaults.items():
            previous_coalesce = f"COALESCE({', '.join(f'{alias}.{dimension}' for alias in previous_aliases)}, {default})"
            current_coalesce = f"COALESCE({current_alias}.{dimension}, {default})"
            on_conditions.append(f"{previous_coalesce} = {current_coalesce}")

        join_statement = f"FULL OUTER JOIN {list(tables_aliases.keys())[i]} {current_alias} ON {' AND '.join(on_conditions)}"
        join_statements.append(join_statement)

    return "\n".join(join_statements)


def generate_metric_expression(metrics: dict[str, str], metric: str) -> str:
    """
    Generates metric expression.
    Args:
        metrics dict[str, str]: A dictionary of the metric statements we're using
        metric (str): The metric in question we want to pull
    Returns:
        str: A Spark SQL expression for given metric
    """
    if metric not in metrics.keys():
        raise ValueError("Invalid metric chosen")

    # Return metric expression
    return metrics[metric] + """ AS """ + metric


def get_formatted_metrics(metric_expressions: Dict[str, str]) -> Dict[str, str]:
    """Convert metric expressions dict to uppercase keys for SQL template.
    Args:
        metric_expressions (Dict[str, str]): A dictionary of metric expressions
    Returns:
        Dict[str, str]: A dictionary with uppercase keys
    """
    return {key.upper(): value for key, value in metric_expressions.items()}


def get_region_mapping(region: str, mapping_const: dict, mappings: dict = None) -> dict:
    """
    Get region-specific configuration.
    Args:
        region: Region identifier (e.g., 'us-west-2', 'eu-west-1')
        mapping_const: Mapping constant to use
        mappings: Optional custom mappings to override defaults
    Returns:
        dict: Region configuration dictionary
    Raises:
        KeyError: If region is not found in mappings
    """
    region_mappings = mappings or mapping_const
    if region not in region_mappings:
        raise KeyError(f"No configuration found for region: {region}")

    return region_mappings[region]


def generate_industry_vertical_case() -> str:
    """Generate the CASE statement for industry vertical mapping."""
    cases = []
    for vertical, industries in INDUSTRY_VERTICALS.items():
        industries_str = "', '".join(industries)
        cases.append(
            f"WHEN {{ALIAS}}.account_industry IN ('{industries_str}') THEN '{vertical}'"
        )

    return f"""
        COALESCE(
            CASE
                {chr(10).join(cases)}
                WHEN {{ALIAS}}.account_industry IS NULL OR {{ALIAS}}.account_industry = 'Unknown'
                    THEN 'Unknown'
            END, 'Unknown'
        ) AS industry_vertical"""


def generate_dimension_template(
    dimension_template: dict,
    dimension_groups: list[str] = None,
    include_industry_vertical: bool = True,
) -> str:
    """
    Generate the dimension template with configurable dimensions.
    Args:
        dimension_groups (list[str], optional): List of dimension groups to include.
                                              Defaults to all groups.
        include_industry_vertical (bool, optional): Whether to include industry vertical.
                                                  Defaults to True.
        dimension_template (dict): Dimension template to use
    Returns:
        str: SQL SELECT statement with requested dimensions
    """
    if dimension_groups is None:
        dimension_groups = dimension_template.keys()

    dimensions = []

    # Add selected dimension groups
    for group in dimension_groups:
        if group in dimension_template:
            dimensions.extend(
                [f"{value} AS {name}" for name, value in dimension_template[group]]
            )

    # Add industry vertical if requested
    if include_industry_vertical:
        dimensions.append(generate_industry_vertical_case())

    # Join dimensions with commas
    dimension_sql = ",\n".join(dimensions)

    return f"""
    SELECT
        {dimension_sql},"""


def generate_metric_query(
    region: str,
    dimensions: str,
    metric_expression: str,
    source: str,
    table_alias: str,
    join_alias: str,
    group_by: str,
    join_config: dict,
    join_override: Optional[dict] = None,
) -> str:
    """
    Generate a single region's metric query.
    Args:
        region (str): Region identifier
        dimensions (str): Dimension columns
        metric_expression (str): Metric expression
        source (str): Source table name
        table_alias (str): Alias for source table
        join_alias (str): Alias for join table
        group_by (str): Custom GROUP BY clause
        join_config (dict): Join configuration dictionary
        join_override (dict): Override set of joins to use (if provided)
    Returns:
        str: SQL query for the given region CTE
    """

    # Use override if available, fallback to default
    join_stmt = (
        join_override[region].format(join_alias=join_alias)
        if join_override and region in join_override
        else join_config[region]["join_statements"].format(join_alias=join_alias)
    )

    return f"""
        {dimensions}
        {metric_expression}
        FROM {source} {table_alias}
        {join_stmt}
        GROUP BY {group_by}
    """


def generate_metric_cte(
    metric_name: str,
    dimensions_by_region: dict[str, str],
    metrics: List[str],
    sources: dict[str, str],
    table_alias: str,
    join_alias: str,
    group_by: str,
    join_config: dict,
    join_override: Optional[dict] = None,
) -> str:
    """
    Dynamically generates a CTE for a given metric across multiple regions.
    Args:
        metric_name (str): Name of the metric
        dimensions_by_region (dict[str, str]): Dimensions for each region
        metrics (List): List of metrics for CTE
        sources (dict[str, str]): Source tables by region
        table_alias (str): Alias for the source table
        join_alias (str): Alias for the join table
        group_by (str): Custom GROUP BY clause
        join_config (dict): Join configuration dictionary
        join_override (dict): Override set of joins to use (if provided)
    Returns:
        str: The SQL CTE for the given metric
    """

    # Generate query for each region
    region_queries = []
    for region in ["us-west-2", "eu-west-1", "ca-central-1"]:
        metric_expression = ",\n".join(metrics)
        query = generate_metric_query(
            region=region,
            dimensions=dimensions_by_region[region],
            metric_expression=metric_expression,
            source=sources[region],
            table_alias=table_alias,
            join_alias=join_alias,
            group_by=group_by,
            join_config=join_config,
            join_override=join_override,
        )
        region_queries.append(query)

    # Combine all region queries with UNION
    return f"""
    {metric_name} AS (
        {chr(10).join(f'{query} UNION' for query in region_queries[:-1])}
        {region_queries[-1]}
    )"""


def get_anchor_partition(
    base_date: datetime, period: str, offset: int = 1
) -> tuple[str, list[str]]:
    """
    Grabs the anchor partition and dates for a given period
    Args:
        base_date (datetime): The date partition being processed.
        period (str): The timeframe period ('week' or 'month').
        offset (int): Period offset
    Returns:
        tuple: (anchor_partition, dates): Anchor partition for query and start/end dates of period
    """
    if period == "week":
        anchor_day = base_date - relativedelta(weeks=offset)
    elif period == "month":
        anchor_day = base_date - relativedelta(months=offset)
    elif period == "day":
        anchor_day = base_date
    else:
        raise ValueError(f"Unsupported period: {period}")

    anchor_partition = anchor_day.strftime("%Y-%m-%d")
    dates = get_needed_dates(anchor_day, period)
    return anchor_partition, dates


def generate_dimensions(
    date_partition: str,
    period: str,
    anchor_partition: str,
    end_date: str,
    dimension_template: dict,
    region_mapping: dict,
    dimension_dict: dict,
    mappings: dict,
    dimension_groups: list[str],
    include_industry_vertical: bool,
) -> dict[str, str]:
    """Generate all dimension combinations based on templates.
    Args:
        date_partition (str): Date for date column
        period (str): The timeframe period ('week' or 'month')
        anchor_partition (str): Anchor partition for query
        end_date (str): End date of period
        dimension_template (dict): Dimension template to use
        region_mapping (dict): Region mapping to use
        dimension_dict (dict): Dimension dictionary to use
        mappings (dict): Mapping override dictionary
        dimension_groups (list[str]): List of dimension groups to include
        include_industry_vertical (bool): Whether to include industry vertical
    Returns:
        dict: Dimension templates for all regions
    """
    dimensions = {}

    for template_name, regions in dimension_template.items():
        for region_name, config in regions.items():
            region_mapping_var = get_region_mapping(
                region=config["region"], mapping_const=region_mapping, mappings=mappings
            )
            dimension_key = f"{template_name}_{region_name}"  # This will now generate keys like "DIMENSIONS_US"

            dimensions[dimension_key] = generate_dimension_template(
                dimension_template=dimension_dict,
                dimension_groups=dimension_groups,
                include_industry_vertical=include_industry_vertical,
            ).format(
                DATEID=date_partition,
                PERIOD=period,
                START_OF_PERIOD=anchor_partition,
                END_OF_PERIOD=end_date,
                CLOUD_REGION=region_mapping_var["cloud_region"],
                REGION_ALIAS=region_mapping_var["region_alias"],
                ALIAS=config["alias"],
            )

    return dimensions


def generate_timeframe_metrics(
    date_partition: str,
    period: str,
    metrics: dict[str, str],
    dimension_template: dict,
    region_mapping: dict,
    metric_template: dict,
    dimension_dict: dict,
    mappings: dict = None,
    dimension_groups: list[str] = None,
    include_industry_vertical: bool = True,
    offset: int = 1,
) -> tuple[str, list[str], dict]:
    """
    Generate metric configurations and dimensions for a given timeframe.
    Args:
        date_partition (str): Date for date column
        period (str): The timeframe period ('week' or 'month')
        metrics (dict[str, str]): Dictionary of metric expressions
        offset (int): Period offset
        dimension_template (dict): Dimension template to use
        region_mapping (dict): Region mapping to use
        metric_template (dict): Metric template to use
        dimension_dict (dict): Dimension dictionary to use
        mappings (dict): Mapping override dictionary, null by default
        dimension_groups (list[str]): List of dimension groups to include, None by default
        include_industry_vertical (bool): Whether to include industry vertical, True by default
    Returns:
        tuple: (anchor_partition, dates, metric_configs)
    """
    base_date = datetime.strptime(date_partition, "%Y-%m-%d")
    anchor_partition, dates = get_anchor_partition(base_date, period, offset)

    # Generate all dimensions using the template system
    dimensions = generate_dimensions(
        date_partition=date_partition,
        period=period,
        anchor_partition=anchor_partition,
        end_date=dates[0],
        dimension_template=dimension_template,
        region_mapping=region_mapping,
        dimension_dict=dimension_dict,
        mappings=mappings,
        dimension_groups=dimension_groups,
        include_industry_vertical=include_industry_vertical,
    )

    # Generate metric expressions
    metric_expressions = {
        metric_name: generate_metric_expression(metrics, metric_name)
        for metric_name in metrics.keys()
    }

    # Generate final metric configs
    metric_configs = {}
    for metric_name, config in metric_template.items():
        for metric in config["metrics"]:
            if metric in metric_expressions:
                metric_configs[metric_name] = {
                    "dimensions_us": dimensions[
                        config["dimensions_template"].format(region="US")
                    ],
                    "dimensions_eu": dimensions[
                        config["dimensions_template"].format(region="EU")
                    ],
                    "dimensions_ca": dimensions[
                        config["dimensions_template"].format(region="CA")
                    ],
                    "metrics": [metric_expressions[m] for m in config["metrics"]],
                    "us_source": config["us_source"],
                    "eu_source": config["eu_source"],
                    "ca_source": config["ca_source"],
                    "table_alias": config["table_alias"],
                    "join_alias": config["join_alias"],
                    "join_override": config.get("join_override"),
                }

    return anchor_partition, dates, metric_configs


def generate_metric_schema(schema: list[dict], date_column_name: str) -> list[dict]:
    """
    Generate metric configurations and dimensions for a given timeframe.
    Args:
        schema (list[dict]): Schema of the table
        date_column_name (str): Name of the date column
    Returns:
        list[dict]: The new schema to use
    """
    temp_schema = copy.deepcopy(schema)
    for column in temp_schema:
        if column["name"] == "date_placeholder":
            column["name"] = date_column_name
    return temp_schema


def debug_asset_execution(context: AssetExecutionContext):
    """
    Debugs an asset materialization by logging partitioning, upstream dependencies,
    and execution context to diagnose partition dependency issues.
    """

    context.log.info(f"Asset being materialized: {context.asset_key}")

    try:
        context.log.info(f"Partition key range {context.partition_key_range}")
    except:
        context.log.info("Partition key range invalid")

    try:
        context.log.info(f"Partitioned config {context.partitioned_config}")
    except:
        context.log.info("Partition config invalid")

    try:
        context.log.info(f"Partitions def {context.partitions_def}")
    except:
        context.log.info("Partition def invalid")

    if context.has_partition_key:
        current_partition_key = context.partition_key
        context.log.info(f"Current partition key: {current_partition_key}")
    else:
        context.log.info("⚠️ This asset is not partitioned.")
        current_partition_key = None

    partitions = partition_key_ranges_from_context(context)
    context.log.info(f"Partitions from context: {partitions}")

    # Retrieve available upstream asset names correctly
    available_input_names = context.assets_def.keys_by_input_name.keys()
    context.log.info(f"Available input names: {list(available_input_names)}")

    try:
        for input_name in available_input_names:
            key_range_population = context.asset_partition_key_range_for_input(
                input_name
            )
            context.log.info(
                f"Key range population metricspopulation metrics: {key_range_population}"
            )
    except Exception as e:
        context.log.info(e)

    # Log execution metadata
    context.log.info(f"Execution Run ID: {context.run_id}")
    context.log.info(f"Log Metadata (Run Tags): {context.run_tags}")

    # Debug `AssetsDefinition` by checking input definitions
    asset_def_inputs = (
        [input_def.name for input_def in context.assets_def.node_def.input_defs]
        if context.assets_def
        else None
    )
    context.log.info(f"AssetsDefinition Input Names: {asset_def_inputs}")


def get_category_table(region: str) -> str:
    """
    Get the category table name based on the region.
    This should only be used for regional metrics (global metrics have a known table at runtime)

    Args:
        region (str): The region identifier (e.g., 'us-west-2', 'eu-west-1').

    Returns:
        str: The category table name for the specified region.
    """

    if region == AWSRegion.US_WEST_2:
        return "product_analytics_staging.stg_organization_categories"
    elif region == AWSRegion.EU_WEST_1:
        return "product_analytics_staging.stg_organization_categories_eu"
    elif region == AWSRegion.CA_CENTRAL_1:
        return "product_analytics_staging.stg_organization_categories_ca"
    else:
        raise ValueError(f"Unsupported region: {region}")


def get_license_table(region: str) -> str:
    """
    Get the license table name based on the region.
    This should only be used for regional metrics (global metrics have a known table at runtime)

    Args:
        region (str): The region identifier (e.g., 'us-west-2', 'eu-west-1').

    Returns:
        str: The category table name for the specified region.
    """

    if region == AWSRegion.US_WEST_2:
        return "edw.silver.fct_license_orders_daily_snapshot"
    elif region in (AWSRegion.EU_WEST_1, AWSRegion.CA_CENTRAL_1):
        return "edw_delta_share.silver.fct_license_orders_daily_snapshot"
    else:
        raise ValueError(f"Unsupported region: {region}")


def generate_left_join_statements(
    tables_aliases: dict[str, str], dimensions: list[str], org_alias: str = "go"
) -> str:
    """
    Generate a series of LEFT JOIN statements for SQL.
    Parameters:
        tables_aliases (dict): A dictionary where the key is the table name and the value is the alias.
        dimensions (dict): A dictionary where the key is a dimension and the value is a default value.
        org_alias (str): Alias of org table
    Returns:
        str: A string containing LEFT JOIN statements for SQL.
    """
    join_statements = []
    table_aliases = list(tables_aliases.values())

    for i in range(0, len(table_aliases)):
        current_alias = table_aliases[i]
        on_conditions = []

        for dimension in dimensions:
            current_coalesce = f"{current_alias}.{dimension}"
            on_conditions.append(f"{org_alias}.{dimension} = {current_coalesce}")

        join_statement = f"LEFT JOIN {list(tables_aliases.keys())[i]} {current_alias} ON {' AND '.join(on_conditions)}"
        join_statements.append(join_statement)

    return "\n".join(join_statements)


def get_trailing_end_of_quarter_dates(partition_start: str) -> list[str]:
    """
    Get the trailing end of quarter dates for a given partition start date.
    Include the partition start date in the list if it is the end of quarter.
    Quarter end dates are always June 30, September 30, December 31, and March 31.
    """
    current_datetime = datetime.strptime(partition_start, "%Y-%m-%d")
    eoq_dates = []

    eoq = [(3, 31), (6, 30), (9, 30), (12, 31)]
    if (current_datetime.month, current_datetime.day) in (eoq):
        eoq_dates.append(partition_start)
        eoq.remove((current_datetime.month, current_datetime.day))
        current_datetime = current_datetime - timedelta(days=1)

    for eoq_date in eoq:
        possible_eoq_date = datetime(current_datetime.year, eoq_date[0], eoq_date[1])
        if possible_eoq_date <= current_datetime:
            eoq_dates.append(possible_eoq_date.strftime("%Y-%m-%d"))
        else:  # if after current_datetime, append the version of the date from the year before
            eoq_dates.append(
                datetime(current_datetime.year - 1, eoq_date[0], eoq_date[1]).strftime(
                    "%Y-%m-%d"
                )
            )
    return eoq_dates


@dataclass
class PartitionRange:
    primary_partition: List[str]
    secondary_partition: List[str]
    secondary_start: str
    secondary_end: str
    selected_secondary_keys: List[str]

    @property
    def date_start(self) -> str:
        return self.primary_partition[0]

    @property
    def date_end(self) -> str:
        return self.primary_partition[-1]

    def format_query(self, query_template: str, **kwargs) -> str:
        return query_template.format(
            date_start=self.date_start,
            date_end=self.date_end,
            **kwargs,
        )


def get_partition_ranges_from_context(
    context: AssetExecutionContext,
    full_secondary_keys: List[str],
) -> PartitionRange:
    partition_ranges = partition_key_ranges_from_range_tags(context)
    primary_partition, secondary_partition = partition_ranges[0], partition_ranges[-1]

    secondary_start = secondary_partition[0]
    secondary_end = secondary_partition[-1]

    selected_secondary_keys = full_secondary_keys[
        full_secondary_keys.index(secondary_start) : full_secondary_keys.index(
            secondary_end
        )
        + 1
    ]

    return PartitionRange(
        primary_partition=primary_partition,
        secondary_partition=secondary_partition,
        secondary_start=secondary_start,
        secondary_end=secondary_end,
        selected_secondary_keys=selected_secondary_keys,
    )


def run_partitioned_queries(
    context: AssetExecutionContext,
    query_builder: Callable[[str], str],  # receives one secondary key at a time
    secondary_keys: List[str],
    repartition_cols: List[str] = ["date", "type"],
) -> DataFrame:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

    dfs = []
    context.log.info(f"Running query for values: {secondary_keys}")

    for value in secondary_keys:
        query = query_builder(value)
        context.log.info(query)
        df = spark.sql(query).repartition(1, *repartition_cols)
        dfs.append(df)

    return merge_pyspark_dataframes(dfs)


def get_region_from_context(context: AssetExecutionContext) -> str:
    """
    Gets the AWS region for the current asset based on the execution context,
    assuming the first part of the asset key is the region name.
    """
    return context.asset_key.path[0]


def get_regional_firmware_s3_bucket(context: AssetExecutionContext) -> str:
    """
    Get the region-specific S3 bucket for firmware/VDP assets.

    Args:
        context: Dagster asset execution context

    Returns:
        str: S3 bucket URL for the current region

    Example:
        EU region: "s3://samsara-eu-databricks-workspace"
        US/other regions: "s3://samsara-databricks-workspace"
    """
    from dataweb.userpkgs.constants import AWSRegion

    region = get_region_from_context(context)
    if region == AWSRegion.US_WEST_2:
        return "s3://samsara-databricks-workspace"
    elif region == AWSRegion.EU_WEST_1:
        return "s3://samsara-eu-databricks-workspace"
    elif region == AWSRegion.CA_CENTRAL_1:
        return "s3://samsara-ca-databricks-workspace"
    else:
        raise ValueError(f"Unsupported region: {region}")


def get_metric_asset_name(asset_name: str) -> str:
    """
    Given the name of a metric (where aggregation is the last part),
    returns the name of the underlying SQL file (which is everything up to that).

    Example: device_licenses_count -> device_licenses
    """
    return '_'.join(asset_name.split('_')[:-1])


def get_final_metric_sql(sql_string: str, region: str) -> str:
    """
    Given a region, returns the final SQL string for that region's metric,
    performing any necessary substitutions.

    Args:
        region (str): The AWS region identifier (e.g., 'us-west-2', 'eu-west-1').
    """
    categories_source = get_category_table(region)
    license_source = get_license_table(region)
    # if this is a EU or CA regional metric, we need to replace the categories/license tables with the EU or CA version (as it'll be using the US version otherwise)
    # otherwise, it's already either hardcoded with correct US table or global (so in US only)
    if region in [AWSRegion.EU_WEST_1, AWSRegion.CA_CENTRAL_1]:
        sql_string = sql_string.replace(
            get_category_table(AWSRegion.US_WEST_2),
            categories_source
        )
        sql_string = sql_string.replace(
            get_license_table(AWSRegion.US_WEST_2),
            license_source
        )
    return sql_string


def get_metric_sql(context: AssetExecutionContext) -> str:
    """
    Reads the SQL file for a given metric definition, performing any substituions as needed

    Parameters:
        context (AssetExecutionContext): The execution context for the asset.

    Returns:
        str: The SQL query string for the metric.
    """

    asset_name = get_metric_asset_name(context.asset_key.path[-1]) # gets metric name minus the aggregation
    sql_file_path = Path(__file__).parent.parent / "assets" / "metrics" / f"{asset_name}.sql"
    with open(sql_file_path, "r") as f:
        sql_string = f.read()
        region = get_region_from_context(context)
        return get_final_metric_sql(sql_string, region)
