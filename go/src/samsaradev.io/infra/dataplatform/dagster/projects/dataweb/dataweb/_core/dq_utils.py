# TODO - unify this library with datamodel project dq utils resources, and move into shared library
import inspect
import json
import os
import re
import time
from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from decimal import Decimal
from enum import Enum
from typing import Any, Dict, List, Literal, Mapping, Optional, Tuple, Union

import delta
from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetsDefinition,
    AutoMaterializePolicy,
    AutoMaterializeRule,
    BackfillPolicy,
    Config,
    DailyPartitionsDefinition,
    ExpectationResult,
    Failure,
    MetadataValue,
    MonthlyPartitionsDefinition,
    MultiPartitionsDefinition,
    OpExecutionContext,
    PartitionsDefinition,
    RetryPolicy,
    asset,
)
from dagster._core.definitions.asset_dep import AssetDep as AssetDep
from dataweb.userpkgs.constants import SLACK_ALERTS_CHANNEL_DATA_ENGINEERING, AWSRegion
from dataweb.userpkgs.utils import get_run_env
from dateutil.relativedelta import relativedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, when
from pyspark.sql.types import *

E2_DATABRICKS_HOST_SSM_PARAMETER_KEY = "E2_DATABRICKS_HOST"

DELTALAKE_DATE_PARTITION_NAME = "date"


class DQConfig(Config):
    unblock: bool = False


class Operator(Enum):
    eq = "eq"
    gt = "gt"
    gte = "gte"
    lt = "lt"
    lte = "lte"
    ne = "ne"
    between = "between"
    all_between = "all_between"


DQResult = Tuple[Literal[1, 0], str]


def build_general_dq_checks(
    asset_name: str,
    primary_keys: List[str],
    non_null_keys: List[str] = None,
    block_before_write: bool = False,
    include_empty_check: bool = True,
):
    """
    Build standard DQ checks for a data asset.

    Args:
        asset_name: Name of the asset for check naming
        primary_keys: List of primary key column names
        non_null_keys: List of non-null column names (defaults to primary_keys if None)
        block_before_write: Whether to block on DQ check failures
        include_empty_check: Whether to include NonEmptyDQCheck (default True for backward compatibility)
    """
    checks = [
        NonNullDQCheck(
            name=f"dq_non_null_{asset_name}",
            non_null_columns=primary_keys if not non_null_keys else non_null_keys,
            block_before_write=block_before_write,
        ),
        PrimaryKeyDQCheck(
            name=f"dq_unique_{asset_name}",
            primary_keys=primary_keys,
            block_before_write=block_before_write,
        ),
    ]

    if include_empty_check:
        checks.append(
            NonEmptyDQCheck(
                name=f"dq_empty_{asset_name}",
                block_before_write=True,
            )
        )

    return checks


@dataclass
class DQCheck:
    def run(
        self,
        context: OpExecutionContext,
        df: Any,
        table: str,
        database: str,
        partition_key: Optional[str] = None,
    ) -> DQResult:
        raise NotImplementedError(
            "You must impelement the run method for your DQ Check"
        )


DQList = Sequence[DQCheck]


@dataclass
class SQLDQCheck(DQCheck):
    name: Union[str, Tuple[str]]
    sql_query: str
    expected_value: Union[int, Tuple[Any]]
    operator: Operator = field(default=Operator.eq)
    check_before_write: bool = field(default=True)
    block_before_write: Union[bool, None] = field(default=None)

    def run(
        self,
        context: OpExecutionContext,
        df: Any,
        table: str,
        database: str,
        partition_key: Optional[str] = None,
    ) -> DQResult:
        return _sql_dq(
            context,
            self.name,
            df,
            database,
            self.sql_query,
            self.expected_value,
            self.operator,
        )

    def format_sql_query(self, sql_query):
        sql_query = re.sub(r" +", " ", sql_query)
        sql_query = re.sub(r" *\n *", "\n", sql_query)
        sql_query = sql_query.strip()
        sql_query = sql_query.replace("\n", " ")

        return sql_query

    def get_details(self):
        return {
            "sql_query": self.format_sql_query(self.sql_query),
            "expected_value": self.expected_value,
            "operator": self.operator.value,
        }

    def get_manual_upstreams(self):
        return self.manual_upstreams


@dataclass
class NonEmptyDQCheck(DQCheck):
    name: Union[str, Tuple[str]]
    check_before_write: bool = field(default=True)
    block_before_write: Union[bool, None] = field(default=True)

    def run(
        self,
        context: OpExecutionContext,
        df: Any,
        table: str,
        database: str,
        partition_key: Optional[str] = None,
    ) -> DQResult:
        return _non_empty_dq(
            context,
            self.name,
            df,
            database,
        )

    def get_details(self):
        return {"name": self.name}

    def get_manual_upstreams(self):
        return None


@dataclass
class TrendDQCheck(DQCheck):
    name: Union[str, Tuple[str]]
    tolerance: float = field(default=0.05)
    check_before_write: bool = field(default=True)
    block_before_write: Union[bool, None] = field(default=None)
    lookback_days: int = field(default=1)
    dimension: Optional[str] = field(default=None)
    min_percent_share: Optional[float] = field(default=0.1)
    period: Optional[str] = field(default="days")

    def run(
        self,
        context: OpExecutionContext,
        df: Any,
        table: str,
        database: str,
        partition_key: Optional[str] = None,
    ) -> DQResult:
        return _trend_dq(
            context=context,
            dq_check_name=self.name,
            df=df,
            database=database,
            table=table,
            tolerance=self.tolerance,
            lookback_days=self.lookback_days,
            dimension=self.dimension,
            min_percent_share=self.min_percent_share,
            partition_key=partition_key,
            period=self.period,
        )

    def get_details(self):
        return {"tolerance": self.tolerance}

    def get_manual_upstreams(self):
        return None


@dataclass
class JoinableDQCheck(DQCheck):
    name: Union[str, Tuple[str]]
    database_2: str
    input_asset_2: str
    join_keys: Sequence[Tuple[str, str]]
    null_right_table_rows_ratio: float = field(default=0.0)
    check_before_write: bool = field(default=True)
    block_before_write: Union[bool, None] = field(default=None)

    def run(
        self,
        context: OpExecutionContext,
        df: Any,
        table: str,
        database: str,
        partition_key: Optional[str] = None,
    ) -> DQResult:
        return _joinable_dq(
            context,
            self.name,
            df,
            self.database_2,
            self.input_asset_2,
            self.join_keys,
            self.null_right_table_rows_ratio,
            partition_key,
        )

    def get_details(self):
        return {
            "database_2": self.database_2,
            "input_asset_2": self.input_asset_2,
        }

    def get_manual_upstreams(self):
        return None


@dataclass
class NonNullDQCheck(DQCheck):
    name: Union[str, Tuple[str]]
    non_null_columns: Sequence[str]
    check_before_write: bool = field(default=True)
    block_before_write: Union[bool, None] = field(default=None)

    def run(
        self,
        context: OpExecutionContext,
        df: Any,
        table: str,
        database: str,
        partition_key: Optional[str] = None,
    ) -> DQResult:
        return _non_null_dq(
            context,
            self.name,
            df,
            database,
            self.non_null_columns,
        )

    def get_details(self):
        return {"non_null_columns": self.non_null_columns}

    def get_manual_upstreams(self):
        return None


@dataclass
class ValueRangeDQCheck(DQCheck):
    name: Union[str, Tuple[str]]
    column_range_map: Mapping[str, Tuple[float, float]]
    check_before_write: bool = field(default=True)
    block_before_write: Union[bool, None] = field(default=None)

    def run(
        self,
        context: OpExecutionContext,
        df: Any,
        table: str,
        database: str,
        partition_key: Optional[str] = None,
    ) -> DQResult:
        return _value_range_dq(
            context,
            self.name,
            df,
            database,
            self.column_range_map,
        )

    def get_details(self):
        return {"column_range_map": self.column_range_map}

    def get_manual_upstreams(self):
        return None


@dataclass
class PrimaryKeyDQCheck(DQCheck):
    name: Union[str, Tuple[str]]
    primary_keys: Sequence[str]
    check_before_write: bool = field(default=True)
    block_before_write: Union[bool, None] = field(default=None)

    def run(
        self,
        context: OpExecutionContext,
        df: Any,
        table: str,
        database: str,
        partition_key: Optional[str] = None,
    ) -> DQResult:
        return _primary_key_dq(
            context,
            self.name,
            df,
            database,
            self.primary_keys,
        )

    def get_details(self):
        return {"primary_keys": self.primary_keys}

    def get_manual_upstreams(self):
        return None


@dataclass
class NonNegativeDQCheck(DQCheck):
    name: Union[str, Tuple[str]]
    columns: Sequence[str]
    check_before_write: bool = field(default=True)
    block_before_write: Union[bool, None] = field(default=None)

    def run(
        self,
        context: OpExecutionContext,
        df: Any,
        table: str,
        database: str,
        partition_key: Optional[str] = None,
    ) -> DQResult:
        return _non_negative_dq(
            context,
            self.name,
            df,
            database,
            self.columns,
        )

    def get_details(self):
        return {"columns": self.columns}

    def get_manual_upstreams(self):
        return None


def _handle_dq_result(
    context: OpExecutionContext,
    dq_check_name: str,
    expected_value: Union[int, float, Tuple[int, float]],
    observed_value: Union[int, float, Tuple[int, float]],
    operator: Operator = Operator.eq,
    dq_check_type: str = "Unknown",
) -> Literal[1, 0]:
    if operator == Operator.eq:
        success = True if observed_value == expected_value else False
    elif operator == Operator.gt:
        success = True if observed_value > expected_value else False
    elif operator == Operator.lt:
        success = True if observed_value < expected_value else False
    elif operator == Operator.gte:
        success = True if observed_value >= expected_value else False
    elif operator == Operator.lte:
        success = True if observed_value <= expected_value else False
    elif operator == Operator.ne:
        success = True if observed_value != expected_value else False
    elif operator == Operator.between:
        success = (
            True if expected_value[0] <= observed_value <= expected_value[1] else False
        )
    elif operator == Operator.all_between:
        success = (
            True
            if all(
                expected_value[0] <= v <= expected_value[1]
                for v in observed_value.values()
            )
            else False
        )

    status = "success" if success else "failure"

    if isinstance(observed_value, float) or isinstance(observed_value, Decimal):
        observed_value = round(float(observed_value), 8)

    result = ExpectationResult(
        success=success,
        label="dq_check_result",
        metadata={
            "DQ Result": MetadataValue.json(
                {
                    "Check Name": dq_check_name,
                    "Status": status,
                    "Operator": str(operator.value),
                    "Comparison Value": expected_value,
                    "Observed Value": observed_value,
                    "DQ Check Type": dq_check_type,
                }
            )
        },
    )

    result_str = json.dumps(result.metadata["DQ Result"].value)

    context.log.info(result_str)

    if not status == "success":
        return 1, result_str
    else:
        return 0, result_str


def get_partition_clause_from_keys(
    partition_keys: Sequence[str], partitions_def: PartitionsDefinition
):

    where_conditions = []

    if isinstance(
        partitions_def, (DailyPartitionsDefinition, MonthlyPartitionsDefinition)
    ):
        values_string = ",".join(
            [
                f"'{item}'" if isinstance(item, str) else f"{item}"
                for item in partition_keys
            ]
        )
        where_conditions.append(f"date IN ({values_string})")

    elif isinstance(partitions_def, (MultiPartitionsDefinition)):

        partition_keys = [key.split("|") for key in partition_keys]

        for idx, partition_dimension_def in enumerate(partitions_def.partitions_defs):
            values_string = ",".join(
                [
                    f"'{item}'" if isinstance(item, str) else f"{item}"
                    for item in tuple(zip(*partition_keys))[idx]
                ]
            )
            where_conditions.append(
                f"{partition_dimension_def.name} IN ({values_string})"
            )

    return " AND ".join(where_conditions)


def get_partition_value_map_from_context(context):
    if hasattr(context.partition_key, "keys_by_dimension"):
        partition_keys_by_dimension = context.partition_key.keys_by_dimension
    elif hasattr(context, "partition_key"):
        partition_keys_by_dimension = {"date": context.partition_key}
    else:
        partition_keys_by_dimension = {}

    return partition_keys_by_dimension


def _sql_dq(
    context: OpExecutionContext,
    dq_check_name: str,
    df: Any,
    database: str,
    sql_query: str,
    expected_value: int,
    operator: Operator = Operator.eq,
    use_macros: bool = field(default=False),
) -> Literal[1, 0]:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()

    df.createOrReplaceTempView("df")

    context.log.info(f"Running DQ query:\n{sql_query}")

    try:
        observed_value = spark.sql(sql_query).collect()[0]["observed_value"]
    except Exception as e:
        context.log.info(e)
        context.log.error(
            """SQL DQ checks must name
                          their return column 'observed_value'
                          """
        )

    return _handle_dq_result(
        context,
        dq_check_name,
        expected_value,
        observed_value,
        operator,
        inspect.currentframe().f_code.co_name.lstrip("_"),
    )


def _non_empty_dq(
    context: OpExecutionContext,
    dq_check_name: str,
    df: Any,
    database: str,
) -> Literal[1, 0]:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()

    observed_value = df.count()

    context.log.info("Running Non Empty DQ Check")
    context.log.info(f"Observed value: {observed_value}")

    return _handle_dq_result(
        context,
        dq_check_name,
        0,
        observed_value,
        Operator.gt,
        inspect.currentframe().f_code.co_name.lstrip("_"),
    )


def _trend_dq(
    context: OpExecutionContext,
    dq_check_name: str,
    df: Any,
    database: str,
    table: str,
    tolerance: float = 0.01,
    lookback_days: int = 1,
    dimension: Optional[str] = None,
    min_percent_share: float = 0.1,
    partition_key: Optional[str] = None,
    period: Optional[str] = "days",
) -> Literal[1, 0]:
    def _find_previous_date(
        input_string: str,
        lookback_days: int = 1,
        period: Literal["days", "weeks", "months"] = "days",
    ) -> str:
        # Attempt to find a date in the format 'YYYY-MM-DD' within the string
        try:
            date_str = input_string.split("'")[
                1
            ]  # Extracts the date assuming it's between single quotes
            date_obj = datetime.strptime(
                date_str, "%Y-%m-%d"
            )  # Converts string to date object
            if period == "days":
                new_date_obj = date_obj - timedelta(
                    days=lookback_days
                )  # Subtracts specified number of days
            elif period == "weeks":
                new_date_obj = date_obj - relativedelta(
                    weeks=lookback_days
                )  # Subtracts specified number of weeks
            elif period == "months":
                new_date_obj = date_obj - relativedelta(
                    months=lookback_days
                )  # Subtracts specified number of months
            new_date_str = new_date_obj.strftime("%Y-%m-%d")  # Converts back to string
            return input_string.replace(
                date_str, new_date_str
            )  # Replaces old date with new date in the string
        except (IndexError, ValueError):
            return "Error: Date format or placement is incorrect."

    spark = SparkSession.builder.enableHiveSupport().getOrCreate()

    df.createOrReplaceTempView("df")

    date_column = "date" if period != "months" else "date_month"

    partition_where_clause = (
        f"{date_column} in ('{partition_key}')" if partition_key else "1=1"
    )

    context.log.info(partition_where_clause)

    partition_where_clause_previous_day = _find_previous_date(
        partition_where_clause, lookback_days, period
    )

    df_previous = spark.sql(
        f"SELECT * FROM {database}.{table} WHERE ({partition_where_clause_previous_day})"
    )
    df_previous.createOrReplaceTempView("df_previous")

    sql_query = f"""
        SELECT SUM(IF({partition_where_clause}, 1, 0)) / GREATEST(SUM(IF({partition_where_clause_previous_day}, 1, 0)), 0.001) AS observed_value
        FROM (SELECT {date_column} FROM df_previous UNION ALL SELECT {date_column} from df)
        WHERE ({partition_where_clause_previous_day}) or ({partition_where_clause})
    """

    if dimension:
        # find unique slices for the dimension
        slice_sql_query = f"""
            WITH t as (
                SELECT {dimension} as slice_value, count(1) as slice_count,
                (COUNT(1) / SUM(COUNT(1)) OVER ()) AS percent_share
                FROM (SELECT {date_column}, {dimension} FROM df_previous UNION ALL SELECT {date_column}, {dimension} from df)
                WHERE {dimension} is not null
                AND {partition_where_clause_previous_day}
                GROUP BY {dimension}
                ORDER BY 2 DESC LIMIT 10
                ),
            t2 as (
                SELECT {dimension} as slice_value, count(1) as slice_count,
                (COUNT(1) / SUM(COUNT(1)) OVER ()) AS percent_share
                FROM (SELECT {date_column}, {dimension} FROM df_previous UNION ALL SELECT {date_column}, {dimension} from df)
                WHERE {dimension} is not null
                AND {partition_where_clause}
                GROUP BY {dimension}
                ORDER BY 2 DESC LIMIT 10
                )
                SELECT slice_value FROM t WHERE percent_share >= {min_percent_share}
                UNION
                SELECT slice_value FROM t2 WHERE percent_share >= {min_percent_share}
        """

        context.log.info(f"Slice SQL Query: {slice_sql_query}")

        slice_values = list(spark.sql(slice_sql_query).toPandas()["slice_value"])

        context.log.info(f"Slice values: {slice_values}")

        observed_values = {}

        for slice_value in slice_values:
            sql_query = f"""
                SELECT SUM(IF({partition_where_clause}, 1, 0)) / GREATEST(SUM(IF({partition_where_clause_previous_day}, 1, 0)), 0.001) AS observed_value
                FROM (SELECT {date_column}, {dimension} FROM df_previous UNION ALL SELECT {date_column}, {dimension} from df)
                WHERE ({partition_where_clause_previous_day} or {partition_where_clause})
                AND {dimension} = '{slice_value}'
            """

            context.log.info(sql_query)

            observed_value = list(spark.sql(sql_query).toPandas()["observed_value"])[0]

            context.log.info(f"Observed value for {slice_value}: {observed_value}")

            if observed_value is None:
                context.log.info(
                    f"Observed value for {slice_value} is None, setting to 0"
                )
                observed_value = 0  # default to 0 if no result is returned

            observed_values[slice_value] = float(observed_value)

    else:
        context.log.info(sql_query)

        result = list(spark.sql(sql_query).toPandas()["observed_value"])[0]
        if result is None:
            context.log.info("Trend DQ check returned no result, setting to 0")
            result = 0  # default to 0 if no result is returned
        observed_values = {"ALL": float(result)}

    context.log.info(f"Observed values: {observed_values}")

    return _handle_dq_result(
        context,
        dq_check_name,
        (1 - tolerance, 1 + tolerance),
        observed_values,
        Operator.all_between,
        inspect.currentframe().f_code.co_name.lstrip("_"),
    )


def _joinable_dq(
    context: OpExecutionContext,
    dq_check_name: str,
    df: Any,
    database_2: str,
    input_asset_2: str,
    join_keys: Sequence[Tuple[str, str]],
    null_right_table_rows_ratio: float = 0.0,
    partition_key: Optional[str] = None,
) -> Literal[1, 0]:
    comparison_value = null_right_table_rows_ratio

    spark = SparkSession.builder.enableHiveSupport().getOrCreate()

    partition_where_clause = f"date in ('{partition_key}')" if partition_key else "1=1"

    df.createOrReplaceTempView("df")

    join_statement = " ON " + " AND ".join(
        [
            f"left_table.{left_key} = right_table.{right_key}"
            for left_key, right_key in join_keys
        ]
    )

    # if the left table is empty, now rows are returned, so we coalesce to a very high value that will always fail the check
    sql_query = f"""WITH t AS (
            SELECT left_table.*, right_table.{join_keys[0][0]} AS right_key
            FROM df left_table
            LEFT JOIN
            (select * from {database_2}.{input_asset_2} where {partition_where_clause}) right_table
            {join_statement}
        )
        SELECT COALESCE(1.0*COUNT_IF(t.right_key IS NULL)/NULLIF(COUNT(*), 0), 9999) AS null_right_table_rows_ratio
        FROM t
    """

    context.log.info(f"Running Joinable DQ query:\n{sql_query}")

    # TODO - allow differently named join keys and convert from USING to ON syntax

    result_df = spark.sql(sql_query).toPandas()

    null_right_table_rows_ratio = list(result_df["null_right_table_rows_ratio"])[0]

    observed_value = null_right_table_rows_ratio

    return _handle_dq_result(
        context,
        dq_check_name,
        comparison_value,
        observed_value,
        Operator.lte,
        inspect.currentframe().f_code.co_name.lstrip("_"),
    )


def _non_null_dq(
    context: OpExecutionContext,
    dq_check_name: str,
    df: Any,
    database: str,
    non_null_columns: Sequence[str],
) -> Literal[1, 0]:
    observed_values = {}

    spark = SparkSession.builder.enableHiveSupport().getOrCreate()

    observed_values = (
        df.groupBy()
        .agg(*[count(when(col(c).isNull(), c)).alias(c) for c in non_null_columns])
        .take(1)[0]
        .asDict()
    )

    comparison_values = {k: 0 for k in non_null_columns}

    return _handle_dq_result(
        context,
        dq_check_name,
        comparison_values,
        observed_values,
        Operator.eq,
        inspect.currentframe().f_code.co_name.lstrip("_"),
    )


def _value_range_dq(
    context: OpExecutionContext,
    dq_check_name: str,
    df: Any,
    database: str,
    column_range_map: Mapping[str, Tuple[int, int]],
) -> Literal[1, 0]:
    observed_values = {}

    spark = SparkSession.builder.enableHiveSupport().getOrCreate()

    df.createOrReplaceTempView("df")

    for col, (min_val, max_val) in column_range_map.items():
        sql_query = f"""
        select count(1) as observed_value FROM df
        WHERE {col} < {min_val} OR {col} > {max_val}"""

        observed_value = list(spark.sql(sql_query).toPandas()["observed_value"])[0]

        observed_values[col] = observed_value

    comparison_values = {k: 0 for k in column_range_map}

    return _handle_dq_result(
        context,
        dq_check_name,
        comparison_values,
        observed_values,
        Operator.eq,
        inspect.currentframe().f_code.co_name.lstrip("_"),
    )


def _non_negative_dq(
    context: OpExecutionContext,
    dq_check_name: str,
    df: Any,
    database: str,
    columns: Sequence[str],
) -> Literal[1, 0]:
    observed_values = {}

    spark = SparkSession.builder.enableHiveSupport().getOrCreate()

    df.createOrReplaceTempView("df")

    for col in columns:
        sql_query = f"""
        select count(1) as observed_value FROM df
        WHERE {col} < 0 """

        observed_value = list(spark.sql(sql_query).toPandas()["observed_value"])[0]
        observed_values[col] = observed_value

    comparison_values = {k: 0 for k in columns}

    return _handle_dq_result(
        context,
        dq_check_name,
        comparison_values,
        observed_values,
        Operator.eq,
        inspect.currentframe().f_code.co_name.lstrip("_"),
    )


def _primary_key_dq(
    context: OpExecutionContext,
    dq_check_name: str,
    df: Any,
    database: str,
    primary_keys: Sequence[str],
) -> Literal[1, 0]:
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()

    context.log.info(f"Running Primary Key DQ Check")

    observed_value = (
        df.groupBy(primary_keys)
        .agg(count("*").alias("num_instances"))
        .filter("num_instances > 1")
        .count()
    )

    return _handle_dq_result(
        context,
        dq_check_name,
        0,
        observed_value,
        Operator.eq,
        inspect.currentframe().f_code.co_name.lstrip("_"),
    )


dq_audit_log_schema = [
    {"name": "partition_str", "type": "long", "nullable": True, "metadata": {}},
    {"name": "op_name", "type": "string", "nullable": False, "metadata": {}},
    {"name": "status", "type": "string", "nullable": False, "metadata": {}},
    {"name": "dagster_run_id", "type": "string", "nullable": False, "metadata": {}},
    {
        "name": "results",
        "type": {
            "type": "map",
            "keyType": "string",
            "valueType": {
                "type": "map",
                "keyType": "string",
                "valueType": "string",
                "valueContainsNull": True,
            },
            "valueContainsNull": True,
        },
        "nullable": False,
        "metadata": {},
    },
]


def to_sql_map(x: Dict[Any, Any]) -> str:
    def value_to_str(value: Any) -> str:
        if isinstance(value, dict):
            # Convert nested dictionaries to string representations and remove single quotes
            dict_str = str(value)
            dict_str_clean = dict_str.replace("'", "")
            return f"'{dict_str_clean}'"
        else:
            return f"'{str(value)}'"

    def first_level_to_map(value: Dict[Any, Any]) -> str:
        return ", ".join(f"'{key}', {value_to_str(val)}" for key, val in value.items())

    map_sql = ", ".join(
        (
            f"'{key}', map({first_level_to_map(value)})"
            if isinstance(value, dict)
            else f"'{key}', {value_to_str(value)}"
        )
        for key, value in x.items()
    )
    return f"map({map_sql})"


def log_dq_checks(
    context: OpExecutionContext,
    result_messages_by_day: Dict,
    database: str,
    within_asset: bool = True,
):
    spark = SparkSession.builder.enableHiveSupport().getOrCreate()
    dagster_run_id = context.run_id

    try:
        if (
            hasattr(context, "partition_key_range")
            and context.partition_key_range
            and context.partition_key_range.start
            and context.partition_key_range.end
        ):
            partition_str = (
                f"{context.partition_key_range.start}:{context.partition_key_range.end}"
            )
        elif hasattr(context, "partition_key") and context.partition_key:
            partition_str = context.partition_key
    except Exception as e:
        context.log.info(f"Error getting partition key: {e}")
        context.log.info(type(e))
        partition_str = (
            datetime.today().strftime("%Y-%m-%d") + "|" + "UNPARTITIONED_RUN"
        )

    dq_check_result_db = "auditlog" if get_run_env() == "prod" else "datamodel_dev"

    op_prefix = "dq_" if within_asset else ""

    op_name = op_prefix + context.op_handle.name.split("__")[-1]

    for partition_str, result_messages in result_messages_by_day.items():
        max_result = 0
        result_message_map = {}
        for result, x in result_messages:
            max_result = max(max_result, result)
            val = json.loads(x)
            check_name = val["Check Name"]
            del val["Check Name"]
            result_message_map[check_name] = val

        status = "success" if max_result == 0 else "failure"

        results_expr = to_sql_map(result_message_map)

        context.log.info(f"results_expr: {results_expr}")
        context.log.info(f"result_message_map: {result_message_map}")

        # for unpartitioned runs, we record the date in the partition_str
        # so that it can be parsed by the DataHub ingestion job (splitting on pipe char)
        if partition_str == "None" or not partition_str:
            partition_str = (
                datetime.today().strftime("%Y-%m-%d") + "|" + "UNPARTITIONED_RUN"
            )

        dq_audit_log_fields = {
            "partition_str": partition_str,
            "op_name": op_name,
            "status": status,
            "dagster_run_id": dagster_run_id,
            "results": results_expr,
        }

        merge_query = f"""
            MERGE INTO {dq_check_result_db}.dq_check_log as target

            USING (SELECT '{partition_str}' as partition_str,
            '{op_name}' as op_name,
            '{status}' as status,
            '{dagster_run_id}' as dagster_run_id,
            {results_expr} as results,
            '{database}' as database
            ) as updates

            ON target.partition_str = updates.partition_str
            and target.op_name = updates.op_name

            when matched then update set *
            when not matched then insert *
            """

        context.log.info(f"merge_query:\n {merge_query}")

        actual_fields = list(dq_audit_log_fields.keys())
        expected_fields = [x["name"] for x in dq_audit_log_schema]

        assert (
            actual_fields == expected_fields
        ), f"""audit log fields must match schema
        expected: {expected_fields}
        actual: {actual_fields}
        """

        success = False
        retry_count = 1

        while not success and retry_count < 5:
            try:
                spark.sql(merge_query)
                success = True
            except delta.exceptions.ConcurrentAppendException:
                retry_count += 1
                delay = (3 * retry_count) ** 2
                context.log.warning(
                    f"merge failed for ConcurrentAppendException, retrying in {delay} seconds"
                )
                time.sleep(delay)

        result_messages = "\n".join([x[1] for x in result_messages])

        context.log.info(f"DQ Results:\n{result_messages}")


def run_dq_checks(
    context,
    config,
    dq_checks,
    partitions_def,
    slack_alerts_channel,
    tolerance,
    database,
    region: str = None,
):
    if os.getenv("MODE") == "DRY_RUN":
        context.log.info("dry run - exiting without materialization")
        return {"dry_run_early_exit": MetadataValue.int(0)}

    result = 0
    result_messages_by_day = defaultdict(list)
    for dq_check in dq_checks:
        if config.unblock:
            context.log.info("manually skipping pipeline blocking settings")
            blocking = False
        else:
            # Temporarily make trend DQ checks non-blocking for Canada region
            # due to volatile row counts during rollout
            if region == AWSRegion.CA_CENTRAL_1 and isinstance(dq_check, TrendDQCheck):
                context.log.info(
                    f"Making trend DQ check {dq_check.name} "
                    f"non-blocking for Canada region"
                )
                blocking = False
            else:
                # Use block_before_write if available, otherwise fall back to blocking
                # block_before_write can be None, which should be treated as non-blocking
                if hasattr(dq_check, "block_before_write"):
                    blocking = dq_check.block_before_write is True
                elif hasattr(dq_check, "blocking"):
                    blocking = dq_check.blocking is True
                else:
                    blocking = False

        if partitions_def:
            partition_keys = context.partition_keys
            partition_str = (
                f"{context.partition_key_range.start}:{context.partition_key_range.end}"
            )

            for key in partition_keys:

                partition_where_clause = get_partition_clause_from_keys(
                    [
                        key,
                    ],
                    partitions_def,
                )
                result_code, result_message = dq_check.run(
                    context,
                    blocking,
                    partition_where_clause,
                    slack_alerts_channel,
                    tolerance,
                )

                result += result_code
                result_messages_by_day[key].append(result_message)

        else:
            key = datetime.today().strftime("%Y-%m-%d")
            partition_str = None
            partition_where_clause = None

            result_code, result_message = dq_check.run(
                context,
                blocking,
                partition_where_clause,
                slack_alerts_channel,
                tolerance,
            )

            result += result_code
            result_messages_by_day[key].append(result_message)

    result_messages_by_day = dict(result_messages_by_day)

    log_dq_checks(context, result_messages_by_day, database)

    return result


def build_dq_asset(
    dq_name: Union[str, Tuple[str]],
    dq_checks: DQList,
    group_name: str,
    partitions_def: Optional[DailyPartitionsDefinition] = None,
    slack_alerts_channel: Optional[str] = SLACK_ALERTS_CHANNEL_DATA_ENGINEERING,
    step_launcher_resource: str = "databricks_pyspark_step_launcher",
    region: str = AWSRegion.US_WEST_2,
    database: str = "no_db",
    backfill_policy: BackfillPolicy = None,
    tolerance: float = 0.01,
) -> AssetsDefinition:
    op_name = f"dq_{'__'.join(list(dq_name) if type(dq_name) is tuple else [dq_name])}"

    input_deps = list(dq_name) if type(dq_name) is tuple else [dq_name]
    input_deps = [AssetKey([region, database, x]) for x in input_deps]

    metadata = {"dq_checks": []}

    for dq_check in dq_checks:
        manual_upstreams = dq_check.get_manual_upstreams()
        if manual_upstreams:
            input_deps = set()
            for upstream in manual_upstreams or []:
                if isinstance(upstream, AssetKey):
                    asset_key = upstream.with_prefix(region)
                    input_deps.add(asset_key)
                elif isinstance(upstream, (Sequence, str)):
                    asset_key = AssetKey(upstream)
                    asset_key = asset_key.with_prefix(region)
                    input_deps.add(asset_key)
                else:
                    raise TypeError("invalid type for 'dq upstreams'")

        metadata["dq_checks"].append(
            {
                "type": dq_check.__class__.__name__,
                "name": dq_check.name,
                "blocking": dq_check.block_before_write,
                "details": dq_check.get_details(),
            }
        )

    assets_with_auto_materialization_policies = [
        "dq_stg_device_activity_daily",
        "dq_lifetime_device_activity",
        "dq_lifetime_device_online",
    ]

    if op_name in assets_with_auto_materialization_policies:
        auto_materialize_policy = AutoMaterializePolicy.eager(
            max_materializations_per_minute=None
        ).with_rules(AutoMaterializeRule.skip_on_not_all_parents_updated())
    else:
        auto_materialize_policy = None

    @asset(
        io_manager_key="metadata_io_manager",
        owners=["team:DataEngineering"],
        key_prefix=[region, database],
        name=op_name,
        compute_kind="dq",
        op_tags={"asset_type": "dq"},
        metadata=metadata,
        required_resource_keys={f"dbx_step_launcher_gp_{region[:2]}"},
        group_name=group_name,
        partitions_def=partitions_def,
        non_argument_deps=set(input_deps),
        auto_materialize_policy=auto_materialize_policy,
        retry_policy=RetryPolicy(max_retries=0),
        backfill_policy=backfill_policy,
    )
    def _asset(context: AssetExecutionContext, config: DQConfig):
        result = run_dq_checks(
            context,
            config,
            dq_checks,
            partitions_def,
            slack_alerts_channel,
            tolerance,
            database,
            region,
        )

        if result > 0:
            raise Failure(description="DQ check failed")

    return _asset
