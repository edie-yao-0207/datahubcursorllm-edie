"""DQ checks for ML ops. Use @table_op for automatic validation."""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Literal, Optional, Tuple, Union

from dagster import Failure, OpExecutionContext

from .utils import get_run_env

DQResult = Tuple[Literal[1, 0], Dict[str, Any]]


class Operator(Enum):
    """Comparison operators."""

    eq = "eq"
    gt = "gt"
    lt = "lt"
    gte = "gte"
    lte = "lte"
    between = "between"


def _get_row_count(df: Any) -> int:
    """Get row count from DataFrame (pandas or PySpark/Spark Connect)."""
    if hasattr(df, "__len__") and hasattr(df, "isnull"):
        # pandas DataFrame
        return len(df)
    elif hasattr(df, "count"):
        # PySpark or Spark Connect DataFrame
        return df.count()
    else:
        raise ValueError("Unsupported DataFrame type")


def _is_spark_df(df: Any) -> bool:
    """Check if DataFrame is PySpark/Spark Connect."""
    # Check for Spark-specific attributes that pandas doesn't have
    # Spark Connect doesn't support 'rdd' access (raises exception),
    # but has 'sparkSession' and 'select'/'filter' methods
    # Regular Spark has 'rdd' and 'sparkSession'
    # Pandas has neither
    # We check for sparkSession first (works for both Spark and Spark Connect)
    # Then check for select/filter combo (Spark Connect specific)
    # Avoid checking 'rdd' directly as it raises exception in Spark Connect
    if hasattr(df, "sparkSession"):
        return True
    # Check for Spark DataFrame methods that pandas doesn't have
    if hasattr(df, "select") and hasattr(df, "filter"):
        # Additional check: Spark DataFrames have 'write' attribute
        if hasattr(df, "write"):
            return True
    return False


def _compare_values(
    observed: Union[int, float],
    expected: Union[int, float, List, Tuple],
    operator: Operator,
) -> bool:
    """Compare observed vs expected using operator.

    For 'between' operator, expected should be a list/tuple of
    [min_value, max_value].
    """
    if operator == Operator.eq:
        return observed == expected
    elif operator == Operator.gt:
        return observed > expected
    elif operator == Operator.lt:
        return observed < expected
    elif operator == Operator.gte:
        return observed >= expected
    elif operator == Operator.lte:
        return observed <= expected
    elif operator == Operator.between:
        # Expected should be [min_value, max_value]
        if not isinstance(expected, (list, tuple)) or len(expected) != 2:
            return False
        min_val, max_val = expected[0], expected[1]
        return min_val <= observed <= max_val
    else:
        return False


@dataclass
class DQCheck:
    """Base class for data quality checks."""

    name: str
    block_on_failure: bool = field(default=True)
    output_name: Optional[str] = field(default=None)

    def run(
        self,
        context: OpExecutionContext,
        df: Any,
        output_name: Optional[str] = None,
    ) -> DQResult:
        """Run the DQ check on the provided dataframe.

        Args:
            context: Dagster execution context
            df: DataFrame to check (pandas or PySpark)
            output_name: Optional name for logging

        Returns:
            Tuple of (result_code, result_dict)
            result_code: 0 = passed, 1 = failed
            result_dict: Dict with Status, Operator, Comparison Value,
                        Observed Value, DQ Check Type
        """
        raise NotImplementedError("Subclasses must implement run()")


def _build_dq_result(
    status: str,
    operator: str,
    comparison_value: Any,
    observed_value: Any,
    dq_check_type: str,
) -> Dict[str, Any]:
    """Build DQ result dict matching DataWeb format."""
    return {
        "Status": status,
        "Operator": operator,
        "Comparison Value": comparison_value,
        "Observed Value": observed_value,
        "DQ Check Type": dq_check_type,
    }


@dataclass
class SQLDQCheck(DQCheck):
    """Check using custom SQL query.

    Query must return a column named 'observed_value' which will be
    compared against expected_value using the specified operator.
    The DataFrame is available as a temp view named 'df'.

    For Operator.between, expected_value should be a list of
    [min_value, max_value].
    """

    sql: str = field(default="")
    expected_value: Union[int, float, List, Tuple] = field(default=0)
    operator: Operator = field(default=Operator.eq)

    def run(
        self,
        context: OpExecutionContext,
        df: Any,
        output_name: Optional[str] = None,
    ) -> DQResult:
        """Run SQL check against the DataFrame."""
        if not self.sql:
            return (
                0,
                _build_dq_result("success", "eq", 0, 0, "sql_dq"),
            )

        try:
            from pyspark.sql import SparkSession

            spark = SparkSession.builder.enableHiveSupport().getOrCreate()
            # Only create temp view if it doesn't exist (it may have been
            # created already in run_dq_checks for optimization)
            # Recreating is safe but unnecessary if already exists
            df.createOrReplaceTempView("df")

            context.log.info(f"Running SQL DQ check:\n{self.sql}")
            result = spark.sql(self.sql).collect()

            if not result or "observed_value" not in result[0].asDict():
                return (
                    1,
                    _build_dq_result(
                        "failure",
                        self.operator.value,
                        self.expected_value,
                        "NULL",
                        "sql_dq",
                    ),
                )

            observed = result[0]["observed_value"]
            passed = _compare_values(observed, self.expected_value, self.operator)
            status = "success" if passed else "failure"

            return (
                0 if passed else 1,
                _build_dq_result(
                    status,
                    self.operator.value,
                    self.expected_value,
                    observed,
                    "sql_dq",
                ),
            )

        except Exception as e:
            return (
                1,
                _build_dq_result(
                    "failure",
                    self.operator.value,
                    self.expected_value,
                    str(e),
                    "sql_dq",
                ),
            )


@dataclass
class NonEmptyDQCheck(DQCheck):
    """Check that DataFrame is not empty."""

    def run(
        self,
        context: OpExecutionContext,
        df: Any,
        output_name: Optional[str] = None,
    ) -> DQResult:
        """Check that DataFrame has at least one row."""
        try:
            row_count = _get_row_count(df)
            status = "success" if row_count > 0 else "failure"

            return (
                0 if row_count > 0 else 1,
                _build_dq_result(status, "gt", 0, row_count, "non_empty_dq"),
            )
        except Exception as e:
            return (
                1,
                _build_dq_result("failure", "gt", 0, str(e), "non_empty_dq"),
            )


@dataclass
class NonNullDQCheck(DQCheck):
    """Check that specified columns have no null values."""

    columns: List[str] = field(default_factory=list)

    def run(
        self,
        context: OpExecutionContext,
        df: Any,
        output_name: Optional[str] = None,
    ) -> DQResult:
        """Check that specified columns have no nulls."""
        if not self.columns:
            return (
                0,
                _build_dq_result("success", "eq", {}, {}, "non_null_dq"),
            )

        try:
            null_counts = {}

            if _is_spark_df(df):
                import pyspark.sql.functions as F

                for col in self.columns:
                    if col not in df.columns:
                        raise ValueError(
                            f"Column '{col}' not found in DataFrame. "
                            f"Available columns: {df.columns}"
                        )
                    null_counts[col] = df.filter(F.col(col).isNull()).count()
            else:
                for col in self.columns:
                    if col not in df.columns:
                        raise ValueError(
                            f"Column '{col}' not found in DataFrame. "
                            f"Available columns: {df.columns}"
                        )
                    null_counts[col] = int(df[col].isnull().sum())

            expected = {col: 0 for col in self.columns}
            all_zero = all(count == 0 for count in null_counts.values())
            status = "success" if all_zero else "failure"

            # Log detailed failure information (which columns have nulls)
            if not all_zero:
                cols_with_nulls = {
                    col: count for col, count in null_counts.items() if count > 0
                }
                context.log.error(
                    f"NonNullDQCheck failed - Columns with nulls: " f"{cols_with_nulls}"
                )

            return (
                0 if all_zero else 1,
                _build_dq_result(status, "eq", expected, null_counts, "non_null_dq"),
            )

        except Exception as e:
            context.log.error(
                f"NonNullDQCheck failed with error: " f"{type(e).__name__}: {str(e)}"
            )
            return (
                1,
                _build_dq_result(
                    "failure",
                    "eq",
                    {col: 0 for col in self.columns},
                    str(e),
                    "non_null_dq",
                ),
            )


@dataclass
class PrimaryKeyDQCheck(DQCheck):
    """Check that specified columns form a unique primary key."""

    columns: List[str] = field(default_factory=list)

    def run(
        self,
        context: OpExecutionContext,
        df: Any,
        output_name: Optional[str] = None,
    ) -> DQResult:
        """Check that columns form a unique primary key."""
        if not self.columns:
            return (
                0,
                _build_dq_result("success", "eq", 0, 0, "primary_key_dq"),
            )

        try:
            if _is_spark_df(df):
                total_rows = df.count()
                unique_rows = df.select(self.columns).distinct().count()
            else:
                total_rows = len(df)
                unique_rows = df[self.columns].drop_duplicates().shape[0]

            duplicates = total_rows - unique_rows
            status = "success" if duplicates == 0 else "failure"

            return (
                0 if duplicates == 0 else 1,
                _build_dq_result(status, "eq", 0, duplicates, "primary_key_dq"),
            )

        except Exception as e:
            return (
                1,
                _build_dq_result("failure", "eq", 0, str(e), "primary_key_dq"),
            )


@dataclass
class ValueRangeDQCheck(DQCheck):
    """Check that column values fall within specified range."""

    column: str = field(default="")
    min_value: Optional[float] = field(default=None)
    max_value: Optional[float] = field(default=None)

    def run(
        self,
        context: OpExecutionContext,
        df: Any,
        output_name: Optional[str] = None,
    ) -> DQResult:
        """Check that column values are within range."""
        if not self.column:
            return (
                0,
                _build_dq_result("success", "eq", 0, 0, "value_range_dq"),
            )

        try:
            if _is_spark_df(df):
                import pyspark.sql.functions as F

                conditions = []
                if self.min_value is not None:
                    conditions.append(F.col(self.column) < self.min_value)
                if self.max_value is not None:
                    conditions.append(F.col(self.column) > self.max_value)

                if len(conditions) == 1:
                    out_of_range = df.filter(conditions[0]).count()
                elif len(conditions) == 2:
                    combined = conditions[0] | conditions[1]
                    out_of_range = df.filter(combined).count()
                else:
                    out_of_range = 0
            else:
                col_data = df[self.column].dropna()
                below_min = (
                    (col_data < self.min_value).sum()
                    if self.min_value is not None
                    else 0
                )
                above_max = (
                    (col_data > self.max_value).sum()
                    if self.max_value is not None
                    else 0
                )
                out_of_range = int(below_min + above_max)

            status = "success" if out_of_range == 0 else "failure"
            expected_range = [self.min_value, self.max_value]

            return (
                0 if out_of_range == 0 else 1,
                _build_dq_result(
                    status,
                    "between",
                    expected_range,
                    out_of_range,
                    "value_range_dq",
                ),
            )

        except Exception as e:
            return (
                1,
                _build_dq_result(
                    "failure",
                    "between",
                    [self.min_value, self.max_value],
                    str(e),
                    "value_range_dq",
                ),
            )


@dataclass
class RowCountDQCheck(DQCheck):
    """Check that DataFrame row count meets expectations."""

    expected_count: Optional[int] = field(default=None)
    min_count: Optional[int] = field(default=None)
    max_count: Optional[int] = field(default=None)

    def run(
        self,
        context: OpExecutionContext,
        df: Any,
        output_name: Optional[str] = None,
    ) -> DQResult:
        """Check that row count meets expectations."""
        try:
            count = _get_row_count(df)

            if self.expected_count is not None:
                passed = count == self.expected_count
                status = "success" if passed else "failure"
                return (
                    0 if passed else 1,
                    _build_dq_result(
                        status,
                        "eq",
                        self.expected_count,
                        count,
                        "row_count_dq",
                    ),
                )

            if self.min_count is not None and count < self.min_count:
                return (
                    1,
                    _build_dq_result(
                        "failure", "gte", self.min_count, count, "row_count_dq"
                    ),
                )

            if self.max_count is not None and count > self.max_count:
                return (
                    1,
                    _build_dq_result(
                        "failure",
                        "lte",
                        self.max_count,
                        count,
                        "row_count_dq",
                    ),
                )

            # Passed
            expected = self.min_count if self.min_count else self.max_count
            return (
                0,
                _build_dq_result("success", "gte", expected, count, "row_count_dq"),
            )

        except Exception as e:
            expected = self.expected_count or self.min_count or self.max_count or 0
            return (
                1,
                _build_dq_result("failure", "eq", expected, str(e), "row_count_dq"),
            )


def run_dq_checks(
    context: OpExecutionContext,
    df: Any,
    dq_checks: List[DQCheck],
    output_name: Optional[str] = None,
    raise_on_failure: bool = True,
    log_to_table: bool = True,
    database: Optional[str] = None,
    table: Optional[str] = None,
) -> bool:
    """Run DQ checks on a dataframe."""
    if not dq_checks:
        return True

    label = f" for {output_name}" if output_name else ""
    context.log.info(f"Running {len(dq_checks)} DQ checks{label}")

    # Cache the dataframe to avoid multiple scans when running multiple checks
    # Each check triggers Spark actions (count, filter, etc.) which would
    # otherwise scan the data multiple times
    cached = False
    if _is_spark_df(df):
        context.log.info("Caching dataframe for DQ checks to avoid multiple scans")
        df = df.cache()
        cached = True

        # Create temp view once before running checks (for SQLDQCheck)
        # This avoids recreating the view for each SQL check
        df.createOrReplaceTempView("df")

        # Don't eagerly materialize the cache - let it materialize lazily
        # when the first check runs. This avoids indeterminate output errors
        # that can occur when forcing materialization on dataframes with
        # long lineage

    all_passed = True
    blocking_failures = []
    check_results = {}

    for check in dq_checks:
        code, result_dict = check.run(context, df, output_name)

        # Store result for audit logging (matching DataWeb format)
        check_results[check.name] = result_dict

        # Log the result with details
        status_str = f"[{check.name}] {result_dict['Status']}"
        if code == 0:
            context.log.info(status_str)
        else:
            # Add detailed failure information
            expected = result_dict.get("Comparison Value")
            observed = result_dict.get("Observed Value")
            check_type = result_dict.get("DQ Check Type", "")

            # Format the error message based on check type
            if check_type == "non_null_dq" and isinstance(observed, dict):
                # Show which columns have nulls
                cols_with_nulls = {
                    col: count for col, count in observed.items() if count > 0
                }
                if cols_with_nulls:
                    details = ", ".join(
                        [
                            f"{col}: {count} nulls"
                            for col, count in cols_with_nulls.items()
                        ]
                    )
                    status_str = f"{status_str} - {details}"
            elif expected is not None and observed is not None:
                status_str = (
                    f"{status_str} - Expected: {expected}, " f"Observed: {observed}"
                )

            context.log.error(status_str)
            all_passed = False
            if check.block_on_failure:
                blocking_failures.append(check.name)

    if all_passed:
        context.log.info(f"All DQ checks passed{label}")
    else:
        context.log.error(
            f"DQ checks failed{label} ({len(blocking_failures)} blocking)"
        )

    # Log results to audit table
    if log_to_table:
        _log_dq_results(context, check_results, all_passed, database, table)

    if blocking_failures and raise_on_failure:
        # Build detailed failure message
        failure_details = []
        for check_name in blocking_failures:
            result = check_results[check_name]
            check_type = result.get("DQ Check Type", "")
            observed = result.get("Observed Value")
            expected = result.get("Comparison Value")

            # Add details based on check type
            if check_type == "non_null_dq" and isinstance(observed, dict):
                cols_with_nulls = {
                    col: count for col, count in observed.items() if count > 0
                }
                if cols_with_nulls:
                    details = ", ".join(
                        [
                            f"{col}: {count} nulls"
                            for col, count in cols_with_nulls.items()
                        ]
                    )
                    failure_details.append(f"{check_name} ({details})")
                else:
                    failure_details.append(check_name)
            elif expected is not None and observed is not None:
                failure_details.append(
                    f"{check_name} (expected: {expected}, " f"observed: {observed})"
                )
            else:
                failure_details.append(check_name)

        raise Failure(
            description=f"Blocking DQ checks failed{label}: "
            + "; ".join(failure_details)
        )

    # Unpersist the cached dataframe to free up memory
    if cached:
        try:
            df.unpersist()
            context.log.info("Unpersisted cached dataframe after DQ checks")
        except Exception as e:
            context.log.warning(f"Failed to unpersist cached dataframe: {e}")

    return all_passed


def _log_dq_results(
    context: OpExecutionContext,
    check_results: Dict[str, Dict[str, Any]],
    all_passed: bool,
    database: Optional[str] = None,
    table: Optional[str] = None,
) -> None:
    """Log DQ check results to dq_check_log table."""
    try:
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.enableHiveSupport().getOrCreate()

        # Determine audit database
        db = "auditlog" if get_run_env() == "prod" else "datamodel_dev"

        # Use dq_{table} pattern if table provided, else use op name
        op_name = f"dq_{table}" if table else context.op.name
        dagster_run_id = context.run_id
        status = "success" if all_passed else "failure"

        # Get partition_str if available, else use date
        partition_str = None
        try:
            if hasattr(context, "partition_key") and context.partition_key:
                partition_str = context.partition_key
        except Exception:
            pass

        if not partition_str:
            date_str = datetime.today().strftime("%Y-%m-%d")
            partition_str = f"{date_str}|UNPARTITIONED_RUN"

        # Convert results to map (matching DataWeb format)
        results_map = _to_sql_map(check_results)

        # Use provided database or default to "ml"
        database = database or "ml"

        # Escape quotes in string values
        partition_str_esc = partition_str.replace("'", "''")
        op_name_esc = op_name.replace("'", "''")
        status_esc = status.replace("'", "''")
        dagster_run_id_esc = dagster_run_id.replace("'", "''")
        database_esc = database.replace("'", "''")

        # MERGE into dq_check_log (match on partition_str + op_name)
        merge_query = f"""
            MERGE INTO {db}.dq_check_log AS target
            USING (
                SELECT
                    '{partition_str_esc}' AS partition_str,
                    '{op_name_esc}' AS op_name,
                    '{status_esc}' AS status,
                    '{dagster_run_id_esc}' AS dagster_run_id,
                    {results_map} AS results,
                    '{database_esc}' AS database
            ) AS updates
            ON target.partition_str = updates.partition_str
                AND target.op_name = updates.op_name
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """

        context.log.info(f"Logging to {db}.dq_check_log")
        spark.sql(merge_query)

    except Exception as e:
        context.log.warning(f"Failed to log DQ results: {e}")


def _to_sql_map(x: Dict[str, Any]) -> str:
    """Convert dict to SQL map expression."""

    def escape_quotes(s: str) -> str:
        """Escape single quotes for SQL by doubling them."""
        return s.replace("'", "''")

    def value_to_str(value: Any) -> str:
        if isinstance(value, dict):
            # For nested dicts, convert to string and remove quotes
            dict_str = str(value).replace("'", "")
            return f"'{escape_quotes(dict_str)}'"
        else:
            return f"'{escape_quotes(str(value))}'"

    def first_level_to_map(value: Dict[str, Any]) -> str:
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
