"""Unit tests for DQ checks module.

Tests the core DQ check functionality including:
- NonEmptyDQCheck
- NonNullDQCheck
- RowCountDQCheck
- PrimaryKeyDQCheck
- ValueRangeDQCheck
- run_dq_checks

Run with: pytest ml_tests/test_dq_checks.py
"""

import pandas as pd
import pytest
from dagster import Failure, build_op_context
from ml.common import (
    NonEmptyDQCheck,
    NonNullDQCheck,
    Operator,
    RowCountDQCheck,
    SQLDQCheck,
    run_dq_checks,
)
from ml.common.dq_checks import _compare_values


class TestCompareValues:
    """Test _compare_values function including between operator."""

    def test_between_operator_passes_in_range(self):
        """Test between operator passes when value is in range."""
        assert _compare_values(50, [40, 60], Operator.between) is True

    def test_between_operator_passes_at_min_boundary(self):
        """Test between operator passes at min boundary."""
        assert _compare_values(40, [40, 60], Operator.between) is True

    def test_between_operator_passes_at_max_boundary(self):
        """Test between operator passes at max boundary."""
        assert _compare_values(60, [40, 60], Operator.between) is True

    def test_between_operator_fails_below_min(self):
        """Test between operator fails when value is below min."""
        assert _compare_values(30, [40, 60], Operator.between) is False

    def test_between_operator_fails_above_max(self):
        """Test between operator fails when value is above max."""
        assert _compare_values(70, [40, 60], Operator.between) is False

    def test_between_operator_fails_with_invalid_format(self):
        """Test between operator fails with invalid expected format."""
        # Not a list/tuple
        assert _compare_values(50, 40, Operator.between) is False
        # List with wrong length
        assert _compare_values(50, [40], Operator.between) is False
        assert _compare_values(50, [40, 50, 60], Operator.between) is False

    def test_other_operators_still_work(self):
        """Test that other operators still work correctly."""
        assert _compare_values(50, 50, Operator.eq) is True
        assert _compare_values(50, 40, Operator.eq) is False
        assert _compare_values(50, 40, Operator.gt) is True
        assert _compare_values(30, 40, Operator.gt) is False
        assert _compare_values(30, 40, Operator.lt) is True
        assert _compare_values(50, 40, Operator.lt) is False
        assert _compare_values(50, 40, Operator.gte) is True
        assert _compare_values(40, 40, Operator.gte) is True
        assert _compare_values(30, 40, Operator.gte) is False
        assert _compare_values(30, 40, Operator.lte) is True
        assert _compare_values(40, 40, Operator.lte) is True
        assert _compare_values(50, 40, Operator.lte) is False


class TestNonEmptyDQCheck:
    """Test NonEmptyDQCheck functionality."""

    def test_nonempty_dataframe_passes(self):
        """Test that nonempty dataframe passes the check."""
        check = NonEmptyDQCheck(name="test_nonempty", block_on_failure=True)
        df = pd.DataFrame({"col1": [1, 2, 3]})
        context = build_op_context()

        result_code, result_dict = check.run(context, df)

        assert result_code == 0
        assert result_dict["Status"] == "success"
        assert result_dict["DQ Check Type"] == "non_empty_dq"

    def test_empty_dataframe_fails(self):
        """Test that empty dataframe fails the check."""
        check = NonEmptyDQCheck(name="test_nonempty", block_on_failure=True)
        df = pd.DataFrame({"col1": []})
        context = build_op_context()

        result_code, result_dict = check.run(context, df)

        assert result_code == 1
        assert result_dict["Status"] == "failure"
        assert result_dict["DQ Check Type"] == "non_empty_dq"

    def test_output_name_in_message(self):
        """Test that output_name is handled correctly."""
        check = NonEmptyDQCheck(
            name="test_nonempty",
            block_on_failure=True,
            output_name="clusters",
        )
        df = pd.DataFrame({"col1": [1, 2, 3]})
        context = build_op_context()

        result_code, result_dict = check.run(context, df, output_name="clusters")

        assert result_code == 0
        assert result_dict["Status"] == "success"


class TestNonNullDQCheck:
    """Test NonNullDQCheck functionality."""

    def test_no_nulls_passes(self):
        """Test that dataframe with no nulls passes."""
        check = NonNullDQCheck(
            name="test_nonnull",
            columns=["col1", "col2"],
            block_on_failure=True,
        )
        df = pd.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]})
        context = build_op_context()

        result_code, result_dict = check.run(context, df)

        assert result_code == 0
        assert result_dict["Status"] == "success"
        assert result_dict["DQ Check Type"] == "non_null_dq"

    def test_nulls_fail(self):
        """Test that dataframe with nulls fails."""
        check = NonNullDQCheck(
            name="test_nonnull", columns=["col1"], block_on_failure=True
        )
        df = pd.DataFrame({"col1": [1, None, 3]})
        context = build_op_context()

        result_code, result_dict = check.run(context, df)

        assert result_code == 1
        assert result_dict["Status"] == "failure"
        assert result_dict["DQ Check Type"] == "non_null_dq"

    def test_empty_columns_skips(self):
        """Test that check with no columns is skipped."""
        check = NonNullDQCheck(name="test_nonnull", columns=[], block_on_failure=True)
        df = pd.DataFrame({"col1": [1, 2, 3]})
        context = build_op_context()

        result_code, result_dict = check.run(context, df)

        assert result_code == 0
        assert result_dict["Status"] == "success"


class TestRowCountDQCheck:
    """Test RowCountDQCheck functionality."""

    def test_exact_count_passes(self):
        """Test exact count validation passes."""
        check = RowCountDQCheck(
            name="test_rowcount", expected_count=3, block_on_failure=True
        )
        df = pd.DataFrame({"col1": [1, 2, 3]})
        context = build_op_context()

        result_code, result_dict = check.run(context, df)

        assert result_code == 0
        assert result_dict["Status"] == "success"
        assert result_dict["DQ Check Type"] == "row_count_dq"

    def test_exact_count_fails(self):
        """Test exact count validation fails."""
        check = RowCountDQCheck(
            name="test_rowcount", expected_count=5, block_on_failure=True
        )
        df = pd.DataFrame({"col1": [1, 2, 3]})
        context = build_op_context()

        result_code, result_dict = check.run(context, df)

        assert result_code == 1
        assert result_dict["Status"] == "failure"

    def test_min_count_passes(self):
        """Test min count validation passes."""
        check = RowCountDQCheck(
            name="test_rowcount", min_count=2, block_on_failure=True
        )
        df = pd.DataFrame({"col1": [1, 2, 3]})
        context = build_op_context()

        result_code, result_dict = check.run(context, df)

        assert result_code == 0
        assert result_dict["Status"] == "success"

    def test_min_count_fails(self):
        """Test min count validation fails."""
        check = RowCountDQCheck(
            name="test_rowcount", min_count=5, block_on_failure=True
        )
        df = pd.DataFrame({"col1": [1, 2, 3]})
        context = build_op_context()

        result_code, result_dict = check.run(context, df)

        assert result_code == 1
        assert result_dict["Status"] == "failure"

    def test_max_count_passes(self):
        """Test max count validation passes."""
        check = RowCountDQCheck(
            name="test_rowcount", max_count=5, block_on_failure=True
        )
        df = pd.DataFrame({"col1": [1, 2, 3]})
        context = build_op_context()

        result_code, result_dict = check.run(context, df)

        assert result_code == 0
        assert result_dict["Status"] == "success"

    def test_max_count_fails(self):
        """Test max count validation fails."""
        check = RowCountDQCheck(
            name="test_rowcount", max_count=2, block_on_failure=True
        )
        df = pd.DataFrame({"col1": [1, 2, 3]})
        context = build_op_context()

        result_code, result_dict = check.run(context, df)

        assert result_code == 1
        assert result_dict["Status"] == "failure"

    def test_min_max_range_passes(self):
        """Test min and max count range validation passes."""
        check = RowCountDQCheck(
            name="test_rowcount",
            min_count=2,
            max_count=5,
            block_on_failure=True,
        )
        df = pd.DataFrame({"col1": [1, 2, 3]})
        context = build_op_context()

        result_code, result_dict = check.run(context, df)

        assert result_code == 0
        assert result_dict["Status"] == "success"


class TestSQLDQCheckBetweenOperator:
    """Test SQLDQCheck with between operator."""

    def test_between_operator_passes(self):
        """Test that between operator works when value is in range."""
        from unittest.mock import MagicMock, patch

        check = SQLDQCheck(
            name="test_between",
            sql="SELECT 50 as observed_value",
            expected_value=[40, 60],  # min=40, max=60
            operator=Operator.between,
            block_on_failure=True,
        )

        # Mock DataFrame with createOrReplaceTempView method
        mock_df = MagicMock()
        context = build_op_context()

        # Mock Spark to return observed_value=50
        mock_result = MagicMock()
        mock_result.asDict.return_value = {"observed_value": 50}
        mock_result.__getitem__ = (
            lambda self, key: 50 if key == "observed_value" else None
        )
        mock_collect_result = [mock_result]

        with patch("pyspark.sql.SparkSession") as mock_spark_session:
            mock_spark = MagicMock()
            mock_spark_session.builder.enableHiveSupport.return_value.getOrCreate.return_value = (
                mock_spark
            )
            mock_spark.sql.return_value.collect.return_value = mock_collect_result

            result_code, result_dict = check.run(context, mock_df)

        assert result_code == 0
        assert result_dict["Status"] == "success"
        assert result_dict["DQ Check Type"] == "sql_dq"
        assert result_dict["Operator"] == "between"

    def test_between_operator_fails_below_min(self):
        """Test that between operator fails when value is below min."""
        from unittest.mock import MagicMock, patch

        check = SQLDQCheck(
            name="test_between",
            sql="SELECT 30 as observed_value",
            expected_value=[40, 60],  # min=40, max=60
            operator=Operator.between,
            block_on_failure=True,
        )

        # Mock DataFrame with createOrReplaceTempView method
        mock_df = MagicMock()
        context = build_op_context()

        # Mock Spark to return observed_value=30 (below min)
        mock_result = MagicMock()
        mock_result.asDict.return_value = {"observed_value": 30}
        mock_result.__getitem__ = (
            lambda self, key: 30 if key == "observed_value" else None
        )
        mock_collect_result = [mock_result]

        with patch("pyspark.sql.SparkSession") as mock_spark_session:
            mock_spark = MagicMock()
            mock_spark_session.builder.enableHiveSupport.return_value.getOrCreate.return_value = (
                mock_spark
            )
            mock_spark.sql.return_value.collect.return_value = mock_collect_result

            result_code, result_dict = check.run(context, mock_df)

        assert result_code == 1
        assert result_dict["Status"] == "failure"
        assert result_dict["DQ Check Type"] == "sql_dq"

    def test_between_operator_fails_above_max(self):
        """Test that between operator fails when value is above max."""
        from unittest.mock import MagicMock, patch

        check = SQLDQCheck(
            name="test_between",
            sql="SELECT 70 as observed_value",
            expected_value=[40, 60],  # min=40, max=60
            operator=Operator.between,
            block_on_failure=True,
        )

        # Mock DataFrame with createOrReplaceTempView method
        mock_df = MagicMock()
        context = build_op_context()

        # Mock Spark to return observed_value=70 (above max)
        mock_result = MagicMock()
        mock_result.asDict.return_value = {"observed_value": 70}
        mock_result.__getitem__ = (
            lambda self, key: 70 if key == "observed_value" else None
        )
        mock_collect_result = [mock_result]

        with patch("pyspark.sql.SparkSession") as mock_spark_session:
            mock_spark = MagicMock()
            mock_spark_session.builder.enableHiveSupport.return_value.getOrCreate.return_value = (
                mock_spark
            )
            mock_spark.sql.return_value.collect.return_value = mock_collect_result

            result_code, result_dict = check.run(context, mock_df)

        assert result_code == 1
        assert result_dict["Status"] == "failure"
        assert result_dict["DQ Check Type"] == "sql_dq"

    def test_between_operator_at_boundaries(self):
        """Test that between operator handles boundary values correctly."""
        from unittest.mock import MagicMock, patch

        # Test at min boundary
        check_min = SQLDQCheck(
            name="test_between_min",
            sql="SELECT 40 as observed_value",
            expected_value=[40, 60],
            operator=Operator.between,
            block_on_failure=True,
        )

        # Mock DataFrame with createOrReplaceTempView method
        mock_df_min = MagicMock()
        context = build_op_context()

        mock_result = MagicMock()
        mock_result.asDict.return_value = {"observed_value": 40}
        mock_result.__getitem__ = (
            lambda self, key: 40 if key == "observed_value" else None
        )
        mock_collect_result = [mock_result]

        with patch("pyspark.sql.SparkSession") as mock_spark_session:
            mock_spark = MagicMock()
            mock_spark_session.builder.enableHiveSupport.return_value.getOrCreate.return_value = (
                mock_spark
            )
            mock_spark.sql.return_value.collect.return_value = mock_collect_result

            result_code, result_dict = check_min.run(context, mock_df_min)

        assert result_code == 0
        assert result_dict["Status"] == "success"

        # Test at max boundary
        check_max = SQLDQCheck(
            name="test_between_max",
            sql="SELECT 60 as observed_value",
            expected_value=[40, 60],
            operator=Operator.between,
            block_on_failure=True,
        )

        # Mock DataFrame with createOrReplaceTempView method
        mock_df_max = MagicMock()

        mock_result = MagicMock()
        mock_result.asDict.return_value = {"observed_value": 60}
        mock_result.__getitem__ = (
            lambda self, key: 60 if key == "observed_value" else None
        )
        mock_collect_result = [mock_result]

        with patch("pyspark.sql.SparkSession") as mock_spark_session:
            mock_spark = MagicMock()
            mock_spark_session.builder.enableHiveSupport.return_value.getOrCreate.return_value = (
                mock_spark
            )
            mock_spark.sql.return_value.collect.return_value = mock_collect_result

            result_code, result_dict = check_max.run(context, mock_df_max)

        assert result_code == 0
        assert result_dict["Status"] == "success"


class TestRunDQChecks:
    """Test run_dq_checks function."""

    def test_all_checks_pass(self):
        """Test successful execution when all checks pass."""
        df = pd.DataFrame({"col1": [1, 2, 3], "col2": [4, 5, 6]})
        checks = [
            NonEmptyDQCheck(name="nonempty"),
            NonNullDQCheck(name="nonnull", columns=["col1", "col2"]),
        ]
        context = build_op_context()

        result = run_dq_checks(
            context, df, checks, raise_on_failure=False, log_to_table=False
        )

        assert result is True

    def test_blocking_check_raises_failure(self):
        """Test that blocking check failure raises Failure."""
        df = pd.DataFrame({"col1": []})  # Empty dataframe
        checks = [NonEmptyDQCheck(name="nonempty")]
        context = build_op_context()

        with pytest.raises(Failure) as exc_info:
            run_dq_checks(
                context, df, checks, raise_on_failure=True, log_to_table=False
            )

        assert "nonempty" in str(exc_info.value)

    def test_non_blocking_check_does_not_raise(self):
        """Test that non-blocking check failure does not raise."""
        df = pd.DataFrame({"col1": []})  # Empty dataframe
        checks = [
            NonEmptyDQCheck(
                name="test_nonempty", block_on_failure=False  # Non-blocking
            )
        ]
        context = build_op_context()

        # Should not raise
        result = run_dq_checks(
            context, df, checks, raise_on_failure=True, log_to_table=False
        )

        assert result is False  # Check failed, but didn't raise

    def test_no_checks_returns_true(self):
        """Test that empty check list returns True."""
        df = pd.DataFrame({"col1": [1, 2, 3]})
        context = build_op_context()

        result = run_dq_checks(
            context, df, [], raise_on_failure=True, log_to_table=False
        )

        assert result is True

    def test_output_name_in_logs(self):
        """Test that output_name is handled correctly."""
        df = pd.DataFrame({"col1": [1, 2, 3]})
        checks = [NonEmptyDQCheck(name="nonempty", output_name="clusters")]
        context = build_op_context()

        # This should work without errors
        result = run_dq_checks(
            context,
            df,
            checks,
            output_name="clusters",
            raise_on_failure=False,
            log_to_table=False,
        )

        assert result is True


if __name__ == "__main__":
    # Run tests with: python -m pytest ml_tests/test_dq_checks.py -v
    pytest.main([__file__, "-v"])
