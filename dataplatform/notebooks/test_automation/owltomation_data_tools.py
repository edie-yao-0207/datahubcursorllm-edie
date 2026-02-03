"""
Tools for using Owltomation results

To use in a databricks notebook:
  %run /backend/test_automation/owltomation_data_tools
And then functions can be called
"""
import math
import pandas as pd
from databricks.sdk.runtime import spark
from datetime import datetime, timedelta, timezone

PLAN_TABLE = "owltomation_results.plan_results"
TEST_TABLE = "owltomation_results.test_results"
STEP_TABLE = "owltomation_results.step_results"


def to_float(x):
    """
    Where possible, convert values to floats.
    Intended to be use with apply to a dataframe column:
      step_df['value'] = step_df['value'].apply(to_float)
    """
    try:
        if x == "":
            return math.nan
        return float(x)
    except ValueError:
        return x


def get_tables_spark(start_time=None, end_time=None, days=30):
    """
    Get the owltomation results as spark tables. Defaults to getting the last 30 days of data,
    unless arguments are provided.
    Args:
        start_time: earliest time of data, in the format 'YYYY-MM-DD'.
        end_time: latest time of data, in the format 'YYYY-MM-DD'.
        days: the last X days to look at data for. Only considered if start_time is not provided,
            and defaults to 30 days.
    """
    if start_time:
        start_time = datetime.strptime(start_time, "%Y-%m-%d")
    else:
        start_time = datetime.now(timezone.utc).replace(
            hour=0, minute=0, second=0, microsecond=0
        ) - timedelta(days=days)

    conditional_statement = f"WHERE datetime >= '{start_time}'"
    if end_time:
        end_time = datetime.strptime(end_time, "%Y-%m-%d")
        conditional_statement += f" AND datetime <= '{end_time}'"

    plan_df = spark.sql(f"select * from {PLAN_TABLE} {conditional_statement}")
    test_df = spark.sql(f"select * from {TEST_TABLE} {conditional_statement}")
    step_df = spark.sql(f"select * from {STEP_TABLE} {conditional_statement}")
    return plan_df, test_df, step_df


def get_tables_pandas(start_time=None, end_time=None, days=30):
    """
    Get the owltomation results as pandas tables. Defaults to getting the last 30 days of data,
    unless arguments are provided.
    Args:
        start_time: earliest time of data, in the format 'YYYY-MM-DD'.
        end_time: latest time of data, in the format 'YYYY-MM-DD'.
        days: the last X days to look at data for. Only considered if start_time is not provided,
            and defaults to 30 days.
    Note: this takes longer to load than the spark tables
    """
    plan_df, test_df, step_df = get_tables_spark(start_time, end_time, days)
    plan_df = plan_df.toPandas()
    test_df = test_df.toPandas()
    step_df = step_df.toPandas()
    step_df["value"] = step_df["value"].apply(to_float)

    return plan_df, test_df, step_df
