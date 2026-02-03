"""
Databricks utilities for PySpark and SQL operations.

This module provides functions to connect to Databricks and run queries
using both PySpark and SQL connectors.
"""

import os
from pathlib import Path

import pandas as pd
from databricks import sql
from databricks.connect import DatabricksSession
from dotenv import load_dotenv

# Load environment variables from the correct path
# Get the directory where this script is located
script_dir = Path(__file__).parent
env_path = script_dir / ".env"
load_dotenv(dotenv_path=env_path, override=True)


def connect_to_databricks_spark():
    """Connect to Databricks with PySpark."""
    server_hostname = os.getenv(
        "DATABRICKS_HOST", "samsara-dev-us-west-2.cloud.databricks.com"
    )
    access_token = os.getenv("DATABRICKS_TOKEN")
    cluster_id = os.getenv("DATABRICKS_CLUSTER_ID", "0809-193945-fon8dc5v")

    if not access_token:
        raise ValueError("DATABRICKS_TOKEN environment variable is required")

    if not cluster_id:
        raise ValueError(
            "DATABRICKS_CLUSTER_ID environment variable is required for PySpark"
        )

    # Configure Databricks session with proper headers for classic compute
    spark = DatabricksSession.builder.remote(
        host=server_hostname, token=access_token, cluster_id=cluster_id
    ).getOrCreate()

    return spark


def connect_to_databricks_sql():
    """Connect to Databricks with SQL (fallback)."""
    server_hostname = os.getenv(
        "DATABRICKS_HOST", "samsara-dev-us-west-2.cloud.databricks.com"
    )
    http_path = os.getenv(
        "DATABRICKS_HTTP_PATH", "/sql/1.0/warehouses/9fb6a34db2b0bbde"
    )
    access_token = os.getenv("DATABRICKS_TOKEN")

    if not access_token:
        raise ValueError("DATABRICKS_TOKEN environment variable is required")

    connection = sql.connect(
        server_hostname=server_hostname,
        http_path=http_path,
        access_token=access_token,
    )

    return connection


def run_pyspark_sql(sql_query, description="PySpark SQL query") -> pd.DataFrame:
    """
    Run a SQL query in PySpark compute and return the PySpark DataFrame.
    Spark session is managed internally and closed after execution.
    """
    spark = None
    try:
        print(f"  📊 Running {description}...")
        print(f"  🔍 Executing SQL: {sql_query}")

        # Create Spark session
        spark = connect_to_databricks_spark()
        print("  🔌 Connected to PySpark")

        # Execute SQL query (returns PySpark DataFrame)
        df = spark.sql(sql_query)

        print("  📊 Returning PySpark DataFrame result...")
        # Return as Pandas DataFrame
        return df.toPandas()

    except Exception as e:
        print(f"  ❌ PySpark SQL error: {type(e).__name__}: {str(e)}")
        raise e
    finally:
        # Always clean up Spark session
        if spark is not None:
            spark.stop()
            print("  🔌 PySpark session stopped")


def run_sql_query(connection, sql_query):
    """Run the SQL query (fallback)."""
    cursor = connection.cursor()
    cursor.execute(sql_query)

    result = cursor.fetchone()
    cursor.close()

    return result[0] if result else None


def get_recent_traces_count(date="2025-10-17"):
    """Get the count of traces for a specific date using PySpark."""
    sql_query = f"""
        SELECT COUNT(1) as trace_count
        FROM dojo.langfuse_clickhouse_traces
        WHERE date = '{date}'
    """
    return run_pyspark_sql(sql_query, f"recent traces count for {date}")


def get_total_traces_count():
    """Get the total count of traces using PySpark."""
    sql_query = """
        SELECT COUNT(1) as total_traces
        FROM dojo.langfuse_clickhouse_traces
    """
    return run_pyspark_sql(sql_query, "total traces count")


def check_connection_status():
    """Check if Databricks credentials are available."""
    has_token = bool(os.getenv("DATABRICKS_TOKEN"))
    has_cluster_id = bool(os.getenv("DATABRICKS_CLUSTER_ID"))

    return {
        "has_token": has_token,
        "has_cluster_id": has_cluster_id,
        "host": os.getenv("DATABRICKS_HOST", "Not set (using default)"),
        "cluster_id": os.getenv("DATABRICKS_CLUSTER_ID", "Not set (using default)"),
    }
