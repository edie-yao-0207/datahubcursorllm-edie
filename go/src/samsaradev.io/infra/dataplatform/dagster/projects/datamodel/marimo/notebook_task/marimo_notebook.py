"""
Simple Marimo notebook to test Databricks serverless connection.

This notebook connects to Databricks and runs queries to verify connectivity.
"""

import marimo

__generated_with = "0.17.0"
app = marimo.App(width="medium")


@app.cell
def _():
    # Import Databricks utilities
    import marimo as mo
    from databricks_utils import (
        check_connection_status,
        get_recent_traces_count,
        get_total_traces_count,
        run_pyspark_sql,
    )

    print("✅ Databricks utilities imported successfully")

    # Check connection status
    status = check_connection_status()

    mo.md(
        f"""
    ## Databricks Connection Status

    **Status:** {'✅ Ready' if status['has_token'] and status['has_cluster_id'] else '❌ Missing credentials'}

    **Configuration:**
    - `DATABRICKS_HOST`: {status['host']}
    - `DATABRICKS_CLUSTER_ID`: {status['cluster_id']}
    - `DATABRICKS_TOKEN`: {'Set' if status['has_token'] else 'Not set'}

    **Ready to run PySpark queries!** 🚀
    """
    )
    return mo, run_pyspark_sql


@app.cell
def _(mo):
    # SQL Query Configuration
    sql_query = """
        SELECT date, COUNT(1) as trace_count 
        FROM dojo.langfuse_clickhouse_traces 
        WHERE date >= '2025-10-20'
        GROUP BY 1
        ORDER BY 1
    """

    mo.md(
        f"""
    ## SQL Query Configuration

    **Query to execute:**
    ```sql
    {sql_query.strip()}
    ```

    Edit the `sql_query` variable above to change what gets executed! 🔧
    """
    )
    return (sql_query,)


@app.cell
def _(mo, run_pyspark_sql, sql_query):

    # Run PySpark query
    try:
        # Execute the query
        result = run_pyspark_sql(sql_query)
        print("✅ Query result:")
        print(result)

    except Exception as e:
        print(f"❌ Error: {e}")
        mo.md(
            f"""
        ## Query Results

        ❌ **Error:** {str(e)}

        Check your Databricks credentials and cluster configuration.
        """
        )
    return


if __name__ == "__main__":
    app.run()
