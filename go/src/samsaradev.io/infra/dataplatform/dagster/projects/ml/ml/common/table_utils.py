"""Table op decorator - like DataWeb's @table."""

from functools import wraps
from typing import Any, Callable, List, Optional

from dagster import OpExecutionContext, op


def table_op(
    dq_checks: List[Any],
    database: Optional[str] = None,
    table: Optional[str] = None,
    owner: Optional[str] = None,
    schema: Optional[Any] = None,
    mode: str = "overwrite",
    partition_by: Optional[List[str]] = None,
    log_to_table: bool = True,
    required_resource_keys: Optional[set] = None,
    **op_kwargs,
) -> Callable:
    """Op decorator with automatic DQ checks and table write."""
    from .dq_checks import run_dq_checks

    def decorator(fn: Callable) -> Callable:
        resources = required_resource_keys or set()
        resources.add("databricks_pyspark_step_launcher")

        if "name" not in op_kwargs:
            op_kwargs["name"] = fn.__name__

        # Build tags from database, table, owner
        tags = op_kwargs.get("tags", {})
        if database:
            tags["database"] = database
        if table:
            tags["table"] = table
        if owner:
            tags["owner"] = owner
        tags["dagster/compute_kind"] = "sql"
        op_kwargs["tags"] = tags

        @wraps(fn)
        def wrapper(context: OpExecutionContext, *args, **kwargs):
            df = fn(context, *args, **kwargs)

            # Apply schema BEFORE DQ checks if provided, to ensure
            # correct types and all expected columns exist for validation
            if schema and database and table:
                from pyspark.sql import SparkSession

                spark = SparkSession.builder.enableHiveSupport().getOrCreate()

                # Apply schema by selecting and casting columns
                # (Spark Connect doesn't support df.rdd, so we can't
                # use createDataFrame with rdd)
                # Only select columns that exist in the DataFrame
                existing_columns = set(df.columns)
                select_exprs = []
                missing_columns = []

                for field in schema.fields:
                    if field.name in existing_columns:
                        # Cast each column to the schema type
                        select_exprs.append(
                            df[field.name].cast(field.dataType).alias(field.name)
                        )
                    else:
                        missing_columns.append(field.name)

                if missing_columns:
                    context.log.warning(
                        f"Schema columns not found in DataFrame: "
                        f"{missing_columns}. "
                        f"Available columns: {sorted(existing_columns)}"
                    )

                if not select_exprs:
                    raise ValueError(
                        f"None of the schema columns were found in DataFrame. "
                        f"Schema expects: {[f.name for f in schema.fields]}. "
                        f"DataFrame has: {sorted(existing_columns)}"
                    )

                df = df.select(*select_exprs)
                context.log.info(
                    f"Applied schema with {len(select_exprs)}/"
                    f"{len(schema.fields)} fields"
                )

            # For DQ checks, we want to check only the data that will actually be written
            # With dynamic partition overwrite, this is all partitions present in the dataframe
            # The dataframe itself may contain historical data, but we only check what's being written
            if dq_checks:
                # The dataframe passed to DQ checks is exactly what will be written to the table
                # (with dynamic partition overwrite, only partitions in the dataframe are written)
                run_dq_checks(
                    context,
                    df,
                    dq_checks,
                    raise_on_failure=True,
                    log_to_table=log_to_table,
                    database=database,
                    table=table,
                )

            if database and table:
                from pyspark.sql import SparkSession

                spark = SparkSession.builder.enableHiveSupport().getOrCreate()
                table_name = f"{database}.{table}"
                context.log.info(
                    f"Writing to table: {table_name} "
                    f"(mode={mode}, partition_by={partition_by})"
                )

                # Check for partition existence when using append mode
                # with partitions to prevent duplicate data on job reruns
                should_write = True
                if mode == "append" and partition_by:
                    # Get partition column value(s) from the dataframe
                    # For now, handle single partition column
                    # (date is most common)
                    if len(partition_by) == 1:
                        partition_col = partition_by[0]
                        # Get distinct partition values from the dataframe
                        partition_values = {
                            row[partition_col]
                            for row in df.select(partition_col).distinct().collect()
                        }

                        if spark.catalog.tableExists(table_name):
                            # Check which partitions already exist
                            existing_partitions_df = spark.sql(
                                f"SHOW PARTITIONS {table_name}"
                            )
                            # Handle different SHOW PARTITIONS response
                            # formats: direct column or "partition" column
                            existing_rows = existing_partitions_df.collect()
                            if partition_col in existing_partitions_df.columns:
                                existing_values = {
                                    row[partition_col] for row in existing_rows
                                }
                            elif "partition" in existing_partitions_df.columns:
                                # Parse "date=2025-12-12" format
                                existing_values = {
                                    row["partition"].split("=")[1]
                                    for row in existing_rows
                                }
                            else:
                                # If we can't determine partition format, fail
                                # to prevent accidentally appending duplicates
                                raise ValueError(
                                    f"Could not determine partition format "
                                    f"from SHOW PARTITIONS for table "
                                    f"{table_name}. Available columns: "
                                    f"{existing_partitions_df.columns}. "
                                    f"Expected '{partition_col}' or "
                                    f"'partition'. This prevents accidentally "
                                    f"appending duplicate partitions."
                                )

                            # Filter out data for partitions that exist
                            partitions_to_skip = partition_values & existing_values

                            if partitions_to_skip:
                                context.log.warning(
                                    f"Skipping write for existing "
                                    f"partitions: "
                                    f"{sorted(partitions_to_skip)}. "
                                    f"This prevents duplicate data on "
                                    f"job reruns."
                                )
                                # Filter the dataframe to exclude existing
                                # partitions using isin for efficiency
                                from pyspark.sql import functions as F

                                df = df.filter(
                                    ~F.col(partition_col).isin(list(partitions_to_skip))
                                )

                                # Check if there's any data left to write
                                if df.count() == 0:
                                    should_write = False
                                    context.log.info(
                                        f"No new data to write to "
                                        f"{table_name} - "
                                        f"all partitions already exist"
                                    )

                if should_write:
                    writer = df.write.mode(mode)
                    if partition_by:
                        writer = writer.partitionBy(*partition_by)
                        # Use dynamic partition overwrite when overwriting
                        # partitioned tables
                        if mode == "overwrite":
                            writer = writer.option("partitionOverwriteMode", "dynamic")
                    writer.saveAsTable(table_name)
                    context.log.info(f"Successfully wrote to {table_name}")

            return "Success"

        return op(required_resource_keys=resources, **op_kwargs)(wrapper)

    return decorator
