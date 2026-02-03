"""
Schema-driven utilities for generating Spark SQL expressions.

This module provides utilities for creating properly typed empty DataFrames
that match Spark schemas, preventing PyArrow schema mismatch errors.
"""

import pandas as pd
from pyspark.sql.types import StructType


def create_empty_dataframe_with_schema(spark_schema: StructType) -> dict:
    """
    Create properly typed empty pandas DataFrame data that matches a Spark schema.

    This utility ensures that empty DataFrames returned from UDFs have the correct
    schema structure, preventing PyArrow schema mismatch errors in distributed processing.

    Args:
        spark_schema: The target Spark schema structure

    Returns:
        Dictionary suitable for creating pandas DataFrame with proper column types

    Example:
        >>> from can_recompiler import get_topological_schema
        >>> schema = get_topological_schema()
        >>> empty_data = create_empty_dataframe_with_schema(schema)
        >>> import pandas as pd
        >>> empty_df = pd.DataFrame(empty_data)  # Properly typed empty DataFrame
    """
    empty_data = {}
    for field in spark_schema.fields:
        # Create proper empty columns based on Spark field types
        if str(field.dataType).startswith("struct"):
            # For struct fields, create None values (will be properly cast by Spark)
            empty_data[field.name] = []
        elif str(field.dataType) == "LongType":
            empty_data[field.name] = pd.Series([], dtype="Int64")
        elif str(field.dataType) == "IntegerType":
            empty_data[field.name] = pd.Series([], dtype="Int32")
        elif str(field.dataType) == "BooleanType":
            empty_data[field.name] = pd.Series([], dtype="boolean")
        elif str(field.dataType) == "StringType":
            empty_data[field.name] = pd.Series([], dtype="string")
        elif str(field.dataType) == "BinaryType":
            empty_data[field.name] = pd.Series([], dtype="object")
        else:
            # Default to object for unknown types
            empty_data[field.name] = pd.Series([], dtype="object")

    return empty_data
