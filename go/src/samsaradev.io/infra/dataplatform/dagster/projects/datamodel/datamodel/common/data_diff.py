import json

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import coalesce, col, concat_ws, lit, round, sha2, to_json
from pyspark.sql.types import StringType, StructType

DECIMAL_PRECISION = 9


def is_primitive_type(dtype):
    return not (
        dtype.startswith("array<")
        or dtype.startswith("map<")
        or dtype.startswith("struct<")
    )


def compare_schemas(schema1: StructType, schema2: StructType) -> bool:
    fields1 = {field.name: field.dataType for field in schema1}
    fields2 = {field.name: field.dataType for field in schema2}

    is_same = True

    for field, dtype in fields1.items():
        if field not in fields2:
            print(
                f"\nField {field} present in DataFrame 1 but missing from DataFrame 2."
            )
            is_same = False
        elif fields2[field] != dtype:
            print(
                f"\nField {field} has different data types: \n\n{dtype} in DataFrame 1 vs \n\n{fields2[field]} in DataFrame 2.\n"
            )
            is_same = False

    for field, dtype in fields2.items():
        if field not in fields1:
            print(f"Field {field} missing from DataFrame 1 but present in DataFrame 2.")
            is_same = False

    return is_same


def compare_dataframes(
    df1: DataFrame, df2: DataFrame, excluded_columns: list = [], json_columns: list = []
) -> float:
    """
    this function is not yet tested and is considered alpha
    """
    df1 = df1.drop(*excluded_columns)
    df2 = df2.drop(*excluded_columns)

    # Check if schemas are the same
    if not compare_schemas(df1.schema, df2.schema):
        print("Error, schemas are different")
        return False
    else:
        print("schemas match!")

    for json_col in json_columns:
        df1 = df1.withColumn(json_col, to_json(json_col))
        df2 = df2.withColumn(json_col, to_json(json_col))

    # Check if the number of rows are the same
    df1_count = df1.count()
    df2_count = df2.count()
    row_ratio = df1_count / df1_count
    if df1_count != df2_count:
        print(f"Number of rows are different: {df1_count} vs {df2_count}")
        print(f"Ratio: {row_ratio}")
        if row_ratio < 1:
            return False
    else:
        print(f"Row counts match with: {df1_count}")

    # Filter out complex types for hashing
    primitive_columns1 = [
        col_name for col_name, dtype in df1.dtypes if is_primitive_type(dtype)
    ]
    primitive_columns2 = [
        col_name for col_name, dtype in df2.dtypes if is_primitive_type(dtype)
    ]
    non_primitive_columns1 = [
        col_name for col_name, dtype in df1.dtypes if not is_primitive_type(dtype)
    ]

    print(f"analyzing primitive columns: {primitive_columns1}\n")
    print(f"skipping non-primitive columns: {non_primitive_columns1}\n")

    # Create a hash column for each DataFrame
    df1_hashed = df1.withColumn("hash", sha2(concat_ws("||", *primitive_columns1), 256))
    df2_hashed = df2.withColumn("hash", sha2(concat_ws("||", *primitive_columns2), 256))

    # Find the differing rows
    common_hashes = df1_hashed.select("hash").intersect(df2_hashed.select("hash"))
    only_in_df1 = df1_hashed.join(common_hashes, "hash", "left_anti")
    only_in_df2 = df2_hashed.join(common_hashes, "hash", "left_anti")

    only_in_df1_count = only_in_df1.count()
    only_in_df2_count = only_in_df2.count()

    if only_in_df1_count > 0 or only_in_df2_count > 0:
        print("DataFrames have different content based on hash comparison.")
        print(f"{only_in_df1_count} different rows in df1")
        print(f"{only_in_df2_count} different rows in df2")

    result = ((only_in_df1_count + only_in_df2_count) / 2) / df1_count

    if result == 0:
        print("Data matches!")

    return result


def compare_dataframes_for_vdp(
    df1: DataFrame, df2: DataFrame, include_plot_lists, show_example_differences=False
) -> float:
    excluded_columns = ["object_stat"]
    df1 = df1.drop(*excluded_columns)
    df2 = df2.drop(*excluded_columns)

    # Check if schemas are the same
    if not compare_schemas(df1.schema, df2.schema):
        print("Error, schemas are different")
        return False
    else:
        print("schemas match!")

    df1 = df1.withColumn("window", to_json("window"))
    df2 = df2.withColumn("window", to_json("window"))

    df1 = df1.withColumn("value_string", col("value.string"))
    df2 = df2.withColumn("value_string", col("value.string"))
    df1 = df1.withColumn("value_double", round(col("value.double"), DECIMAL_PRECISION))
    df2 = df2.withColumn("value_double", round(col("value.double"), DECIMAL_PRECISION))

    # Check if the number of rows are the same
    df1_count = df1.count()
    df2_count = df2.count()
    row_ratio = df1_count / df1_count
    if df1_count != df2_count:
        print(f"Number of rows are different: {df1_count} vs {df2_count}")
        print(f"Ratio: {row_ratio}")
        if row_ratio < 0.99:
            return False
    else:
        print(f"Row counts match with: {df1_count}")

    if include_plot_lists:
        print("running while including plot lists")
        df1 = df1.withColumn(
            "rounded_x",
            F.expr("TRANSFORM(value.plot_list, el -> printf('%.1f', round(el.x, 1)))"),
        ).withColumn("rounded_x", F.array_join("rounded_x", ","))
        df2 = df2.withColumn(
            "rounded_x",
            F.expr("TRANSFORM(value.plot_list, el -> printf('%.1f', round(el.x, 1)))"),
        ).withColumn("rounded_x", F.array_join("rounded_x", ","))

        df1 = df1.withColumn(
            "rounded_y",
            F.expr("TRANSFORM(value.plot_list, el -> printf('%.1f', round(el.x, 1)))"),
        ).withColumn("rounded_y", F.array_join("rounded_y", ","))
        df2 = df2.withColumn(
            "rounded_y",
            F.expr("TRANSFORM(value.plot_list, el -> printf('%.1f', round(el.x, 1)))"),
        ).withColumn("rounded_y", F.array_join("rounded_y", ","))
    else:
        print("running while excluding plot lists")

    # Filter out complex types for hashing
    primitive_columns1 = [
        col_name for col_name, dtype in df1.dtypes if is_primitive_type(dtype)
    ]
    primitive_columns2 = [
        col_name for col_name, dtype in df2.dtypes if is_primitive_type(dtype)
    ]
    non_primitive_columns1 = [
        col_name for col_name, dtype in df1.dtypes if not is_primitive_type(dtype)
    ]

    print(f"analyzing primitive columns: {primitive_columns1}\n")
    print(f"skipping non-primitive columns: {non_primitive_columns1}\n")

    # Create a hash column for each DataFrame
    df1_hashed = df1.withColumn("hash", sha2(concat_ws("||", *primitive_columns1), 256))
    df2_hashed = df2.withColumn("hash", sha2(concat_ws("||", *primitive_columns2), 256))

    # Find the differing rows
    common_hashes = df1_hashed.select("hash").intersect(df2_hashed.select("hash"))
    only_in_df1 = df1_hashed.join(common_hashes, "hash", "left_anti")
    only_in_df2 = df2_hashed.join(common_hashes, "hash", "left_anti")

    only_in_df1_count = only_in_df1.count()
    only_in_df2_count = only_in_df2.count()

    if only_in_df1_count > 0 or only_in_df2_count > 0:
        print("DataFrames have different content based on hash comparison.")
        print(f"{only_in_df1_count} different rows in df1")
        print(f"{only_in_df2_count} different rows in df2")

        field_list = [
            "type",
            "analysis",
            "org_id",
            "object_id",
            "product_id",
            "make",
            "model",
            "year",
            "engine_model",
            "primary_fuel_type",
            "secondary_fuel_type",
            "electrification_level",
            "value_double",
        ]

        if show_example_differences:
            if include_plot_lists:
                field_list.extend(["rounded_x", "rounded_y"])

            if only_in_df1_count > 0:
                print("Rows only in DataFrame 1:")
                only_in_df1.drop("hash").orderBy(*field_list).select(*field_list).show(
                    5, truncate=True
                )
            if only_in_df2_count > 0:
                print("Rows only in DataFrame 2:")
                only_in_df2.drop("hash").orderBy(*field_list).select(*field_list).show(
                    5, truncate=True
                )

            if only_in_df1_count > 0:
                df3a = only_in_df1.drop("hash").select(*field_list)
                df3b = only_in_df2.drop("hash").select(*field_list)
                df3 = df3a.join(df3b, field_list[:-2], "inner")
                grouped_df = df3.groupBy(["type", "analysis"]).count()
                sorted_grouped_df = grouped_df.orderBy(F.desc("count"))
                sorted_grouped_df.count()
                sorted_grouped_df.show(100, truncate=False)

    result = ((only_in_df1_count + only_in_df2_count) / 2) / df1_count

    if result == 0:
        print("Data matches!")

    return result
