# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from datetime import date
from pyspark.sql.functions import col, current_date, datediff, to_date
from datetime import date

# pyspark entry point
spark = SparkSession.builder.getOrCreate()

# Get query, route mapping from the last week
query_route = spark.sql("SELECT * FROM perf_infra.org_gql_query_loads")

query_route = query_route.withColumn("date", to_date(col("date"), "yyyy-MM-dd"))

current_date_df = current_date()

query_route_week = query_route.filter(datediff(current_date_df, col("date")) <= 7)

query_route_week = query_route_week.select("query_name", "resource")

query_route_week = query_route_week.dropDuplicates()

# Get query, fieldfunc mapping from S3

schema = StructType(
    [
        StructField("query_name", StringType(), True),
        StructField("field_func", StringType(), True),
    ]
)

current_date = date.today()

query_field_func = (
    spark.read.format("csv")
    .schema(schema)
    .load(
        f"s3://samsara-gql-mapping/{current_date.strftime('%Y-%m-%d')}/query_fieldfunc.csv"
    )
)

# Join spark dataframes to get query, fieldfunc, route mapping

query_fieldfunc_resource = query_route_week.join(
    query_field_func, on="query_name", how="inner"
).select(
    query_route_week["query_name"],
    query_field_func["field_func"],
    query_route_week["resource"],
)

query_fieldfunc_resource = query_fieldfunc_resource.dropDuplicates()

# Write query, fieldfunc, route mapping to S3

query_fieldfunc_resource.repartition(1).write.mode("overwrite").option(
    "header", "true"
).csv(
    f"s3://samsara-gql-mapping/{current_date.strftime('%Y-%m-%d')}/query_fieldfunc_resource"
)
