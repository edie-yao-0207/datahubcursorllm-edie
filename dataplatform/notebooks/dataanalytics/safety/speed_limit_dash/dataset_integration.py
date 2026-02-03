# Databricks notebook source
import re

# Add these constants at the top of the file, after the imports
DATASET_WITH_START_DATES = {
    "iowa_dot": "2024-11-20",
    "ml_cv": "2025-03-10",
}


def get_table_list(schema_name):
    """
    This function returns a list of tables in the specified schema
    Input Parameters:
      schema_name: The name of the schema
    Output:
      A list of tables in the specified schema stored in a "temp_table_list" temp view
    """
    tables_df = spark.sql(f"SHOW TABLES IN default.{schema_name}")
    tables_df.createOrReplaceTempView("temp_table_list")


def get_active_data_sources(schema_name, table_name_like, base_table):
    """
    This function returns a temp view of data sources and when they were active
    Input Parameters:
      schema_name: The name of the schema
      table_name_like: A unique string to find applicable tables related to desired speed limit data source
    Output:
      A temp view of data sources and when they were active
    """
    # create a temp view of the tables in the specified schema
    get_table_list(schema_name)

    if base_table == "iowa_dot_raw" or base_table == "iowa_dot_mapmatched":
        date_created = "TO_DATE(SUBSTRING(tableName, -8), 'yyyyMMdd')"
    elif base_table == "tomtom_raw" or base_table == "tomtom_mapmatched":
        date_created = "MAKE_DATE(CAST(REGEXP_EXTRACT(tableName, 'tomtom_([0-9]{4})([0-9]{2})__', 1) AS INT), CAST(REGEXP_EXTRACT(tableName, 'tomtom_([0-9]{4})([0-9]{2})__', 2) AS INT), 1)"
    elif base_table == "ml_cv_mapmatched":
        date_created = "TO_DATE(REGEXP_EXTRACT(tableName, 'created_on_([0-9]{4}_[0-9]{2}_[0-9]{2})', 1), 'yyyy_MM_dd')"

    # create query to get the start and end dates for each speed limit data source version
    query = (
        """
  WITH base AS (
    SELECT database,
          tableName,
          '"""
        + base_table
        + """' AS base_table,
          """
        + date_created
        + """ AS date_created
    FROM temp_table_list
    WHERE tableName like '%"""
        + table_name_like
        + """%'
  ),
  start_end AS (
    SELECT database,
          base_table,
          tableName,
          CONCAT(database,'.',tableName) AS query_table,
          STRING(date_created) AS available_start,
          STRING(DATE_SUB(LEAD(date_created) OVER(PARTITION BY base_table ORDER BY date_created),1)) AS available_end
    FROM base
    WHERE date_created IS NOT NULL
  )
  SELECT a.date,
        b.*
  FROM definitions.445_calendar a
  INNER JOIN start_end b
    ON a.date BETWEEN b.available_start AND COALESCE(b.available_end,current_date())
  """
    )

    # query and store in a temp view
    df = spark.sql(query)
    df.createOrReplaceTempView(base_table)


# COMMAND ----------

# DBTITLE 1,Functions to Calculate Map Match Figures
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    DoubleType,
    LongType,
    DateType,
    StringType,
)


def get_match_stats_spark(
    raw_view,
    matched_view,
    type,
    start=dbutils.widgets.get("start_date"),
    end=dbutils.widgets.get("end_date"),
):
    if type == "iowa_dot" and start != "" and start < "2024-11-20":
        print(
            f"Start date must be at least 2024-11-20 when evaluating 'iowa_dot'. Received: {start}. Stopping evaluation"
        )

        # Define the schema for the empty DataFrame
        schema = StructType(
            [
                StructField("map_matched_raw", LongType(), True),
                StructField("total_raw", LongType(), True),
                StructField("pct_matched_raw", DoubleType(), True),
                StructField("pct_unmatched_raw", DoubleType(), True),
                StructField("map_matched_traveled", LongType(), True),
                StructField("total_traveled", LongType(), True),
                StructField("pct_matched_traveled", DoubleType(), True),
                StructField("pct_unmatched_traveled", DoubleType(), True),
                StructField("run_date", DateType(), True),
                StructField("state_code", StringType(), True),
                StructField("country_code", StringType(), True),
                StructField("speed_limit_data_source", StringType(), True),
                StructField("speed_limit_data_source_version", StringType(), True),
            ]
        )

        # Create an empty DataFrame with the defined schema
        empty_df = spark.createDataFrame([], schema)

        # Create a temporary view
        empty_df.createOrReplaceTempView(type + "_match_rates")

    else:
        # Read raw and matched data
        daily_raws = spark.sql(
            f"""
                            SELECT date, query_table
                            FROM {raw_view}
                            WHERE date BETWEEN COALESCE(NULLIF('{start}',''),DATE_SUB(CURRENT_DATE(),3)) AND COALESCE(NULLIF('{end}',''),DATE_SUB(CURRENT_DATE(),1))
                            """
        )
        daily_matched = spark.sql(
            f"""
                                SELECT date, query_table
                                FROM {matched_view}
                                WHERE date BETWEEN COALESCE(NULLIF('{start}',''),DATE_SUB(CURRENT_DATE(),3)) AND COALESCE(NULLIF('{end}',''),DATE_SUB(CURRENT_DATE(),1))
                                """
        )

        # Collect date and table mappings as dictionaries
        daily_raws_date_table_dict = {
            row["date"]: row["query_table"] for row in daily_raws.collect()
        }
        daily_matched_date_table_dict = {
            row["date"]: row["query_table"] for row in daily_matched.collect()
        }

        # Initialize an empty list to store results
        results = []

        if type == "iowa_dot":
            for date in daily_raws_date_table_dict.keys():
                q1 = f"""
                    SELECT a.dataset_way_id,
                        b.dataset_way_id AS map_matched
                    FROM {daily_raws_date_table_dict[date]} a
                    LEFT JOIN (SELECT dataset_way_id
                            FROM {daily_matched_date_table_dict[date]}
                            GROUP BY 1) b
                    ON a.dataset_way_id = b.dataset_way_id
                """
                q2 = f"""
                    SELECT a.way_id,
                        matched.osm_way_id AS map_matched
                    FROM (SELECT way_id
                        FROM dataanalytics_dev.way_id_states
                        WHERE revgeo_state = 'IA'
                        GROUP BY 1) a
                    LEFT JOIN (SELECT osm_way_id
                            FROM {daily_matched_date_table_dict[date]}
                            GROUP BY 1) matched
                    ON a.way_id = matched.osm_way_id
                """

                # Execute Spark SQL queries
                df1 = spark.sql(q1)
                df2 = spark.sql(q2)

                # Calculate statistics for raw data
                stats1 = (
                    df1.agg(
                        F.count(F.col("map_matched")).alias("map_matched_raw"),
                        F.count(F.col("dataset_way_id")).alias("total_raw"),
                    )
                    .withColumn(
                        "pct_matched_raw", F.col("map_matched_raw") / F.col("total_raw")
                    )
                    .withColumn("pct_unmatched_raw", 1 - F.col("pct_matched_raw"))
                )

                # Calculate statistics for traveled data
                stats2 = (
                    df2.agg(
                        F.count(F.col("map_matched")).alias("map_matched_traveled"),
                        F.count(F.col("way_id")).alias("total_traveled"),
                    )
                    .withColumn(
                        "pct_matched_traveled",
                        F.col("map_matched_traveled") / F.col("total_traveled"),
                    )
                    .withColumn(
                        "pct_unmatched_traveled", 1 - F.col("pct_matched_traveled")
                    )
                )

                # Combine stats with metadata
                combined_stats = (
                    stats1.join(stats2, how="cross")
                    .withColumn("run_date", F.lit(date))
                    .withColumn("state_code", F.lit("IA"))
                    .withColumn("country_code", F.lit("US"))
                    .withColumn("speed_limit_data_source", F.lit(type))
                    .withColumn(
                        "speed_limit_data_source_version",
                        F.lit(daily_matched_date_table_dict[date]),
                    )
                )

                # Append to result DataFrame
                results.append(combined_stats)

        elif type == "tomtom":
            state_codes = (
                spark.sql(
                    f"SELECT state_code FROM safety_map_data.tomtom_202412__usa__full_ways GROUP BY 1"
                )
                .toPandas()["state_code"]
                .to_list()
            )

            for date in daily_raws_date_table_dict.keys():
                for state in state_codes:
                    q1 = f"""
                        SELECT a.id AS dataset_way_id,
                            b.tomtom_way_id AS map_matched
                        FROM {daily_raws_date_table_dict[date]} a
                        LEFT JOIN (SELECT tomtom_way_id
                                FROM {daily_matched_date_table_dict[date]}
                                WHERE tomtom_way_id IS NOT NULL
                                GROUP BY 1) b
                        ON a.id = b.tomtom_way_id
                        WHERE a.state_code = '{state}'
                    """
                    q2 = f"""
                        SELECT a.way_id,
                            matched.osm_way_id AS map_matched
                        FROM (SELECT way_id
                            FROM dataanalytics_dev.way_id_states
                            WHERE revgeo_state = '{state}'
                            GROUP BY 1) a
                        LEFT JOIN (SELECT osm_way_id
                                FROM {daily_matched_date_table_dict[date]}
                                WHERE tomtom_way_id IS NOT NULL
                                GROUP BY 1) matched
                        ON a.way_id = matched.osm_way_id
                    """

                    # Execute Spark SQL queries
                    df1 = spark.sql(q1)
                    df2 = spark.sql(q2)

                    # Calculate statistics for raw data
                    stats1 = (
                        df1.agg(
                            F.count(F.col("map_matched")).alias("map_matched_raw"),
                            F.count(F.col("dataset_way_id")).alias("total_raw"),
                        )
                        .withColumn(
                            "pct_matched_raw",
                            F.col("map_matched_raw") / F.col("total_raw"),
                        )
                        .withColumn("pct_unmatched_raw", 1 - F.col("pct_matched_raw"))
                    )

                    # Calculate statistics for traveled data
                    stats2 = (
                        df2.agg(
                            F.count(F.col("map_matched")).alias("map_matched_traveled"),
                            F.count(F.col("way_id")).alias("total_traveled"),
                        )
                        .withColumn(
                            "pct_matched_traveled",
                            F.col("map_matched_traveled") / F.col("total_traveled"),
                        )
                        .withColumn(
                            "pct_unmatched_traveled", 1 - F.col("pct_matched_traveled")
                        )
                    )

                    # Combine stats with metadata
                    combined_stats = (
                        stats1.join(stats2, how="cross")
                        .withColumn("run_date", F.lit(date))
                        .withColumn("state_code", F.lit(state))
                        .withColumn("country_code", F.lit("US"))
                        .withColumn("speed_limit_data_source", F.lit(type))
                        .withColumn(
                            "speed_limit_data_source_version",
                            F.lit(daily_matched_date_table_dict[date]),
                        )
                    )

                    # Append to result DataFrame
                    results.append(combined_stats)
        else:
            raise ValueError("Type not recognized")

        result = results[0]
        for r in results[1:]:
            result = result.union(r)

        # Save the result as a temporary view
        result.createOrReplaceTempView(type + "_match_rates")


# COMMAND ----------

# DBTITLE 1,Functions to Calculate Map Match Figures by Road Type
from pyspark.sql import functions as F
from pyspark.sql.types import StringType


def get_match_stats_by_road_type(
    matched_view,
    type,
    start=dbutils.widgets.get("start_date"),
    end=dbutils.widgets.get("end_date"),
):
    from pyspark.sql.functions import lit

    if (
        type in DATASET_WITH_START_DATES
        and start != ""
        and start < DATASET_WITH_START_DATES[type]
    ):
        print(
            f"Start date must be at least {DATASET_WITH_START_DATES[type]} when evaluating '{type}'. Received: {start}. Stopping evaluation"
        )

        # Define the schema for the empty DataFrame
        schema = StructType(
            [
                StructField("osm_highway", StringType(), True),
                StructField("matched", LongType(), True),
                StructField("total_matched", LongType(), True),
                StructField("pct_road_type", DoubleType(), True),
                StructField("run_date", StringType(), True),
                StructField("speed_limit_data_source", StringType(), True),
                StructField("speed_limit_data_source_version", StringType(), True),
                StructField("state_code", StringType(), True),
                StructField("country_code", StringType(), True),
            ]
        )

        # Create an empty DataFrame with the defined schema
        empty_df = spark.createDataFrame([], schema)

        # Create a temporary view
        empty_df.createOrReplaceTempView(type + "_match_rates_by_road_type")

    else:
        # Read raw and matched data
        daily_matched = spark.sql(
            f"""
                                SELECT date, query_table
                                FROM {matched_view}
                                WHERE date BETWEEN COALESCE(NULLIF('{start}',''),DATE_SUB(CURRENT_DATE(),3)) AND COALESCE(NULLIF('{end}',''),DATE_SUB(CURRENT_DATE(),1))
                                """
        )

        # Collect date and table mappings as dictionaries
        daily_matched_date_table_dict = {
            row["date"]: row["query_table"] for row in daily_matched.collect()
        }

        # Determine the appropriate column names and state/country handling
        if type == "iowa_dot":
            column_name = "dataset_way_id"
            has_state_column = False
            query_states = ["IA"]
            query_countries = ["US"]
        elif type == "tomtom":
            column_name = "tomtom_way_id"
            # Fetch unique state codes from TomTom data
            state_codes = (
                spark.sql(
                    f"SELECT tomtom_state_code FROM safety_map_data.osm_20240619__tomtom_202406__usa__map_match WHERE tomtom_state_code IS NOT NULL GROUP BY 1"
                )
                .toPandas()["tomtom_state_code"]
                .to_list()
            )
            has_state_column = True
            query_states = state_codes
            query_countries = ["US"]
        elif type == "ml_cv":
            column_name = "osm_way_id"
            # Fetch unique state and country codes from ML CV data
            state_codes = (
                spark.sql(
                    f"SELECT state FROM dojo.ml_speed_limit_data_created_on_2025_03_10_v0 WHERE state IS NOT NULL GROUP BY 1"
                )
                .toPandas()["state"]
                .to_list()
            )
            country_codes = (
                spark.sql(
                    f"SELECT country FROM dojo.ml_speed_limit_data_created_on_2025_03_10_v0 WHERE country IS NOT NULL GROUP BY 1"
                )
                .toPandas()["country"]
                .to_list()
            )
            has_state_column = True
            query_states = state_codes
            query_countries = country_codes
        else:
            raise ValueError("Type not recognized")

        # Initialize an empty DataFrame to store results
        result = None

        for date in daily_matched_date_table_dict.keys():
            for country in query_countries:
                if not has_state_column:  # Skip state loop for Iowa DOT
                    state = "IA"
                    state_filter = ""
                    state_code_expr = f"'{state}' AS state_code"
                    country_code_expr = f"'{country}' AS country_code"
                    query_states_for_country = [state]
                else:
                    query_states_for_country = query_states
                    country_code_expr = f"'{country}' AS country_code"

                for state in query_states_for_country:
                    if has_state_column:
                        if type == "tomtom":
                            state_filter = f"AND tomtom_state_code = '{state}'"
                        elif type == "ml_cv":
                            state_filter = (
                                f"AND state = '{state}' AND country = '{country}'"
                            )
                        state_code_expr = f"'{state}' AS state_code"

                    q1 = f"""
                    WITH base AS (
                        SELECT osm_highway,
                              COUNT(DISTINCT {column_name}) AS matched
                        FROM {daily_matched_date_table_dict[date]}
                        WHERE {column_name} IS NOT NULL {state_filter}
                        GROUP BY 1
                    ),
                    total AS (
                        SELECT COUNT(DISTINCT {column_name}) AS total_matched
                        FROM {daily_matched_date_table_dict[date]}
                        WHERE {column_name} IS NOT NULL {state_filter}
                    )
                    SELECT a.osm_highway,
                          a.matched,
                          b.total_matched,
                          a.matched / b.total_matched AS pct_road_type,
                          '{date}' AS run_date,
                          '{type}' AS speed_limit_data_source,
                          '{daily_matched_date_table_dict[date]}' AS speed_limit_data_source_version,
                          {state_code_expr},
                          {country_code_expr}
                    FROM base a
                    JOIN total b
                      ON 1=1
                    """

                    # Execute Spark SQL queries
                    combined_stats = spark.sql(q1)

                    # Append to result DataFrame
                    result = (
                        combined_stats
                        if result is None
                        else result.union(combined_stats)
                    )

        # Save the result as a temporary view
        result.createOrReplaceTempView(type + "_match_rates_by_road_type")


# COMMAND ----------

# DBTITLE 1,Iowa DOT Raw Data Sources
get_active_data_sources("safety_map_data", "regulatory_iowadot_raw", "iowa_dot_raw")

# COMMAND ----------

# DBTITLE 1,Iowa DOT Map Matched Data Sources
get_active_data_sources(
    "safety_map_data", "regulatory_iowadot_map_matched", "iowa_dot_mapmatched"
)

# COMMAND ----------

# DBTITLE 1,TomTom Raw Data Sources
get_active_data_sources("safety_map_data", "tomtom_%__usa__full_ways", "tomtom_raw")

# COMMAND ----------

# DBTITLE 1,TomTom Map Matched Data Sources
get_active_data_sources(
    "safety_map_data", "osm_%tomtom%__usa__map_match", "tomtom_mapmatched"
)

# COMMAND ----------

get_active_data_sources("dojo", "ml_speed_limit_data_created_on_", "ml_cv_mapmatched")

# COMMAND ----------

# DBTITLE 1,Get Iowa DOT Map Match Figures
get_match_stats_spark("iowa_dot_raw", "iowa_dot_mapmatched", "iowa_dot")

# COMMAND ----------

# DBTITLE 1,Get TomTom Map Match Figures
get_match_stats_spark("tomtom_raw", "tomtom_mapmatched", "tomtom")

# COMMAND ----------

# DBTITLE 1,Get Iowa DOT Map Match Figures by Road Type
get_match_stats_by_road_type("iowa_dot_mapmatched", "iowa_dot")

# COMMAND ----------

# DBTITLE 1,Get TomTom Map Match Figures by Road Type
get_match_stats_by_road_type("tomtom_mapmatched", "tomtom")

# COMMAND ----------

# DBTITLE 1,Get ML CV Map Match Figures by Road Type
get_match_stats_by_road_type("ml_cv_mapmatched", "ml_cv")

# COMMAND ----------

# DBTITLE 1,Save Results
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS data_analytics.speed_limit_source_full_match_rates
# MAGIC USING delta
# MAGIC PARTITIONED BY (run_date)
# MAGIC AS (
# MAGIC   SELECT * FROM tomtom_match_rates
# MAGIC   UNION
# MAGIC   SELECT * FROM iowa_dot_match_rates
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW speed_limit_source_full_match_rates_updates AS (
# MAGIC   SELECT * FROM tomtom_match_rates
# MAGIC   UNION
# MAGIC   SELECT * FROM iowa_dot_match_rates
# MAGIC );
# MAGIC
# MAGIC MERGE INTO data_analytics.speed_limit_source_full_match_rates AS target
# MAGIC USING speed_limit_source_full_match_rates_updates AS updates
# MAGIC ON target.run_date = updates.run_date
# MAGIC AND target.state_code = updates.state_code
# MAGIC AND target.country_code = updates.country_code
# MAGIC AND target.speed_limit_data_source = updates.speed_limit_data_source
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT * ;

# COMMAND ----------

# DBTITLE 1,Save Results by Road Type
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS data_analytics.speed_limit_source_full_match_rates_by_road_type
# MAGIC USING delta
# MAGIC PARTITIONED BY (run_date)
# MAGIC AS (
# MAGIC   SELECT * FROM tomtom_match_rates_by_road_type
# MAGIC   UNION
# MAGIC   SELECT * FROM iowa_dot_match_rates_by_road_type
# MAGIC   UNION
# MAGIC   SELECT * FROM ml_cv_match_rates_by_road_type
# MAGIC );

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW speed_limit_source_full_match_rates_by_road_type_updates AS (
# MAGIC   SELECT * FROM tomtom_match_rates_by_road_type
# MAGIC   UNION
# MAGIC   SELECT * FROM iowa_dot_match_rates_by_road_type
# MAGIC   UNION
# MAGIC   SELECT * FROM ml_cv_match_rates_by_road_type
# MAGIC );
# MAGIC
# MAGIC MERGE INTO data_analytics.speed_limit_source_full_match_rates_by_road_type AS target
# MAGIC USING speed_limit_source_full_match_rates_by_road_type_updates AS updates
# MAGIC ON target.run_date = updates.run_date
# MAGIC AND target.state_code = updates.state_code
# MAGIC AND target.country_code = updates.country_code
# MAGIC AND target.speed_limit_data_source = updates.speed_limit_data_source
# MAGIC AND target.osm_highway = updates.osm_highway
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT * ;
