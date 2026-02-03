# Databricks notebook source

# MAGIC %run /backend/dataplatform/boto3_helpers

# COMMAND ----------

import datadog
import boto3
import time


def setUpDataDog():
    # Get Datadog keys from parameter store and initialize datadog
    ssm_client = get_ssm_client("standard-read-parameters-ssm")
    api_key = get_ssm_parameter(ssm_client, "DATADOG_API_KEY")
    app_key = get_ssm_parameter(ssm_client, "DATADOG_APP_KEY")
    datadog.initialize(api_key=api_key, app_key=app_key)


def postMetrics(type, start_time, success, tableName):
    datadog.api.Metric.send(
        metric=f"datastreams.{type}.count",
        points=1,
        type="count",
        tags=[
            f"stream:{tableName}",
            f"region:{boto3.session.Session().region_name}",
            f"success:{success}",
        ],
    )
    datadog.api.Metric.send(
        metric=f"datastreams.{type}.duration",
        points=time.time() - start_time,
        type="gauge",
        tags=[
            f"stream:{tableName}",
            f"region:{boto3.session.Session().region_name}",
            f"success:{success}",
        ],
    )


def main():
    setUpDataDog()
    tablesInDataStreamDeltaLake = spark.sql("show tables in datastreams_history")
    tableNames = (
        tablesInDataStreamDeltaLake.select("tableName")
        .rdd.map(lambda row: row.asDict())
        .collect()
    )

    errors = []
    for t in tableNames:
        tableName = t["tableName"]
        try:
            # Run optimize query
            optimizeQuery = f"OPTIMIZE datastreams_history.{tableName} WHERE date >= date_sub(current_date(), 2)"

            # For api_logs, we want to zorder by org_id as it significantly improves performance for most query
            # cases (i.e. by 1 org), including the async api log downloader page that is used by customers.
            if tableName == "api_logs":
                optimizeQuery += " ZORDER BY org_id"
            # For mobile_logs, we want to zorder by event type because almost every query on this table
            # will only query for 1 event type, and the table is extremely large.
            elif tableName == "mobile_logs":
                optimizeQuery += " ZORDER BY event_type"
            print(f"Starting optimize for {tableName}, query: {optimizeQuery}")
            optimize_start_time = time.time()
            spark.sql(optimizeQuery)
            postMetrics("optimize", optimize_start_time, True, tableName)
            print(f"Finished optimize for {tableName}")
        except Exception as e:
            # Print exception for tables that are not partitioned
            print(f"Error: {t} exception {e}")
            postMetrics("optimize", optimize_start_time, False, tableName)
            errors.append(str(e))

        try:
            # Run vacuum query
            vacuumQuery = "VACUUM datastreams_history.{tableName}"
            print(f"Starting vacuum for {tableName}, query: {vacuumQuery}")
            vacuum_start_time = time.time()
            spark.sql(f"VACUUM datastreams_history.{tableName}")
            postMetrics("vacuum", vacuum_start_time, True, tableName)
            print(f"Finished vacuum for {tableName}")
        except Exception as e:
            # Print exception for tables that are not partitioned
            print(f"Error: {t} exception {e}")
            postMetrics("vacuum", vacuum_start_time, False, tableName)
            errors.append(str(e))

    if len(errors) > 0:
        print(errors)
        raise Exception(
            "failed to optimize/vacuum some tables; see logs for failure information."
        )


if __name__ == "__main__":
    main()
