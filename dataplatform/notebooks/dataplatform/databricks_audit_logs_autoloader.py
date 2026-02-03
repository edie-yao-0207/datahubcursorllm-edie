# Databricks notebook source
destinationPath = "s3://samsara-databricks-warehouse/auditlog.db/databricks_audit_log"
checkpointsPrefix = "s3://samsara-databricks-workspace/dataplatform/databricks_audit_log_autoloader/checkpoints"

df = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.includeExistingFiles", "true")
    .option("cloudFiles.useIncrementalListing", "true")
    # infer the schema
    .option("cloudFiles.schemaLocation", f"{checkpointsPrefix}/schema_checkpoint/")
    .option(
        "cloudFiles.inferColumnTypes", "true"
    )  # https://docs.databricks.com/spark/latest/structured-streaming/auto-loader-json.html#inferring-nested-json-data
    .option("cloudFiles.partitionColumns", "date")
    .load("s3://samsara-databricks-audit-log/e2")
)

df.writeStream.format("delta").option(
    "checkpointLocation", f"{checkpointsPrefix}/stream_checkpoint/"
).option("mergeSchema", "true").trigger(processingTime="5 minutes").partitionBy(
    "date"
).start(
    destinationPath
).awaitTermination()

"""
After this job has started once and writes to the destination, this command will be run in a notebook to create the metastore entry so we can query this more easily:

CREATE TABLE auditlog.databricks_audit_log USING DELTA LOCATION 's3://samsara-databricks-warehouse/auditlog.db/databricks_audit_log'
"""
