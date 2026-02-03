-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Migrate TestMetrics to go_test_metrics
-- MAGIC
-- MAGIC Click `Run all` above.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Step 1
-- MAGIC
-- MAGIC Make a view of the bigquery table. This allows us to use SQL queries on `testmetrics`.

-- COMMAND ----------

-- DBTITLE 1,Create testmetrics Temp View in SparkSQL from BigQuery TestMetrics Table
-- MAGIC %python
-- MAGIC spark.read.format("bigquery").option("table", "backend.TestMetrics").load().createOrReplaceTempView("testmetrics")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Step 2
-- MAGIC
-- MAGIC The Python script below will read the table and write it to s3 (in multiple parts) to the developerexperience databricks volume. Developer Experience will write a script that translates the testmetrics CSVs to go_test_times records.

-- COMMAND ----------

-- DBTITLE 1,Select Avg Total Test Time by Go Pkg from testmetrics DESC Order
-- MAGIC %python
-- MAGIC
-- MAGIC # do not write extra logging files when generating the csv.
-- MAGIC spark.conf.set("spark.databricks.io.directoryCommit.createSuccessFile","false") 
-- MAGIC spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
-- MAGIC spark.conf.set("spark.sql.sources.commitProtocolClass", "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol")
-- MAGIC
-- MAGIC query = """
-- MAGIC select * from testmetrics 
-- MAGIC order by StartTime desc
-- MAGIC """
-- MAGIC df = spark.sql(query)
-- MAGIC df.write.format("csv").mode("overwrite").option("header", "true").save("/Volumes/s3/databricks-workspace/developerexperience/migrate_bigquery_testmetrics")