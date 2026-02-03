-- Databricks notebook source
-- MAGIC %md
-- MAGIC This notebook was originally cloned from [Backend Go Test Times - For Rebalancing & Analysis](https://samsara-dev-us-west-2.cloud.databricks.com/editor/notebooks/2996600192431510?o=5924096274798303#command/2996600192431686), which was made to calculate go test suite runtimes from a to-be-deprecated bigquery table called testmetrics.
-- MAGIC
-- MAGIC This notebook is edited to calculate go test suite runtimes from the go_test_metrics datastream table.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Rebalance Go Tests
-- MAGIC
-- MAGIC Hit "Run cell" (triangle/play) button to generate new table with testsuite and avg_time_secs columns.

-- COMMAND ----------

-- DBTITLE 1,Select Avg Total Test Time by Go Pkg from testmetrics DESC Order
-- calculate average runtime for each testsuite.
select testsuite, avg(total_elapsed) as avg_time_secs
from 
  (
    -- sum the runtime of a testsuite across all of the parallels in one buildkite build.
    select testsuite, sum(elapsed) as total_elapsed
    from datastreams.go_test_metrics
    where git_branch = "master" 
    and start_time > date_sub(NOW(), 7) -- average time for the last week
    and job_result = "pass" 
    and test_result = "pass"
    and build_number != 123456 -- test data build number
    group by testsuite, build_number
  )
group by testsuite
order by avg_time_secs desc

-- COMMAND ----------

-- DBTITLE 1,Export Results to a Databricks Volume / S3 Location
-- MAGIC %python
-- MAGIC
-- MAGIC # do not write extra files when generating the csv.
-- MAGIC spark.conf.set("spark.databricks.io.directoryCommit.createSuccessFile","false") 
-- MAGIC spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
-- MAGIC spark.conf.set("spark.sql.sources.commitProtocolClass", "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol")
-- MAGIC
-- MAGIC # currentdate stores the date in YYYY-MM-DD format.
-- MAGIC currentdate=spark.sql("select string(current_date)").collect()[0][0]
-- MAGIC
-- MAGIC query = """
-- MAGIC -- calculate average runtime for each testsuite.
-- MAGIC select testsuite, avg(total_elapsed) as avg_time_secs
-- MAGIC from 
-- MAGIC   (
-- MAGIC     -- sum the runtime of a testsuite across all of the parallels in one buildkite build.
-- MAGIC     select testsuite, sum(elapsed) as total_elapsed
-- MAGIC     from datastreams.go_test_metrics
-- MAGIC     where git_branch = "master" 
-- MAGIC     and start_time > date_sub(NOW(), 7) -- average time for the last week
-- MAGIC     and job_result = "pass" 
-- MAGIC     and test_result = "pass"
-- MAGIC     and build_number != 123456 -- test data build number
-- MAGIC     group by testsuite, build_number
-- MAGIC   )
-- MAGIC group by testsuite
-- MAGIC order by avg_time_secs desc
-- MAGIC """
-- MAGIC df = spark.sql(query)
-- MAGIC df.write.format("csv").mode("overwrite").option("header", "true").save("/Volumes/s3/databricks-workspace/developerexperience/go-test-metrics/gotesttimes_%s" % currentdate)
-- MAGIC df.write.format("csv").mode("overwrite").option("header", "true").save("/Volumes/s3/databricks-workspace/developerexperience/go-test-metrics/gotesttimes_latest")