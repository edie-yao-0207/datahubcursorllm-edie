-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Rebalance Go Tests - Step 1/2
-- MAGIC
-- MAGIC Click `Run all` above.

-- COMMAND ----------

-- DBTITLE 1,Create testmetrics Temp View in SparkSQL from BigQuery TestMetrics Table
-- MAGIC %python
-- MAGIC spark.read.format("bigquery").option("table", "backend.TestMetrics").load().createOrReplaceTempView("testmetrics")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Rebalance Go Tests - Step 2/2
-- MAGIC
-- MAGIC Download the results from the `Table` in this step once it's run.

-- COMMAND ----------

-- DBTITLE 1,Select Avg Total Test Time by Go Pkg from testmetrics DESC Order
select col.Testcase, avg(col.Elapsed) as avg_time_secs 
from (
    select explode(Tests) 
    from testmetrics 
    where GitBranch = "master" and StartTime > date_sub(NOW(), 30)
  ) 
where col.Result = "pass"
group by col.Testcase
order by avg_time_secs desc

-- COMMAND ----------

