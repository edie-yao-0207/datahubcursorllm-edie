-- Databricks notebook source
-- MAGIC %run /backend/dataplatform/boto3_helpers

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC import boto3
-- MAGIC import datadog
-- MAGIC
-- MAGIC ssm_client = get_ssm_client("standard-read-parameters-ssm")
-- MAGIC api_key = get_ssm_parameter(ssm_client, "DATADOG_API_KEY")
-- MAGIC app_key = get_ssm_parameter(ssm_client, "DATADOG_APP_KEY")
-- MAGIC
-- MAGIC datadog.initialize(api_key=api_key, app_key=app_key)
-- MAGIC
-- MAGIC metric_prefix = "mobile.client.aggregate"
-- MAGIC
-- MAGIC class Aggregator:
-- MAGIC   def __init__(self):
-- MAGIC     # We'll populate this list over the course of this job, then fire off all the
-- MAGIC     # metrics at once.  That way, the metrics from a single run will have similar
-- MAGIC     # timestamps in datadog, even if aggregation takes a while.
-- MAGIC     self.metrics_to_send = []
-- MAGIC
-- MAGIC   def __make_tags(self, dictionary):
-- MAGIC     output = []
-- MAGIC     for (key, value) in dictionary.items():
-- MAGIC       output.append("{}:{}".format(key, value))
-- MAGIC     output.append("region:{}".format(boto3.session.Session().region_name))
-- MAGIC     return output
-- MAGIC
-- MAGIC   def __make_metric_name(self, metric_name):
-- MAGIC     return "mobile.client.aggregate.{}".format(metric_name)
-- MAGIC
-- MAGIC   def __build(self, metric_type, metric_name, value, tags):
-- MAGIC     metric = {
-- MAGIC       "metric": self.__make_metric_name(metric_name),
-- MAGIC       "value": value,
-- MAGIC       "type": metric_type,
-- MAGIC       "tags": self.__make_tags(tags),
-- MAGIC     }
-- MAGIC     self.metrics_to_send.append(metric)
-- MAGIC     return metric
-- MAGIC
-- MAGIC   def emit(self):
-- MAGIC     """
-- MAGIC     When we've aggregated all the metrics, we will call this to emit them
-- MAGIC     to datadog.
-- MAGIC     """
-- MAGIC     for metric in self.metrics_to_send:
-- MAGIC       datadog.api.Metric.send(
-- MAGIC         metric = metric["metric"],
-- MAGIC         points = [metric["value"]],
-- MAGIC         type = metric["type"],
-- MAGIC         tags = metric["tags"]
-- MAGIC       )
-- MAGIC
-- MAGIC   def debug(self):
-- MAGIC     """
-- MAGIC     Print the metrics without actually writing them to datadog
-- MAGIC     """
-- MAGIC     for metric in self.metrics_to_send:
-- MAGIC       tags = "no tags" if len(metric["tags"]) == 0 else ",".join(metric["tags"])
-- MAGIC       print("{}: {} ({})".format(metric["metric"], metric["value"], tags))
-- MAGIC
-- MAGIC   def count(self, metric_name, value, tags = {}):
-- MAGIC     """
-- MAGIC     Create a count metric to be emitted at the end of the job.
-- MAGIC
-- MAGIC     Returns the created metric for convenience: put this at the end of your
-- MAGIC     command so that you can see the metric in spark output.
-- MAGIC     """
-- MAGIC     return self.__build("count", metric_name, value, tags)
-- MAGIC
-- MAGIC   def gauge(self, metric_name, value, tags = {}):
-- MAGIC     """
-- MAGIC     Create a gauge metric to be emitted at the end of the job.
-- MAGIC
-- MAGIC     Returns the created metric for convenience: put this at the end of your
-- MAGIC     command so that you can see the metric in spark output.
-- MAGIC     """
-- MAGIC     return self.__build("gauge", metric_name, value, tags)
-- MAGIC
-- MAGIC aggregator = Aggregator()

-- COMMAND ----------

-- When this job runs, we explicity are only interested in recent-ish data, so
-- we will refresh the table.
refresh table datastreams.mobile_logs;

-- Subsequent commands should get their logs from this view, when possible, to
-- ensure that both,
-- - all queries use date partitions for efficiency
-- - all metrics use the same time range
create or replace temp view logs as (
  select *
  from datastreams.mobile_logs l
  -- account for queries near day boundaries.
  where l.date >= date_add(current_date(), -1)
  -- this interval should be the same as the cron expression in the metadata file.
    and l.timestamp between (current_timestamp - interval 60 minutes) and (current_timestamp - interval 30 minutes)
)

-- COMMAND ----------

create or replace temp view log_count as (
  select
    count(*) as value
  from logs l
);

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC log_count = spark.sql("select value from log_count;").first()["value"]
-- MAGIC aggregator.count("log_count", log_count)

-- COMMAND ----------

create or replace temp view drivers as (
  select
    count(distinct(l.driver_id)) as value
  from logs l
);

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC driver_count = spark.sql("select value from drivers;").first()["value"]
-- MAGIC aggregator.count("driver_count", driver_count)

-- COMMAND ----------

create or replace temp view codepush_stats as (
  with codepush_attempts as (
    select
      starts.driver_id as driver_id,

      case
        when ends.timestamp is null then interval 0 seconds
        else ends.timestamp - starts.timestamp
      end as duration,

      case
        when ends.timestamp is null then false
        else true
      end as did_succeed

    from (
      select *
      from logs
      where event_type = "GLOBAL_CODEPUSH_INSTALL_UPDATE"
    ) starts

    left join (
      select *
      from logs
      where event_type = "GLOBAL_CODEPUSH_SUCCEEDED_UPDATE"
    ) ends

    on
      starts.device_uuid = ends.device_uuid and
      ends.timestamp between starts.timestamp and (starts.timestamp + interval 2 minutes)
  )

  select
    (select count(*) from codepush_attempts where did_succeed = true) as `successes`,
    (select count(*) from codepush_attempts where did_succeed = false) as `failures`
)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC stats = spark.sql("select successes, failures from codepush_stats;").first()
-- MAGIC aggregator.count("codepush_attempt", stats["successes"], {"result": "success"})
-- MAGIC aggregator.count("codepush_attempt", stats["failures"], {"result": "failure"})

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC aggregator.debug()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # This should always be the last cell.
-- MAGIC aggregator.emit()
